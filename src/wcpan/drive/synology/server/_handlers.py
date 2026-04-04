"""aiohttp route handlers for the wcpan.drive.synology server."""

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import replace
from functools import partial
from logging import getLogger

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionResetError

from ..exceptions import (
    SynologyNetworkError,
    SynologyUploadConflictError,
    SynologyUploadError,
)
from ..lib import (
    guess_mime_type,
    node_record_to_dict,
    utc_from_timestamp,
    utc_now,
)
from ..types import NodeRecord
from ._api import files as synology_files
from ._api.changes import convert_file_info
from ._api.files import SynologyFileInfo
from ._db import Storage
from ._enricher import enrich_media_before_upsert
from ._keys import (
    folders_key,
    network_key,
    off_main_key,
    ready_key,
    storage_key,
    trigger_event_key,
    upload_sessions_key,
    volume_map_key,
    webhook_token_key,
    write_queue_key,
)
from ._lib import OffMainThread
from ._types import WriteQueue
from ._upload_session import (
    delete_temp_sync,
    parse_content_range,
    read_chunk_sync,
    write_chunk_sync,
)
from ._virtual_ids import SERVER_ROOT_ID, is_virtual, synology_parent_ref
from ._workers import run_queued_write


_L = getLogger(__name__)

_MAX_CHANGES = 1000
_CHUNK_SIZE = 64 * 1024  # 64 KiB for streaming


def _parse_query_bool(raw: str) -> bool:
    v = raw.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    raise web.HTTPBadRequest(reason=f"Invalid boolean query value: {raw!r}")


def _query_nonneg_int(q: Mapping[str, str], name: str) -> int:
    if name not in q:
        return 0
    raw = q.get(name, "")
    if raw is None:
        return 0
    try:
        value = int(str(raw), 10)
    except ValueError:
        raise web.HTTPBadRequest(reason=f"Invalid integer for {name!r}") from None
    if value < 0:
        raise web.HTTPBadRequest(reason=f"Negative integer for {name!r}")
    return value


def _client_media_overlay(
    q: Mapping[str, str],
    *,
    is_image: bool,
    is_video: bool,
) -> tuple[int, int, int, bool, bool]:
    """Apply optional client-provided media metadata from query parameters."""
    has_dims = "width" in q or "height" in q or "ms_duration" in q
    width = _query_nonneg_int(q, "width") if has_dims else 0
    height = _query_nonneg_int(q, "height") if has_dims else 0
    ms_duration = _query_nonneg_int(q, "ms_duration") if has_dims else 0
    if "media_image" in q:
        is_image = _parse_query_bool(str(q.get("media_image", "")))
    if "media_video" in q:
        is_video = _parse_query_bool(str(q.get("media_video", "")))
    return width, height, ms_duration, is_image, is_video


def _record_to_response(record: NodeRecord) -> dict:
    return node_record_to_dict(record)


def _require_ready(request: web.Request) -> tuple[Storage, OffMainThread, WriteQueue]:
    if not request.app[ready_key]:
        raise web.HTTPServiceUnavailable(reason="Server not ready")
    return (
        request.app[storage_key],
        request.app[off_main_key],
        request.app[write_queue_key],
    )


# ---------- Change feed ----------


async def get_cursor(request: web.Request) -> web.Response:
    storage, off_main, _wq = _require_ready(request)
    cursor = await off_main(storage.get_cursor)
    return web.json_response({"cursor": cursor})


async def get_root(request: web.Request) -> web.Response:
    storage, off_main, _wq = _require_ready(request)
    record = await off_main(storage.get_node_by_id, SERVER_ROOT_ID)
    if record is None:
        raise web.HTTPNotFound()
    return web.json_response(_record_to_response(record))


async def get_changes(request: web.Request) -> web.Response:
    storage, off_main, _wq = _require_ready(request)
    try:
        cursor = int(request.rel_url.query.get("cursor", "0"))
        max_size = min(
            int(request.rel_url.query.get("max_size", str(_MAX_CHANGES))),
            _MAX_CHANGES,
        )
    except ValueError:
        raise web.HTTPBadRequest()

    rows, new_cursor, has_more = await off_main(
        storage.get_changes_since, cursor, max_size
    )

    changes = []
    for node_id, is_removed, record in rows:
        if is_removed:
            changes.append({"removed": True, "node_id": node_id})
        elif record is not None:
            changes.append({"removed": False, "node": _record_to_response(record)})

    return web.json_response(
        {
            "cursor": new_cursor,
            "has_more": has_more,
            "changes": changes,
        }
    )


# ---------- Node operations ----------


async def get_node(request: web.Request) -> web.Response:
    storage, off_main, _wq = _require_ready(request)
    node_id = request.match_info["id"]
    record = await off_main(storage.get_node_by_id, node_id)
    if record is None:
        raise web.HTTPNotFound()
    return web.json_response(_record_to_response(record))


async def download_node(request: web.Request) -> web.StreamResponse:
    storage, off_main, _wq = _require_ready(request)
    node_id = request.match_info["id"]
    record = await off_main(storage.get_node_by_id, node_id)
    if record is None:
        raise web.HTTPNotFound()

    network = request.app[network_key]

    range_header = request.headers.get("Range")
    range_: tuple[int, int] | None = None
    if range_header and range_header.startswith("bytes="):
        parts = range_header[6:].split("-")
        try:
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else record.size - 1
            range_ = (start, end)
        except (IndexError, ValueError):
            pass

    response = web.StreamResponse(
        status=206 if range_ else 200,
        headers={"Content-Type": record.mime_type or "application/octet-stream"},
    )
    await response.prepare(request)

    try:
        async with synology_files.download_file(
            network, node_id, range_
        ) as syno_response:
            async for chunk in syno_response.content.iter_chunked(_CHUNK_SIZE):
                await response.write(chunk)
        await response.write_eof()
    except (ConnectionError, ClientConnectionResetError):
        pass  # client disconnected
    except SynologyNetworkError as e:
        if isinstance(e.original_error, (ConnectionError, ClientConnectionResetError)):
            pass  # client disconnect wrapped by network.fetch
        else:
            _L.warning("Download stream error for node %s: %s", node_id, e)

    return response


async def create_node(request: web.Request) -> web.Response:
    """Create a directory."""
    storage, off_main, wq = _require_ready(request)
    network = request.app[network_key]
    folders = request.app[folders_key]
    volume_map = request.app[volume_map_key]

    body = await request.json()
    name: str = body.get("name", "")
    parent_id: str = body.get("parent_id", "")

    if not name or not parent_id:
        raise web.HTTPBadRequest()

    parent_ref = synology_parent_ref(parent_id, folders)
    info = await synology_files.create_folder(network, parent_ref, name)
    record = NodeRecord(
        node_id=info["file_id"],
        parent_id=parent_id,
        name=info["name"],
        is_directory=True,
        ctime=utc_from_timestamp(info.get("created_time", 0)),
        mtime=utc_from_timestamp(info.get("modified_time", 0)),
        mime_type="application/x-directory",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )
    record = await enrich_media_before_upsert(
        record, storage, folders, volume_map, off_main
    )
    await run_queued_write(wq, partial(storage.upsert_node_and_emit_change, record))
    return web.json_response(_record_to_response(record), status=201)


async def update_node(request: web.Request) -> web.Response:
    """Rename and/or move a node."""
    storage, off_main, wq = _require_ready(request)
    network = request.app[network_key]
    folders = request.app[folders_key]
    volume_map = request.app[volume_map_key]
    node_id = request.match_info["id"]

    if is_virtual(node_id):
        raise web.HTTPForbidden(reason="Cannot modify virtual nodes")

    record = await off_main(storage.get_node_by_id, node_id)
    if record is None:
        raise web.HTTPNotFound()

    body = await request.json()
    new_name: str | None = body.get("name")
    new_parent_id: str | None = body.get("parent_id")

    updated_record = record

    if new_name and new_name != record.name:
        info = await synology_files.rename_file(network, node_id, new_name)
        updated_record = NodeRecord(
            node_id=updated_record.node_id,
            parent_id=updated_record.parent_id,
            name=info["name"],
            is_directory=updated_record.is_directory,
            ctime=utc_from_timestamp(info.get("created_time", 0))
            or updated_record.ctime,
            mtime=utc_from_timestamp(info.get("modified_time", 0))
            or updated_record.mtime,
            mime_type=guess_mime_type(
                info["name"], is_directory=updated_record.is_directory
            ),
            hash=info.get("hash", updated_record.hash),
            size=info.get("size", updated_record.size),
            is_image=updated_record.is_image,
            is_video=updated_record.is_video,
            width=updated_record.width,
            height=updated_record.height,
            ms_duration=updated_record.ms_duration,
        )

    if new_parent_id and new_parent_id != record.parent_id:
        new_parent_ref = synology_parent_ref(new_parent_id, folders)
        await synology_files.move_file(network, node_id, new_parent_ref)
        updated_record = NodeRecord(
            node_id=updated_record.node_id,
            parent_id=new_parent_id,
            name=updated_record.name,
            is_directory=updated_record.is_directory,
            ctime=updated_record.ctime,
            mtime=utc_now(),
            mime_type=updated_record.mime_type,
            hash=updated_record.hash,
            size=updated_record.size,
            is_image=updated_record.is_image,
            is_video=updated_record.is_video,
            width=updated_record.width,
            height=updated_record.height,
            ms_duration=updated_record.ms_duration,
        )

    updated_record = await enrich_media_before_upsert(
        updated_record, storage, folders, volume_map, off_main
    )
    await run_queued_write(
        wq, partial(storage.upsert_node_and_emit_change, updated_record)
    )
    return web.json_response(_record_to_response(updated_record))


async def delete_node(request: web.Request) -> web.Response:
    storage, off_main, wq = _require_ready(request)
    network = request.app[network_key]
    node_id = request.match_info["id"]

    if is_virtual(node_id):
        raise web.HTTPForbidden(reason="Cannot delete virtual nodes")

    record = await off_main(storage.get_node_by_id, node_id)
    if record is None:
        raise web.HTTPNotFound()

    await synology_files.delete_file(network, node_id)
    await run_queued_write(
        wq, partial(storage.delete_subtree_and_emit_changes, node_id)
    )
    return web.Response(status=204)


async def _enrich_and_upsert_synology_node(
    *,
    info: SynologyFileInfo,
    parent_id: str,
    storage: Storage,
    folders: dict[str, str],
    volume_map: dict[str, str] | None,
    off_main: OffMainThread,
    wq: WriteQueue,
    client_query: Mapping[str, str] | None = None,
) -> NodeRecord:
    record = convert_file_info(info, parent_id)
    if client_query is not None:
        width, height, ms_duration, is_image, is_video = _client_media_overlay(
            client_query,
            is_image=record.is_image,
            is_video=record.is_video,
        )
        record = replace(
            record,
            is_image=is_image,
            is_video=is_video,
            width=width,
            height=height,
            ms_duration=ms_duration,
        )
    record = await enrich_media_before_upsert(
        record, storage, folders, volume_map, off_main
    )
    await run_queued_write(wq, partial(storage.upsert_node_and_emit_change, record))
    return record


async def upload_node(request: web.Request) -> web.Response:
    storage, off_main, wq = _require_ready(request)
    network = request.app[network_key]
    folders = request.app[folders_key]
    volume_map = request.app[volume_map_key]
    parent_id = request.match_info["parent_id"]

    if parent_id == SERVER_ROOT_ID:
        raise web.HTTPForbidden(reason="Cannot upload to virtual root")

    name = request.rel_url.query.get("name", "")
    if not name:
        raise web.HTTPBadRequest(reason="Missing 'name' query parameter")

    mime_type = request.rel_url.query.get("mime_type") or None
    parent_ref = synology_parent_ref(parent_id, folders)

    try:
        upload_info = await synology_files.upload_file(
            network=network,
            parent_ref=parent_ref,
            name=name,
            data=request.content.iter_chunked(_CHUNK_SIZE),
            mime_type=mime_type,
        )
    except SynologyUploadConflictError as e:
        raise web.HTTPConflict(reason=str(e))
    except SynologyUploadError as e:
        raise web.HTTPServiceUnavailable(reason=str(e))
    info = synology_files.synology_file_info_from_api_dict(upload_info)

    record = await _enrich_and_upsert_synology_node(
        info=info,
        parent_id=parent_id,
        storage=storage,
        folders=folders,
        volume_map=volume_map,
        off_main=off_main,
        wq=wq,
        client_query=request.rel_url.query,
    )
    return web.json_response(_record_to_response(record), status=201)


async def _upsert_from_api(
    network,
    storage,
    folders,
    volume_map,
    off_main: OffMainThread,
    wq: WriteQueue,
    parent_id: str,
    file_id: str,
) -> bool:
    """Fetch metadata via GET ``/files`` (``path=id:{file_id}``) and upsert into DB.

    *parent_id* is the client tree parent (e.g. virtual mount ids); it is not
    taken from the API response so mount semantics stay correct.

    Returns False if GET metadata failed.
    """
    info = await synology_files.get_file_metadata_by_id(network, file_id)
    if info is None:
        return False
    await _enrich_and_upsert_synology_node(
        info=info,
        parent_id=parent_id,
        storage=storage,
        folders=folders,
        volume_map=volume_map,
        off_main=off_main,
        wq=wq,
        client_query=None,
    )
    return True


async def handle_synology_webhook(request: web.Request) -> web.Response:
    webhook_token = request.app[webhook_token_key]
    token = request.headers.get("x-synology-token", "")
    if token != webhook_token:
        return web.Response(status=403, text="Invalid token")

    data = await request.json()
    _L.debug("received webhook: %s", data)

    if not request.app[ready_key]:
        request.app[trigger_event_key].set()
        return web.Response(text="OK")

    storage = request.app[storage_key]
    off_main = request.app[off_main_key]
    wq = request.app[write_queue_key]
    network = request.app[network_key]
    folders = request.app[folders_key]
    volume_map = request.app[volume_map_key]

    needs_scan = False

    for item in data:
        event_type = item.get("event_type", "")
        file_id = item.get("file_id", "")
        file_type = item.get("file_type", "")
        parent_id = item.get("parent_id", "")

        if not file_id:
            needs_scan = True
            continue

        try:
            if event_type == "file_deleted":
                await run_queued_write(
                    wq, partial(storage.delete_subtree_and_emit_changes, file_id)
                )

            elif event_type == "file_modified" and file_type == "file":
                if item.get("is_privilege_changed"):
                    # Upload/write complete — upsert with final state.
                    if not parent_id or not await _upsert_from_api(
                        network,
                        storage,
                        folders,
                        volume_map,
                        off_main,
                        wq,
                        parent_id,
                        file_id,
                    ):
                        _L.warning(
                            "file_id %s not found in parent %s; falling back to scan",
                            file_id,
                            parent_id,
                        )
                        needs_scan = True
                # else: in-progress write — ignore; the final is_privilege_changed
                # event will follow and trigger an upsert then.

            elif event_type == "file_modified":
                # Directory mtime changed because a child changed — children fire
                # their own events, so nothing to do here.
                pass

            elif event_type == "file_created" and file_type == "dir":
                # Directories are created atomically; upsert immediately.
                if not parent_id or not await _upsert_from_api(
                    network,
                    storage,
                    folders,
                    volume_map,
                    off_main,
                    wq,
                    parent_id,
                    file_id,
                ):
                    _L.warning(
                        "new dir %s not found in parent %s; falling back to scan",
                        file_id,
                        parent_id,
                    )
                    needs_scan = True

            elif event_type in ("file_moved", "file_renamed"):
                # parent_id (move) or name (rename) already reflects new state;
                # fetch and upsert from current location.
                if not parent_id or not await _upsert_from_api(
                    network,
                    storage,
                    folders,
                    volume_map,
                    off_main,
                    wq,
                    parent_id,
                    file_id,
                ):
                    _L.warning(
                        "file_id %s not found after %s; falling back to scan",
                        file_id,
                        event_type,
                    )
                    needs_scan = True

            else:
                # file_created for files (upload may be in progress), or unknown.
                needs_scan = True

        except Exception:
            _L.exception(
                "Failed targeted update for %s %s; falling back to scan",
                event_type,
                file_id,
            )
            needs_scan = True

    if needs_scan:
        request.app[trigger_event_key].set()

    return web.Response(text="OK")


# ---------- Resumable upload sessions ----------


_MEDIA_QUERY_PARAMS = frozenset(
    ("width", "height", "ms_duration", "media_image", "media_video")
)
_SESSION_READ_CHUNK = 4 * 1024 * 1024  # 4 MiB


async def create_upload_session(request: web.Request) -> web.Response:
    """POST /api/v1/nodes/{parent_id}/upload-session"""
    _require_ready(request)
    parent_id = request.match_info["parent_id"]

    if parent_id == SERVER_ROOT_ID:
        raise web.HTTPForbidden(reason="Cannot upload to virtual root")

    q = request.rel_url.query
    name = q.get("name", "")
    if not name:
        raise web.HTTPBadRequest(reason="Missing 'name' query parameter")

    size_str = q.get("size", "")
    if not size_str:
        raise web.HTTPBadRequest(reason="Missing 'size' query parameter")
    try:
        total_size = int(size_str)
    except ValueError:
        raise web.HTTPBadRequest(reason="Invalid 'size': must be an integer")
    if total_size <= 0:
        raise web.HTTPBadRequest(reason="'size' must be a positive integer")

    mime_type = q.get("mime_type") or None
    client_query = {k: v for k, v in q.items() if k in _MEDIA_QUERY_PARAMS}

    store = request.app[upload_sessions_key]
    session = store.create(parent_id, name, total_size, mime_type, client_query)

    return web.json_response(
        {"session_id": session.session_id, "received": 0}, status=201
    )


async def put_upload_chunk(request: web.Request) -> web.Response:
    """PUT /api/v1/upload-sessions/{session_id}"""
    store = request.app[upload_sessions_key]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound()

    parsed = parse_content_range(request.headers.get("Content-Range"))
    if parsed is None:
        raise web.HTTPBadRequest(reason="Missing or invalid Content-Range header")
    start, end, total = parsed

    if total != session.total_size:
        raise web.HTTPConflict(reason="Content-Range total does not match session size")

    async with session.lock:
        # If all bytes are already on disk, skip the write and go straight to finalise.
        if session.received < session.total_size:
            if start != session.received:
                return web.json_response(
                    {"received": session.received}, status=409
                )

            data = await request.read()
            expected_len = end - start + 1
            if len(data) != expected_len:
                raise web.HTTPBadRequest(
                    reason=f"Body length {len(data)} does not match Content-Range {expected_len}"
                )

            off_main = request.app[off_main_key]
            await off_main.untimed(write_chunk_sync, session.temp_path, start, data)
            session.received = end + 1

        if session.received < session.total_size:
            return web.json_response({"received": session.received})

        # All bytes received — upload to Synology.
        return await _finalise_upload_session(request, store, session_id)


async def _finalise_upload_session(
    request: web.Request,
    store,
    session_id: str,
) -> web.Response:
    """Upload the complete temp file to Synology and clean up the session."""
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound()

    storage, off_main, wq = _require_ready(request)
    network = request.app[network_key]
    folders = request.app[folders_key]
    volume_map = request.app[volume_map_key]
    parent_ref = synology_parent_ref(session.parent_id, folders)

    async def _iter_temp() -> AsyncIterator[bytes]:
        offset = 0
        while offset < session.total_size:
            chunk = await off_main.untimed(
                read_chunk_sync, session.temp_path, offset, _SESSION_READ_CHUNK
            )
            if not chunk:
                break
            yield chunk
            offset += len(chunk)

    try:
        upload_info = await synology_files.upload_file(
            network=network,
            parent_ref=parent_ref,
            name=session.name,
            data=_iter_temp(),
            mime_type=session.mime_type,
        )
    except SynologyUploadConflictError as e:
        store.remove(session_id)
        loop = asyncio.get_running_loop()
        asyncio.ensure_future(
            loop.run_in_executor(None, delete_temp_sync, session.temp_path)
        )
        raise web.HTTPConflict(reason=str(e))
    except SynologyUploadError as e:
        # Keep the session so the client can retry this PUT.
        raise web.HTTPServiceUnavailable(reason=str(e))

    info = synology_files.synology_file_info_from_api_dict(upload_info)
    record = await _enrich_and_upsert_synology_node(
        info=info,
        parent_id=session.parent_id,
        storage=storage,
        folders=folders,
        volume_map=volume_map,
        off_main=off_main,
        wq=wq,
        client_query=session.client_query,
    )

    store.remove(session_id)
    await off_main(delete_temp_sync, session.temp_path)

    return web.json_response(_record_to_response(record), status=201)


async def get_upload_session(request: web.Request) -> web.Response:
    """GET /api/v1/upload-sessions/{session_id}"""
    store = request.app[upload_sessions_key]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound()
    return web.json_response(
        {"received": session.received, "total": session.total_size}
    )


async def delete_upload_session(request: web.Request) -> web.Response:
    """DELETE /api/v1/upload-sessions/{session_id}"""
    store = request.app[upload_sessions_key]
    session_id = request.match_info["session_id"]
    session = store.remove(session_id)
    if session is None:
        raise web.HTTPNotFound()
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(
        loop.run_in_executor(None, delete_temp_sync, session.temp_path)
    )
    return web.Response(status=204)
