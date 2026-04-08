"""Resumable upload session handlers."""

from collections.abc import AsyncIterator
from logging import getLogger
from pathlib import Path

from aiohttp import web
from wcpan.drive.core.types import MediaInfo

from ...exceptions import SynologyUploadConflictError, SynologyUploadError
from ..api import files as synology_files
from ..keys import (
    CHANGE_SERVICE_KEY,
    NETWORK_KEY,
    OFF_MAIN_KEY,
    READY_KEY,
    SYNOLOGY_PATH_KEY,
    UPLOAD_SESSIONS_KEY,
)
from ..services.paths import SERVER_ROOT_ID
from ..services.upload import UploadSessionService
from .lib import (
    enrich_and_upsert_synology_node,
    record_to_response,
    require_ready,
    resolve_name_conflict_and_upsert,
)


_MEDIA_FIELDS = ("width", "height", "ms_duration", "media_image", "media_video")


def _media_info_from_body(body: dict[str, object]) -> MediaInfo | None:
    if not any(k in body for k in _MEDIA_FIELDS):
        return None
    return MediaInfo(
        width=int(str(body.get("width", 0))),
        height=int(str(body.get("height", 0))),
        ms_duration=int(str(body.get("ms_duration", 0))),
        is_image=bool(body.get("media_image", False)),
        is_video=bool(body.get("media_video", False)),
    )


_L = getLogger(__name__)

_FILE_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB for both reads and writes to temp file


def _write_chunk_sync(path: Path, offset: int, data: bytes) -> None:
    """Write data at offset in the temp file (blocking)."""
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)


def _read_chunk_sync(path: Path, offset: int, size: int) -> bytes:
    """Read up to size bytes from path at offset (blocking)."""
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(size)


@require_ready
async def create_upload_session(request: web.Request) -> web.Response:
    """POST /api/v1/nodes/{parent_id}/uploads"""
    parent_id = request.match_info["parent_id"]

    if parent_id == SERVER_ROOT_ID:
        raise web.HTTPForbidden(reason="Cannot upload to virtual root")

    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(reason="Invalid JSON body")

    name = body.get("name", "")
    if not name:
        raise web.HTTPBadRequest(reason="Missing 'name' in request body")

    raw_size = body.get("size")
    if raw_size is None:
        raise web.HTTPBadRequest(reason="Missing 'size' in request body")
    try:
        total_size = int(raw_size)
    except (TypeError, ValueError):
        raise web.HTTPBadRequest(reason="Invalid 'size': must be an integer")
    if total_size <= 0:
        raise web.HTTPBadRequest(reason="'size' must be a positive integer")

    mime_type = body.get("mime_type") or None
    media_info = _media_info_from_body(body)
    _L.debug(
        "create upload session name=%r parent_id=%r size=%d mime_type=%r media_info=%r",
        name,
        parent_id,
        total_size,
        mime_type,
        media_info,
    )

    store = request.app[UPLOAD_SESSIONS_KEY]
    session = store.create(parent_id, name, total_size, mime_type, media_info)

    return web.Response(
        status=201,
        headers={
            "Location": f"/api/v1/uploads/{session.session_id}",
            "Upload-Length": str(total_size),
        },
    )


async def patch_upload_chunk(request: web.Request) -> web.Response:
    """PATCH /api/v1/uploads/{session_id}"""
    store = request.app[UPLOAD_SESSIONS_KEY]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound()

    offset_str = request.headers.get("Upload-Offset")
    if offset_str is None:
        raise web.HTTPBadRequest(reason="Missing Upload-Offset header")
    try:
        start = int(offset_str)
    except ValueError:
        raise web.HTTPBadRequest(reason="Invalid Upload-Offset: must be an integer")
    if start < 0:
        raise web.HTTPBadRequest(reason="Invalid Upload-Offset: must be non-negative")

    if session.uploading:
        return web.Response(
            status=409,
            headers={"Upload-Offset": str(session.received)},
        )

    if start != session.received:
        return web.Response(
            status=409,
            headers={"Upload-Offset": str(session.received)},
        )

    session.uploading = True
    off_main = request.app[OFF_MAIN_KEY]
    try:
        async for chunk in request.content.iter_chunked(_FILE_CHUNK_SIZE):
            if session.received + len(chunk) > session.total_size:
                raise web.HTTPBadRequest(reason="Upload-Offset exceeds Upload-Length")
            await off_main.untimed(
                _write_chunk_sync, session.temp_path, session.received, chunk
            )
            session.received += len(chunk)
    except web.HTTPException:
        raise
    finally:
        session.uploading = False

    if session.received == session.total_size:
        return await _finalise_upload_session(request, store, session_id)

    return web.Response(
        status=204,
        headers={"Upload-Offset": str(session.received)},
    )


async def _finalise_upload_session(
    request: web.Request,
    store: UploadSessionService,
    session_id: str,
) -> web.Response:
    """Upload the complete temp file to Synology and clean up the session."""
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound()

    _L.debug(
        "finalise upload session session_id=%r name=%r parent_id=%r",
        session_id,
        session.name,
        session.parent_id,
    )

    if not request.app[READY_KEY]:
        raise web.HTTPServiceUnavailable(reason="Server not ready")
    off_main = request.app[OFF_MAIN_KEY]
    node_sync = request.app[CHANGE_SERVICE_KEY]
    network = request.app[NETWORK_KEY]
    syno_paths = request.app[SYNOLOGY_PATH_KEY]
    parent_ref = syno_paths.synology_parent_ref(session.parent_id)

    async def _iter_temp() -> AsyncIterator[bytes]:
        offset = 0
        while offset < session.total_size:
            chunk = await off_main.untimed(
                _read_chunk_sync, session.temp_path, offset, _FILE_CHUNK_SIZE
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
    except SynologyUploadConflictError:
        store.remove(session_id)
        session.temp_path.unlink(missing_ok=True)
        record = await resolve_name_conflict_and_upsert(
            request=request,
            parent_id=session.parent_id,
            name=session.name,
            prefer_directory=False,
            media_info=session.media_info,
        )
        return web.json_response(record_to_response(record), status=409)
    except SynologyUploadError as e:
        # Keep the session so the client can retry this PATCH.
        raise web.HTTPServiceUnavailable(reason=str(e))

    record = await enrich_and_upsert_synology_node(
        info=upload_info,
        parent_id=session.parent_id,
        node_sync=node_sync,
        media_info=session.media_info,
    )

    store.remove(session_id)
    session.temp_path.unlink(missing_ok=True)

    return web.json_response(record_to_response(record), status=201)


async def head_upload_session(request: web.Request) -> web.Response:
    """HEAD /api/v1/uploads/{session_id}"""
    store = request.app[UPLOAD_SESSIONS_KEY]
    session_id = request.match_info["session_id"]
    session = store.get(session_id)
    if session is None:
        raise web.HTTPNotFound()
    return web.Response(
        status=200,
        headers={
            "Upload-Offset": str(session.received),
            "Upload-Length": str(session.total_size),
        },
    )


async def delete_upload_session(request: web.Request) -> web.Response:
    """DELETE /api/v1/uploads/{session_id}"""
    store = request.app[UPLOAD_SESSIONS_KEY]
    session_id = request.match_info["session_id"]
    session = store.remove(session_id)
    if session is None:
        raise web.HTTPNotFound()
    session.temp_path.unlink(missing_ok=True)
    return web.Response(status=204)
