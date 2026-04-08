"""Node CRUD, download, and single-shot upload handlers."""

from logging import getLogger

from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionResetError

from ..._lib import guess_mime_type, utc_from_timestamp, utc_now
from ...exceptions import (
    SynologyNetworkError,
    SynologyUploadConflictError,
    SynologyUploadError,
)
from ...types import NodeRecord
from ..api import files as synology_files
from ..keys import (
    CHANGE_SERVICE_KEY,
    NETWORK_KEY,
    STORAGE_KEY,
    SYNOLOGY_PATH_KEY,
)
from ..services.paths import SERVER_ROOT_ID, is_virtual
from .lib import (
    enrich_and_upsert_synology_node,
    media_info_from_query,
    record_to_response,
    require_ready,
    resolve_name_conflict_and_upsert,
)


_L = getLogger(__name__)


@require_ready
async def get_node(request: web.Request) -> web.Response:
    storage = request.app[STORAGE_KEY]
    node_id = request.match_info["id"]
    record = await storage.get_node_by_id(node_id)
    if record is None:
        raise web.HTTPNotFound()
    return web.json_response(record_to_response(record))


@require_ready
async def download_node(request: web.Request) -> web.StreamResponse:
    storage = request.app[STORAGE_KEY]
    node_id = request.match_info["id"]
    record = await storage.get_node_by_id(node_id)
    if record is None:
        raise web.HTTPNotFound()

    network = request.app[NETWORK_KEY]

    range_ = request.http_range if "Range" in request.headers else None

    response = web.StreamResponse(
        status=206 if range_ else 200,
        headers={"Content-Type": record.mime_type or "application/octet-stream"},
    )
    await response.prepare(request)

    try:
        async with synology_files.download_file(
            network, node_id, range_
        ) as syno_response:
            async for chunk in syno_response.content.iter_any():
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


@require_ready
async def create_node(request: web.Request) -> web.Response:
    """Create a directory."""
    node_sync = request.app[CHANGE_SERVICE_KEY]
    network = request.app[NETWORK_KEY]
    syno_paths = request.app[SYNOLOGY_PATH_KEY]

    body = await request.json()
    name: str = body.get("name", "")
    parent_id: str = body.get("parent_id", "")

    if not name or not parent_id:
        raise web.HTTPBadRequest()

    parent_ref = syno_paths.synology_parent_ref(parent_id)
    try:
        info = await synology_files.create_folder(network, parent_ref, name)
    except SynologyUploadConflictError:
        record = await resolve_name_conflict_and_upsert(
            request=request,
            parent_id=parent_id,
            name=name,
            prefer_directory=True,
            media_info=None,
        )
        return web.json_response(record_to_response(record), status=409)

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
    record = await node_sync.upsert(record)
    return web.json_response(record_to_response(record), status=201)


@require_ready
async def update_node(request: web.Request) -> web.Response:
    """Rename and/or move a node."""
    storage = request.app[STORAGE_KEY]
    node_sync = request.app[CHANGE_SERVICE_KEY]
    network = request.app[NETWORK_KEY]
    syno_paths = request.app[SYNOLOGY_PATH_KEY]
    node_id = request.match_info["id"]

    if is_virtual(node_id):
        raise web.HTTPForbidden(reason="Cannot modify virtual nodes")

    record = await storage.get_node_by_id(node_id)
    if record is None:
        raise web.HTTPNotFound()

    body = await request.json()
    new_name: str | None = body.get("name")
    new_parent_id: str | None = body.get("parent_id")

    updated_record = record

    if new_name and new_name != record.name:
        try:
            info = await synology_files.rename_file(network, node_id, new_name)
        except SynologyUploadConflictError:
            raise web.HTTPConflict(reason=f"{new_name!r} already exists in this folder")
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
        new_parent_ref = syno_paths.synology_parent_ref(new_parent_id)
        try:
            await synology_files.move_file(network, node_id, new_parent_ref)
        except Exception as e:
            raise web.HTTPServiceUnavailable(reason=str(e))
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

    updated_record = await node_sync.upsert(updated_record)
    return web.json_response(record_to_response(updated_record))


@require_ready
async def delete_node(request: web.Request) -> web.Response:
    storage = request.app[STORAGE_KEY]
    node_sync = request.app[CHANGE_SERVICE_KEY]
    network = request.app[NETWORK_KEY]
    node_id = request.match_info["id"]

    if is_virtual(node_id):
        raise web.HTTPForbidden(reason="Cannot delete virtual nodes")

    record = await storage.get_node_by_id(node_id)
    if record is None:
        raise web.HTTPNotFound()

    await synology_files.delete_file(network, node_id)
    await node_sync.delete(node_id)
    return web.Response(status=204)


@require_ready
async def upload_node(request: web.Request) -> web.Response:
    node_sync = request.app[CHANGE_SERVICE_KEY]
    network = request.app[NETWORK_KEY]
    syno_paths = request.app[SYNOLOGY_PATH_KEY]
    parent_id = request.match_info["parent_id"]

    if parent_id == SERVER_ROOT_ID:
        raise web.HTTPForbidden(reason="Cannot upload to virtual root")

    name = request.rel_url.query.get("name", "")
    if not name:
        raise web.HTTPBadRequest(reason="Missing 'name' query parameter")

    mime_type = request.rel_url.query.get("mime_type") or None
    media_info = media_info_from_query(request.rel_url.query)
    _L.debug(
        "upload name=%r parent_id=%r mime_type=%r media_info=%r",
        name,
        parent_id,
        mime_type,
        media_info,
    )
    parent_ref = syno_paths.synology_parent_ref(parent_id)

    try:
        upload_info = await synology_files.upload_file(
            network=network,
            parent_ref=parent_ref,
            name=name,
            data=request.content.iter_any(),
            mime_type=mime_type,
        )
    except SynologyUploadConflictError:
        record = await resolve_name_conflict_and_upsert(
            request=request,
            parent_id=parent_id,
            name=name,
            prefer_directory=False,
            media_info=media_info,
        )
        return web.json_response(record_to_response(record), status=409)
    except SynologyUploadError as e:
        raise web.HTTPServiceUnavailable(reason=str(e))

    record = await enrich_and_upsert_synology_node(
        info=upload_info,
        parent_id=parent_id,
        node_sync=node_sync,
        media_info=media_info,
    )
    return web.json_response(record_to_response(record), status=201)
