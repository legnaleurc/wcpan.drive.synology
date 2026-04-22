"""Synology WebStation API file operations (SYNO.SynologyDrive.Files v11)."""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterable
from contextlib import asynccontextmanager

from aiohttp import ClientResponse

from ....exceptions import (
    SynologyApiError,
    SynologyNetworkError,
    SynologyUploadConflictError,
    SynologyUploadError,
)
from ...types import (
    SynologyFolderRef,
    SynologyLookupRef,
    SynologyNodeRef,
    SynologyParentRef,
    SynologyPath,
)
from ..types import (
    SynologyAsyncTaskResponse,
    SynologyFileInfo,
    SynologyFileListResponse,
    SynologyTaskInfo,
)
from .network import WebStationNetworkService


_FILES_API = "SYNO.SynologyDrive.Files"
_FILES_VERSION = 11
_TASKS_API = "SYNO.SynologyDrive.Tasks"
_TASKS_VERSION = 1

_TASK_POLL_DELAYS = [0.5, 1.0, 2.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0]


async def _poll_task(task_id: str, *, network: WebStationNetworkService) -> None:
    """Poll SYNO.SynologyDrive.Tasks.get until finished; raise on error."""
    for delay in _TASK_POLL_DELAYS:
        await asyncio.sleep(delay)
        data: SynologyTaskInfo = await network.request(
            _TASKS_API, _TASKS_VERSION, "get", task_id=task_id
        )
        status = data["status"]
        if status == "finished":
            result = data.get("result")
            errors = result["errors"] if result else []
            if errors:
                code = errors[0]["code"]
                msg = errors[0].get("message", "task failed")
                raise SynologyApiError(msg, error_code=code)
            return
    raise SynologyApiError(f"Task {task_id!r} did not finish in time")


async def get_node_metadata(
    node_ref: SynologyLookupRef,
    *,
    network: WebStationNetworkService,
) -> SynologyFileInfo | None:
    try:
        data: SynologyFileInfo = await network.request(
            _FILES_API, _FILES_VERSION, "get", path=str(node_ref)
        )
    except SynologyApiError:
        return None
    return data


async def get_file_metadata_by_path(
    syno_path: SynologyPath,
    *,
    network: WebStationNetworkService,
) -> SynologyFileInfo | None:
    try:
        data: SynologyFileInfo = await network.request(
            _FILES_API, _FILES_VERSION, "get", path=str(syno_path)
        )
    except SynologyApiError:
        return None
    return data


async def list_folder(
    folder_ref: SynologyFolderRef,
    *,
    network: WebStationNetworkService,
    offset: int = 0,
    limit: int = 1000,
) -> tuple[list[SynologyFileInfo], int]:
    data: SynologyFileListResponse = await network.request(
        _FILES_API,
        _FILES_VERSION,
        "list",
        path=str(folder_ref),
        sort_by="name",
        sort_direction="asc",
        offset=offset,
        limit=limit,
    )
    return data["items"], data["total"]


async def list_folder_all(
    folder_ref: SynologyFolderRef,
    *,
    network: WebStationNetworkService,
    page_size: int = 1000,
) -> list[SynologyFileInfo]:
    all_items: list[SynologyFileInfo] = []
    offset = 0
    while True:
        items, total = await list_folder(
            folder_ref,
            network=network,
            offset=offset,
            limit=page_size,
        )
        all_items.extend(items)
        offset += len(items)
        if offset >= total or not items:
            break
    return all_items


async def create_folder(
    parent_ref: SynologyParentRef,
    name: str,
    *,
    network: WebStationNetworkService,
) -> SynologyFileInfo:
    try:
        data: SynologyFileInfo = await network.request(
            _FILES_API,
            _FILES_VERSION,
            "create",
            type="folder",
            path=f"{parent_ref}/{name}",
            conflict_action="stop",
        )
    except SynologyApiError as e:
        if e.error_code == 1022:
            raise SynologyUploadConflictError(
                f"Folder {name!r} already exists under {parent_ref!r}",
                file_name=name,
            ) from e
        raise
    return data


async def rename_node(
    node_ref: SynologyNodeRef,
    new_name: str,
    *,
    network: WebStationNetworkService,
) -> SynologyFileInfo:
    try:
        data: SynologyFileInfo = await network.request(
            _FILES_API,
            _FILES_VERSION,
            "update",
            path=str(node_ref),
            name=new_name,
        )
    except SynologyApiError as e:
        if e.error_code == 1022:
            raise SynologyUploadConflictError(
                f"Rename conflict: {new_name!r} already exists in the same folder",
                file_name=new_name,
            ) from e
        raise
    return data


async def move_node(
    node_ref: SynologyNodeRef,
    new_parent_ref: SynologyParentRef,
    *,
    network: WebStationNetworkService,
) -> None:
    data: SynologyAsyncTaskResponse = await network.request(
        _FILES_API,
        _FILES_VERSION,
        "move",
        files=[str(node_ref)],
        to_parent_folder=str(new_parent_ref),
        conflict_action="stop",
    )
    await _poll_task(data["async_task_id"], network=network)


async def delete_node(
    node_ref: SynologyNodeRef,
    *,
    network: WebStationNetworkService,
) -> None:
    data: SynologyAsyncTaskResponse = await network.request(
        _FILES_API,
        _FILES_VERSION,
        "delete",
        files=[str(node_ref)],
    )
    await _poll_task(data["async_task_id"], network=network)


async def upload_file(
    parent_ref: SynologyParentRef,
    name: str,
    data: AsyncIterable[bytes],
    *,
    network: WebStationNetworkService,
    mime_type: str | None = None,
) -> SynologyFileInfo:
    form_fields: dict[str, object] = {
        "path": f"{parent_ref}/{name}",
        "type": "file",
        "conflict_action": "stop",
    }
    try:
        result: SynologyFileInfo = await network.upload(
            _FILES_API,
            _FILES_VERSION,
            "upload",
            form_fields=form_fields,
            file_data=data,
            file_name=name,
            mime_type=mime_type or "application/octet-stream",
        )
    except SynologyApiError as e:
        if e.error_code == 1022:
            raise SynologyUploadConflictError(
                f"Upload conflict for {name!r}: file already exists at destination",
                file_name=name,
            ) from e
        raise SynologyUploadError(
            f"Upload failed for {name!r}: {e}",
            file_name=name,
        ) from e
    except SynologyNetworkError as e:
        raise SynologyUploadError(
            f"Upload failed for {name!r}: {e}",
            file_name=name,
        ) from e
    return result


@asynccontextmanager
async def download_file(
    node_ref: SynologyNodeRef,
    *,
    network: WebStationNetworkService,
    range_: slice | None = None,
) -> AsyncGenerator[ClientResponse]:
    """Download a file, yielding the aiohttp ClientResponse for streaming."""
    extra_headers: dict[str, str] | None = None
    if range_:
        start = range_.start or 0
        end = f"-{range_.stop - 1}" if range_.stop is not None else ""
        extra_headers = {"Range": f"bytes={start}{end}"}

    async with network.download(
        _FILES_API,
        _FILES_VERSION,
        "download",
        extra_headers=extra_headers,
        files=[str(node_ref)],
        force_download=True,
    ) as response:
        yield response
