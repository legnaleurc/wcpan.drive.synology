"""Synology Drive API v1 file operations."""

import secrets
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, NotRequired, TypedDict

from aiohttp import ClientResponse

from .._network import Network


class SynologyFileInfo(TypedDict):
    """Key fields from FileInfo_v3_0 schema."""

    file_id: str
    parent_id: str
    name: str
    type: str  # "file" or "dir"
    content_type: str  # "dir", "document", "image", "audio", "video", "file"
    hash: NotRequired[str]
    size: int
    created_time: int  # Unix timestamp seconds
    modified_time: int  # Unix timestamp seconds
    sync_id: int
    max_id: NotRequired[int]
    removed: NotRequired[bool]
    image_metadata: NotRequired[dict[str, Any]]


def file_id_path(file_id: str) -> str:
    """Format a file_id as a Synology path reference."""
    return f"id:{file_id}"


async def list_folder(
    network: Network,
    folder_id: str,
    offset: int = 0,
    limit: int = 1000,
) -> tuple[list[SynologyFileInfo], int]:
    """List contents of a folder. Returns (items, total)."""
    url = f"{network.api_base}/files/list"
    async with network.fetch(
        "POST",
        url,
        params={
            "path": file_id_path(folder_id),
            "offset": offset,
            "limit": limit,
            "sort_direction": "asc",
            "sort_by": "name",
        },
    ) as response:
        data = await response.json()

    if not data.get("success", True):  # Drive API may omit success field
        raise Exception(f"Failed to list folder {folder_id}: {data}")

    result = data.get("data", {})
    return result.get("items", []), result.get("total", 0)


async def list_folder_all(
    network: Network,
    folder_id: str,
    page_size: int = 1000,
) -> list[SynologyFileInfo]:
    """List all children of a folder by file_id (handling pagination)."""
    all_items: list[SynologyFileInfo] = []
    offset = 0
    while True:
        items, total = await list_folder(
            network, folder_id, offset=offset, limit=page_size
        )
        all_items.extend(items)
        offset += len(items)
        if offset >= total or not items:
            break
    return all_items


async def list_folder_by_path(
    network: Network,
    syno_path: str,
    offset: int = 0,
    limit: int = 1000,
) -> tuple[list[SynologyFileInfo], int]:
    """List folder contents using a Synology path string (not id: format)."""
    url = f"{network.api_base}/files/list"
    async with network.fetch(
        "POST",
        url,
        params={
            "path": syno_path,
            "offset": offset,
            "limit": limit,
            "sort_direction": "asc",
            "sort_by": "name",
        },
    ) as response:
        data = await response.json()

    if not data.get("success", True):
        raise Exception(f"Failed to list path {syno_path!r}: {data}")

    result = data.get("data", {})
    return result.get("items", []), result.get("total", 0)


async def list_folder_all_by_path(
    network: Network,
    syno_path: str,
    page_size: int = 1000,
) -> list[SynologyFileInfo]:
    """List all children of a folder using a Synology path string."""
    all_items: list[SynologyFileInfo] = []
    offset = 0
    while True:
        items, total = await list_folder_by_path(
            network, syno_path, offset=offset, limit=page_size
        )
        all_items.extend(items)
        offset += len(items)
        if offset >= total or not items:
            break
    return all_items


async def get_file_info(
    network: Network,
    parent_id: str,
    file_id: str,
) -> "SynologyFileInfo | None":
    """Fetch info for a specific file by listing its parent folder."""
    items = await list_folder_all(network, parent_id)
    for item in items:
        if item["file_id"] == file_id:
            return item
    return None


async def create_folder(
    network: Network,
    parent_ref: str,
    name: str,
) -> SynologyFileInfo:
    """Create a new folder.

    parent_ref is either ``id:{file_id}`` or a Synology path string.
    """
    url = f"{network.api_base}/files"
    async with network.fetch(
        "POST",
        url,
        params={
            "path": f"{parent_ref}/{name}",
            "type": "folder",
            "conflict_action": "stop",
        },
        json={},
    ) as response:
        data = await response.json()

    if not data.get("success", True):
        raise Exception(f"Failed to create folder {name!r}: {data}")

    return data["data"]


async def rename_file(
    network: Network,
    file_id: str,
    new_name: str,
) -> SynologyFileInfo:
    """Rename a file or folder."""
    url = f"{network.api_base}/files"
    async with network.fetch(
        "PUT",
        url,
        params={"path": file_id_path(file_id)},
        json={"name": new_name},
    ) as response:
        data = await response.json()

    if not data.get("success", True):
        raise Exception(f"Failed to rename {file_id!r} to {new_name!r}: {data}")

    return data["data"]


async def move_file(
    network: Network,
    file_id: str,
    new_parent_ref: str,
) -> None:
    """Move a file to a different parent folder (async, fire-and-forget).

    new_parent_ref is either ``id:{file_id}`` or a Synology path string.
    """
    url = f"{network.api_base}/files/move"
    async with network.fetch(
        "POST",
        url,
        json={
            "files": [file_id_path(file_id)],
            "to_parent_folder": new_parent_ref,
            "conflict_action": "stop",
        },
    ) as response:
        await response.json()  # discard async_task_id — no status endpoint


async def delete_file(network: Network, file_id: str) -> None:
    """Delete a file or folder (async, fire-and-forget)."""
    url = f"{network.api_base}/files/delete"
    async with network.fetch(
        "POST",
        url,
        json={"files": [file_id_path(file_id)]},
    ) as response:
        await response.json()  # discard async_task_id


async def upload_file(
    network: Network,
    parent_ref: str,
    name: str,
    data: AsyncIterable[bytes],
    mime_type: str | None = None,
) -> SynologyFileInfo:
    """Upload a file to the given parent folder.

    parent_ref is either ``id:{file_id}`` or a Synology path string.
    """
    url = f"{network.api_base}/files/upload"
    content_type_hdr, body = _multipart_body(
        path=f"{parent_ref}/{name}",
        file_name=name,
        file_data=data,
        file_content_type=mime_type or "application/octet-stream",
    )
    async with network.fetch(
        "PUT",
        url,
        headers={"Content-Type": content_type_hdr},
        body=body,
        timeout=False,
    ) as response:
        result = await response.json(content_type=None)

    if not result.get("success", True):
        raise Exception(f"Upload failed for {name!r}: {result}")

    return result["data"]


@asynccontextmanager
async def download_file(
    network: Network,
    file_id: str,
    range_: tuple[int, int] | None = None,
) -> AsyncIterator[ClientResponse]:
    """Download a file, yielding the aiohttp ClientResponse for streaming."""
    url = f"{network.api_base}/files/download"
    headers = {}
    if range_:
        start, end = range_
        headers["Range"] = f"bytes={start}-{end}"

    async with network.fetch(
        "POST",
        url,
        headers=headers if headers else None,
        json={"files": [file_id_path(file_id)]},
        timeout=False,
    ) as response:
        yield response


def _multipart_body(
    path: str,
    file_name: str,
    file_data: AsyncIterable[bytes],
    file_content_type: str,
) -> tuple[str, AsyncIterable[bytes]]:
    """Build multipart/form-data body for the Drive API upload."""
    boundary = secrets.token_hex(16)
    content_type = f"multipart/form-data; boundary={boundary}"
    bnd = boundary.encode()

    async def _generate() -> AsyncIterator[bytes]:
        # path field
        yield (
            b"--" + bnd + b"\r\n"
            b'Content-Disposition: form-data; name="path"\r\n'
            b"\r\n" + path.encode() + b"\r\n"
        )
        # type field
        yield (
            b"--" + bnd + b"\r\n"
            b'Content-Disposition: form-data; name="type"\r\n'
            b"\r\nfile\r\n"
        )
        # conflict_action field
        yield (
            b"--" + bnd + b"\r\n"
            b'Content-Disposition: form-data; name="conflict_action"\r\n'
            b"\r\nstop\r\n"
        )
        # file field
        yield (
            b"--" + bnd + b"\r\n"
            b'Content-Disposition: form-data; name="file"; filename="'
            + file_name.encode()
            + b'"\r\n'
            b"Content-Type: " + file_content_type.encode() + b"\r\n"
            b"\r\n"
        )
        async for chunk in file_data:
            yield chunk
        yield b"\r\n--" + bnd + b"--\r\n"

    return content_type, _generate()
