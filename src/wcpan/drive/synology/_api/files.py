"""
Synology File Station file operations API wrappers.
"""

import asyncio
import mimetypes
import secrets
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import PurePosixPath
from typing import Any

from aiohttp import ClientResponse
from wcpan.drive.core.exceptions import NodeNotFoundError

from .._lib import SynologyFileDict
from .._network import HeaderDict, Network, QueryDict


def _multipart_body(
    fields: dict[str, str],
    file_name: str,
    file_data: AsyncIterable[bytes],
    file_content_type: str,
) -> tuple[str, AsyncIterable[bytes]]:
    """Build a multipart/form-data body compatible with requests_toolbelt.

    The Synology server requires that text fields do NOT include a
    Content-Type header (aiohttp's FormData adds one, which the server
    rejects with error 401).  This helper builds the multipart body
    manually in the same format that requests_toolbelt.MultipartEncoder
    produces.

    Returns:
        (content_type_header, async_iterable_of_body_chunks)
    """
    boundary = secrets.token_hex(16)
    content_type = f"multipart/form-data; boundary={boundary}"
    bnd = boundary.encode()

    async def _generate() -> AsyncIterator[bytes]:
        # Text fields — no Content-Type header, just Content-Disposition
        for field_name, field_value in fields.items():
            yield (
                b"--" + bnd + b"\r\n"
                b'Content-Disposition: form-data; name="'
                + field_name.encode()
                + b'"\r\n'
                b"\r\n" + field_value.encode() + b"\r\n"
            )
        # File field header
        encoded_filename = file_name.encode()
        yield (
            b"--" + bnd + b"\r\n"
            b'Content-Disposition: form-data; name="files"; filename="'
            + encoded_filename
            + b'"\r\n'
            b"Content-Type: " + file_content_type.encode() + b"\r\n"
            b"\r\n"
        )
        # File data
        async for chunk in file_data:
            yield chunk
        # Closing boundary
        yield b"\r\n--" + bnd + b"--\r\n"

    return content_type, _generate()


async def get_file_info(
    network: Network,
    file_path: PurePosixPath,
) -> SynologyFileDict:
    """
    Get information about a specific file.

    Args:
        network: Network instance
        file_path: Path to the file

    Returns:
        File data

    Reference:
        SYNO.FileStation.List API (getinfo method)
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.List",
        "version": "2",
        "method": "getinfo",
        "path": str(file_path),
        "additional": '["time","size","owner","real_path","type"]',
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to get file info: {error}")

        result_data = data.get("data", {})
        files = result_data.get("files", [])

        if not files:
            raise NodeNotFoundError(str(file_path))

        first = files[0]
        if "code" in first:
            if first["code"] == 408:
                raise NodeNotFoundError(str(file_path))
            raise Exception(f"Failed to get file info: code={first['code']}")

        return _convert_file_info(first, file_path.parent)


@asynccontextmanager
async def download(
    network: Network,
    file_path: PurePosixPath,
    range_: tuple[int, int] | None = None,
) -> AsyncIterator[ClientResponse]:
    """
    Download a file.

    Args:
        network: Network instance
        file_path: Path to the file to download
        range_: Optional byte range (start, end)

    Yields:
        ClientResponse for streaming download

    Reference:
        SYNO.FileStation.Download API
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.Download",
        "version": "2",
        "method": "download",
        "path": str(file_path),
        "mode": "download",
    }

    headers: HeaderDict = {}
    if range_:
        start, end = range_
        headers["Range"] = f"bytes={start}-{end}"

    async with network.fetch(
        "GET", url, query=query, headers=headers, timeout=False
    ) as response:
        if not response.ok:
            raise Exception(f"Failed to download file: status {response.status}")

        yield response


async def upload_file(
    network: Network,
    parent_path: PurePosixPath,
    name: str,
    data: AsyncIterable[bytes],
    mime_type: str | None = None,
) -> SynologyFileDict:
    """
    Upload a file, streaming data directly into the HTTP body.

    Uses aiohttp's AsyncGeneratorPayload so no full-file buffer is held in
    memory — data flows from the caller's async iterable straight to the
    multipart form body.

    Args:
        network: Network instance
        parent_path: Parent folder path
        name: File name
        data: File data as async iterable
        mime_type: Optional MIME type

    Returns:
        Uploaded file data

    Reference:
        SYNO.FileStation.Upload API
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.Upload",
        "version": "2",
        "method": "upload",
    }

    file_content_type = mime_type or "application/octet-stream"
    content_type_header, body_stream = _multipart_body(
        fields={
            "path": str(parent_path),
            "create_parents": "true",
            "overwrite": "false",
        },
        file_name=name,
        file_data=data,
        file_content_type=file_content_type,
    )

    async with network.fetch(
        "POST",
        url,
        query=query,
        headers={"Content-Type": content_type_header},
        body=body_stream,
        timeout=False,
    ) as response:
        result = await response.json(content_type=None)

        if not result.get("success"):
            error = result.get("error", {})
            raise Exception(f"Failed to upload file: {error}")

        return await get_file_info(network, parent_path / name)


async def delete_file(
    network: Network,
    file_path: PurePosixPath,
) -> None:
    """
    Delete a file.

    Args:
        network: Network instance
        file_path: Path to the file to delete

    Reference:
        SYNO.FileStation.Delete API
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.Delete",
        "version": "2",
        "method": "delete",
        "path": str(file_path),
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to delete file: {error}")


async def update_file(
    network: Network,
    file_path: PurePosixPath,
    new_name: str | None = None,
    new_parent_path: PurePosixPath | None = None,
) -> SynologyFileDict:
    """
    Update file properties (rename, move, etc.).

    Args:
        network: Network instance
        file_path: Current file path
        new_name: New file name (optional)
        new_parent_path: New parent path for moving (optional)

    Returns:
        Updated file data

    Reference:
        SYNO.FileStation.Rename and SYNO.FileStation.CopyMove APIs
    """
    current_path = file_path
    updated_path = file_path

    # Handle rename
    if new_name:
        updated_path = await _rename_file(network, current_path, new_name)
        current_path = updated_path

    # Handle move
    if new_parent_path:
        updated_path = await _move_file(network, current_path, new_parent_path)
        current_path = updated_path

    # Get updated file info
    return await get_file_info(network, updated_path)


async def _rename_file(
    network: Network,
    file_path: PurePosixPath,
    new_name: str,
) -> PurePosixPath:
    """
    Rename a file.

    Args:
        network: Network instance
        file_path: Current file path
        new_name: New file name

    Returns:
        New file path
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.Rename",
        "version": "2",
        "method": "rename",
        "path": str(file_path),
        "name": new_name,
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to rename file: {error}")

    return file_path.parent / new_name


async def _move_file(
    network: Network,
    file_path: PurePosixPath,
    dest_folder_path: PurePosixPath,
    *,
    poll_interval: float = 0.5,
) -> PurePosixPath:
    """
    Move a file to a different folder.

    Args:
        network: Network instance
        file_path: Current file path
        dest_folder_path: Destination folder path

    Returns:
        New file path
    """
    url = network.entry_url

    # Start the async move task
    async with network.fetch(
        "GET",
        url,
        query={
            "api": "SYNO.FileStation.CopyMove",
            "version": "3",
            "method": "start",
            "path": str(file_path),
            "dest_folder_path": str(dest_folder_path),
            "overwrite": "true",
            "remove_src": "true",  # Move (not copy)
        },
    ) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to move file: {error}")

        task_id: str = data["data"]["taskid"]

    # Poll until done, always stop on exit
    try:
        while True:
            await asyncio.sleep(poll_interval)
            async with network.fetch(
                "GET",
                url,
                query={
                    "api": "SYNO.FileStation.CopyMove",
                    "version": "3",
                    "method": "status",
                    "taskid": task_id,
                },
            ) as response:
                data = await response.json()
                if not data.get("success"):
                    raise Exception(
                        f"Move status check failed: {data.get('error', {})}"
                    )
                if data["data"].get("finished"):
                    if data["data"].get("error", {}).get("total", 0) > 0:
                        raise Exception(
                            f"Move task failed: {data['data'].get('error')}"
                        )
                    break
    except BaseException:
        with suppress(Exception):
            async with network.fetch(
                "GET",
                url,
                query={
                    "api": "SYNO.FileStation.CopyMove",
                    "version": "3",
                    "method": "stop",
                    "taskid": task_id,
                },
            ) as response:
                await response.json()
        raise

    return dest_folder_path / file_path.name


async def compute_md5(
    network: Network,
    file_path: PurePosixPath,
    *,
    poll_interval: float = 0.5,
) -> str:
    url = network.entry_url

    # Start task
    async with network.fetch(
        "GET",
        url,
        query={
            "api": "SYNO.FileStation.MD5",
            "version": "2",
            "method": "start",
            "file_path": str(file_path),
        },
    ) as response:
        data = await response.json()
        if not data.get("success"):
            raise Exception(f"Failed to start MD5 task: {data.get('error', {})}")
        task_id: str = data["data"]["taskid"]

    # Poll until done, always stop on exit
    try:
        while True:
            await asyncio.sleep(poll_interval)
            async with network.fetch(
                "GET",
                url,
                query={
                    "api": "SYNO.FileStation.MD5",
                    "version": "2",
                    "method": "status",
                    "taskid": f'"{task_id}"',
                },
            ) as response:
                data = await response.json()
                if not data.get("success"):
                    raise Exception(f"MD5 status check failed: {data.get('error', {})}")
                if data["data"].get("finished"):
                    return data["data"]["md5"]
    except BaseException:
        with suppress(Exception):
            async with network.fetch(
                "GET",
                url,
                query={
                    "api": "SYNO.FileStation.MD5",
                    "version": "2",
                    "method": "stop",
                    "taskid": task_id,
                },
            ) as response:
                await response.json()
        raise


def _convert_file_info(
    file_info: dict[str, Any], parent_path: PurePosixPath
) -> SynologyFileDict:
    """
    Convert Synology file info to SynologyFileDict.

    Args:
        file_info: Raw file info from API
        parent_path: Parent folder path

    Returns:
        Standardized file dict
    """
    from datetime import UTC, datetime

    path = file_info.get("path", "")
    name = file_info.get("name", "")
    is_folder = file_info.get("isdir", False)

    # Extract time info
    additional = file_info.get("additional", {})
    time_info = additional.get("time", {})
    created_time = time_info.get("crtime", 0)
    modified_time = time_info.get("mtime", 0)

    # Convert timestamps to ISO format
    created_dt = (
        datetime.fromtimestamp(created_time, UTC) if created_time else datetime.now(UTC)
    )
    modified_dt = (
        datetime.fromtimestamp(modified_time, UTC)
        if modified_time
        else datetime.now(UTC)
    )

    # Extract size and type
    size = additional.get("size", 0)
    mime_type = (
        "application/x-folder"
        if is_folder
        else (mimetypes.guess_type(name)[0] or "application/octet-stream")
    )

    return SynologyFileDict(
        id=path,
        name=name,
        parent_id=str(parent_path),
        is_folder=is_folder,
        trashed=False,
        created_time=created_dt.isoformat(),
        modified_time=modified_dt.isoformat(),
        mime_type=mime_type,
        size=size,
    )
