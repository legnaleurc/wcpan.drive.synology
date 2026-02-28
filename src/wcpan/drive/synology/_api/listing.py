"""
Synology File Station listing API wrappers.
"""

import asyncio
import mimetypes
from collections.abc import AsyncIterator
from logging import getLogger
from pathlib import PurePosixPath
from typing import Any

from .._lib import SynologyFileDict
from .._network import Network, QueryDict


_L = getLogger(__name__)


async def get_root(network: Network, root_path: PurePosixPath) -> SynologyFileDict:
    return SynologyFileDict(
        id=str(root_path),
        name="",
        parent_id=None,
        is_folder=True,
        trashed=False,
        created_time="1970-01-01T00:00:00Z",
        modified_time="1970-01-01T00:00:00Z",
        mime_type="application/x-folder",
        size=0,
    )


async def list_all_files(
    network: Network,
    folder_path: PurePosixPath = PurePosixPath("/"),
    limit: int = 1000,
    concurrency: int = 8,
) -> AsyncIterator[list[SynologyFileDict]]:
    """
    List all files recursively starting from a folder using parallel BFS.

    Folders at each tree level are fetched concurrently (up to `concurrency`
    simultaneous HTTP requests), which dramatically reduces wall-clock time on
    deep or wide directory trees compared to sequential DFS.

    Parent folders are always yielded before their children (BFS order).

    Args:
        network: Network instance
        folder_path: Starting folder path
        limit: Items per page per API request
        concurrency: Max simultaneous HTTP requests

    Yields:
        Batches of SynologyFileDicts, parents before children
    """
    sem = asyncio.Semaphore(concurrency)

    async def fetch_folder(
        path: PurePosixPath,
    ) -> tuple[list[SynologyFileDict], list[PurePosixPath]]:
        """Fetch all pages of one folder; return (items, subfolder_paths)."""
        all_items: list[SynologyFileDict] = []
        subfolder_paths: list[PurePosixPath] = []
        offset = 0
        while True:
            async with sem:
                data = await list_folder(network, path, offset=offset, limit=limit)
            files = data.get("files", [])
            if not files:
                break
            for file_info in files:
                item = _convert_file_info(file_info, path)
                all_items.append(item)
                if item["is_folder"]:
                    subfolder_paths.append(PurePosixPath(item["id"]))
            total = data.get("total", 0)
            offset += limit
            if offset >= total:
                break
        return all_items, subfolder_paths

    # BFS: process all folders at each level in parallel, yield before descending
    current_level: list[PurePosixPath] = [folder_path]
    while current_level:
        tasks = [asyncio.create_task(fetch_folder(p)) for p in current_level]
        results = await asyncio.gather(*tasks)

        next_level: list[PurePosixPath] = []
        for items, subfolders in results:
            if items:
                yield items
            next_level.extend(subfolders)

        current_level = next_level


async def list_folder(
    network: Network,
    folder_path: PurePosixPath,
    offset: int = 0,
    limit: int = 1000,
) -> dict[str, Any]:
    """
    List contents of a specific folder (non-recursive).

    Args:
        network: Network instance
        folder_path: Folder path to list
        offset: Pagination offset
        limit: Number of items to return

    Returns:
        Response data with files list

    Reference:
        SYNO.FileStation.List API
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.List",
        "version": "2",
        "method": "list",
        "folder_path": str(folder_path),
        "limit": limit,
        "offset": offset,
        "additional": '["time","size","owner","real_path","type"]',
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to list folder: {error}")

        return data.get("data", {})


async def list_folder_children(
    network: Network,
    folder_path: PurePosixPath,
    limit: int = 1000,
) -> list[SynologyFileDict]:
    """
    List immediate children of a folder (non-recursive, paginated).

    Args:
        network: Network instance
        folder_path: Folder path to list
        limit: Items per page

    Returns:
        All child SynologyFileDicts for the folder
    """
    result: list[SynologyFileDict] = []
    offset = 0

    while True:
        data = await list_folder(network, folder_path, offset=offset, limit=limit)
        files = data.get("files", [])

        if not files:
            break

        for file_info in files:
            result.append(_convert_file_info(file_info, folder_path))

        total = data.get("total", 0)
        offset += limit
        if offset >= total:
            break

    return result


async def get_trash(network: Network) -> list[SynologyFileDict]:
    """
    Get trashed files.

    Note: Synology File Station doesn't have a built-in trash API.
    This is a placeholder that returns an empty list.
    Trashed files might be in a #recycle folder.

    Args:
        network: Network instance

    Returns:
        List of trashed files
    """
    # Synology doesn't have a standard trash API
    # Typically trashed files are in #recycle folder
    # For now, return empty list
    return []


def _convert_file_info(
    file_info: dict[str, Any], parent_path: PurePosixPath
) -> SynologyFileDict:
    """
    Convert Synology File Station file info to SynologyFileDict.

    Args:
        file_info: Raw file info from API
        parent_path: Parent folder path

    Returns:
        Standardized file dict
    """
    # Extract basic info
    path = file_info.get("path", "")
    name = file_info.get("name", "")
    is_folder = file_info.get("isdir", False)

    # Extract time info
    additional = file_info.get("additional", {})
    time_info = additional.get("time", {})
    created_time = time_info.get("crtime", 0)
    modified_time = time_info.get("mtime", 0)

    # Convert timestamps to ISO format
    from datetime import UTC, datetime

    created_dt = (
        datetime.fromtimestamp(created_time, UTC) if created_time else datetime.now(UTC)
    )
    modified_dt = (
        datetime.fromtimestamp(modified_time, UTC)
        if modified_time
        else datetime.now(UTC)
    )

    # Extract size
    size = file_info.get("additional", {}).get("size", 0)

    # Determine mime type
    mime_type = (
        "application/x-folder"
        if is_folder
        else (mimetypes.guess_type(name)[0] or "application/octet-stream")
    )

    return SynologyFileDict(
        id=path,  # Use path as ID
        name=name,
        parent_id=str(parent_path) if str(parent_path) != path else None,
        is_folder=is_folder,
        trashed=False,  # Synology doesn't track this in standard API
        created_time=created_dt.isoformat(),
        modified_time=modified_dt.isoformat(),
        mime_type=mime_type,
        size=size,
    )
