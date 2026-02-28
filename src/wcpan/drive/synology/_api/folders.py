"""
Synology File Station folder operations API wrappers.
"""

from pathlib import PurePosixPath
from typing import Any

from .._lib import SynologyFileDict
from .._network import Network, QueryDict


async def create_folder(
    network: Network,
    name: str,
    parent_path: PurePosixPath,
) -> SynologyFileDict:
    """
    Create a new folder.

    Args:
        network: Network instance
        name: Folder name
        parent_path: Parent folder path

    Returns:
        Created folder data

    Reference:
        SYNO.FileStation.CreateFolder API
    """
    url = network.entry_url

    folder_path = parent_path / name

    query: QueryDict = {
        "api": "SYNO.FileStation.CreateFolder",
        "version": "2",
        "method": "create",
        "folder_path": str(parent_path),
        "name": name,
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to create folder: {error}")

        # Get folder info from response
        result_data = data.get("data", {})
        folders = result_data.get("folders", [])

        if folders:
            folder_info = folders[0]
            return _convert_folder_info(folder_info, parent_path)

        # If no folder info in response, construct it
        from datetime import UTC, datetime

        now = datetime.now(UTC)
        return SynologyFileDict(
            id=str(folder_path),
            name=name,
            parent_id=str(parent_path),
            is_folder=True,
            trashed=False,
            created_time=now.isoformat(),
            modified_time=now.isoformat(),
            mime_type="application/x-folder",
            size=0,
        )


async def get_folder_info(
    network: Network,
    folder_path: PurePosixPath,
) -> SynologyFileDict:
    """
    Get information about a specific folder.

    Args:
        network: Network instance
        folder_path: Path to the folder

    Returns:
        Folder data

    Reference:
        SYNO.FileStation.List API (get info for specific path)
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.List",
        "version": "2",
        "method": "getinfo",
        "path": str(folder_path),
        "additional": '["time","size","owner","real_path","type"]',
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to get folder info: {error}")

        result_data = data.get("data", {})
        files = result_data.get("files", [])

        if not files:
            raise Exception(f"Folder not found: {folder_path}")

        file_entry = files[0]
        if file_entry.get("code") == 408:
            raise Exception(f"Folder not found: {folder_path}")

        return _convert_folder_info(file_entry, folder_path.parent)


async def list_folder(
    network: Network,
    folder_path: PurePosixPath,
    offset: int = 0,
    limit: int = 1000,
) -> dict[str, Any]:
    """
    List contents of a folder.

    Args:
        network: Network instance
        folder_path: Path to the folder
        offset: Pagination offset
        limit: Number of items per page

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
        "offset": offset,
        "limit": limit,
        "additional": '["time","size","owner","real_path","type"]',
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to list folder: {error}")

        return data.get("data", {})


async def rename_folder(
    network: Network,
    folder_path: PurePosixPath,
    new_name: str,
) -> SynologyFileDict:
    """
    Rename a folder.

    Args:
        network: Network instance
        folder_path: Current folder path
        new_name: New folder name

    Returns:
        Updated folder data

    Reference:
        SYNO.FileStation.Rename API
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.Rename",
        "version": "2",
        "method": "rename",
        "path": str(folder_path),
        "name": new_name,
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to rename folder: {error}")

        return await get_folder_info(network, folder_path.parent / new_name)


async def delete_folder(
    network: Network,
    folder_path: PurePosixPath,
    recursive: bool = True,
) -> None:
    """
    Delete a folder.

    Args:
        network: Network instance
        folder_path: Path to the folder to delete
        recursive: Whether to delete recursively (default: True)

    Reference:
        SYNO.FileStation.Delete API
    """
    url = network.entry_url

    query: QueryDict = {
        "api": "SYNO.FileStation.Delete",
        "version": "2",
        "method": "delete",
        "path": str(folder_path),
        "recursive": "true" if recursive else "false",
    }

    async with network.fetch("GET", url, query=query) as response:
        data = await response.json()

        if not data.get("success"):
            error = data.get("error", {})
            raise Exception(f"Failed to delete folder: {error}")


def _convert_folder_info(
    folder_info: dict[str, Any], parent_path: PurePosixPath
) -> SynologyFileDict:
    """
    Convert Synology folder info to SynologyFileDict.

    Args:
        folder_info: Raw folder info from API
        parent_path: Parent folder path

    Returns:
        Standardized folder dict
    """
    from datetime import UTC, datetime

    path = folder_info.get("path", "")
    name = folder_info.get("name", "")

    # Extract time info
    additional = folder_info.get("additional", {})
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

    return SynologyFileDict(
        id=path,
        name=name,
        parent_id=str(parent_path),
        is_folder=True,
        trashed=False,
        created_time=created_dt.isoformat(),
        modified_time=modified_dt.isoformat(),
        mime_type="application/x-folder",
        size=0,
    )
