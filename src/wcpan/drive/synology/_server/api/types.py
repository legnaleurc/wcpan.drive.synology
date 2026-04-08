"""Synology Drive API data types."""

from typing import Any, NotRequired, TypedDict


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


def _int_field(raw: dict[str, Any], key: str, default: int = 0) -> int:
    v = raw.get(key, default)
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.isdigit():
        return int(v, 10)
    try:
        return int(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def synology_file_info_from_api_dict(raw: dict[str, Any]) -> SynologyFileInfo:
    """Normalize a Files.get / list ``data`` object into ``SynologyFileInfo``."""
    info: SynologyFileInfo = {
        "file_id": str(raw["file_id"]),
        "parent_id": str(raw.get("parent_id", "")),
        "name": str(raw.get("name", "")),
        "type": str(raw.get("type", "file")),
        "content_type": str(raw.get("content_type", "file")),
        "size": _int_field(raw, "size", 0),
        "created_time": _int_field(raw, "created_time", 0),
        "modified_time": _int_field(raw, "modified_time", 0),
        "sync_id": _int_field(raw, "sync_id", 0),
    }
    if "hash" in raw and raw["hash"] is not None:
        info["hash"] = str(raw["hash"])
    if "max_id" in raw and raw["max_id"] is not None:
        info["max_id"] = _int_field(raw, "max_id", 0)
    if "removed" in raw:
        info["removed"] = bool(raw["removed"])
    if raw.get("image_metadata") is not None:
        info["image_metadata"] = raw["image_metadata"]
    return info
