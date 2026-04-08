"""Synology file listing entries to ``NodeRecord``."""

from collections.abc import Mapping
from typing import Any, cast

from ..._lib import guess_mime_type, utc_from_timestamp
from ...types import NodeRecord
from ..api.types import SynologyFileInfo


def _int_from_meta(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str) and v.strip():
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default
    return default


def _dimensions_from_image_metadata(
    meta: Mapping[str, Any],
) -> tuple[int, int, int]:
    """Best-effort width, height, ms_duration from Synology ``image_metadata``."""
    w = _int_from_meta(meta.get("width"))
    h = _int_from_meta(meta.get("height"))
    ms = _int_from_meta(meta.get("duration"))
    if w == 0 and h == 0:
        res_any = meta.get("resolution")
        if isinstance(res_any, Mapping):
            res = cast(Mapping[str, Any], res_any)
            w = _int_from_meta(res.get("width"), w)
            h = _int_from_meta(res.get("height"), h)
    if ms == 0:
        ms = _int_from_meta(meta.get("video_duration"))
    return w, h, ms


def convert_file_info(info: SynologyFileInfo, parent_id: str | None) -> NodeRecord:
    is_dir = info["type"] == "dir"
    name = info["name"]
    is_image = info.get("content_type") == "image"
    is_video = info.get("content_type") == "video"
    width = height = ms_duration = 0
    if not is_dir and (is_image or is_video):
        raw_meta = info.get("image_metadata")
        if isinstance(raw_meta, Mapping):
            meta = cast(Mapping[str, Any], raw_meta)
            width, height, ms_duration = _dimensions_from_image_metadata(meta)

    return NodeRecord(
        node_id=info["file_id"],
        parent_id=parent_id,
        name=name,
        is_directory=is_dir,
        ctime=utc_from_timestamp(info.get("created_time", 0)),
        mtime=utc_from_timestamp(info.get("modified_time", 0)),
        mime_type=guess_mime_type(name, is_directory=is_dir),
        hash=info.get("hash", ""),
        size=info.get("size", 0),
        is_image=is_image,
        is_video=is_video,
        width=width,
        height=height,
        ms_duration=ms_duration,
    )
