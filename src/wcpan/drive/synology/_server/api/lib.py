"""Helpers for adapting Synology API payloads into internal models."""

from logging import getLogger

from ..._lib import guess_mime_type, utc_from_timestamp
from ...types import MirrorMutableId, MirrorStableId, NodeRecord
from .types import SynologyFileInfo


_L = getLogger(__name__)


def convert_file_info(
    info: SynologyFileInfo,
    parent_id: MirrorStableId | None,
) -> NodeRecord | None:
    is_dir = info["type"] == "dir"
    name = info["name"]
    is_image = info.get("content_type") == "image"
    is_video = info.get("content_type") == "video"
    width = height = ms_duration = 0

    permanent_link = info.get("permanent_link")
    if not permanent_link:
        _L.warning(
            "SynologyFileInfo missing permanent_link for file_id=%s; skipping",
            info["file_id"],
        )
        return None
    return NodeRecord(
        id=MirrorStableId(permanent_link),
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
        mutable_id=MirrorMutableId(info["file_id"]),
    )
