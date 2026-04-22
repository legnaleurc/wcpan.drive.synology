from dataclasses import dataclass
from datetime import datetime
from typing import NewType


MirrorStableId = NewType("MirrorStableId", str)
MirrorMutableId = NewType("MirrorMutableId", str)


@dataclass(frozen=True, kw_only=True)
class NodeRecord:
    """Wire-format node.

    ``id`` / ``parent_id`` are the public mirror IDs.
    ``mutable_id`` is the mutable Synology locator used for API calls.
    """

    id: MirrorStableId
    mutable_id: MirrorMutableId
    parent_id: MirrorStableId | None
    name: str
    is_directory: bool
    ctime: datetime
    mtime: datetime
    mime_type: str
    hash: str
    size: int
    is_image: bool
    is_video: bool
    width: int
    height: int
    ms_duration: int
