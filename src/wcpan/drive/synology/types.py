from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, kw_only=True)
class NodeRecord:
    """Wire-format node — uses Synology file_id as node_id."""

    node_id: str
    parent_id: str | None
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
