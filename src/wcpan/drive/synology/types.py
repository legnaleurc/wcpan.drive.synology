from dataclasses import dataclass
from datetime import datetime
from typing import Literal


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


@dataclass(frozen=True, kw_only=True)
class RemovedChange:
    removed: Literal[True]
    node_id: str


@dataclass(frozen=True, kw_only=True)
class UpdatedChange:
    removed: Literal[False]
    node: NodeRecord


type MergedChange = RemovedChange | UpdatedChange


@dataclass(frozen=True, kw_only=True)
class ServerConfig:
    host: str
    port: int
    database_url: str
    synology_url: str
    username: str
    password: str
    folders: dict[str, str]
    public_url: str
    webhook_app_id: str
    otp_code: str | None = None
    log_path: str | None = None
    volume_map: dict[str, str] | None = None
    upload_tmp_dir: str | None = None
