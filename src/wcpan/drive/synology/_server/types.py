"""Server-internal types (not part of client / shared lib surface)."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import NewType, Self

from ..types import MirrorMutableId, MirrorStableId, NodeRecord


# ---------------------------------------------------------------------------
# Path-space type aliases
# ---------------------------------------------------------------------------

SynologyPath = NewType("SynologyPath", PurePosixPath)
"""Absolute path on the Synology NAS, e.g. /Photos/2024."""

VirtualPath = NewType("VirtualPath", PurePosixPath)
"""Client-facing virtual path, e.g. /photos/2024."""


@dataclass(frozen=True, slots=True)
class SynologyPermanentLink:
    permanent_link: str

    def __str__(self) -> str:
        return f"link:{self.permanent_link}"

    def to_mirror_stable_id(self) -> MirrorStableId:
        return MirrorStableId(self.permanent_link)

    @classmethod
    def from_mirror_stable_id(cls, node_id: MirrorStableId) -> Self:
        return cls(permanent_link=str(node_id))


@dataclass(frozen=True, slots=True)
class SynologyFileId:
    file_id: str

    def __str__(self) -> str:
        return f"id:{self.file_id}"

    def to_mirror_mutable_id(self) -> MirrorMutableId:
        return MirrorMutableId(self.file_id)

    @classmethod
    def from_mirror_mutable_id(cls, file_id: MirrorMutableId) -> Self:
        return cls(file_id=str(file_id))


type SynologyNodeRef = SynologyPermanentLink | SynologyFileId
"""Stable node ref for object operations, preferring permanent link where possible."""

type SynologyParentRef = SynologyPath | SynologyFileId
"""Parent target for create/upload operations."""


@dataclass(frozen=True, slots=True)
class SynologyChildRef:
    parent_ref: SynologyParentRef
    name: str

    def __str__(self) -> str:
        return f"{self.parent_ref}/{self.name}"


type SynologyLookupRef = SynologyNodeRef | SynologyChildRef
"""Lookup target for fetching one node by direct ref or parent/name composition."""

type SynologyFolderRef = SynologyLookupRef | SynologyPath
"""Folder locator accepted by list operations."""


# ---------------------------------------------------------------------------
# Worker / queue types
# ---------------------------------------------------------------------------

type WriteQueue = asyncio.Queue[Callable[[], Awaitable[None]]]


@dataclass(frozen=True, slots=True)
class MetadataWorkItem:
    """One async media-enrichment job; consumed by ``metadata_worker``."""

    record: NodeRecord
    force_refresh: bool


type MetadataQueue = asyncio.Queue[MetadataWorkItem]


@dataclass(frozen=True, kw_only=True)
class ServerConfig:
    host: str
    port: int
    database_url: str
    synology_url: str
    username: str
    password: str
    mounts: dict[str, SynologyPath]
    public_url: str
    webhook_app_id: str
    local_paths: dict[str, str]
    otp_code: str | None = None
    log_path: str | None = None
    upload_tmp_dir: str | None = None
