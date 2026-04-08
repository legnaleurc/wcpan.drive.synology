"""Server-internal types (not part of client / shared lib surface)."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, NewType, Required, TypedDict

from ..types import NodeRecord


# ---------------------------------------------------------------------------
# Path-space type aliases
# ---------------------------------------------------------------------------

SynologyPath = NewType("SynologyPath", PurePosixPath)
"""Absolute path on the Synology NAS, e.g. /Photos/2024."""

VirtualPath = NewType("VirtualPath", PurePosixPath)
"""Client-facing virtual path, e.g. /photos/2024."""

SynologyIdRef = NewType("SynologyIdRef", str)
"""Synology API id-reference, e.g. id:12345."""

type SynologyApiRef = SynologyPath | SynologyIdRef
"""Either a SynologyPath or a SynologyIdRef — both accepted by the Synology API."""


# ---------------------------------------------------------------------------
# Worker / queue types
# ---------------------------------------------------------------------------

type WriteQueue = asyncio.Queue[Callable[[], Awaitable[None]]]
type WebhookQueue = asyncio.Queue[Any]


@dataclass(frozen=True, slots=True)
class MetadataWorkItem:
    """One async media-enrichment job; consumed by ``metadata_worker``."""

    record: NodeRecord
    force_refresh: bool


type MetadataQueue = asyncio.Queue[MetadataWorkItem]


# ---------------------------------------------------------------------------
# Server configuration
# ---------------------------------------------------------------------------


class RawServerConfig(TypedDict, total=False):
    version: Required[int]
    database_url: Required[str]
    synology_url: Required[str]
    username: Required[str]
    password: Required[str]
    mounts: Required[dict[str, str]]
    public_url: Required[str]
    host: str
    port: int
    local_paths: Required[dict[str, str]]
    webhook_app_id: str
    otp_code: str | None
    log_path: str | None
    upload_tmp_dir: str | None


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
