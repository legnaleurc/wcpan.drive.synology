"""Path-service classes for virtual ID ↔ Synology API bridging.

Three focused service classes:

- ``SynologyPathService`` — bridge virtual IDs ↔ Synology API refs (holds *mounts*)
- ``LocalPathService``    — resolve node records to host-filesystem paths
                            (holds *storage*, *mounts*, *local_paths*)
- ``VirtualPathService``  — resolve client-facing virtual paths to node IDs
                            (holds *storage*)
"""

from pathlib import Path

from ...types import NodeRecord
from ..api.files import list_folder_all, list_folder_all_by_path
from ..api.types import SynologyFileInfo
from ..lib.mounts import SERVER_ROOT_ID, MountRegistry, is_virtual, mount_name
from ..types import (
    SynologyApiRef,
    SynologyIdRef,
    SynologyPath,
    VirtualPath,
)
from .network import NetworkService
from .storage import StorageService


# ---------------------------------------------------------------------------
# SynologyPathService
# ---------------------------------------------------------------------------


class SynologyPathService:
    """Bridge between virtual ID space and Synology Drive API.

    Captures *mounts* so callers no longer pass it on every call.
    ``network`` remains a method parameter because it varies per context
    (handlers pull from app keys, services hold their own reference).
    """

    def __init__(self, registry: MountRegistry) -> None:
        self._registry = registry

    @property
    def mounts(self) -> dict[str, SynologyPath]:
        return self._registry.mounts

    def synology_parent_ref(self, parent_id: str) -> SynologyApiRef:
        """Translate ``parent_id`` into a Synology API reference (path or ``id:…``)."""
        key = mount_name(parent_id)
        if key is not None:
            return self._registry.mounts[key]
        return SynologyIdRef(f"id:{parent_id}")

    async def list_children(
        self, network: NetworkService, parent_id: str
    ) -> list[SynologyFileInfo]:
        """List folder children; mount virtual parents use path listing, else ``id:`` listing."""
        mkey = mount_name(parent_id)
        if mkey is not None:
            return await list_folder_all_by_path(network, self._registry.mounts[mkey])
        return await list_folder_all(network, parent_id)

    async def find_child_by_name(
        self,
        network: NetworkService,
        parent_id: str,
        name: str,
        *,
        is_directory: bool | None,
    ) -> SynologyFileInfo | None:
        """Return the first child with ``name``, optionally filtered by directory vs file."""
        children = await self.list_children(network, parent_id)
        for info in children:
            if info["name"] != name:
                continue
            if is_directory is not None:
                got_dir = info["type"] == "dir"
                if got_dir != is_directory:
                    continue
            return info
        return None


# ---------------------------------------------------------------------------
# LocalPathService
# ---------------------------------------------------------------------------


class LocalPathService:
    """Resolve node records to host-filesystem paths for media probing.

    Absorbs the ancestor-fetching logic (formerly in the media enricher)
    so callers just pass a ``NodeRecord``.
    """

    def __init__(
        self,
        storage: StorageService,
        mounts: dict[str, SynologyPath],
        local_paths: dict[str, str],
    ) -> None:
        self._storage = storage
        self._mounts = mounts
        self._local_paths = local_paths

    async def resolve_local_path(self, record: NodeRecord) -> Path | None:
        """Reconstruct the local filesystem path for *record*, or ``None``."""
        local_paths = self._local_paths
        if not local_paths:
            return None
        node_cache: dict[str, NodeRecord | None]
        if not record.parent_id or is_virtual(record.parent_id):
            node_cache = {}
        else:
            ancestors = await self._storage.get_ancestors(record.parent_id)
            node_cache = {a.node_id: a for a in ancestors}

        return _resolve_local_path(self._mounts, local_paths, record, node_cache)


# ---------------------------------------------------------------------------
# VirtualPathService
# ---------------------------------------------------------------------------


class VirtualPathService:
    """Resolve client-facing virtual paths to directory node IDs."""

    def __init__(self, storage: StorageService) -> None:
        self._storage = storage

    async def resolve_to_directory_node_id(self, virtual_path: VirtualPath) -> str:
        """Resolve a server virtual path (``/`` = root ``_``) to a directory ``node_id``."""
        segments = _virtual_path_segments(virtual_path)

        root = await self._storage.get_node_by_id(SERVER_ROOT_ID)
        if root is None:
            raise ValueError("Server root is missing from the database")
        if not root.is_directory:
            raise ValueError("Server root is not a directory in the database")

        if not segments:
            return SERVER_ROOT_ID

        node_id = await self._storage.resolve_path_to_id(segments)
        if node_id is None:
            raise ValueError(
                f"No directory found at virtual path: {str(virtual_path)!r}"
            )

        return node_id


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _virtual_path_segments(virtual_path: VirtualPath) -> list[str]:
    """Split a virtual path into directory name segments (no leading/trailing slashes)."""
    raw = str(virtual_path).strip()
    if not raw or raw == "/":
        return []
    parts = [p for p in raw.split("/") if p]
    for p in parts:
        if p in (".", ".."):
            raise ValueError(f"Invalid virtual path segment: {p!r}")
    return parts


def _resolve_local_path(
    mounts: dict[str, SynologyPath],
    local_paths: dict[str, str],
    record: NodeRecord,
    node_cache: dict[str, NodeRecord | None],
) -> Path | None:
    """Walk the pre-fetched ancestor cache to reconstruct a local filesystem path."""
    parts: list[str] = [record.name]
    current = record

    while current.parent_id and not is_virtual(current.parent_id):
        parent = node_cache.get(current.parent_id)
        if parent is None:
            return None
        parts.append(parent.name)
        current = parent

    if not current.parent_id:
        return None
    mname = mount_name(current.parent_id)
    if mname is None:
        return None

    syno_path: SynologyPath | None = mounts.get(mname)
    if syno_path is None:
        return None

    relative = "/".join(reversed(parts))
    full_synology_path = str(syno_path).rstrip("/") + "/" + relative

    # Apply longest-prefix match from local_paths
    best_prefix = ""
    for prefix in local_paths:
        if full_synology_path.startswith(prefix) and len(prefix) > len(best_prefix):
            best_prefix = prefix

    if not best_prefix:
        return None

    local_str = local_paths[best_prefix] + full_synology_path[len(best_prefix) :]
    return Path(local_str)
