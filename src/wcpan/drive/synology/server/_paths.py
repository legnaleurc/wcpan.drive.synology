"""Path resolution across the three path spaces used by the server.

**Virtual path** (e.g. ``/photos/2024``)
    Client-facing path string.  The root is ``/``; each component matches a
    directory name in the DB tree starting from the synthetic server root.

**Synology path** (e.g. ``/Photos/2024``)
    Path on the Synology NAS.  These are the *values* stored in the ``folders``
    config mapping (``folder_key → synology_path``).

**Local path** (e.g. ``/mnt/nas/photos/2024``)
    Host-filesystem path.  Derived from a synology path via the ``volume_map``
    config (``synology_path_prefix → local_path_prefix``).
"""

from __future__ import annotations

from pathlib import Path

from ..types import NodeRecord
from ._db import Storage
from ._virtual_ids import SERVER_ROOT_ID, is_virtual, mount_name


def _virtual_path_segments(virtual_path: str) -> list[str]:
    """Split a virtual path into directory name segments (no leading/trailing slashes)."""
    raw = virtual_path.strip()
    if not raw or raw == "/":
        return []
    parts = [p for p in raw.split("/") if p]
    for p in parts:
        if p in (".", ".."):
            raise ValueError(f"Invalid virtual path segment: {p!r}")
    return parts


def virtual_path_to_directory_node_id(storage: Storage, virtual_path: str) -> str:
    """Resolve a server virtual path (``/`` = root ``_``) to a directory ``node_id``.

    Each path component is matched against a **directory** child name in the DB,
    starting from the synthetic root, then mounts (e.g. ``/photos`` → ``_photos``).
    """
    segments = _virtual_path_segments(virtual_path)
    current = SERVER_ROOT_ID
    root = storage.get_node_by_id(current)
    if root is None:
        raise ValueError("Server root is missing from the database")
    if not root.is_directory:
        raise ValueError("Server root is not a directory in the database")
    if not segments:
        return current
    for seg in segments:
        children = storage.get_children(current)
        nxt = next((c for c in children if c.name == seg and c.is_directory), None)
        if nxt is None:
            raise ValueError(
                f"No directory named {seg!r} under virtual path ending at {current!r}"
            )
        current = nxt.node_id
    return current


def resolve_local_path(
    record: NodeRecord,
    node_cache: dict[str, NodeRecord | None],
    folders: dict[str, str],
    volume_map: dict[str, str],
) -> Path | None:
    """Reconstruct the local filesystem path for a node.

    Walks up the DB tree (using the pre-fetched cache) to find the mount ancestor,
    then applies volume_map prefix substitution.
    """
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

    synology_path = folders.get(mname)
    if synology_path is None:
        return None

    relative = "/".join(reversed(parts))
    full_synology_path = synology_path.rstrip("/") + "/" + relative

    # Apply longest-prefix match from volume_map
    best_prefix = ""
    for prefix in volume_map:
        if full_synology_path.startswith(prefix) and len(prefix) > len(best_prefix):
            best_prefix = prefix

    if not best_prefix:
        return None

    local_str = volume_map[best_prefix] + full_synology_path[len(best_prefix) :]
    return Path(local_str)
