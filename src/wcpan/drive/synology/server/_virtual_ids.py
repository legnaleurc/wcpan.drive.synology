"""Synthetic node_id grammar for the Synology Drive server.

Synology ``file_id`` values (typically decimal digit strings) identify real nodes.
Virtual nodes use a leading underscore:

- ``_`` — synthetic server root
- ``_<name>`` — configured mount (e.g. ``_folder-a`` → ``folders["folder-a"]``)
"""

VIRTUAL_ID_PREFIX = "_"
SERVER_ROOT_ID = VIRTUAL_ID_PREFIX


def is_virtual(node_id: str) -> bool:
    return node_id.startswith(VIRTUAL_ID_PREFIX)


def is_mount_node_id(node_id: str) -> bool:
    """True for mount directory ids; false for the bare root ``_``."""
    return len(node_id) > 1 and node_id.startswith(VIRTUAL_ID_PREFIX)


def mount_id(name: str) -> str:
    return f"{VIRTUAL_ID_PREFIX}{name}"


def mount_name(node_id: str) -> str | None:
    """Return the mount config key for ``node_id``, or None if not a mount node."""
    if not is_mount_node_id(node_id):
        return None
    return node_id[1:]


def synology_parent_ref(parent_id: str, folders: dict[str, str]) -> str:
    """Translate ``parent_id`` into a Synology API reference (path string or ``id:…``)."""
    key = mount_name(parent_id)
    if key is not None:
        return folders[key]
    return f"id:{parent_id}"
