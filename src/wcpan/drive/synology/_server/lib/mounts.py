"""Virtual-ID grammar and mount-node helpers."""

from ..api.files import get_file_metadata_by_path
from ..services.network import NetworkService
from ..types import SynologyPath


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


def _check_no_nested_mounts(mounts: dict[str, SynologyPath]) -> None:
    """Raise if any mount point is a subdirectory of another mount point."""
    paths = sorted(str(path).rstrip("/") for path in mounts.values())
    for i, path_a in enumerate(paths):
        for path_b in paths[i + 1 :]:
            if path_b.startswith(path_a + "/"):
                raise ValueError(
                    f"Mount path {path_b!r} is a subdirectory of {path_a!r}; "
                    "nested mount points are not supported"
                )


class MountRegistry:
    def __init__(
        self,
        mounts: dict[str, SynologyPath],
        root_ids: dict[str, str],
    ) -> None:
        self._mounts = mounts
        self._root_ids = root_ids

    @property
    def mounts(self) -> dict[str, SynologyPath]:
        return self._mounts

    def lookup_mount_virtual_id(self, synology_id: str) -> str | None:
        """Return the mount virtual ID for a Synology file ID, or None."""
        return self._root_ids.get(synology_id)


async def create_mount_registry(
    network: NetworkService,
    mounts: dict[str, SynologyPath],
) -> MountRegistry:
    """Resolve Synology root IDs for all mounts. Raises RuntimeError on any failure."""
    _check_no_nested_mounts(mounts)
    root_ids: dict[str, str] = {}
    for name, syno_path in mounts.items():
        info = await get_file_metadata_by_path(network, syno_path)
        if info is None:
            raise RuntimeError(
                f"Mount {name!r} ({syno_path}) not found on Synology Drive"
            )
        root_ids[info["file_id"]] = mount_id(name)
    return MountRegistry(mounts, root_ids)
