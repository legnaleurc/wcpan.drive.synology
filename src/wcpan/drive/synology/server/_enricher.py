"""Media dimensions from local disk — applied before each upsert that emits a change."""

from dataclasses import replace
from logging import getLogger
from pathlib import Path

from pymediainfo import MediaInfo  # type: ignore[import-untyped]

from ..types import NodeRecord
from ._db import Storage
from ._lib import OffMainThread
from ._virtual_ids import is_virtual, mount_name


_L = getLogger(__name__)


def _resolve_local_path(
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

    syno_path = folders.get(mname)
    if syno_path is None:
        return None

    relative = "/".join(reversed(parts))
    full_syno = syno_path.rstrip("/") + "/" + relative

    # Apply longest-prefix match from volume_map
    best_prefix = ""
    for prefix in volume_map:
        if full_syno.startswith(prefix) and len(prefix) > len(best_prefix):
            best_prefix = prefix

    if not best_prefix:
        return None

    local_str = volume_map[best_prefix] + full_syno[len(best_prefix) :]
    return Path(local_str)


def _probe_sync(path: Path) -> tuple[int, int, int] | None:
    """Probe width, height, ms_duration using pymediainfo. Runs in a thread."""
    try:
        info = MediaInfo.parse(str(path))
    except Exception:
        _L.warning("Failed to probe %s", path, exc_info=True)
        return None

    width = 0
    height = 0
    ms_duration = 0

    for track in info.tracks:
        if track.track_type in ("Video", "Image"):
            width = int(track.width or 0)
            height = int(track.height or 0)
        if track.track_type == "General":
            ms_duration = int(track.duration or 0)

    return width, height, ms_duration


async def _node_cache_for_path_resolution(
    record: NodeRecord,
    storage: Storage,
    off_main: OffMainThread,
) -> dict[str, NodeRecord | None]:
    needed_ids: set[str] = set()
    pid = record.parent_id
    while pid and not is_virtual(pid):
        needed_ids.add(pid)
        parent = await off_main(storage.get_node_by_id, pid)
        pid = parent.parent_id if parent else None
    return {nid: await off_main(storage.get_node_by_id, nid) for nid in needed_ids}


async def enrich_media_before_upsert(
    record: NodeRecord,
    storage: Storage,
    folders: dict[str, str],
    volume_map: dict[str, str] | None,
    off_main: OffMainThread,
) -> NodeRecord:
    """Set width/height/ms_duration via pymediainfo when missing and the file is reachable locally."""
    if (
        not volume_map
        or record.is_directory
        or not (record.is_image or record.is_video)
        or (record.width > 0 and record.height > 0)
    ):
        return record

    node_cache = await _node_cache_for_path_resolution(record, storage, off_main)
    local_path = _resolve_local_path(record, node_cache, folders, volume_map)
    if local_path is None or not local_path.exists():
        return record

    result = await off_main.untimed(_probe_sync, local_path)
    if result is None:
        return record
    w, h, ms = result
    if w == 0 and h == 0:
        return record

    return replace(
        record,
        width=w,
        height=h,
        ms_duration=ms if ms > 0 else record.ms_duration,
    )


def _node_cache_for_path_resolution_sync(
    record: NodeRecord,
    storage: Storage,
) -> dict[str, NodeRecord | None]:
    needed_ids: set[str] = set()
    pid = record.parent_id
    while pid and not is_virtual(pid):
        needed_ids.add(pid)
        parent = storage.get_node_by_id(pid)
        pid = parent.parent_id if parent else None
    return {nid: storage.get_node_by_id(nid) for nid in needed_ids}


def backfill_media_metadata(
    dsn: str,
    folders: dict[str, str],
    volume_map: dict[str, str],
) -> int:
    """Backfill width/height/ms_duration for media files; does not emit change rows."""
    storage = Storage(dsn)
    storage.ensure_schema()
    updated = 0
    for record in storage.list_media_backfill_candidates():
        node_cache = _node_cache_for_path_resolution_sync(record, storage)
        local_path = _resolve_local_path(record, node_cache, folders, volume_map)
        if local_path is None or not local_path.exists():
            continue
        result = _probe_sync(local_path)
        if result is None:
            continue
        w, h, ms = result
        if w == 0 and h == 0:
            continue
        storage.upsert_node(
            replace(
                record,
                width=w,
                height=h,
                ms_duration=ms if ms > 0 else record.ms_duration,
            )
        )
        updated += 1
    return updated
