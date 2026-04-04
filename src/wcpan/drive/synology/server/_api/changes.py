"""BFS scan for detecting changes via Synology Drive API sync_id fields.

Deletions are deferred to the end of each ``scan_all_mounts`` pass. Per-folder
diffs would eagerly delete nodes that disappeared from one parent's listing
before another mount's listing could reparent the same ``file_id``, causing
remove-then-add in the change feed. Instead we collect *seen_ids* from every
successful list and *subtree_preserve_roots* for folders skipped due to
``max_id`` pruning or failed API calls; their DB subtrees stay in the preserved
set until a final cleanup removes only Synology nodes under configured mounts
that are still not preserved.
"""

from dataclasses import dataclass, field
from functools import partial
from logging import getLogger

from ...lib import guess_mime_type, utc_from_timestamp
from ...types import NodeRecord
from .._db import Storage
from .._enricher import enrich_media_before_upsert
from .._lib import OffMainThread
from .._network import Network
from .._types import WriteQueue
from .._virtual_ids import mount_id
from .._workers import run_queued_write
from .files import SynologyFileInfo, list_folder_all, list_folder_all_by_path


_L = getLogger(__name__)


@dataclass
class ScanAccumulator:
    """State for one ``scan_all_mounts`` pass (deferred deletion)."""

    seen_ids: set[str] = field(default_factory=set)
    subtree_preserve_roots: set[str] = field(default_factory=set)


def convert_file_info(info: SynologyFileInfo, parent_id: str | None) -> NodeRecord:
    is_dir = info["type"] == "dir"
    name = info["name"]
    is_image = info.get("content_type") == "image"
    is_video = info.get("content_type") == "video"

    return NodeRecord(
        node_id=info["file_id"],
        parent_id=parent_id,
        name=name,
        is_directory=is_dir,
        ctime=utc_from_timestamp(info.get("created_time", 0)),
        mtime=utc_from_timestamp(info.get("modified_time", 0)),
        mime_type=guess_mime_type(name, is_directory=is_dir),
        hash=info.get("hash", ""),
        size=info.get("size", 0),
        is_image=is_image,
        is_video=is_video,
        width=0,
        height=0,
        ms_duration=0,
    )


async def _scan_mount_level(
    network: Network,
    storage: Storage,
    mid: str,
    syno_path: str,
    last_max_id: int,
    acc: ScanAccumulator,
    *,
    folders: dict[str, str],
    volume_map: dict[str, str] | None,
    off_main: OffMainThread,
    write_queue: WriteQueue,
) -> tuple[int, list[tuple[str, int, bool]]]:
    """Scan the first level of a mount via Synology path string.

    Returns (highest_sync_id, subfolders) where subfolders is a list of
    (file_id, max_id, force_scan) tuples for deeper BFS traversal.
    force_scan is True when the folder was not previously in the DB and
    must be entered regardless of max_id pruning.
    """
    try:
        items = await list_folder_all_by_path(network, syno_path)
    except Exception:
        _L.exception("Failed to list mount path %r", syno_path)
        acc.subtree_preserve_roots.add(mid)
        return last_max_id, []

    _L.debug("Mount %r: %d item(s) from API", syno_path, len(items))

    db_children = await off_main(storage.get_children, mid)
    db_child_ids = {n.node_id for n in db_children}

    for item in items:
        acc.seen_ids.add(item["file_id"])

    highest = last_max_id
    subfolders: list[tuple[str, int, bool]] = []
    pending_upserts: list[NodeRecord] = []

    for item in items:
        sync_id = item.get("sync_id", 0)
        max_id = item.get("max_id", sync_id)
        if sync_id > highest:
            highest = sync_id

        is_new = item["file_id"] not in db_child_ids
        if sync_id > last_max_id or is_new:
            record = convert_file_info(item, parent_id=mid)
            record = await enrich_media_before_upsert(
                record, storage, folders, volume_map, off_main
            )
            pending_upserts.append(record)

        if item["type"] == "dir":
            subfolders.append((item["file_id"], max_id, is_new))

    if pending_upserts:
        await run_queued_write(
            write_queue,
            partial(storage.apply_scan_folder_batch, [], pending_upserts),
        )

    _L.debug(
        "Mount %r: %d upsert(s) (deletions deferred to end of scan)",
        syno_path,
        len(pending_upserts),
    )
    return highest, subfolders


async def _scan_subtree_bfs(
    network: Network,
    storage: Storage,
    initial: list[tuple[str, int, bool]],
    last_max_id: int,
    acc: ScanAccumulator,
    *,
    folders: dict[str, str],
    volume_map: dict[str, str] | None,
    off_main: OffMainThread,
    write_queue: WriteQueue,
) -> int:
    """BFS scan of subfolders using Synology file_ids with max_id pruning.

    Returns the highest sync_id seen.
    """
    queue = list(initial)
    highest = last_max_id

    while queue:
        folder_id, this_max_id, force_scan = queue.pop(0)

        if not force_scan and last_max_id > 0 and this_max_id <= last_max_id:
            db_children = await off_main(storage.get_children, folder_id)
            if db_children:
                _L.debug(
                    "Skipping folder %s (max_id=%d <= last_max_id=%d)",
                    folder_id,
                    this_max_id,
                    last_max_id,
                )
                acc.subtree_preserve_roots.add(folder_id)
                continue
            _L.debug(
                "Force-entering folder %s: in DB but no children (max_id=%d)",
                folder_id,
                this_max_id,
            )

        _L.debug("Entering folder %s (max_id=%d)", folder_id, this_max_id)
        try:
            items = await list_folder_all(network, folder_id)
        except Exception:
            _L.exception("Failed to list folder %s", folder_id)
            acc.subtree_preserve_roots.add(folder_id)
            continue

        for item in items:
            acc.seen_ids.add(item["file_id"])

        db_children = await off_main(storage.get_children, folder_id)
        db_child_ids = {n.node_id for n in db_children}

        pending_upserts: list[NodeRecord] = []

        for item in items:
            sync_id = item.get("sync_id", 0)
            max_id = item.get("max_id", sync_id)
            if sync_id > highest:
                highest = sync_id

            is_new = item["file_id"] not in db_child_ids
            if sync_id > last_max_id or is_new:
                record = convert_file_info(item, parent_id=folder_id)
                record = await enrich_media_before_upsert(
                    record, storage, folders, volume_map, off_main
                )
                pending_upserts.append(record)

            if item["type"] == "dir":
                queue.append((item["file_id"], max_id, is_new))

        if pending_upserts:
            await run_queued_write(
                write_queue,
                partial(storage.apply_scan_folder_batch, [], pending_upserts),
            )

    return highest


async def scan_all_mounts(
    network: Network,
    storage: Storage,
    folders: dict[str, str],
    last_max_id: int,
    *,
    volume_map: dict[str, str] | None = None,
    off_main: OffMainThread,
    write_queue: WriteQueue,
) -> int:
    """Scan all mounts. Pass last_max_id=0 for a full initial scan.

    Returns the highest sync_id seen across all mounts.
    """
    highest = last_max_id
    acc = ScanAccumulator()

    for name, syno_path in folders.items():
        _L.debug("Scanning mount %r (%s)", name, syno_path)
        mid = mount_id(name)
        try:
            level_highest, subfolders = await _scan_mount_level(
                network,
                storage,
                mid,
                syno_path,
                last_max_id,
                acc,
                folders=folders,
                volume_map=volume_map,
                off_main=off_main,
                write_queue=write_queue,
            )
            highest = max(highest, level_highest)

            subtree_highest = await _scan_subtree_bfs(
                network,
                storage,
                subfolders,
                last_max_id,
                acc,
                folders=folders,
                volume_map=volume_map,
                off_main=off_main,
                write_queue=write_queue,
            )
            highest = max(highest, subtree_highest)
        except Exception:
            _L.exception("Error scanning mount %r (%s)", name, syno_path)
            acc.subtree_preserve_roots.add(mid)

    preserved = await off_main(
        storage.build_deferred_preserved_set,
        acc.seen_ids,
        acc.subtree_preserve_roots,
    )
    mount_ids = {mount_id(n) for n in folders}
    await run_queued_write(
        write_queue,
        partial(storage.apply_deferred_scan_removals, preserved, mount_ids),
    )

    return highest
