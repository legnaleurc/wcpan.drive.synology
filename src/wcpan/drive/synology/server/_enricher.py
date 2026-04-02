"""Media dimensions from local disk — applied before each upsert that emits a change."""

from dataclasses import replace
from logging import getLogger
from pathlib import Path
from typing import Any

from pymediainfo import MediaInfo  # type: ignore[import-untyped]

from ..types import NodeRecord
from ._db import Storage
from ._lib import OffMainThread
from ._paths import resolve_local_path
from ._virtual_ids import is_virtual


_L = getLogger(__name__)


def _probe_sync(path: Path, *, is_image: bool) -> tuple[int, int, int] | None:
    """Probe width, height, ms_duration using pymediainfo. Runs in a thread."""
    opts = {"File_TestContinuousFileNames": "0"} if is_image else {}
    try:
        info: Any = MediaInfo.parse(str(path), mediainfo_options=opts)
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
            ms_duration = int(float(track.duration or 0))

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
        or (
            record.width > 0
            and record.height > 0
            and (not record.is_video or record.ms_duration > 0)
        )
    ):
        return record

    node_cache = await _node_cache_for_path_resolution(record, storage, off_main)
    local_path = resolve_local_path(record, node_cache, folders, volume_map)
    if local_path is None or not local_path.exists():
        return record

    result = await off_main.untimed(_probe_sync, local_path, is_image=record.is_image)
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


def enrich_subtree(
    dsn: str,
    folders: dict[str, str],
    volume_map: dict[str, str],
    root_node_id: str,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Enrich width/height/ms_duration for image/video nodes under *root_node_id*.

    Only processes nodes already in the DB that have width=height=0.
    Emits change rows for updated nodes.
    """
    from ._virtual_ids import is_virtual

    storage = Storage(dsn)
    storage.ensure_schema()
    subtree_ids = storage.collect_subtree_node_ids(root_node_id)

    checked = updated = skipped = 0
    for nid in subtree_ids:
        if is_virtual(nid):
            continue
        record = storage.get_node_by_id(nid)
        if record is None or record.is_directory:
            continue
        if not (record.is_image or record.is_video):
            continue
        if (
            record.width != 0
            and record.height != 0
            and (not record.is_video or record.ms_duration != 0)
        ):
            continue
        checked += 1
        node_cache = _node_cache_for_path_resolution_sync(record, storage)
        local_path = resolve_local_path(record, node_cache, folders, volume_map)
        if local_path is None or not local_path.exists():
            skipped += 1
            continue
        result = _probe_sync(local_path, is_image=record.is_image)
        if result is None or (result[0] == 0 and result[1] == 0):
            skipped += 1
            continue
        _L.debug(f"{local_path} -> {result}")
        w, h, ms = result
        updated += 1
        if not dry_run:
            storage.upsert_node_and_emit_change(
                replace(
                    record,
                    width=w,
                    height=h,
                    ms_duration=ms if ms > 0 else record.ms_duration,
                )
            )

    return {"checked": checked, "updated": updated, "skipped": skipped}


def enrich_record_sync(
    record: NodeRecord,
    storage: Storage,
    folders: dict[str, str],
    volume_map: dict[str, str],
) -> NodeRecord:
    """Probe and set width/height/ms_duration for a single record.

    Unlike enrich_media_before_upsert, does not skip nodes that already have
    dimensions, so stale values can be refreshed during backfill.
    """
    if record.is_directory or not (record.is_image or record.is_video):
        return record

    node_cache = _node_cache_for_path_resolution_sync(record, storage)
    local_path = resolve_local_path(record, node_cache, folders, volume_map)
    if local_path is None or not local_path.exists():
        return record

    result = _probe_sync(local_path, is_image=record.is_image)
    if result is None or (result[0] == 0 and result[1] == 0):
        return record

    w, h, ms = result
    return replace(
        record,
        width=w,
        height=h,
        ms_duration=ms if ms > 0 else record.ms_duration,
    )


def backfill_media_metadata(
    dsn: str,
    folders: dict[str, str],
    volume_map: dict[str, str],
) -> int:
    """Backfill width/height/ms_duration for media files; emits change rows for updated nodes."""
    storage = Storage(dsn)
    storage.ensure_schema()
    updated = 0
    for record in storage.list_media_backfill_candidates():
        node_cache = _node_cache_for_path_resolution_sync(record, storage)
        local_path = resolve_local_path(record, node_cache, folders, volume_map)
        if local_path is None or not local_path.exists():
            continue
        result = _probe_sync(local_path, is_image=record.is_image)
        if result is None:
            continue
        w, h, ms = result
        if w == 0 and h == 0:
            continue
        storage.upsert_node_and_emit_change(
            replace(
                record,
                width=w,
                height=h,
                ms_duration=ms if ms > 0 else record.ms_duration,
            )
        )
        updated += 1
    return updated
