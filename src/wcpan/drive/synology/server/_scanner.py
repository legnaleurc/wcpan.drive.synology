import asyncio
from functools import partial
from logging import getLogger

from ..lib import utc_now
from ..types import NodeRecord
from ._api.changes import scan_all_mounts
from ._db import Storage
from ._lib import OffMainThread
from ._network import Network
from ._types import WriteQueue
from ._virtual_ids import SERVER_ROOT_ID, mount_id
from ._workers import run_queued_write


_L = getLogger(__name__)

_DEBOUNCE_SECONDS = 30.0


def _make_root_record() -> NodeRecord:
    now = utc_now()
    return NodeRecord(
        node_id=SERVER_ROOT_ID,
        parent_id=None,
        name="",
        is_directory=True,
        ctime=now,
        mtime=now,
        mime_type="application/x-directory",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


def _make_mount_record(name: str) -> NodeRecord:
    now = utc_now()
    return NodeRecord(
        node_id=mount_id(name),
        parent_id=SERVER_ROOT_ID,
        name=name,
        is_directory=True,
        ctime=now,
        mtime=now,
        mime_type="application/x-directory",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


def _structural_records(folders: dict[str, str]) -> list[NodeRecord]:
    return [_make_root_record()] + [_make_mount_record(name) for name in folders]


async def _scanner_initial_sync(
    network: Network,
    storage: Storage,
    folders: dict[str, str],
    off_main: OffMainThread,
    write_queue: WriteQueue,
    volume_map: dict[str, str] | None,
) -> None:
    """First full scan or resume: align nodes + change feed before serving."""
    wq = write_queue
    last_max_id = await off_main(storage.get_last_max_id)

    if last_max_id == 0:
        _L.info("First run: performing full scan of %d mount(s)", len(folders))
        _L.debug("Creating root node and %d mount node(s)", len(folders))
        structural = _structural_records(folders)
        await run_queued_write(
            wq, partial(storage.apply_scan_folder_batch, [], structural)
        )

        new_max_id = await scan_all_mounts(
            network,
            storage,
            folders,
            last_max_id=0,
            volume_map=volume_map,
            off_main=off_main,
            write_queue=wq,
        )
        await run_queued_write(wq, partial(storage.set_last_max_id, new_max_id))
        _L.info("Full scan complete; last_max_id=%d", new_max_id)
        return

    _L.info("Resuming from last_max_id=%d", last_max_id)
    # First run already emitted structural rows into `changes`; keep `nodes` in
    # sync (e.g. new mounts in config) without inserting duplicate feed rows.
    structural = _structural_records(folders)
    _L.debug("Ensuring %d mount node(s) exist", len(folders))
    await run_queued_write(wq, partial(storage.bulk_upsert_nodes, structural))

    _L.info("Resuming: running incremental scan to catch up on downtime changes")
    new_max_id = await scan_all_mounts(
        network,
        storage,
        folders,
        last_max_id=last_max_id,
        volume_map=volume_map,
        off_main=off_main,
        write_queue=wq,
    )
    if new_max_id > last_max_id:
        await run_queued_write(wq, partial(storage.set_last_max_id, new_max_id))
    _L.info("Resume scan complete; last_max_id=%d", new_max_id)


async def _scanner_incremental_loop(
    network: Network,
    storage: Storage,
    folders: dict[str, str],
    trigger_event: asyncio.Event,
    off_main: OffMainThread,
    write_queue: WriteQueue,
    volume_map: dict[str, str] | None,
) -> None:
    """Wait for webhook trigger, scan Synology for deltas, advance last_max_id."""
    wq = write_queue
    while True:
        await trigger_event.wait()
        while True:
            trigger_event.clear()
            try:
                await asyncio.wait_for(trigger_event.wait(), timeout=_DEBOUNCE_SECONDS)
            except asyncio.TimeoutError:
                break
        try:
            last_max_id = await off_main(storage.get_last_max_id)
            new_max_id = await scan_all_mounts(
                network,
                storage,
                folders,
                last_max_id,
                volume_map=volume_map,
                off_main=off_main,
                write_queue=wq,
            )
            if new_max_id > last_max_id:
                await run_queued_write(wq, partial(storage.set_last_max_id, new_max_id))
                _L.debug("Updated last_max_id to %d", new_max_id)
        except Exception:
            _L.exception("Error during incremental scan")


async def run_scanner(
    network: Network,
    storage: Storage,
    folders: dict[str, str],
    trigger_event: asyncio.Event,
    ready_event: asyncio.Event,
    off_main: OffMainThread,
    write_queue: WriteQueue,
    volume_map: dict[str, str] | None = None,
) -> None:
    """Run startup scan then wait for webhook triggers until cancelled."""
    await _scanner_initial_sync(
        network,
        storage,
        folders,
        off_main,
        write_queue,
        volume_map,
    )
    ready_event.set()
    _L.info("Server ready")
    await _scanner_incremental_loop(
        network,
        storage,
        folders,
        trigger_event,
        off_main,
        write_queue,
        volume_map,
    )
