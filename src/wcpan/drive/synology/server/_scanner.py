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
from ._workers import enqueue_write


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
    last_max_ids = await off_main(storage.get_mount_max_ids, folders)
    is_first_run = not any(last_max_ids.values())

    if is_first_run:
        _L.info("First run: performing full scan of %d mount(s)", len(folders))
        _L.debug("Creating root node and %d mount node(s)", len(folders))
        structural = _structural_records(folders)
        await enqueue_write(
            wq, partial(storage.apply_scan_folder_batch, [], structural)
        )
    else:
        _L.info("Resuming with per-mount max_ids: %s", last_max_ids)
        # First run already emitted structural rows into `changes`; keep `nodes`
        # in sync (e.g. new mounts in config) without duplicate feed rows.
        structural = _structural_records(folders)
        _L.debug("Ensuring %d mount node(s) exist", len(folders))
        await enqueue_write(wq, partial(storage.bulk_upsert_nodes, structural))

    new_max_ids = await scan_all_mounts(
        network,
        storage,
        folders,
        last_max_ids=last_max_ids,
        volume_map=volume_map,
        off_main=off_main,
        write_queue=wq,
    )
    for name, new_max_id in new_max_ids.items():
        if new_max_id > last_max_ids.get(name, 0):
            await enqueue_write(
                wq,
                partial(storage.set_mount_state, name, folders[name], new_max_id),
            )
    _L.info("Scan complete; per-mount max_ids: %s", new_max_ids)


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
            last_max_ids = await off_main(storage.get_mount_max_ids, folders)
            new_max_ids = await scan_all_mounts(
                network,
                storage,
                folders,
                last_max_ids=last_max_ids,
                volume_map=volume_map,
                off_main=off_main,
                write_queue=wq,
            )
            for name, new_max_id in new_max_ids.items():
                if new_max_id > last_max_ids.get(name, 0):
                    await enqueue_write(
                        wq,
                        partial(
                            storage.set_mount_state, name, folders[name], new_max_id
                        ),
                    )
                    _L.debug("Updated max_id for %r to %d", name, new_max_id)
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
