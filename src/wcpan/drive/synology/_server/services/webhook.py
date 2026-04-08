"""Webhook event processing service with delayed file retry handling."""

import asyncio
from collections.abc import Coroutine
from logging import getLogger
from typing import Any, TypedDict

from ...types import NodeRecord
from ..api import files as synology_files
from ..lib.bfs import parallel_bfs
from ..lib.mounts import MountRegistry
from ..lib.nodes import convert_file_info
from ..types import WebhookQueue, WriteQueue
from .network import NetworkService
from .paths import SynologyPathService
from .storage import StorageService
from .sync import NodeSyncService


_L = getLogger(__name__)

_RETRY_DELAYS = (10.0, 20.0, 30.0)


async def _guarded(file_id: str, coro: Coroutine[Any, Any, None]) -> None:
    """Run *coro*, logging (but not re-raising) any non-cancellation exception."""
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception:
        _L.exception("Pending file task failed for %s", file_id)


class _PendingFileScheduler:
    """Tracks one delayed-upsert task per file_id within the server's TaskGroup."""

    def __init__(self, group: asyncio.TaskGroup) -> None:
        self._group = group
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def schedule(self, file_id: str, coro: Coroutine[Any, Any, None]) -> None:
        """Cancel any existing pending task for *file_id* and schedule *coro*."""
        if old := self._tasks.pop(file_id, None):
            old.cancel()
        task = self._group.create_task(_guarded(file_id, coro))
        self._tasks[file_id] = task
        task.add_done_callback(lambda _: self._tasks.pop(file_id, None))

    def cancel(self, file_id: str) -> None:
        """Cancel the pending task for *file_id* if one exists."""
        if task := self._tasks.pop(file_id, None):
            task.cancel()


class _WebhookNamespace(TypedDict, total=False):
    id: str
    name: str
    type: str


class _WebhookItem(TypedDict, total=False):
    event_type: str
    file_id: str
    file_type: str
    parent_id: str
    namespace: _WebhookNamespace
    path: str


async def _fetch_and_enrich(
    network: NetworkService,
    storage: StorageService,
    node_sync: NodeSyncService,
    file_id: str,
    parent_id: str,
    event_type: str,
    mount_registry: MountRegistry,
) -> bool:
    """Fetch metadata, enrich, and upsert. Returns True on success, False if not ready."""
    if not parent_id:
        _L.warning(
            "file_id %s %s without parent_id; skipping",
            file_id,
            event_type,
        )
        return True  # no point retrying
    parent_node = await storage.get_node_by_id(parent_id)
    effective_parent_id = parent_id
    if parent_node is None:
        resolved = mount_registry.lookup_mount_virtual_id(parent_id)
        if resolved is None:
            _L.warning(
                "parent_id %s for %s (%s) not in DB and not a known mount; skipping",
                parent_id,
                file_id,
                event_type,
            )
            return True  # no point retrying
        effective_parent_id = resolved
    info = await synology_files.get_file_metadata_by_id(network, file_id)
    if not info:
        _L.warning(
            "file_id %s not found after %s; will retry",
            file_id,
            event_type,
        )
        return False
    record = convert_file_info(info, effective_parent_id)
    await node_sync.upsert(record)
    return True


async def _delayed_file_upsert(
    file_id: str,
    parent_id: str,
    event_type: str,
    network: NetworkService,
    storage: StorageService,
    node_sync: NodeSyncService,
    mount_registry: MountRegistry,
) -> None:
    """Attempt to upsert a file after a delay, retrying with increasing intervals."""
    for delay in _RETRY_DELAYS:
        await asyncio.sleep(delay)
        done = await _fetch_and_enrich(
            network,
            storage,
            node_sync,
            file_id,
            parent_id,
            event_type,
            mount_registry,
        )
        if done:
            return
        _L.debug("file %s not ready yet, retrying", file_id)
    _L.warning("delayed upsert for %s failed after all retries", file_id)


async def _process_webhook_item(
    item: _WebhookItem,
    network: NetworkService,
    storage: StorageService,
    node_sync: NodeSyncService,
    mount_registry: MountRegistry,
) -> tuple[str | None, bool]:
    """Process one webhook event. Returns (delete_node_id | None, needs_schedule).

    needs_schedule=True means the caller should schedule a delayed file upsert.
    Raises on unexpected errors; the caller catches and logs.
    """
    event_type = item.get("event_type", "")
    file_id = item.get("file_id", "")
    file_type = item.get("file_type", "")
    parent_id = item.get("parent_id", "")

    if not file_id:
        return None, False

    if event_type == "file_removed":
        return file_id, False

    if event_type == "file_modified" and file_type == "file":
        # Schedule a delayed upsert regardless of in-progress vs complete state.
        return None, True

    if event_type == "file_modified":
        # Directory mtime changed because a child changed — children fire their own events.
        return None, False

    if event_type == "file_created" and file_type == "dir":
        # Directories are created atomically; upsert immediately.
        await _fetch_and_enrich(
            network,
            storage,
            node_sync,
            file_id,
            parent_id,
            event_type,
            mount_registry,
        )
        return None, False

    if event_type in ("file_moved", "file_renamed"):
        # parent_id (move) or name (rename) already reflects new state.
        await _fetch_and_enrich(
            network,
            storage,
            node_sync,
            file_id,
            parent_id,
            event_type,
            mount_registry,
        )
        return None, False

    if event_type == "file_created" and file_type == "file":
        # Upload may be in progress; schedule delayed upsert.
        return None, True

    # Unknown event type — ignore.
    return None, False


async def _scan_moved_dir_subtree(
    folder_id: str,
    network: NetworkService,
    syno_paths: SynologyPathService,
    node_sync: NodeSyncService,
    write_queue: WriteQueue,
) -> None:
    """BFS-populate children of a moved directory not delivered by the webhook."""

    async def _visit(current_id: str) -> list[str]:
        try:
            items = await syno_paths.list_children(network, current_id)
        except Exception:
            _L.exception("moved-dir scan: failed to list children of %s", current_id)
            return []
        dir_records: list[NodeRecord] = []
        file_records: list[NodeRecord] = []
        children: list[str] = []
        for item in items:
            record = convert_file_info(item, parent_id=current_id)
            if item["type"] == "dir":
                dir_records.append(record)
                children.append(str(item["file_id"]))
            else:
                file_records.append(record)
        if dir_records:
            await node_sync.upsert_batch(dir_records)
            await write_queue.join()
        if file_records:
            await node_sync.upsert_file_batch(file_records)
        return children

    await parallel_bfs([folder_id], _visit)


class WebhookService:
    """Processes Synology webhook events as a background consumer."""

    def __init__(
        self,
        network: NetworkService,
        storage: StorageService,
        node_sync: NodeSyncService,
        syno_paths: SynologyPathService,
        write_queue: WriteQueue,
        mount_registry: MountRegistry,
    ) -> None:
        self._network = network
        self._storage = storage
        self._node_sync = node_sync
        self._syno_paths = syno_paths
        self._write_queue = write_queue
        self._mount_registry = mount_registry

    async def run(
        self,
        queue: WebhookQueue,
        group: asyncio.TaskGroup,
        scan_done_event: asyncio.Event,
    ) -> None:
        pending = _PendingFileScheduler(group)
        await scan_done_event.wait()
        while True:
            item: _WebhookItem = await queue.get()
            try:
                file_id = item.get("file_id", "")
                parent_id = item.get("parent_id", "")
                event_type = item.get("event_type", "")
                file_type = item.get("file_type", "")
                delete_id, needs_schedule = await _process_webhook_item(
                    item,
                    self._network,
                    self._storage,
                    self._node_sync,
                    self._mount_registry,
                )
                if event_type == "file_created" and file_type == "dir":
                    await self._write_queue.join()
                if event_type == "file_moved" and file_type == "dir" and file_id:
                    await self._write_queue.join()
                    await _scan_moved_dir_subtree(
                        file_id,
                        self._network,
                        self._syno_paths,
                        self._node_sync,
                        self._write_queue,
                    )
                if delete_id is not None:
                    await self._node_sync.delete(delete_id)
                if needs_schedule and file_id:
                    pending.schedule(
                        file_id,
                        _delayed_file_upsert(
                            file_id,
                            parent_id,
                            event_type,
                            self._network,
                            self._storage,
                            self._node_sync,
                            self._mount_registry,
                        ),
                    )
            except Exception:
                _L.exception("Failed to process webhook item %s; ignoring", item)
            finally:
                queue.task_done()
