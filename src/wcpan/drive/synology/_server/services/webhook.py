"""Webhook event processing service with debounced file upsert handling."""

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from logging import getLogger
from typing import Any

from ...types import MirrorStableId, NodeRecord
from ..api.drive import SynologyDriveApi
from ..api.lib import convert_file_info
from ..api.types import SynologyWebhookEvent
from ..lib.bfs import parallel_bfs
from ..lib.mounts import MountRegistry
from ..types import (
    SynologyFileId,
    SynologyPermanentLink,
    WebhookQueue,
    WriteQueue,
)
from .paths import SynologyPathService
from .storage import StorageService
from .sync import NodeSyncService


_L = getLogger(__name__)

_PENDING_FILE_DELAY = 10.0


async def _guarded(file_id: str, coro: Coroutine[Any, Any, None]) -> None:
    """Run *coro*, logging (but not re-raising) any non-cancellation exception."""
    try:
        await coro
    except Exception:
        _L.exception("Pending file task failed for %s", file_id)


class _Debouncer:
    """Runs one delayed task without canceling it after the body starts."""

    def __init__(
        self,
        group: asyncio.TaskGroup,
        delay: float,
        factory: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        self._group = group
        self._delay = delay
        self._factory = factory
        self._task: asyncio.Task[None] | None = None
        self.started = False

    def start(self) -> asyncio.Task[None]:
        task = self._group.create_task(self._run())
        self._task = task
        return task

    def cancel_pending(self) -> bool:
        if self.started or self._task is None:
            return False
        self._task.cancel()
        return True

    def is_task(self, task: asyncio.Task[None]) -> bool:
        return self._task is task

    async def _run(self) -> None:
        await asyncio.sleep(self._delay)
        self.started = True
        await self._factory()


class _TaskIdDebouncer:
    """Debounces work independently for each task id."""

    def __init__(
        self, group: asyncio.TaskGroup, delay: float = _PENDING_FILE_DELAY
    ) -> None:
        self._group = group
        self._delay = delay
        self._debouncers: dict[str, _Debouncer] = {}

    def schedule(
        self,
        task_id: str,
        factory: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        current = self._debouncers.get(task_id)
        if current is not None:
            if current.started:
                return
            current.cancel_pending()

        debouncer = _Debouncer(
            self._group,
            self._delay,
            lambda: _guarded(task_id, factory()),
        )
        task = debouncer.start()
        self._debouncers[task_id] = debouncer

        def cleanup(done: asyncio.Task[None]) -> None:
            if debouncer.is_task(done) and self._debouncers.get(task_id) is debouncer:
                self._debouncers.pop(task_id, None)

        task.add_done_callback(cleanup)

    def cancel(self, task_id: str) -> None:
        if (current := self._debouncers.get(task_id)) and not current.started:
            self._debouncers.pop(task_id, None)
            current.cancel_pending()


@dataclass(frozen=True, slots=True)
class _WebhookActionPlan:
    event_type: str
    schedule_key: str
    file_ref: SynologyFileId
    permanent_link_ref: SynologyPermanentLink
    parent_file_ref: SynologyFileId | None
    fetch_immediately: bool = False
    wait_for_writes: bool = False
    scan_moved_dir_subtree: bool = False
    delete_id: MirrorStableId | None = None
    schedule_delayed_upsert: bool = False


class WebhookService:
    """Processes Synology webhook events as a background consumer."""

    def __init__(
        self,
        *,
        drive_api: SynologyDriveApi,
        storage: StorageService,
        node_sync: NodeSyncService,
        syno_paths: SynologyPathService,
        write_queue: WriteQueue,
        mount_registry: MountRegistry,
    ) -> None:
        self._drive_api = drive_api
        self._storage = storage
        self._node_sync = node_sync
        self._syno_paths = syno_paths
        self._write_queue = write_queue
        self._mount_registry = mount_registry

    async def _fetch_and_enrich(
        self,
        file_ref: SynologyFileId,
        permanent_link_ref: SynologyPermanentLink,
        parent_file_ref: SynologyFileId | None,
        event_type: str,
    ) -> None:
        if parent_file_ref is None:
            _L.warning(
                "file_id %s %s without parent_id; skipping",
                file_ref.file_id,
                event_type,
            )
            return
        parent_node = await self._storage.get_node_by_mutable_id(
            parent_file_ref.to_mirror_mutable_id()
        )
        if parent_node is None:
            resolved = self._mount_registry.lookup_mount_virtual_id(parent_file_ref)
            if resolved is None:
                _L.warning(
                    "parent_id %s for %s (%s) not in DB and not a known mount; skipping",
                    parent_file_ref.file_id,
                    file_ref.file_id,
                    event_type,
                )
                return
            effective_parent_id = resolved
        else:
            effective_parent_id = parent_node.id
        info = await self._drive_api.get_node_metadata(permanent_link_ref)
        if not info:
            _L.warning(
                "%s not found after %s; skipping",
                permanent_link_ref,
                event_type,
            )
            return
        record = convert_file_info(info, effective_parent_id)
        if record is None:
            return
        if record.id != permanent_link_ref.permanent_link:
            _L.debug(
                "Webhook permanent_link mismatch for file_id %s: payload=%s fetched=%s",
                file_ref.file_id,
                permanent_link_ref.permanent_link,
                record.id,
            )
        await self._node_sync.upsert(record)

    def _classify_webhook_item(
        self,
        item: SynologyWebhookEvent,
    ) -> _WebhookActionPlan:
        """Classify one webhook event into an execution plan."""
        event_type = item["event_type"]
        file_id = item["file_id"]
        permanent_link = item["permanent_link"]
        file_type = item["file_type"]
        parent_id = item["parent_id"]

        permanent_link_ref = SynologyPermanentLink(permanent_link=permanent_link)
        file_ref = SynologyFileId(file_id=file_id)
        parent_file_ref = SynologyFileId(file_id=parent_id) if parent_id else None

        if event_type == "file_removed":
            return _WebhookActionPlan(
                event_type=event_type,
                schedule_key=permanent_link,
                file_ref=file_ref,
                permanent_link_ref=permanent_link_ref,
                parent_file_ref=parent_file_ref,
                delete_id=permanent_link_ref.to_mirror_stable_id(),
            )

        if event_type == "file_modified" and file_type == "file":
            return _WebhookActionPlan(
                event_type=event_type,
                schedule_key=permanent_link,
                file_ref=file_ref,
                permanent_link_ref=permanent_link_ref,
                parent_file_ref=parent_file_ref,
                schedule_delayed_upsert=True,
            )

        if event_type == "file_modified":
            return _WebhookActionPlan(
                event_type=event_type,
                schedule_key=permanent_link,
                file_ref=file_ref,
                permanent_link_ref=permanent_link_ref,
                parent_file_ref=parent_file_ref,
            )

        if event_type == "file_created" and file_type == "dir":
            return _WebhookActionPlan(
                event_type=event_type,
                schedule_key=permanent_link,
                file_ref=file_ref,
                permanent_link_ref=permanent_link_ref,
                parent_file_ref=parent_file_ref,
                fetch_immediately=True,
                wait_for_writes=True,
            )

        if event_type in ("file_moved", "file_renamed"):
            return _WebhookActionPlan(
                event_type=event_type,
                schedule_key=permanent_link,
                file_ref=file_ref,
                permanent_link_ref=permanent_link_ref,
                parent_file_ref=parent_file_ref,
                fetch_immediately=True,
                wait_for_writes=event_type == "file_moved" and file_type == "dir",
                scan_moved_dir_subtree=event_type == "file_moved"
                and file_type == "dir",
            )

        if event_type == "file_created" and file_type == "file":
            return _WebhookActionPlan(
                event_type=event_type,
                schedule_key=permanent_link,
                file_ref=file_ref,
                permanent_link_ref=permanent_link_ref,
                parent_file_ref=parent_file_ref,
                schedule_delayed_upsert=True,
            )

        return _WebhookActionPlan(
            event_type=event_type,
            schedule_key=permanent_link,
            file_ref=file_ref,
            permanent_link_ref=permanent_link_ref,
            parent_file_ref=parent_file_ref,
        )

    async def _execute_webhook_plan(
        self,
        plan: _WebhookActionPlan,
        *,
        pending: _TaskIdDebouncer,
    ) -> None:
        if plan.fetch_immediately:
            await self._fetch_and_enrich(
                plan.file_ref,
                plan.permanent_link_ref,
                plan.parent_file_ref,
                plan.event_type,
            )
        if plan.wait_for_writes:
            await self._write_queue.join()
        if plan.scan_moved_dir_subtree:
            subtree_root_id = await self._resolve_moved_dir_root_id(
                plan.file_ref,
                plan.permanent_link_ref,
            )
            if subtree_root_id is None:
                _L.warning(
                    "Skipping moved-dir subtree scan for unresolved root file_id=%s permanent_link=%s",
                    plan.file_ref.file_id,
                    plan.permanent_link_ref.permanent_link,
                )
            else:
                await self._scan_moved_dir_subtree(subtree_root_id)
        if plan.delete_id is not None:
            await self._node_sync.delete(plan.delete_id)
        if plan.schedule_delayed_upsert:
            pending.schedule(
                plan.schedule_key,
                lambda: self._fetch_and_enrich(
                    plan.file_ref,
                    plan.permanent_link_ref,
                    plan.parent_file_ref,
                    plan.event_type,
                ),
            )

    async def _scan_moved_dir_subtree(self, folder_id: MirrorStableId) -> None:
        """BFS-populate children of a moved directory not delivered by the webhook."""

        async def _visit(current_id: MirrorStableId) -> list[MirrorStableId]:
            try:
                items = await self._syno_paths.list_children(
                    self._drive_api, current_id
                )
            except Exception:
                _L.exception(
                    "moved-dir scan: failed to list children of %s", current_id
                )
                return []
            dir_records: list[NodeRecord] = []
            file_records: list[NodeRecord] = []
            children: list[MirrorStableId] = []
            for item in items:
                record = convert_file_info(item, parent_id=current_id)
                if record is None:
                    continue
                if item["type"] == "dir":
                    dir_records.append(record)
                    children.append(record.id)
                else:
                    file_records.append(record)
            if dir_records:
                await self._node_sync.upsert_batch(dir_records)
                await self._write_queue.join()
            if file_records:
                await self._node_sync.upsert_file_batch(file_records)
            return children

        await parallel_bfs([folder_id], _visit)

    async def _resolve_moved_dir_root_id(
        self,
        file_ref: SynologyFileId,
        permanent_link_ref: SynologyPermanentLink,
    ) -> MirrorStableId | None:
        record = await self._storage.get_node_by_id(
            permanent_link_ref.to_mirror_stable_id()
        )
        if record is not None:
            return record.id
        info = await self._drive_api.get_node_metadata(permanent_link_ref)
        if info is not None:
            return (
                MirrorStableId(info["permanent_link"])
                if info["permanent_link"]
                else permanent_link_ref.to_mirror_stable_id()
            )
        record = await self._storage.get_node_by_mutable_id(
            file_ref.to_mirror_mutable_id()
        )
        if record is not None:
            return record.id
        return None

    async def run(
        self,
        queue: WebhookQueue,
        group: asyncio.TaskGroup,
        scan_done_event: asyncio.Event,
    ) -> None:
        pending = _TaskIdDebouncer(group)
        await scan_done_event.wait()
        while True:
            item = await queue.get()
            _L.debug("webhook %r", item)
            try:
                plan = self._classify_webhook_item(item)
                await self._execute_webhook_plan(plan, pending=pending)
            except Exception:
                _L.exception("Failed to process webhook item %s; ignoring", item)
            finally:
                queue.task_done()
