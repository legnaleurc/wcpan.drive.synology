"""Webhook event processing service with debounced file upsert handling."""

import asyncio
from collections.abc import Coroutine
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

    async def _delayed_file_upsert(
        self,
        file_ref: SynologyFileId,
        permanent_link_ref: SynologyPermanentLink,
        parent_file_ref: SynologyFileId | None,
        event_type: str,
    ) -> None:
        await asyncio.sleep(_PENDING_FILE_DELAY)
        await self._fetch_and_enrich(
            file_ref,
            permanent_link_ref,
            parent_file_ref,
            event_type,
        )

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
        pending: _PendingFileScheduler,
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
                self._delayed_file_upsert(
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
        pending = _PendingFileScheduler(group)
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
