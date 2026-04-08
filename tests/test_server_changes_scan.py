"""Tests for deferred deletion in ``scan_all_mounts``."""

import asyncio
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology.lib import FOLDER_MIME_TYPE
from wcpan.drive.synology.server._api import changes as changes_mod
from wcpan.drive.synology.server._api.changes import scan_all_mounts
from wcpan.drive.synology.server._db import Storage
from wcpan.drive.synology.server._lib import OffMainThread
from wcpan.drive.synology.server._types import WriteQueue
from wcpan.drive.synology.server._virtual_ids import SERVER_ROOT_ID, mount_id
from wcpan.drive.synology.server._workers import create_write_queue
from wcpan.drive.synology.types import NodeRecord


def _node(
    node_id: str,
    name: str,
    parent_id: str | None,
    *,
    is_directory: bool = False,
) -> NodeRecord:
    t = datetime(2024, 1, 1, tzinfo=UTC)
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name=name,
        is_directory=is_directory,
        ctime=t,
        mtime=t,
        mime_type=FOLDER_MIME_TYPE if is_directory else "text/plain",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


def _syno_item(
    file_id: str,
    name: str,
    *,
    is_dir: bool = False,
    sync_id: int = 200,
    max_id: int | None = None,
) -> dict:
    mid = max_id if max_id is not None else sync_id
    return {
        "file_id": file_id,
        "parent_id": "",
        "name": name,
        "type": "dir" if is_dir else "file",
        "content_type": "dir" if is_dir else "file",
        "size": 0,
        "created_time": 0,
        "modified_time": 0,
        "sync_id": sync_id,
        "max_id": mid,
    }


async def _noop_enrich(record: NodeRecord, *args, **kwargs) -> NodeRecord:
    return record


class TestDeferredScan(IsolatedAsyncioTestCase):
    async def _drain_writes(self, q: WriteQueue, off_main: OffMainThread) -> None:
        try:
            while True:
                job = await q.get()
                try:
                    await off_main(job)
                finally:
                    q.task_done()
        except asyncio.CancelledError:
            raise

    async def test_cross_mount_reparent_no_remove_change_for_file_id(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node(mount_id("b"), "b", SERVER_ROOT_ID, is_directory=True),
                    _node("x1", "moved.txt", mount_id("a")),
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return []
                if path == "/vol/b":
                    return [_syno_item("x1", "moved.txt", is_dir=False)]
                return []

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a", "b": "/vol/b"},
                            last_max_ids={"a": 50, "b": 50},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            row = storage.get_node_by_id("x1")
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row.parent_id, mount_id("b"))

            changes, _, _ = storage.get_changes_since(0, max_size=500)
            x1_removes = [c for c in changes if c[0] == "x1" and c[1] is True]
            self.assertEqual(x1_removes, [])
        finally:
            os.unlink(db_path)

    async def test_max_id_prune_preserves_db_subtree(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node("dirF", "F", mount_id("a"), is_directory=True),
                    _node("childC", "c.txt", "dirF"),
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [_syno_item("dirF", "F", is_dir=True, sync_id=10, max_id=10)]
                return []

            async def list_all(_net, folder_id: str) -> list:
                raise AssertionError(f"unexpected list of {folder_id}")

            # Count matches DB (1 child) → folder is safely skipped
            async def list_folder_fn(
                _net, folder_id: str, offset: int, limit: int
            ) -> tuple:
                return [], 1

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder",
                            new=AsyncMock(side_effect=list_folder_fn),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a"},
                            last_max_ids={"a": 100},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertIsNotNone(storage.get_node_by_id("dirF"))
            self.assertIsNotNone(storage.get_node_by_id("childC"))
        finally:
            os.unlink(db_path)

    async def test_initial_scan_enters_folder_with_zero_max_id(self) -> None:
        """Full initial scan (last_max_id=0) must enter folders whose max_id is 0."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [_syno_item("dirF", "F", is_dir=True, sync_id=0, max_id=0)]
                return []

            async def list_all(_net, folder_id: str) -> list:
                if folder_id == "dirF":
                    return [_syno_item("childC", "c.txt", sync_id=0)]
                return []

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a"},
                            last_max_ids={"a": 0},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertIsNotNone(storage.get_node_by_id("dirF"))
            self.assertIsNotNone(storage.get_node_by_id("childC"))
        finally:
            os.unlink(db_path)

    async def test_new_folder_with_stale_max_id_is_force_scanned(self) -> None:
        """A folder absent from DB must be entered even when max_id <= last_max_id.

        Scenario: folder was added while the server was down and other activity
        pushed last_max_id past the folder's max_id before the next scan.
        """
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                ]
            )

            # dirF has max_id=50 which is less than last_max_id=100,
            # but it has never been scanned (not in DB).
            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [_syno_item("dirF", "F", is_dir=True, sync_id=40, max_id=50)]
                return []

            async def list_all(_net, folder_id: str) -> list:
                if folder_id == "dirF":
                    return [_syno_item("childC", "c.txt", sync_id=50)]
                return []

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a"},
                            last_max_ids={"a": 100},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertIsNotNone(storage.get_node_by_id("dirF"))
            self.assertIsNotNone(storage.get_node_by_id("childC"))
        finally:
            os.unlink(db_path)

    async def test_db_folder_with_no_children_is_force_scanned(self) -> None:
        """A folder already in DB but with no DB children must be entered even
        when max_id <= last_max_id (its contents were never successfully scanned)."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            # dirF is in DB but has NO children — it was added at mount level
            # but BFS skipped it in a prior scan.
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node("dirF", "F", mount_id("a"), is_directory=True),
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [_syno_item("dirF", "F", is_dir=True, sync_id=40, max_id=50)]
                return []

            async def list_all(_net, folder_id: str) -> list:
                if folder_id == "dirF":
                    return [_syno_item("childC", "c.txt", sync_id=50)]
                return []

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a"},
                            last_max_ids={"a": 100},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertIsNotNone(storage.get_node_by_id("dirF"))
            self.assertIsNotNone(storage.get_node_by_id("childC"))
        finally:
            os.unlink(db_path)

    async def test_list_failure_preserves_db_subtree(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node("dirF", "F", mount_id("a"), is_directory=True),
                    _node("childC", "c.txt", "dirF"),
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [
                        _syno_item("dirF", "F", is_dir=True, sync_id=200, max_id=200)
                    ]
                return []

            async def list_all(_net, folder_id: str) -> list:
                if folder_id == "dirF":
                    raise OSError("boom")
                return []

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                        # list_folder_all fails by design; avoid ERROR traceback on stderr
                        patch.object(changes_mod._L, "exception", MagicMock()),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a"},
                            last_max_ids={"a": 50},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertIsNotNone(storage.get_node_by_id("dirF"))
            self.assertIsNotNone(storage.get_node_by_id("childC"))
        finally:
            os.unlink(db_path)

    async def test_max_id_prune_detects_deletion_via_count_mismatch(self) -> None:
        """max_id unchanged but a child deleted: count check triggers a full scan."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node("dirF", "F", mount_id("a"), is_directory=True),
                    _node("child1", "a.txt", "dirF"),
                    _node("child2", "b.txt", "dirF"),  # deleted from Synology
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [_syno_item("dirF", "F", is_dir=True, sync_id=10, max_id=10)]
                return []

            # API only has 1 child remaining
            async def list_all(_net, folder_id: str) -> list:
                if folder_id == "dirF":
                    return [_syno_item("child1", "a.txt")]
                return []

            # Count-check: DB has 2 children, API total is 1
            async def list_folder_fn(
                _net, folder_id: str, offset: int, limit: int
            ) -> tuple:
                if folder_id == "dirF":
                    return [], 1
                return [], 0

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder",
                            new=AsyncMock(side_effect=list_folder_fn),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a"},
                            last_max_ids={"a": 100},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertIsNotNone(storage.get_node_by_id("child1"))
            self.assertIsNone(storage.get_node_by_id("child2"))
        finally:
            os.unlink(db_path)

    async def test_failed_mount_does_not_advance_its_max_id(self) -> None:
        """A mount whose top-level list fails returns its input last_max_id unchanged."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node(mount_id("b"), "b", SERVER_ROOT_ID, is_directory=True),
                ]
            )

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    raise OSError("mount a unavailable")
                # mount b has a new item with sync_id=200
                return [_syno_item("f1", "file.txt", sync_id=200)]

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                        patch.object(changes_mod._L, "exception", MagicMock()),
                    ):
                        result = await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a", "b": "/vol/b"},
                            last_max_ids={"a": 100, "b": 100},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            # mount a failed: its value must stay at the input (100), not advance
            self.assertEqual(result["a"], 100)
            # mount b succeeded: its value advances to 200
            self.assertEqual(result["b"], 200)
        finally:
            os.unlink(db_path)

    async def test_per_mount_pruning_thresholds_are_independent(self) -> None:
        """Each mount uses its own last_max_id threshold for BFS pruning."""
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            # Both mounts have a subfolder "dirX" with max_id=80 already in DB.
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("a"), "a", SERVER_ROOT_ID, is_directory=True),
                    _node(mount_id("b"), "b", SERVER_ROOT_ID, is_directory=True),
                    _node("dirX_a", "X", mount_id("a"), is_directory=True),
                    _node("child_a", "c.txt", "dirX_a"),
                    _node("dirX_b", "X", mount_id("b"), is_directory=True),
                    _node("child_b", "c.txt", "dirX_b"),
                ]
            )

            entered: list[str] = []

            async def by_path(_net, path: str) -> list:
                if path == "/vol/a":
                    return [
                        _syno_item("dirX_a", "X", is_dir=True, sync_id=50, max_id=80)
                    ]
                return [_syno_item("dirX_b", "X", is_dir=True, sync_id=50, max_id=80)]

            async def list_all(_net, folder_id: str) -> list:
                entered.append(folder_id)
                return []

            async def list_one(_net, folder_id: str, *, offset: int, limit: int):
                # count-check: return same count as DB so pruning holds
                return [], 1

            q = create_write_queue()
            with ThreadPoolExecutor(2) as pool:
                off_main = OffMainThread(pool)
                drain = asyncio.create_task(self._drain_writes(q, off_main))
                try:
                    with (
                        patch.object(
                            changes_mod,
                            "list_folder_all_by_path",
                            new=AsyncMock(side_effect=by_path),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder_all",
                            new=AsyncMock(side_effect=list_all),
                        ),
                        patch.object(
                            changes_mod,
                            "list_folder",
                            new=AsyncMock(side_effect=list_one),
                        ),
                        patch.object(
                            changes_mod,
                            "enrich_media_before_upsert",
                            new=_noop_enrich,
                        ),
                    ):
                        await scan_all_mounts(
                            network=None,  # type: ignore[arg-type]
                            storage=storage,
                            folders={"a": "/vol/a", "b": "/vol/b"},
                            # mount a threshold=100 > max_id=80 → dirX_a skipped
                            # mount b threshold=50 < max_id=80 → dirX_b entered
                            last_max_ids={"a": 100, "b": 50},
                            volume_map=None,
                            off_main=off_main,
                            write_queue=q,
                        )
                finally:
                    await q.join()
                    drain.cancel()
                    with suppress(asyncio.CancelledError):
                        await drain

            self.assertNotIn("dirX_a", entered)
            self.assertIn("dirX_b", entered)
        finally:
            os.unlink(db_path)
