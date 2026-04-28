"""Replay-safety tests for storage change paging semantics."""

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology._lib import FOLDER_MIME_TYPE
from wcpan.drive.synology._server.services.off_main import OffMainThreadService
from wcpan.drive.synology._server.services.paths import SERVER_ROOT_ID
from wcpan.drive.synology._server.services.storage import StorageService
from wcpan.drive.synology.types import MirrorMutableId, NodeRecord


_NOW = datetime(2024, 1, 1, tzinfo=UTC)


def _make_node(
    node_id: str,
    *,
    parent_id: str | None = SERVER_ROOT_ID,
    is_directory: bool = False,
) -> NodeRecord:
    return NodeRecord(
        id=node_id,
        parent_id=parent_id,
        name=node_id,
        is_directory=is_directory,
        ctime=_NOW,
        mtime=_NOW,
        mime_type=FOLDER_MIME_TYPE if is_directory else "text/plain",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
        mutable_id=MirrorMutableId(node_id),
    )


def _make_storage(db_path: str, pool: ThreadPoolExecutor) -> StorageService:
    return StorageService(db_path, off_main=OffMainThreadService(pool=pool))


class TestStorageChangesPaging(IsolatedAsyncioTestCase):
    async def test_cursor_advances_by_raw_rows_even_with_same_node_repeats(
        self,
    ) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            with ThreadPoolExecutor() as pool:
                storage = _make_storage(db_path, pool)
                await storage.ensure_schema()
                n1 = _make_node("n1")
                await storage.upsert_node_and_emit_change(n1)
                await storage.upsert_node_and_emit_change(n1)
                await storage.delete_subtree_and_emit_changes("n1")

                page1, cursor1, has_more1 = await storage.get_changes_since(0, 2)
                self.assertEqual(len(page1), 2)
                self.assertTrue(has_more1)
                self.assertTrue(all(change[0] == "n1" for change in page1))

                page2, cursor2, has_more2 = await storage.get_changes_since(cursor1, 2)
                self.assertEqual(len(page2), 1)
                self.assertFalse(has_more2)
                self.assertEqual(page2[0][0], "n1")
                self.assertTrue(page2[0][1])
                self.assertGreater(cursor2, cursor1)
        finally:
            os.unlink(db_path)

    async def test_delete_then_recreate_same_node_is_replayable(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            with ThreadPoolExecutor() as pool:
                storage = _make_storage(db_path, pool)
                await storage.ensure_schema()
                n1 = _make_node("n1")
                await storage.upsert_node_and_emit_change(n1)
                await storage.delete_subtree_and_emit_changes("n1")
                await storage.upsert_node_and_emit_change(n1)

                changes, _, _ = await storage.get_changes_since(0, 10)
                self.assertEqual([c[0] for c in changes], ["n1", "n1", "n1"])
                self.assertEqual([c[1] for c in changes], [False, True, False])
        finally:
            os.unlink(db_path)

    async def test_subtree_delete_emits_parent_and_child_removals(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            with ThreadPoolExecutor() as pool:
                storage = _make_storage(db_path, pool)
                await storage.ensure_schema()
                parent = _make_node("parent", is_directory=True)
                child = _make_node("child", parent_id="parent")
                await storage.bulk_upsert_nodes([parent, child])
                await storage.delete_subtree_and_emit_changes("parent")

                changes, _, _ = await storage.get_changes_since(0, 10)
                removed_ids = {
                    node_id for node_id, is_removed, _ in changes if is_removed
                }
                self.assertEqual(removed_ids, {"parent", "child"})
        finally:
            os.unlink(db_path)
