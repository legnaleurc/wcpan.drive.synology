"""Tests for cleanup_dangling_nodes, reset_change_history, and backfill."""

import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from wcpan.drive.synology.lib import FOLDER_MIME_TYPE
from wcpan.drive.synology.server._db import (
    Storage,
    cleanup_dangling_nodes,
    list_media_backfill_candidates,
    reset_change_history,
)
from wcpan.drive.synology.server._enricher import (
    backfill_media_metadata,
    enrich_subtree,
)
from wcpan.drive.synology.server._virtual_ids import SERVER_ROOT_ID, mount_id
from wcpan.drive.synology.types import NodeRecord


_NOW = datetime(2024, 1, 1, tzinfo=UTC)


def _make_node(
    node_id: str,
    *,
    parent_id: str | None = SERVER_ROOT_ID,
    name: str = "test.txt",
    is_directory: bool = False,
    is_image: bool = False,
    is_video: bool = False,
    width: int = 0,
    height: int = 0,
) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name=name,
        is_directory=is_directory,
        ctime=_NOW,
        mtime=_NOW,
        mime_type=FOLDER_MIME_TYPE if is_directory else "image/jpeg",
        hash="",
        size=0,
        is_image=is_image,
        is_video=is_video,
        width=width,
        height=height,
        ms_duration=0,
    )


class TestCleanupDanglingNodes(unittest.TestCase):
    def test_no_dangling_nodes_returns_zero(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes([_make_node("node-001")])
            count = cleanup_dangling_nodes(db_path)
            self.assertEqual(count, 0)
            self.assertIsNotNone(storage.get_node_by_id("node-001"))
        finally:
            os.unlink(db_path)

    def test_removes_node_with_broken_parent(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            orphan = _make_node("orphan-001", parent_id="nonexistent-parent")
            storage.bulk_upsert_nodes([orphan])
            count = cleanup_dangling_nodes(db_path)
            self.assertEqual(count, 1)
            self.assertIsNone(storage.get_node_by_id("orphan-001"))
        finally:
            os.unlink(db_path)

    def test_emits_removal_changes_for_dangling_nodes(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            orphan = _make_node("orphan-001", parent_id="nonexistent-parent")
            storage.bulk_upsert_nodes([orphan])
            cleanup_dangling_nodes(db_path)
            changes, _, _ = storage.get_changes_since(0, 1000)
            self.assertEqual(len(changes), 1)
            self.assertTrue(changes[0][1])
            self.assertEqual(changes[0][0], "orphan-001")
        finally:
            os.unlink(db_path)

    def test_keeps_reachable_nodes(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            reachable = _make_node("node-001")
            orphan = _make_node("orphan-001", parent_id="nonexistent-parent")
            storage.bulk_upsert_nodes([reachable, orphan])
            count = cleanup_dangling_nodes(db_path)
            self.assertEqual(count, 1)
            self.assertIsNotNone(storage.get_node_by_id("node-001"))
            self.assertIsNone(storage.get_node_by_id("orphan-001"))
        finally:
            os.unlink(db_path)

    def test_does_not_remove_server_root(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            root = _make_node(
                SERVER_ROOT_ID,
                parent_id=None,
                name="",
                is_directory=True,
            )
            storage.bulk_upsert_nodes([root])
            cleanup_dangling_nodes(db_path)
            self.assertIsNotNone(storage.get_node_by_id(SERVER_ROOT_ID))
        finally:
            os.unlink(db_path)


class TestResetChangeHistory(unittest.TestCase):
    def test_clears_existing_changes(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            node = _make_node("node-001")
            storage.bulk_upsert_nodes([node])
            storage.upsert_node_and_emit_change(node)
            storage.upsert_node_and_emit_change(node)
            storage.upsert_node_and_emit_change(node)
            reset_change_history(db_path)
            changes, _, _ = storage.get_changes_since(0, 1000)
            ids = [c[0] for c in changes]
            self.assertEqual(ids.count("node-001"), 1)
        finally:
            os.unlink(db_path)

    def test_returns_count_of_inserted_records(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes([_make_node("node-001"), _make_node("node-002")])
            count = reset_change_history(db_path)
            self.assertEqual(count, 2)
        finally:
            os.unlink(db_path)

    def test_server_root_not_included_in_changes(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            root = _make_node(
                SERVER_ROOT_ID,
                parent_id=None,
                name="",
                is_directory=True,
            )
            storage.bulk_upsert_nodes([root, _make_node("node-001")])
            reset_change_history(db_path)
            changes, _, _ = storage.get_changes_since(0, 1000)
            ids = [c[0] for c in changes]
            self.assertNotIn(SERVER_ROOT_ID, ids)
        finally:
            os.unlink(db_path)

    def test_all_new_changes_are_updates(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes([_make_node("node-001")])
            reset_change_history(db_path)
            changes, _, _ = storage.get_changes_since(0, 1000)
            self.assertTrue(all(not c[1] for c in changes))
        finally:
            os.unlink(db_path)


class TestListMediaBackfillCandidates(unittest.TestCase):
    def test_empty_when_no_candidates(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            Storage(db_path).ensure_schema()
            self.assertEqual(list_media_backfill_candidates(db_path), [])
        finally:
            os.unlink(db_path)


class TestBackfillMediaMetadata(unittest.TestCase):
    def test_backfill_updates_node_when_probe_succeeds(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        local_root = tempfile.mkdtemp()
        try:
            Path(local_root, "a.jpg").touch()
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _make_node(
                        SERVER_ROOT_ID,
                        parent_id=None,
                        name="",
                        is_directory=True,
                    ),
                    _make_node(
                        mount_id("vol"),
                        parent_id=SERVER_ROOT_ID,
                        name="vol",
                        is_directory=True,
                    ),
                    _make_node(
                        "file-1",
                        parent_id=mount_id("vol"),
                        name="a.jpg",
                        is_image=True,
                    ),
                ]
            )
            folders = {"vol": "/volume1/photos"}
            volume_map = {"/volume1/photos": local_root}
            with patch(
                "wcpan.drive.synology.server._enricher._probe_sync",
                return_value=(640, 480, 0),
            ):
                n = backfill_media_metadata(db_path, folders, volume_map)
            self.assertEqual(n, 1)
            updated = storage.get_node_by_id("file-1")
            assert updated is not None
            self.assertEqual(updated.width, 640)
            self.assertEqual(updated.height, 480)
        finally:
            os.unlink(db_path)
            Path(local_root, "a.jpg").unlink(missing_ok=True)
            os.rmdir(local_root)


class TestEnrichSubtree(unittest.TestCase):
    def _setup(self, db_path: str, local_root: str, *, width: int = 0) -> Storage:
        storage = Storage(db_path)
        storage.ensure_schema()
        storage.bulk_upsert_nodes(
            [
                _make_node(SERVER_ROOT_ID, parent_id=None, name="", is_directory=True),
                _make_node(
                    mount_id("vol"),
                    parent_id=SERVER_ROOT_ID,
                    name="vol",
                    is_directory=True,
                ),
                _make_node(
                    "file-1",
                    parent_id=mount_id("vol"),
                    name="a.jpg",
                    is_image=True,
                    width=width,
                ),
            ]
        )
        return storage

    def test_updates_image_with_zero_dimensions(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        local_root = tempfile.mkdtemp()
        try:
            Path(local_root, "a.jpg").touch()
            storage = self._setup(db_path, local_root)
            folders = {"vol": "/volume1/photos"}
            volume_map = {"/volume1/photos": local_root}
            with patch(
                "wcpan.drive.synology.server._enricher._probe_sync",
                return_value=(1920, 1080, 0),
            ):
                stats = enrich_subtree(db_path, folders, volume_map, mount_id("vol"))
            self.assertEqual(stats["checked"], 1)
            self.assertEqual(stats["updated"], 1)
            self.assertEqual(stats["skipped"], 0)
            node = storage.get_node_by_id("file-1")
            assert node is not None
            self.assertEqual(node.width, 1920)
            self.assertEqual(node.height, 1080)
        finally:
            os.unlink(db_path)
            Path(local_root, "a.jpg").unlink(missing_ok=True)
            os.rmdir(local_root)

    def test_skips_already_enriched(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        local_root = tempfile.mkdtemp()
        try:
            Path(local_root, "a.jpg").touch()
            storage = self._setup(db_path, local_root, width=50)
            folders = {"vol": "/volume1/photos"}
            volume_map = {"/volume1/photos": local_root}
            stats = enrich_subtree(db_path, folders, volume_map, mount_id("vol"))
            self.assertEqual(stats["checked"], 0)
            self.assertEqual(stats["updated"], 0)
            node = storage.get_node_by_id("file-1")
            assert node is not None
            self.assertEqual(node.width, 50)
        finally:
            os.unlink(db_path)
            Path(local_root, "a.jpg").unlink(missing_ok=True)
            os.rmdir(local_root)

    def test_dry_run_does_not_write(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        local_root = tempfile.mkdtemp()
        try:
            Path(local_root, "a.jpg").touch()
            storage = self._setup(db_path, local_root)
            folders = {"vol": "/volume1/photos"}
            volume_map = {"/volume1/photos": local_root}
            with patch(
                "wcpan.drive.synology.server._enricher._probe_sync",
                return_value=(800, 600, 0),
            ):
                stats = enrich_subtree(
                    db_path, folders, volume_map, mount_id("vol"), dry_run=True
                )
            self.assertEqual(stats["updated"], 1)
            node = storage.get_node_by_id("file-1")
            assert node is not None
            self.assertEqual(node.width, 0)
        finally:
            os.unlink(db_path)
            Path(local_root, "a.jpg").unlink(missing_ok=True)
            os.rmdir(local_root)
