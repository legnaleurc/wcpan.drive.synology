"""Tests for API backfill / subtree reconcile."""

import os
import tempfile
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock, patch

from wcpan.drive.synology.lib import FOLDER_MIME_TYPE
from wcpan.drive.synology.server._db import Storage
from wcpan.drive.synology.server._reconcile import (
    reconcile_subtree,
    virtual_path_to_directory_node_id,
)
from wcpan.drive.synology.server._virtual_ids import SERVER_ROOT_ID, mount_id
from wcpan.drive.synology.types import NodeRecord


_TS = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())


def _node(
    node_id: str,
    name: str,
    parent_id: str | None,
    *,
    is_directory: bool = False,
    size: int = 0,
    width: int = 0,
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
        size=size,
        is_image=False,
        is_video=False,
        width=width,
        height=0,
        ms_duration=0,
    )


def _syno_item(
    file_id: str,
    name: str,
    *,
    is_dir: bool = False,
    size: int = 0,
    modified_time: int = _TS,
    created_time: int = _TS,
) -> dict:
    return {
        "file_id": file_id,
        "parent_id": "",
        "name": name,
        "type": "dir" if is_dir else "file",
        "content_type": "dir" if is_dir else "file",
        "size": size,
        "created_time": created_time,
        "modified_time": modified_time,
        "sync_id": 1,
    }


class TestVirtualPathToDirectoryNodeId(TestCase):
    def _storage_with_tree(self) -> Storage:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.addCleanup(os.unlink, db_path)
        storage = Storage(db_path)
        storage.ensure_schema()
        storage.bulk_upsert_nodes(
            [
                _node(SERVER_ROOT_ID, "", None, is_directory=True),
                _node(mount_id("docs"), "docs", SERVER_ROOT_ID, is_directory=True),
                _node("dir-1", "Projects", mount_id("docs"), is_directory=True),
            ]
        )
        return storage

    def test_root_slash(self) -> None:
        storage = self._storage_with_tree()
        self.assertEqual(
            virtual_path_to_directory_node_id(storage, "/"), SERVER_ROOT_ID
        )

    def test_root_empty(self) -> None:
        storage = self._storage_with_tree()
        self.assertEqual(
            virtual_path_to_directory_node_id(storage, "  "), SERVER_ROOT_ID
        )

    def test_mount_only(self) -> None:
        storage = self._storage_with_tree()
        self.assertEqual(
            virtual_path_to_directory_node_id(storage, "/docs"),
            mount_id("docs"),
        )

    def test_nested(self) -> None:
        storage = self._storage_with_tree()
        self.assertEqual(
            virtual_path_to_directory_node_id(storage, "/docs/Projects"),
            "dir-1",
        )

    def test_rejects_dotdot(self) -> None:
        storage = self._storage_with_tree()
        with self.assertRaises(ValueError):
            virtual_path_to_directory_node_id(storage, "/docs/../x")

    def test_missing_segment_raises(self) -> None:
        storage = self._storage_with_tree()
        with self.assertRaises(ValueError):
            virtual_path_to_directory_node_id(storage, "/docs/Nope")


class TestReconcileSubtree(IsolatedAsyncioTestCase):
    async def test_updates_mismatch_and_preserves_width(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("docs"), "docs", SERVER_ROOT_ID, is_directory=True),
                    _node("dir-1", "Projects", mount_id("docs"), is_directory=True),
                    _node("f-1", "x.txt", "dir-1", size=0, width=42),
                ]
            )
            folders = {"docs": "/volume1/docs"}
            network = MagicMock()

            async def _list_children(
                _nw: object, parent_id: str, _fd: dict[str, str]
            ) -> list[dict]:
                if parent_id == "dir-1":
                    return [_syno_item("f-1", "x.txt", size=999)]
                return []

            with patch(
                "wcpan.drive.synology.server._reconcile.list_children_for_parent",
                side_effect=_list_children,
            ):
                stats = await reconcile_subtree(
                    storage, network, folders, "dir-1", dry_run=False
                )

            self.assertEqual(stats["checked"], 1)
            self.assertEqual(stats["updated"], 1)
            self.assertEqual(stats["added"], 0)
            updated = storage.get_node_by_id("f-1")
            assert updated is not None
            self.assertEqual(updated.size, 999)
            self.assertEqual(updated.width, 42)
        finally:
            os.unlink(db_path)

    async def test_dry_run_no_write(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("docs"), "docs", SERVER_ROOT_ID, is_directory=True),
                    _node("dir-1", "Projects", mount_id("docs"), is_directory=True),
                    _node("f-1", "x.txt", "dir-1", size=0),
                ]
            )
            folders = {"docs": "/volume1/docs"}
            network = MagicMock()

            async def _list_children(
                _nw: object, parent_id: str, _fd: dict[str, str]
            ) -> list[dict]:
                if parent_id == "dir-1":
                    return [_syno_item("f-1", "x.txt", size=500)]
                return []

            with patch(
                "wcpan.drive.synology.server._reconcile.list_children_for_parent",
                side_effect=_list_children,
            ):
                stats = await reconcile_subtree(
                    storage, network, folders, "dir-1", dry_run=True
                )

            self.assertEqual(stats["updated"], 1)
            updated = storage.get_node_by_id("f-1")
            assert updated is not None
            self.assertEqual(updated.size, 0)
        finally:
            os.unlink(db_path)

    async def test_adds_missing_node(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("docs"), "docs", SERVER_ROOT_ID, is_directory=True),
                    _node("dir-1", "Projects", mount_id("docs"), is_directory=True),
                ]
            )
            folders = {"docs": "/volume1/docs"}
            network = MagicMock()

            async def _list_children(
                _nw: object, parent_id: str, _fd: dict[str, str]
            ) -> list[dict]:
                if parent_id == "dir-1":
                    return [_syno_item("f-new", "new.txt", size=100)]
                return []

            with patch(
                "wcpan.drive.synology.server._reconcile.list_children_for_parent",
                side_effect=_list_children,
            ):
                stats = await reconcile_subtree(
                    storage, network, folders, "dir-1", dry_run=False
                )

            self.assertEqual(stats["checked"], 1)
            self.assertEqual(stats["added"], 1)
            self.assertEqual(stats["updated"], 0)
            added = storage.get_node_by_id("f-new")
            self.assertIsNotNone(added)
            assert added is not None
            self.assertEqual(added.name, "new.txt")
            self.assertEqual(added.parent_id, "dir-1")
        finally:
            os.unlink(db_path)

    async def test_server_root_queues_mounts(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("docs"), "docs", SERVER_ROOT_ID, is_directory=True),
                ]
            )
            folders = {"docs": "/volume1/docs"}
            network = MagicMock()
            listed: list[str] = []

            async def _list_children(
                _nw: object, parent_id: str, _fd: dict[str, str]
            ) -> list[dict]:
                listed.append(parent_id)
                if parent_id == mount_id("docs"):
                    return [_syno_item("f-1", "readme.txt")]
                return []

            with patch(
                "wcpan.drive.synology.server._reconcile.list_children_for_parent",
                side_effect=_list_children,
            ):
                stats = await reconcile_subtree(
                    storage, network, folders, SERVER_ROOT_ID, dry_run=False
                )

            self.assertIn(mount_id("docs"), listed)
            self.assertEqual(stats["checked"], 1)
            self.assertEqual(stats["added"], 1)
        finally:
            os.unlink(db_path)

    async def test_adds_missing_node_dry_run(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            storage = Storage(db_path)
            storage.ensure_schema()
            storage.bulk_upsert_nodes(
                [
                    _node(SERVER_ROOT_ID, "", None, is_directory=True),
                    _node(mount_id("docs"), "docs", SERVER_ROOT_ID, is_directory=True),
                    _node("dir-1", "Projects", mount_id("docs"), is_directory=True),
                ]
            )
            folders = {"docs": "/volume1/docs"}
            network = MagicMock()

            async def _list_children(
                _nw: object, parent_id: str, _fd: dict[str, str]
            ) -> list[dict]:
                if parent_id == "dir-1":
                    return [_syno_item("f-new", "new.txt")]
                return []

            with patch(
                "wcpan.drive.synology.server._reconcile.list_children_for_parent",
                side_effect=_list_children,
            ):
                stats = await reconcile_subtree(
                    storage, network, folders, "dir-1", dry_run=True
                )

            self.assertEqual(stats["added"], 1)
            self.assertIsNone(storage.get_node_by_id("f-new"))
        finally:
            os.unlink(db_path)
