"""Tests for Synology file API helpers."""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology.server._api.files import (
    file_id_path,
    get_file_metadata_by_id,
    list_children_for_parent,
    list_folder,
    list_folder_all,
)
from wcpan.drive.synology.server._network import Network


def _item(fid: str, name: str) -> dict:
    return {
        "file_id": fid,
        "parent_id": "p",
        "name": name,
        "type": "file",
        "content_type": "file",
        "size": 0,
        "created_time": 0,
        "modified_time": 0,
        "sync_id": 1,
    }


def _fetch_cm(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json = AsyncMock(return_value=payload)
    response.raise_for_status = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestFileIdPath(IsolatedAsyncioTestCase):
    def test_formats_id_prefix(self):
        # given
        fid = "synology-node-7"
        # when
        result = file_id_path(fid)
        # then
        self.assertEqual(result, "id:synology-node-7")


class TestListFolder(IsolatedAsyncioTestCase):
    async def test_returns_items_and_total(self):
        # given
        network = MagicMock(spec=Network)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        payload = {
            "data": {
                "items": [_item("1", "a.txt")],
                "total": 1,
            }
        }
        network.fetch = MagicMock(return_value=_fetch_cm(payload))
        # when
        items, total = await list_folder(network, "folder-z")
        # then
        self.assertEqual(total, 1)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["file_id"], "1")


class TestListFolderAll(IsolatedAsyncioTestCase):
    async def test_paginates_until_exhausted(self):
        # given
        network = MagicMock(spec=Network)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        page1 = {
            "data": {
                "items": [_item("1", "a"), _item("2", "b")],
                "total": 3,
            }
        }
        page2 = {
            "data": {
                "items": [_item("3", "c")],
                "total": 3,
            }
        }
        network.fetch = MagicMock(
            side_effect=[_fetch_cm(page1), _fetch_cm(page2)],
        )
        # when
        all_items = await list_folder_all(network, "root-id", page_size=2)
        # then
        self.assertEqual(len(all_items), 3)
        self.assertEqual({i["file_id"] for i in all_items}, {"1", "2", "3"})
        self.assertEqual(network.fetch.call_count, 2)


def _get_payload(
    *,
    fid: str = "99",
    hash_: str = "deadbeef",
    size: int = 10,
    mtime: int = 100,
) -> dict:
    return {
        "success": True,
        "data": {
            "file_id": fid,
            "parent_id": "p1",
            "name": "x.bin",
            "type": "file",
            "content_type": "file",
            "hash": hash_,
            "size": size,
            "created_time": 1,
            "modified_time": mtime,
            "sync_id": 42,
        },
    }


class TestGetFileMetadataById(IsolatedAsyncioTestCase):
    async def test_get_uses_path_query(self) -> None:
        network = MagicMock(spec=Network)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        network.fetch = MagicMock(return_value=_fetch_cm(_get_payload()))
        # when
        info = await get_file_metadata_by_id(network, "99")
        # then
        self.assertIsNotNone(info)
        assert info is not None
        self.assertEqual(info["file_id"], "99")
        self.assertEqual(info["hash"], "deadbeef")
        network.fetch.assert_called_once()
        args, kwargs = network.fetch.call_args
        self.assertEqual(args[0], "GET")
        self.assertIn("/files", args[1])
        self.assertEqual(kwargs.get("params"), {"path": "id:99"})

    async def test_unsuccessful_returns_none(self) -> None:
        network = MagicMock(spec=Network)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        network.fetch = MagicMock(
            return_value=_fetch_cm({"success": False, "error": {}}),
        )
        info = await get_file_metadata_by_id(network, "99")
        self.assertIsNone(info)


class TestListChildrenForParent(IsolatedAsyncioTestCase):
    async def test_mount_parent_lists_by_synology_path(self) -> None:
        network = MagicMock(spec=Network)
        folders = {"docs": "/volume1/docs"}
        expected = [_item("1", "a.txt")]
        with patch(
            "wcpan.drive.synology.server._api.files.list_folder_all_by_path",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_path:
            items = await list_children_for_parent(network, "_docs", folders)
        mock_path.assert_awaited_once_with(network, "/volume1/docs")
        self.assertEqual(items, expected)

    async def test_real_parent_lists_by_id(self) -> None:
        network = MagicMock(spec=Network)
        folders = {"docs": "/volume1/docs"}
        expected = [_item("2", "b.txt")]
        with patch(
            "wcpan.drive.synology.server._api.files.list_folder_all",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_id:
            items = await list_children_for_parent(network, "real-folder-id", folders)
        mock_id.assert_awaited_once_with(network, "real-folder-id")
        self.assertEqual(items, expected)
