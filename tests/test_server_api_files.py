"""Tests for Synology file API helpers."""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from wcpan.drive.synology.server._api.files import (
    file_id_path,
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
