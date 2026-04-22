"""Tests for Synology WebStation file API helpers."""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from wcpan.drive.synology._server.api.webstation.files import upload_file
from wcpan.drive.synology._server.api.webstation.network import (
    WebStationNetworkService,
)
from wcpan.drive.synology._server.types import SynologyFileId
from wcpan.drive.synology.exceptions import (
    SynologyApiError,
    SynologyNetworkError,
    SynologyUploadConflictError,
    SynologyUploadError,
)


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


class TestUploadFile(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock(spec=WebStationNetworkService)
        network.upload = AsyncMock(return_value=_item("uploaded", "test.bin"))

        async def _chunks():
            yield b"hello"

        info = await upload_file(
            SynologyFileId(file_id="parent"),
            "test.bin",
            _chunks(),
            network=network,
        )
        self.assertEqual(info["file_id"], "uploaded")

    async def test_conflict_raises(self):
        network = MagicMock(spec=WebStationNetworkService)
        network.upload = AsyncMock(
            side_effect=SynologyApiError("conflict", error_code=1022)
        )

        async def _chunks():
            yield b"x"

        with self.assertRaises(SynologyUploadConflictError):
            await upload_file(
                SynologyFileId(file_id="p"),
                "dup.bin",
                _chunks(),
                network=network,
            )

    async def test_api_error_raises_upload_error(self):
        network = MagicMock(spec=WebStationNetworkService)
        network.upload = AsyncMock(side_effect=SynologyApiError("bad", error_code=108))

        async def _chunks():
            yield b"x"

        with self.assertRaises(SynologyUploadError):
            await upload_file(
                SynologyFileId(file_id="p"),
                "bad.bin",
                _chunks(),
                network=network,
            )

    async def test_network_error_raises_upload_error(self):
        network = MagicMock(spec=WebStationNetworkService)
        network.upload = AsyncMock(side_effect=SynologyNetworkError("connection reset"))

        async def _chunks():
            yield b"x"

        with self.assertRaises(SynologyUploadError):
            await upload_file(
                SynologyFileId(file_id="p"),
                "bad.bin",
                _chunks(),
                network=network,
            )
