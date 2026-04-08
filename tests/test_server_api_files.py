"""Tests for Synology file API helpers."""

from pathlib import PurePosixPath
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology._server.api.files import (
    _multipart_body,
    create_folder,
    delete_file,
    download_file,
    file_id_path,
    get_file_metadata_by_id,
    list_folder,
    list_folder_all,
    list_folder_all_by_path,
    list_folder_by_path,
    move_file,
    rename_file,
    upload_file,
)
from wcpan.drive.synology._server.api.types import (
    _int_field,
    synology_file_info_from_api_dict,
)
from wcpan.drive.synology._server.lib.mounts import MountRegistry
from wcpan.drive.synology._server.services.network import NetworkService
from wcpan.drive.synology._server.services.paths import SynologyPathService
from wcpan.drive.synology._server.types import SynologyIdRef, SynologyPath
from wcpan.drive.synology.exceptions import (
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
        network = MagicMock(spec=NetworkService)
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
        network = MagicMock(spec=NetworkService)
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
        network = MagicMock(spec=NetworkService)
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
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        network.fetch = MagicMock(
            return_value=_fetch_cm({"success": False, "error": {}}),
        )
        info = await get_file_metadata_by_id(network, "99")
        self.assertIsNone(info)


class TestListChildren(IsolatedAsyncioTestCase):
    async def test_mount_parent_lists_by_synology_path(self) -> None:
        network = MagicMock(spec=NetworkService)
        mounts = {"docs": "/volume1/docs"}
        svc = SynologyPathService(MountRegistry(mounts, {}))
        expected = [_item("1", "a.txt")]
        with patch(
            "wcpan.drive.synology._server.services.paths.list_folder_all_by_path",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_path:
            items = await svc.list_children(network, "_docs")
        mock_path.assert_awaited_once_with(network, "/volume1/docs")
        self.assertEqual(items, expected)

    async def test_real_parent_lists_by_id(self) -> None:
        network = MagicMock(spec=NetworkService)
        mounts = {"docs": "/volume1/docs"}
        svc = SynologyPathService(MountRegistry(mounts, {}))
        expected = [_item("2", "b.txt")]
        with patch(
            "wcpan.drive.synology._server.services.paths.list_folder_all",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_id:
            items = await svc.list_children(network, "real-folder-id")
        mock_id.assert_awaited_once_with(network, "real-folder-id")
        self.assertEqual(items, expected)


# ---------------------------------------------------------------------------
# _int_field
# ---------------------------------------------------------------------------


class TestIntField(IsolatedAsyncioTestCase):
    def test_int_value(self):
        self.assertEqual(_int_field({"k": 42}, "k"), 42)

    def test_bool_value(self):
        self.assertEqual(_int_field({"k": True}, "k"), 1)
        self.assertEqual(_int_field({"k": False}, "k"), 0)

    def test_string_digit(self):
        self.assertEqual(_int_field({"k": "123"}, "k"), 123)

    def test_non_numeric_string(self):
        self.assertEqual(_int_field({"k": "abc"}, "k"), 0)

    def test_missing_key(self):
        self.assertEqual(_int_field({}, "k"), 0)

    def test_missing_key_with_default(self):
        self.assertEqual(_int_field({}, "k", 99), 99)

    def test_none_value(self):
        self.assertEqual(_int_field({"k": None}, "k", 5), 5)


# ---------------------------------------------------------------------------
# synology_file_info_from_api_dict
# ---------------------------------------------------------------------------


class TestSynologyFileInfoFromApiDict(IsolatedAsyncioTestCase):
    def test_minimal_fields(self):
        raw = {
            "file_id": "99",
            "parent_id": "p1",
            "name": "f.txt",
            "type": "file",
            "content_type": "file",
            "size": 10,
            "created_time": 1,
            "modified_time": 2,
            "sync_id": 3,
        }
        info = synology_file_info_from_api_dict(raw)
        self.assertEqual(info["file_id"], "99")
        self.assertEqual(info["size"], 10)
        self.assertNotIn("hash", info)
        self.assertNotIn("max_id", info)
        self.assertNotIn("removed", info)
        self.assertNotIn("image_metadata", info)

    def test_optional_fields(self):
        raw = {
            "file_id": "1",
            "parent_id": "",
            "name": "x",
            "type": "file",
            "content_type": "image",
            "size": 5,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 0,
            "hash": "deadbeef",
            "max_id": 100,
            "removed": True,
            "image_metadata": {"width": 640},
        }
        info = synology_file_info_from_api_dict(raw)
        self.assertEqual(info["hash"], "deadbeef")
        self.assertEqual(info["max_id"], 100)
        self.assertTrue(info["removed"])
        self.assertEqual(info["image_metadata"], {"width": 640})

    def test_null_hash_excluded(self):
        raw = {
            "file_id": "1",
            "size": 0,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 0,
            "hash": None,
        }
        info = synology_file_info_from_api_dict(raw)
        self.assertNotIn("hash", info)

    def test_null_max_id_excluded(self):
        raw = {
            "file_id": "1",
            "size": 0,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 0,
            "max_id": None,
        }
        info = synology_file_info_from_api_dict(raw)
        self.assertNotIn("max_id", info)


# ---------------------------------------------------------------------------
# list_folder_by_path / list_folder_all_by_path
# ---------------------------------------------------------------------------


class TestListFolderByPath(IsolatedAsyncioTestCase):
    async def test_uses_string_path(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        payload = {"data": {"items": [_item("1", "a")], "total": 1}}
        network.fetch = MagicMock(return_value=_fetch_cm(payload))
        syno_path = SynologyPath(PurePosixPath("/team-folders/docs"))
        items, total = await list_folder_by_path(network, syno_path)
        self.assertEqual(total, 1)
        self.assertEqual(len(items), 1)


class TestListFolderAllByPath(IsolatedAsyncioTestCase):
    async def test_paginates(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        page1 = {"data": {"items": [_item("1", "a")], "total": 2}}
        page2 = {"data": {"items": [_item("2", "b")], "total": 2}}
        network.fetch = MagicMock(side_effect=[_fetch_cm(page1), _fetch_cm(page2)])
        syno_path = SynologyPath(PurePosixPath("/team-folders/docs"))
        items = await list_folder_all_by_path(network, syno_path, page_size=1)
        self.assertEqual(len(items), 2)


# ---------------------------------------------------------------------------
# create_folder
# ---------------------------------------------------------------------------


class TestCreateFolder(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {
            "success": True,
            "data": _item("new-dir", "docs"),
        }
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        ref = SynologyIdRef("id:parent")
        info = await create_folder(network, ref, "docs")
        self.assertEqual(info["file_id"], "new-dir")

    async def test_conflict_raises(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": False, "error": {"code": 1022}}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        with self.assertRaises(SynologyUploadConflictError):
            await create_folder(network, SynologyIdRef("id:p"), "dup")

    async def test_other_error_raises(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": False, "error": {"code": 999}}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        with self.assertRaises(Exception, msg="Failed to create folder"):
            await create_folder(network, SynologyIdRef("id:p"), "bad")

    async def test_missing_data_raises(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": True, "data": "not-a-dict"}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        with self.assertRaises(Exception, msg="missing data"):
            await create_folder(network, SynologyIdRef("id:p"), "x")


# ---------------------------------------------------------------------------
# rename_file
# ---------------------------------------------------------------------------


class TestRenameFile(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": True, "data": {"name": "new.txt"}}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        info = await rename_file(network, "f1", "new.txt")
        self.assertEqual(info["name"], "new.txt")

    async def test_conflict_raises(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": False, "error": {"code": 1022}}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        with self.assertRaises(SynologyUploadConflictError):
            await rename_file(network, "f1", "conflict.txt")


# ---------------------------------------------------------------------------
# move_file
# ---------------------------------------------------------------------------


class TestMoveFile(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": True}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        await move_file(network, "f1", SynologyIdRef("id:new-parent"))

    async def test_failure_raises(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"success": False, "error": {}}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        with self.assertRaises(Exception):
            await move_file(network, "f1", SynologyIdRef("id:bad"))


# ---------------------------------------------------------------------------
# delete_file
# ---------------------------------------------------------------------------


class TestDeleteFile(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        data = {"data": {"async_task_id": "t1"}}
        network.fetch = MagicMock(return_value=_fetch_cm(data))
        await delete_file(network, "f1")
        network.fetch.assert_called_once()


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------


class TestUploadFile(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        result = {
            "success": True,
            "data": _item("uploaded", "test.bin"),
        }
        network.fetch = MagicMock(return_value=_fetch_cm(result))

        async def _chunks():
            yield b"hello"

        info = await upload_file(
            network, SynologyIdRef("id:parent"), "test.bin", _chunks()
        )
        self.assertEqual(info["file_id"], "uploaded")

    async def test_conflict_raises(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        result = {"success": False, "error": {"code": 1022}}
        network.fetch = MagicMock(return_value=_fetch_cm(result))

        async def _chunks():
            yield b"x"

        with self.assertRaises(SynologyUploadConflictError):
            await upload_file(network, SynologyIdRef("id:p"), "dup.bin", _chunks())

    async def test_other_error_raises_upload_error(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        result = {"success": False, "error": {"code": 500}}
        network.fetch = MagicMock(return_value=_fetch_cm(result))

        async def _chunks():
            yield b"x"

        with self.assertRaises(SynologyUploadError):
            await upload_file(network, SynologyIdRef("id:p"), "bad.bin", _chunks())


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------


class TestDownloadFile(IsolatedAsyncioTestCase):
    async def test_without_range(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        response = MagicMock()
        response.raise_for_status = MagicMock()
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=response)
        cm.__aexit__ = AsyncMock(return_value=None)
        network.fetch = MagicMock(return_value=cm)
        async with download_file(network, "f1") as resp:
            self.assertIs(resp, response)
        # No Range header
        call_kw = network.fetch.call_args.kwargs
        self.assertIsNone(call_kw.get("headers"))

    async def test_with_range(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        response = MagicMock()
        response.raise_for_status = MagicMock()
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=response)
        cm.__aexit__ = AsyncMock(return_value=None)
        network.fetch = MagicMock(return_value=cm)
        async with download_file(network, "f1", slice(100, 201)) as resp:
            self.assertIs(resp, response)
        call_kw = network.fetch.call_args.kwargs
        self.assertEqual(call_kw["headers"]["Range"], "bytes=100-200")


# ---------------------------------------------------------------------------
# _multipart_body
# ---------------------------------------------------------------------------


class TestMultipartBody(IsolatedAsyncioTestCase):
    async def test_generates_correct_structure(self):
        async def _data():
            yield b"FILEDATA"

        content_type, body = _multipart_body(
            path="/parent/test.bin",
            file_name="test.bin",
            file_data=_data(),
            file_content_type="application/octet-stream",
        )
        self.assertIn("multipart/form-data", content_type)
        self.assertIn("boundary=", content_type)

        chunks = []
        async for chunk in body:
            chunks.append(chunk)
        full = b"".join(chunks)
        self.assertIn(b'name="path"', full)
        self.assertIn(b"/parent/test.bin", full)
        self.assertIn(b'name="type"', full)
        self.assertIn(b"file", full)
        self.assertIn(b'name="conflict_action"', full)
        self.assertIn(b"stop", full)
        self.assertIn(b'name="file"', full)
        self.assertIn(b"FILEDATA", full)
