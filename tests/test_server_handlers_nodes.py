"""Tests for node CRUD, download, and single-shot upload handlers."""

import asyncio
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from wcpan.drive.synology._lib import node_record_from_dict
from wcpan.drive.synology._server.handlers.nodes import (
    create_node,
    delete_node,
    download_node,
    get_node,
    update_node,
    upload_node,
)
from wcpan.drive.synology._server.keys import (
    CHANGE_SERVICE_KEY,
    NETWORK_KEY,
    OFF_MAIN_KEY,
    READY_KEY,
    STORAGE_KEY,
    SYNOLOGY_PATH_KEY,
    UPLOAD_SESSIONS_KEY,
    WRITE_QUEUE_KEY,
)
from wcpan.drive.synology._server.lib.mounts import MountRegistry
from wcpan.drive.synology._server.services.paths import SynologyPathService
from wcpan.drive.synology._server.services.sync import NodeSyncService
from wcpan.drive.synology._server.services.upload import UploadSessionService
from wcpan.drive.synology._server.workers import create_write_queue
from wcpan.drive.synology.exceptions import (
    SynologyUploadConflictError,
    SynologyUploadError,
)
from wcpan.drive.synology.types import NodeRecord


_EPOCH = datetime.fromtimestamp(0, UTC)

_FAKE_SYNO_INFO = {
    "file_id": "new-dir",
    "name": "docs",
    "type": "dir",
    "content_type": "dir",
    "size": 0,
    "created_time": 1_000_000,
    "modified_time": 1_000_000,
    "sync_id": 1,
}

_PATCH_CREATE = (
    "wcpan.drive.synology._server.handlers.nodes.synology_files.create_folder"
)
_PATCH_RENAME = "wcpan.drive.synology._server.handlers.nodes.synology_files.rename_file"
_PATCH_MOVE = "wcpan.drive.synology._server.handlers.nodes.synology_files.move_file"
_PATCH_DELETE = "wcpan.drive.synology._server.handlers.nodes.synology_files.delete_file"
_PATCH_UPLOAD = "wcpan.drive.synology._server.handlers.nodes.synology_files.upload_file"
_PATCH_DOWNLOAD = (
    "wcpan.drive.synology._server.handlers.nodes.synology_files.download_file"
)


class _FakeOffMain:
    async def __call__(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def untimed(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


class _FakeStorage:
    def __init__(self) -> None:
        self._nodes: dict[str, NodeRecord] = {}

    async def get_node_by_id(self, node_id: str) -> NodeRecord | None:
        return self._nodes.get(node_id)

    async def upsert_node_and_emit_change(self, record: NodeRecord) -> None:
        self._nodes[record.node_id] = record

    async def delete_subtree_and_emit_changes(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)


def _make_node(
    node_id: str = "n1",
    parent_id: str = "p1",
    name: str = "test.txt",
    *,
    is_directory: bool = False,
    mime_type: str = "text/plain",
    size: int = 100,
) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name=name,
        is_directory=is_directory,
        ctime=_EPOCH,
        mtime=_EPOCH,
        mime_type=mime_type,
        hash="abc",
        size=size,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


def _make_app(storage: _FakeStorage) -> web.Application:
    app = web.Application()
    off_main = _FakeOffMain()
    wq = create_write_queue()
    tmp = tempfile.mkdtemp(prefix="wcpan_test_")
    app[READY_KEY] = True
    app[STORAGE_KEY] = storage
    app[OFF_MAIN_KEY] = off_main
    app[WRITE_QUEUE_KEY] = wq
    app[UPLOAD_SESSIONS_KEY] = UploadSessionService(tmp_dir=Path(tmp))
    app[SYNOLOGY_PATH_KEY] = SynologyPathService(MountRegistry({}, {}))
    app[CHANGE_SERVICE_KEY] = NodeSyncService(
        storage, wq, off_main, {}, {}, metadata_queue=asyncio.Queue()
    )  # type: ignore[arg-type]
    app[NETWORK_KEY] = MagicMock()

    app.router.add_get("/api/v1/nodes/{id}", get_node)
    app.router.add_get("/api/v1/nodes/{id}/download", download_node)
    app.router.add_post("/api/v1/nodes", create_node)
    app.router.add_patch("/api/v1/nodes/{id}", update_node)
    app.router.add_delete("/api/v1/nodes/{id}", delete_node)
    app.router.add_post("/api/v1/nodes/{parent_id}/upload", upload_node)
    return app


# ---------------------------------------------------------------------------
# get_node
# ---------------------------------------------------------------------------


class TestGetNode(IsolatedAsyncioTestCase):
    async def test_returns_node(self):
        storage = _FakeStorage()
        node = _make_node()
        storage._nodes["n1"] = node
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/v1/nodes/n1")
            self.assertEqual(resp.status, 200)
            record = node_record_from_dict(await resp.json())
        self.assertEqual(record.node_id, "n1")
        self.assertEqual(record.name, "test.txt")

    async def test_not_found(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/v1/nodes/missing")
            self.assertEqual(resp.status, 404)


# ---------------------------------------------------------------------------
# create_node (success path)
# ---------------------------------------------------------------------------


class TestCreateNode(IsolatedAsyncioTestCase):
    async def test_success(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        with patch(_PATCH_CREATE, new_callable=AsyncMock, return_value=_FAKE_SYNO_INFO):
            async with TestClient(TestServer(app)) as client:
                resp = await client.post(
                    "/api/v1/nodes",
                    json={"name": "docs", "parent_id": "p1"},
                )
                self.assertEqual(resp.status, 201)
                record = node_record_from_dict(await resp.json())
        self.assertEqual(record.node_id, "new-dir")
        self.assertTrue(record.is_directory)

    async def test_missing_name(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/v1/nodes",
                json={"parent_id": "p1"},
            )
            self.assertEqual(resp.status, 400)

    async def test_missing_parent_id(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/v1/nodes",
                json={"name": "docs"},
            )
            self.assertEqual(resp.status, 400)


# ---------------------------------------------------------------------------
# update_node
# ---------------------------------------------------------------------------


class TestUpdateNode(IsolatedAsyncioTestCase):
    async def test_rename(self):
        storage = _FakeStorage()
        storage._nodes["n1"] = _make_node()
        app = _make_app(storage)
        rename_result = {
            "name": "renamed.txt",
            "created_time": 1000,
            "modified_time": 2000,
            "hash": "abc",
            "size": 100,
        }
        with patch(_PATCH_RENAME, new_callable=AsyncMock, return_value=rename_result):
            async with TestClient(TestServer(app)) as client:
                resp = await client.patch(
                    "/api/v1/nodes/n1",
                    json={"name": "renamed.txt"},
                )
                self.assertEqual(resp.status, 200)
                record = node_record_from_dict(await resp.json())
        self.assertEqual(record.name, "renamed.txt")

    async def test_move(self):
        storage = _FakeStorage()
        storage._nodes["n1"] = _make_node()
        app = _make_app(storage)
        with patch(_PATCH_MOVE, new_callable=AsyncMock):
            async with TestClient(TestServer(app)) as client:
                resp = await client.patch(
                    "/api/v1/nodes/n1",
                    json={"parent_id": "new-parent"},
                )
                self.assertEqual(resp.status, 200)
                record = node_record_from_dict(await resp.json())
        self.assertEqual(record.parent_id, "new-parent")

    async def test_rename_conflict(self):
        storage = _FakeStorage()
        storage._nodes["n1"] = _make_node()
        app = _make_app(storage)
        with patch(
            _PATCH_RENAME,
            new_callable=AsyncMock,
            side_effect=SynologyUploadConflictError("exists", file_name="x"),
        ):
            async with TestClient(TestServer(app)) as client:
                resp = await client.patch(
                    "/api/v1/nodes/n1",
                    json={"name": "conflict.txt"},
                )
                self.assertEqual(resp.status, 409)

    async def test_virtual_node_forbidden(self):
        storage = _FakeStorage()
        storage._nodes["_root"] = _make_node(node_id="_root")
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.patch(
                "/api/v1/nodes/_root",
                json={"name": "new"},
            )
            self.assertEqual(resp.status, 403)

    async def test_not_found(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.patch(
                "/api/v1/nodes/missing",
                json={"name": "new"},
            )
            self.assertEqual(resp.status, 404)

    async def test_move_error(self):
        storage = _FakeStorage()
        storage._nodes["n1"] = _make_node()
        app = _make_app(storage)
        with patch(
            _PATCH_MOVE,
            new_callable=AsyncMock,
            side_effect=Exception("move failed"),
        ):
            async with TestClient(TestServer(app)) as client:
                resp = await client.patch(
                    "/api/v1/nodes/n1",
                    json={"parent_id": "bad"},
                )
                self.assertEqual(resp.status, 503)


# ---------------------------------------------------------------------------
# delete_node
# ---------------------------------------------------------------------------


class TestDeleteNode(IsolatedAsyncioTestCase):
    async def test_success(self):
        storage = _FakeStorage()
        storage._nodes["n1"] = _make_node()
        app = _make_app(storage)
        with patch(_PATCH_DELETE, new_callable=AsyncMock):
            async with TestClient(TestServer(app)) as client:
                resp = await client.delete("/api/v1/nodes/n1")
                self.assertEqual(resp.status, 204)

    async def test_not_found(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.delete("/api/v1/nodes/missing")
            self.assertEqual(resp.status, 404)

    async def test_virtual_node_forbidden(self):
        storage = _FakeStorage()
        storage._nodes["_mount"] = _make_node(node_id="_mount")
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.delete("/api/v1/nodes/_mount")
            self.assertEqual(resp.status, 403)


# ---------------------------------------------------------------------------
# upload_node (success path)
# ---------------------------------------------------------------------------


class TestUploadNode(IsolatedAsyncioTestCase):
    async def test_success(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        upload_info = {
            "file_id": "uploaded-1",
            "name": "data.bin",
            "type": "file",
            "content_type": "file",
            "size": 5,
            "created_time": 1000,
            "modified_time": 1000,
            "sync_id": 1,
        }
        with patch(_PATCH_UPLOAD, new_callable=AsyncMock, return_value=upload_info):
            async with TestClient(TestServer(app)) as client:
                resp = await client.post(
                    "/api/v1/nodes/p1/upload",
                    data=b"hello",
                    params={"name": "data.bin"},
                )
                self.assertEqual(resp.status, 201)
                record = node_record_from_dict(await resp.json())
        self.assertEqual(record.node_id, "uploaded-1")
        self.assertEqual(record.name, "data.bin")

    async def test_missing_name(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/v1/nodes/p1/upload",
                data=b"hello",
            )
            self.assertEqual(resp.status, 400)

    async def test_upload_to_virtual_root_forbidden(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/v1/nodes/_/upload",
                data=b"hello",
                params={"name": "x.bin"},
            )
            self.assertEqual(resp.status, 403)

    async def test_upload_error(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        with patch(
            _PATCH_UPLOAD,
            new_callable=AsyncMock,
            side_effect=SynologyUploadError("fail", file_name="x"),
        ):
            async with TestClient(TestServer(app)) as client:
                resp = await client.post(
                    "/api/v1/nodes/p1/upload",
                    data=b"hello",
                    params={"name": "x.bin"},
                )
                self.assertEqual(resp.status, 503)


# ---------------------------------------------------------------------------
# download_node
# ---------------------------------------------------------------------------


class TestDownloadNode(IsolatedAsyncioTestCase):
    async def test_not_found(self):
        storage = _FakeStorage()
        app = _make_app(storage)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/v1/nodes/missing/download")
            self.assertEqual(resp.status, 404)
