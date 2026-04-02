"""Integration tests for the client–server HTTP contract.

These tests spin up the real aiohttp handlers against a minimal in-process
app (fake Storage, stub OffMainThread) and make actual HTTP requests via
aiohttp.test_utils.TestClient.  No real Synology connection is needed.
"""

from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from wcpan.drive.core.types import MediaInfo

from wcpan.drive.synology.client._writable import _media_info_to_params
from wcpan.drive.synology.lib import node_record_from_dict
from wcpan.drive.synology.server._handlers import (
    create_upload_session,
    delete_upload_session,
    get_root,
    get_upload_session,
    put_upload_chunk,
    upload_node,
)
from wcpan.drive.synology.server._keys import (
    folders_key,
    network_key,
    off_main_key,
    ready_key,
    storage_key,
    upload_sessions_key,
    volume_map_key,
    write_queue_key,
)
from wcpan.drive.synology.server._upload_session import UploadSessionStore
from wcpan.drive.synology.server._virtual_ids import SERVER_ROOT_ID
from wcpan.drive.synology.server._workers import create_write_queue
from wcpan.drive.synology.types import NodeRecord


_EPOCH = datetime.fromtimestamp(0, UTC)

# Fake Synology file metadata returned by the patched upload_file call.
_FAKE_SYNO_INFO = {
    "file_id": "syno-99",
    "name": "photo.jpg",
    "type": "file",
    "content_type": "image",
    "size": 10,
    "created_time": 1_000_000,
    "modified_time": 1_000_000,
    "hash": "abc123",
}

_UPLOAD_FILE_PATH = "wcpan.drive.synology.server._handlers.synology_files.upload_file"


class _FakeOffMain:
    """Synchronous passthrough — runs callables directly on the event loop thread."""

    async def __call__(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def untimed(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


class _FakeStorage:
    def __init__(self) -> None:
        self._nodes: dict[str, NodeRecord] = {}

    def get_node_by_id(self, node_id: str) -> NodeRecord | None:
        return self._nodes.get(node_id)

    def upsert_node_and_emit_change(self, record: NodeRecord) -> None:
        self._nodes[record.node_id] = record


def _make_app(
    storage: _FakeStorage, session_store: UploadSessionStore
) -> web.Application:
    app = web.Application()
    app[ready_key] = True
    app[storage_key] = storage
    app[off_main_key] = _FakeOffMain()
    app[write_queue_key] = create_write_queue()
    app[upload_sessions_key] = session_store
    app[folders_key] = {}
    app[volume_map_key] = None
    app[network_key] = MagicMock()

    app.router.add_get("/api/v1/root", get_root)
    app.router.add_post("/api/v1/nodes/{parent_id}/upload", upload_node)
    app.router.add_post(
        "/api/v1/nodes/{parent_id}/upload-session", create_upload_session
    )
    app.router.add_put("/api/v1/upload-sessions/{session_id}", put_upload_chunk)
    app.router.add_get("/api/v1/upload-sessions/{session_id}", get_upload_session)
    app.router.add_delete("/api/v1/upload-sessions/{session_id}", delete_upload_session)
    return app


class TestMediaInfoContract(IsolatedAsyncioTestCase):
    """Client MediaInfo encoding must be correctly decoded by the server."""

    def setUp(self) -> None:
        self._session_store = UploadSessionStore()

    def tearDown(self) -> None:
        self._session_store.close_all()

    async def test_upload_session_carries_media_info(self) -> None:
        """MediaInfo dimensions and flags survive the upload-session protocol."""
        storage = _FakeStorage()
        app = _make_app(storage, self._session_store)

        media = MediaInfo(
            is_image=True,
            is_video=False,
            width=1920,
            height=1080,
            ms_duration=0,
        )
        params = _media_info_to_params(media)

        with patch(
            _UPLOAD_FILE_PATH, new_callable=AsyncMock, return_value=_FAKE_SYNO_INFO
        ):
            async with TestClient(TestServer(app)) as client:
                # Step 1 — create session
                resp = await client.post(
                    "/api/v1/nodes/parent-a/upload-session",
                    params={"name": "photo.jpg", "size": "10", **params},
                )
                self.assertEqual(resp.status, 201)
                body = await resp.json()
                session_id = body["session_id"]

                # Step 2 — send the single chunk; server finalises and returns 201
                resp2 = await client.put(
                    f"/api/v1/upload-sessions/{session_id}",
                    data=b"x" * 10,
                    headers={"Content-Range": "bytes 0-9/10"},
                )
                self.assertEqual(resp2.status, 201)
                record = node_record_from_dict(await resp2.json())

        self.assertTrue(record.is_image)
        self.assertFalse(record.is_video)
        self.assertEqual(record.width, 1920)
        self.assertEqual(record.height, 1080)

    async def test_direct_upload_carries_media_info(self) -> None:
        """MediaInfo is applied when using the direct (non-session) upload endpoint."""
        storage = _FakeStorage()
        app = _make_app(storage, self._session_store)

        media = MediaInfo(
            is_image=True,
            is_video=False,
            width=640,
            height=480,
            ms_duration=0,
        )
        params = _media_info_to_params(media)

        with patch(
            _UPLOAD_FILE_PATH, new_callable=AsyncMock, return_value=_FAKE_SYNO_INFO
        ):
            async with TestClient(TestServer(app)) as client:
                resp = await client.post(
                    "/api/v1/nodes/parent-a/upload",
                    data=b"x" * 10,
                    params={"name": "photo.jpg", **params},
                )
                self.assertEqual(resp.status, 201)
                record = node_record_from_dict(await resp.json())

        self.assertTrue(record.is_image)
        self.assertEqual(record.width, 640)
        self.assertEqual(record.height, 480)

    async def test_video_media_info_roundtrip(self) -> None:
        """is_video and ms_duration survive the upload-session protocol."""
        storage = _FakeStorage()
        syno_info = {**_FAKE_SYNO_INFO, "content_type": "video", "name": "clip.mp4"}
        app = _make_app(storage, self._session_store)

        media = MediaInfo(
            is_image=False,
            is_video=True,
            width=1280,
            height=720,
            ms_duration=30_000,
        )
        params = _media_info_to_params(media)

        with patch(_UPLOAD_FILE_PATH, new_callable=AsyncMock, return_value=syno_info):
            async with TestClient(TestServer(app)) as client:
                resp = await client.post(
                    "/api/v1/nodes/parent-a/upload-session",
                    params={"name": "clip.mp4", "size": "10", **params},
                )
                self.assertEqual(resp.status, 201)
                session_id = (await resp.json())["session_id"]

                resp2 = await client.put(
                    f"/api/v1/upload-sessions/{session_id}",
                    data=b"v" * 10,
                    headers={"Content-Range": "bytes 0-9/10"},
                )
                self.assertEqual(resp2.status, 201)
                record = node_record_from_dict(await resp2.json())

        self.assertFalse(record.is_image)
        self.assertTrue(record.is_video)
        self.assertEqual(record.width, 1280)
        self.assertEqual(record.height, 720)
        self.assertEqual(record.ms_duration, 30_000)


class TestNodeRecordRoundTrip(IsolatedAsyncioTestCase):
    """NodeRecord JSON serialization must survive an HTTP round-trip."""

    def setUp(self) -> None:
        self._session_store = UploadSessionStore()

    def tearDown(self) -> None:
        self._session_store.close_all()

    async def test_get_root_datetime_timezone_preserved(self) -> None:
        """ctime/mtime timezone info is preserved through server JSON serialization."""
        storage = _FakeStorage()
        root = NodeRecord(
            node_id=SERVER_ROOT_ID,
            parent_id=None,
            name="root",
            is_directory=True,
            ctime=_EPOCH,
            mtime=_EPOCH,
            mime_type="application/x-directory",
            hash="",
            size=0,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        )
        storage._nodes[SERVER_ROOT_ID] = root
        app = _make_app(storage, self._session_store)

        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/v1/root")
            self.assertEqual(resp.status, 200)
            record = node_record_from_dict(await resp.json())

        self.assertEqual(record.node_id, SERVER_ROOT_ID)
        self.assertIsNotNone(record.ctime.tzinfo)
        self.assertIsNotNone(record.mtime.tzinfo)
        self.assertEqual(record.ctime, _EPOCH)
        self.assertEqual(record.mtime, _EPOCH)


class TestUploadSessionProtocol(IsolatedAsyncioTestCase):
    """Multi-step upload session protocol: create → chunk → finalise."""

    def setUp(self) -> None:
        self._session_store = UploadSessionStore()

    def tearDown(self) -> None:
        self._session_store.close_all()

    async def test_chunked_upload_returns_node_record(self) -> None:
        """PUT with full Content-Range returns 201 with a parseable NodeRecord."""
        storage = _FakeStorage()
        app = _make_app(storage, self._session_store)

        with patch(
            _UPLOAD_FILE_PATH, new_callable=AsyncMock, return_value=_FAKE_SYNO_INFO
        ):
            async with TestClient(TestServer(app)) as client:
                resp = await client.post(
                    "/api/v1/nodes/parent-a/upload-session",
                    params={"name": "photo.jpg", "size": "10"},
                )
                self.assertEqual(resp.status, 201)
                session_id = (await resp.json())["session_id"]

                resp2 = await client.put(
                    f"/api/v1/upload-sessions/{session_id}",
                    data=b"y" * 10,
                    headers={"Content-Range": "bytes 0-9/10"},
                )
                self.assertEqual(resp2.status, 201)
                record = node_record_from_dict(await resp2.json())

        self.assertEqual(record.node_id, "syno-99")
        self.assertEqual(record.name, "photo.jpg")

    async def test_partial_chunk_returns_200_with_received(self) -> None:
        """PUT with a partial Content-Range returns 200 and the byte count received."""
        storage = _FakeStorage()
        app = _make_app(storage, self._session_store)

        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/v1/nodes/parent-a/upload-session",
                params={"name": "big.bin", "size": "20"},
            )
            self.assertEqual(resp.status, 201)
            session_id = (await resp.json())["session_id"]

            # First chunk (bytes 0–9 of a 20-byte file)
            resp2 = await client.put(
                f"/api/v1/upload-sessions/{session_id}",
                data=b"a" * 10,
                headers={"Content-Range": "bytes 0-9/20"},
            )
            self.assertEqual(resp2.status, 200)
            body = await resp2.json()
            self.assertEqual(body["received"], 10)

    async def test_session_not_found_returns_404(self) -> None:
        storage = _FakeStorage()
        app = _make_app(storage, self._session_store)

        async with TestClient(TestServer(app)) as client:
            resp = await client.put(
                "/api/v1/upload-sessions/no-such-session",
                data=b"x",
                headers={"Content-Range": "bytes 0-0/1"},
            )
            self.assertEqual(resp.status, 404)
