"""Tests for the four resumable upload session handlers."""

import asyncio
import json
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web

from wcpan.drive.synology._server.handlers.upload import (
    create_upload_session,
    delete_upload_session,
    head_upload_session,
    patch_upload_chunk,
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


class _MockOffMain:
    """Executes functions synchronously for testing."""

    async def __call__(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def untimed(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


def _make_app(store: UploadSessionService) -> dict:
    app: dict = {}
    off_main = _MockOffMain()
    wq: asyncio.Queue = asyncio.Queue()
    storage = MagicMock()
    app[UPLOAD_SESSIONS_KEY] = store
    app[READY_KEY] = True
    app[OFF_MAIN_KEY] = off_main
    app[STORAGE_KEY] = storage
    app[WRITE_QUEUE_KEY] = wq
    app[NETWORK_KEY] = MagicMock()
    app[SYNOLOGY_PATH_KEY] = SynologyPathService(
        MountRegistry({"tmp": "/team-folders/download"}, {})
    )
    app[CHANGE_SERVICE_KEY] = NodeSyncService(
        storage, wq, off_main, {}, {}, metadata_queue=asyncio.Queue()
    )  # type: ignore[arg-type]
    return app


def _async_chunks(data: bytes, chunk_size: int = 65536):
    """Return an async iterable that yields data in chunk_size pieces."""

    async def _gen():
        for i in range(0, max(len(data), 1), chunk_size):
            yield data[i : i + chunk_size]

    return _gen()


def _make_request(
    *,
    app: dict | None = None,
    match_info: dict | None = None,
    headers: dict | None = None,
    body: bytes = b"",
    json_body: dict | None = None,
) -> MagicMock:
    req = MagicMock(spec=web.Request)
    req.app = app if app is not None else {}
    req.match_info = match_info or {}
    req.headers = headers or {}
    req.read = AsyncMock(return_value=body)
    req.json = AsyncMock(return_value=json_body or {})
    # Mock streaming content for iter_chunked
    content = MagicMock()
    content.iter_chunked = lambda _size: _async_chunks(body)
    req.content = content
    return req


# ---------- create_upload_session ----------


class TestCreateUploadSession(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="wcpan_test_")
        self._store = UploadSessionService(tmp_dir=Path(self._tmp.name))

    def tearDown(self) -> None:
        self._store.close_all()
        self._tmp.cleanup()

    async def test_success_returns_201_with_location_and_upload_length(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": "p1"},
            json_body={"name": "file.bin", "size": 1024},
        )
        resp = await create_upload_session(req)
        self.assertEqual(resp.status, 201)
        self.assertIn("/api/v1/uploads/", resp.headers["Location"])
        self.assertEqual(resp.headers["Upload-Length"], "1024")

    async def test_missing_name_raises_400(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": "p1"},
            json_body={"size": 1024},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_missing_size_raises_400(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": "p1"},
            json_body={"name": "file.bin"},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_invalid_size_raises_400(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": "p1"},
            json_body={"name": "file.bin", "size": "abc"},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_zero_size_raises_400(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": "p1"},
            json_body={"name": "file.bin", "size": 0},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_virtual_root_raises_403(self):
        from wcpan.drive.synology._server.services.paths import SERVER_ROOT_ID

        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": SERVER_ROOT_ID},
            json_body={"name": "file.bin", "size": 1024},
        )
        with self.assertRaises(web.HTTPForbidden):
            await create_upload_session(req)

    async def test_media_params_stored_in_media_info(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"parent_id": "p1"},
            json_body={"name": "img.jpg", "size": 500, "width": 800, "height": 600},
        )
        resp = await create_upload_session(req)
        location = resp.headers["Location"]
        session_id = location.split("/")[-1]
        session = self._store.get(session_id)
        self.assertIsNotNone(session)
        assert session is not None
        self.assertIsNotNone(session.media_info)
        assert session.media_info is not None
        self.assertEqual(session.media_info.width, 800)
        self.assertEqual(session.media_info.height, 600)


# ---------- head_upload_session ----------


class TestHeadUploadSession(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="wcpan_test_")
        self._store = UploadSessionService(tmp_dir=Path(self._tmp.name))

    def tearDown(self) -> None:
        self._store.close_all()
        self._tmp.cleanup()

    async def test_returns_200_with_upload_offset_and_length(self):
        session = self._store.create("p", "f.bin", 2048, None, None)
        session.received = 512

        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
        )
        resp = await head_upload_session(req)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.headers["Upload-Offset"], "512")
        self.assertEqual(resp.headers["Upload-Length"], "2048")

    async def test_nothing_received_returns_200_with_zero_offset(self):
        session = self._store.create("p", "f.bin", 2048, None, None)

        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
        )
        resp = await head_upload_session(req)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.headers["Upload-Offset"], "0")
        self.assertEqual(resp.headers["Upload-Length"], "2048")

    async def test_unknown_session_raises_404(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": "nonexistent"},
        )
        with self.assertRaises(web.HTTPNotFound):
            await head_upload_session(req)


# ---------- delete_upload_session ----------


class TestDeleteUploadSession(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="wcpan_test_")
        self._store = UploadSessionService(tmp_dir=Path(self._tmp.name))

    def tearDown(self) -> None:
        self._store.close_all()
        self._tmp.cleanup()

    async def test_returns_204_and_removes_session(self):
        session = self._store.create("p", "f.bin", 100, None, {})
        session_id = session.session_id

        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session_id},
        )
        resp = await delete_upload_session(req)
        self.assertEqual(resp.status, 204)
        self.assertIsNone(self._store.get(session_id))

    async def test_unknown_session_raises_404(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": "nonexistent"},
        )
        with self.assertRaises(web.HTTPNotFound):
            await delete_upload_session(req)


# ---------- patch_upload_chunk ----------


class TestPatchUploadChunk(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="wcpan_test_")
        self._store = UploadSessionService(tmp_dir=Path(self._tmp.name))

    def tearDown(self) -> None:
        self._store.close_all()
        self._tmp.cleanup()

    def _make_session(self, total: int = 100):
        return self._store.create("p1", "f.bin", total, None, None)

    async def test_partial_upload_returns_204_with_upload_offset(self):
        session = self._make_session(total=200)
        chunk = b"x" * 100  # only half the file

        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
            headers={"Upload-Offset": "0"},
            body=chunk,
        )
        resp = await patch_upload_chunk(req)
        self.assertEqual(resp.status, 204)
        self.assertEqual(resp.headers["Upload-Offset"], "100")

    async def test_wrong_offset_returns_409_with_upload_offset(self):
        session = self._make_session(total=200)
        session.received = 50  # server has 50 bytes

        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
            headers={"Upload-Offset": "0"},
            body=b"x" * 100,
        )
        resp = await patch_upload_chunk(req)
        self.assertEqual(resp.status, 409)
        self.assertEqual(resp.headers["Upload-Offset"], "50")

    async def test_missing_upload_offset_raises_400(self):
        session = self._make_session()
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
            headers={},
            body=b"x" * 50,
        )
        with self.assertRaises(web.HTTPBadRequest):
            await patch_upload_chunk(req)

    async def test_non_integer_upload_offset_raises_400(self):
        session = self._make_session()
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
            headers={"Upload-Offset": "abc"},
            body=b"x" * 50,
        )
        with self.assertRaises(web.HTTPBadRequest):
            await patch_upload_chunk(req)

    async def test_negative_upload_offset_raises_400(self):
        session = self._make_session()
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
            headers={"Upload-Offset": "-1"},
            body=b"x" * 50,
        )
        with self.assertRaises(web.HTTPBadRequest):
            await patch_upload_chunk(req)

    async def test_body_exceeding_total_size_raises_400(self):
        session = self._make_session(total=50)
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": session.session_id},
            headers={"Upload-Offset": "0"},
            body=b"x" * 100,  # twice the declared size
        )
        with self.assertRaises(web.HTTPBadRequest):
            await patch_upload_chunk(req)

    async def test_unknown_session_raises_404(self):
        req = _make_request(
            app=_make_app(self._store),
            match_info={"session_id": "nonexistent"},
            headers={"Upload-Offset": "0"},
            body=b"x" * 10,
        )
        with self.assertRaises(web.HTTPNotFound):
            await patch_upload_chunk(req)

    async def test_final_chunk_triggers_synology_upload(self):
        from datetime import UTC, datetime

        from wcpan.drive.synology.types import NodeRecord

        session = self._make_session(total=50)
        chunk = b"y" * 50

        now = datetime(2024, 1, 1, tzinfo=UTC)
        node_record = NodeRecord(
            node_id="new-node-1",
            parent_id="p1",
            name="f.bin",
            is_directory=False,
            ctime=now,
            mtime=now,
            mime_type="application/octet-stream",
            hash="abc",
            size=50,
            is_image=False,
            is_video=False,
            width=0,
            height=0,
            ms_duration=0,
        )

        with (
            patch(
                "wcpan.drive.synology._server.handlers.upload.synology_files.upload_file",
                new_callable=AsyncMock,
                return_value={
                    "file_id": "new-node-1",
                    "parent_id": "p1",
                    "name": "f.bin",
                    "type": "file",
                    "content_type": "file",
                    "size": 50,
                    "created_time": 0,
                    "modified_time": 0,
                    "sync_id": 1,
                },
            ),
            patch(
                "wcpan.drive.synology._server.handlers.lib.enrich_and_upsert_synology_node",
                new_callable=AsyncMock,
                return_value=node_record,
            ),
        ):
            req = _make_request(
                app=_make_app(self._store),
                match_info={"session_id": session.session_id},
                headers={"Upload-Offset": "0"},
                body=chunk,
            )
            resp = await patch_upload_chunk(req)

        self.assertEqual(resp.status, 201)
        # Session should be removed after successful upload.
        self.assertIsNone(self._store.get(session.session_id))
