"""Tests for the four resumable upload session handlers."""

import asyncio
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web

from wcpan.drive.synology.server._handlers import (
    create_upload_session,
    delete_upload_session,
    get_upload_session,
    put_upload_chunk,
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


class _MockOffMain:
    """Executes functions synchronously for testing."""

    async def __call__(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)

    async def untimed(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


def _make_app(store: UploadSessionStore | None = None) -> dict:
    app: dict = {}
    app[upload_sessions_key] = store or UploadSessionStore()
    app[ready_key] = True
    app[off_main_key] = _MockOffMain()
    app[storage_key] = MagicMock()
    app[write_queue_key] = asyncio.Queue()
    app[network_key] = MagicMock()
    app[folders_key] = {"tmp": "/team-folders/download"}
    app[volume_map_key] = None
    return app


def _make_request(
    *,
    app: dict | None = None,
    match_info: dict | None = None,
    query: dict | None = None,
    headers: dict | None = None,
    body: bytes = b"",
) -> MagicMock:
    req = MagicMock(spec=web.Request)
    req.app = app or _make_app()
    req.match_info = match_info or {}
    req.rel_url.query = query or {}
    req.headers = headers or {}
    req.read = AsyncMock(return_value=body)
    return req


# ---------- create_upload_session ----------


class TestCreateUploadSession(IsolatedAsyncioTestCase):
    async def test_success_returns_201_with_session_id(self):
        req = _make_request(
            match_info={"parent_id": "p1"},
            query={"name": "file.bin", "size": "1024"},
        )
        resp = await create_upload_session(req)
        self.assertEqual(resp.status, 201)
        body = resp.body  # type: ignore[attr-defined]
        import json
        data = json.loads(body)
        self.assertIn("session_id", data)
        self.assertEqual(data["received"], 0)
        # cleanup
        store: UploadSessionStore = req.app[upload_sessions_key]
        session = store.get(data["session_id"])
        if session:
            session.temp_path.unlink(missing_ok=True)

    async def test_missing_name_raises_400(self):
        req = _make_request(
            match_info={"parent_id": "p1"},
            query={"size": "1024"},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_missing_size_raises_400(self):
        req = _make_request(
            match_info={"parent_id": "p1"},
            query={"name": "file.bin"},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_invalid_size_raises_400(self):
        req = _make_request(
            match_info={"parent_id": "p1"},
            query={"name": "file.bin", "size": "abc"},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_zero_size_raises_400(self):
        req = _make_request(
            match_info={"parent_id": "p1"},
            query={"name": "file.bin", "size": "0"},
        )
        with self.assertRaises(web.HTTPBadRequest):
            await create_upload_session(req)

    async def test_virtual_root_raises_403(self):
        from wcpan.drive.synology.server._virtual_ids import SERVER_ROOT_ID
        req = _make_request(
            match_info={"parent_id": SERVER_ROOT_ID},
            query={"name": "file.bin", "size": "1024"},
        )
        with self.assertRaises(web.HTTPForbidden):
            await create_upload_session(req)

    async def test_media_params_stored_in_client_query(self):
        req = _make_request(
            match_info={"parent_id": "p1"},
            query={"name": "img.jpg", "size": "500", "width": "800", "height": "600"},
        )
        resp = await create_upload_session(req)
        import json
        data = json.loads(resp.body)  # type: ignore[attr-defined]
        store: UploadSessionStore = req.app[upload_sessions_key]
        session = store.get(data["session_id"])
        self.assertIsNotNone(session)
        assert session is not None
        self.assertEqual(session.client_query.get("width"), "800")
        self.assertEqual(session.client_query.get("height"), "600")
        session.temp_path.unlink(missing_ok=True)


# ---------- get_upload_session ----------


class TestGetUploadSession(IsolatedAsyncioTestCase):
    async def test_returns_received_and_total(self):
        store = UploadSessionStore()
        session = store.create("p", "f.bin", 2048, None, {})
        session.received = 512

        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session.session_id},
        )
        resp = await get_upload_session(req)
        self.assertEqual(resp.status, 200)
        import json
        data = json.loads(resp.body)  # type: ignore[attr-defined]
        self.assertEqual(data["received"], 512)
        self.assertEqual(data["total"], 2048)
        session.temp_path.unlink(missing_ok=True)

    async def test_unknown_session_raises_404(self):
        req = _make_request(match_info={"session_id": "nonexistent"})
        with self.assertRaises(web.HTTPNotFound):
            await get_upload_session(req)


# ---------- delete_upload_session ----------


class TestDeleteUploadSession(IsolatedAsyncioTestCase):
    async def test_returns_204_and_removes_session(self):
        store = UploadSessionStore()
        session = store.create("p", "f.bin", 100, None, {})
        session_id = session.session_id

        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session_id},
        )
        resp = await delete_upload_session(req)
        self.assertEqual(resp.status, 204)
        self.assertIsNone(store.get(session_id))

    async def test_unknown_session_raises_404(self):
        req = _make_request(match_info={"session_id": "nonexistent"})
        with self.assertRaises(web.HTTPNotFound):
            await delete_upload_session(req)


# ---------- put_upload_chunk ----------


class TestPutUploadChunk(IsolatedAsyncioTestCase):
    def _make_session(self, store: UploadSessionStore, total: int = 100):
        return store.create("p1", "f.bin", total, None, {})

    async def test_partial_chunk_returns_200_with_received(self):
        store = UploadSessionStore()
        session = self._make_session(store, total=200)
        chunk = b"x" * 100

        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session.session_id},
            headers={"Content-Range": f"bytes 0-99/200"},
            body=chunk,
        )
        resp = await put_upload_chunk(req)

        import json
        data = json.loads(resp.body)  # type: ignore[attr-defined]
        self.assertEqual(resp.status, 200)
        self.assertEqual(data["received"], 100)
        self.assertEqual(session.received, 100)
        session.temp_path.unlink(missing_ok=True)

    async def test_wrong_offset_returns_409_with_received(self):
        store = UploadSessionStore()
        session = self._make_session(store, total=200)
        session.received = 50  # server has 50 bytes

        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session.session_id},
            headers={"Content-Range": "bytes 0-99/200"},
            body=b"x" * 100,
        )
        resp = await put_upload_chunk(req)

        import json
        data = json.loads(resp.body)  # type: ignore[attr-defined]
        self.assertEqual(resp.status, 409)
        self.assertEqual(data["received"], 50)
        session.temp_path.unlink(missing_ok=True)

    async def test_missing_content_range_raises_400(self):
        store = UploadSessionStore()
        session = self._make_session(store)
        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session.session_id},
            headers={},
            body=b"x" * 50,
        )
        with self.assertRaises(web.HTTPBadRequest):
            await put_upload_chunk(req)
        session.temp_path.unlink(missing_ok=True)

    async def test_wrong_total_raises_409(self):
        store = UploadSessionStore()
        session = self._make_session(store, total=100)
        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session.session_id},
            headers={"Content-Range": "bytes 0-49/999"},
            body=b"x" * 50,
        )
        with self.assertRaises(web.HTTPConflict):
            await put_upload_chunk(req)
        session.temp_path.unlink(missing_ok=True)

    async def test_body_length_mismatch_raises_400(self):
        store = UploadSessionStore()
        session = self._make_session(store, total=100)
        req = _make_request(
            app=_make_app(store),
            match_info={"session_id": session.session_id},
            headers={"Content-Range": "bytes 0-49/100"},
            body=b"x" * 30,  # claims 50 bytes but only sends 30
        )
        with self.assertRaises(web.HTTPBadRequest):
            await put_upload_chunk(req)
        session.temp_path.unlink(missing_ok=True)

    async def test_unknown_session_raises_404(self):
        req = _make_request(
            match_info={"session_id": "nonexistent"},
            headers={"Content-Range": "bytes 0-9/100"},
            body=b"x" * 10,
        )
        with self.assertRaises(web.HTTPNotFound):
            await put_upload_chunk(req)

    async def test_final_chunk_triggers_synology_upload(self):
        from datetime import UTC, datetime
        from wcpan.drive.synology.types import NodeRecord

        store = UploadSessionStore()
        session = self._make_session(store, total=50)
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
                "wcpan.drive.synology.server._handlers.synology_files.upload_file",
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
                "wcpan.drive.synology.server._handlers._enrich_and_upsert_synology_node",
                new_callable=AsyncMock,
                return_value=node_record,
            ),
        ):
            req = _make_request(
                app=_make_app(store),
                match_info={"session_id": session.session_id},
                headers={"Content-Range": "bytes 0-49/50"},
                body=chunk,
            )
            resp = await put_upload_chunk(req)

        self.assertEqual(resp.status, 201)
        # Session should be removed after successful upload.
        self.assertIsNone(store.get(session.session_id))
