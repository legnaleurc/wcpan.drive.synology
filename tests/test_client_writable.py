"""Tests for _ResumableWritableFile and create_writable."""

import asyncio
import json
import tempfile
from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from wcpan.drive.synology.client._writable import (
    _MAX_SPOOL,
    _ResumableWritableFile,
    create_writable,
)
from wcpan.drive.synology.exceptions import SynologyUploadError


def _response_cm(status: int, body: dict) -> MagicMock:
    """Build a mock aiohttp context manager that returns a response."""
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=body)
    response.raise_for_status = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _node_dict() -> dict:
    return {
        "node_id": "node-1",
        "parent_id": "parent-1",
        "name": "file.bin",
        "is_directory": False,
        "ctime": "2024-01-01T00:00:00+00:00",
        "mtime": "2024-01-01T00:00:00+00:00",
        "mime_type": "application/octet-stream",
        "hash": "abc123",
        "size": 100,
        "is_image": False,
        "is_video": False,
        "width": 0,
        "height": 0,
        "ms_duration": 0,
    }


def _make_session(
    session_id: str = "test-session",
    total_size: int = 100,
) -> MagicMock:
    """Build a mock aiohttp ClientSession pre-wired for resumable upload."""
    session = MagicMock(spec=aiohttp.ClientSession)
    # POST /upload-session → 201 {"session_id": ..., "received": 0}
    session.post = MagicMock(
        return_value=_response_cm(201, {"session_id": session_id, "received": 0})
    )
    # GET /upload-sessions/{id} → 200 {"received": 0}
    session.get = MagicMock(
        return_value=_response_cm(200, {"received": 0, "total": total_size})
    )
    # PUT /upload-sessions/{id} → 201 with node (successful upload)
    session.put = MagicMock(
        return_value=_response_cm(201, _node_dict())
    )
    # DELETE /upload-sessions/{id} → 204
    session.delete = MagicMock(
        return_value=_response_cm(204, {})
    )
    return session


# ---------- _ResumableWritableFile ----------


def _make_writable(
    client,
    session_id: str = "sid",
    total_size: int = 100,
    buf: tempfile.SpooledTemporaryFile | None = None,
) -> _ResumableWritableFile:
    if buf is None:
        buf = tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b")
    return _ResumableWritableFile(client, "http://srv", session_id, total_size, "f.bin", buf)


class TestResumableWritableFileFlush(IsolatedAsyncioTestCase):
    async def test_flush_sends_data_and_returns_node(self):
        # given
        data = b"x" * 100
        client = _make_session("sid", total_size=100)
        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid", 100, buf)

            # when
            await writable.write(data)
            await writable.flush()

            # then — PUT was called with Content-Range covering entire file
            node = await writable.node()
        self.assertEqual(node.id, "node-1")
        client.put.assert_called_once()
        _, put_kwargs = client.put.call_args
        self.assertIn("Content-Range", put_kwargs.get("headers", {}))
        self.assertEqual(put_kwargs["headers"]["Content-Range"], "bytes 0-99/100")

    async def test_flush_is_idempotent(self):
        data = b"y" * 50
        client = _make_session("sid2", total_size=50)
        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid2", 50, buf)
            await writable.write(data)
            await writable.flush()
            # second flush should not re-upload
            await writable.flush()
        self.assertEqual(client.put.call_count, 1)

    async def test_flush_splits_into_chunks(self):
        # Write more than _CHUNK_SIZE (4 MiB) to force multiple PUTs.
        from wcpan.drive.synology.client._writable import _CHUNK_SIZE

        total = _CHUNK_SIZE + 1024  # slightly more than one chunk
        data = b"z" * total

        client = _make_session("sid3", total_size=total)
        # First PUT returns 200 (incomplete), second returns 201 (done).
        client.put = MagicMock(
            side_effect=[
                _response_cm(200, {"received": _CHUNK_SIZE}),
                _response_cm(201, _node_dict()),
            ]
        )

        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid3", total, buf)
            await writable.write(data)
            await writable.flush()

        self.assertEqual(client.put.call_count, 2)
        # First call: bytes 0-{CHUNK_SIZE-1}/{total}
        _, kw1 = client.put.call_args_list[0]
        self.assertEqual(
            kw1["headers"]["Content-Range"],
            f"bytes 0-{_CHUNK_SIZE - 1}/{total}",
        )
        # Second call: bytes {CHUNK_SIZE}-{total-1}/{total}
        _, kw2 = client.put.call_args_list[1]
        self.assertEqual(
            kw2["headers"]["Content-Range"],
            f"bytes {_CHUNK_SIZE}-{total - 1}/{total}",
        )


class TestResumableWritableFileRetry(IsolatedAsyncioTestCase):
    async def test_offset_mismatch_seeks_and_retries(self):
        # Server says it has 50 bytes when client thought it had 0.
        total = 100
        client = _make_session("sid-mismatch", total_size=total)
        # First PUT → 409 with server_received=50
        # Second PUT → 201 (success from offset 50)
        client.put = MagicMock(
            side_effect=[
                _response_cm(409, {"received": 50}),
                _response_cm(201, _node_dict()),
            ]
        )
        client.get = MagicMock(
            return_value=_response_cm(200, {"received": 0, "total": total})
        )

        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid-mismatch", total, buf)
            await writable.write(b"a" * 100)
            await writable.flush()

        self.assertEqual(client.put.call_count, 2)
        # Second PUT must start at offset 50
        _, kw2 = client.put.call_args_list[1]
        self.assertEqual(kw2["headers"]["Content-Range"], "bytes 50-99/100")

    async def test_connection_error_retries_from_server_received(self):
        total = 100
        client = _make_session("sid-retry", total_size=total)
        # First PUT raises ClientError; second PUT succeeds.
        client.put = MagicMock(
            side_effect=[
                _cm_raises(aiohttp.ClientError("connection reset")),
                _response_cm(201, _node_dict()),
            ]
        )
        # GET → server received 40 bytes before the drop
        client.get = MagicMock(
            return_value=_response_cm(200, {"received": 40, "total": total})
        )

        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid-retry", total, buf)
            await writable.write(b"b" * 100)
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await writable.flush()

        self.assertEqual(client.put.call_count, 2)
        _, kw2 = client.put.call_args_list[1]
        self.assertEqual(kw2["headers"]["Content-Range"], "bytes 40-99/100")

    async def test_503_raises_upload_error_without_retry(self):
        client = _make_session("sid-503", total_size=50)
        client.put = MagicMock(return_value=_response_cm(503, {}))

        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid-503", 50, buf)
            await writable.write(b"c" * 50)
            with self.assertRaises(SynologyUploadError):
                await writable.flush()

        # Must not retry on 503
        self.assertEqual(client.put.call_count, 1)

    async def test_404_raises_upload_error(self):
        client = _make_session("sid-404", total_size=50)
        client.put = MagicMock(return_value=_response_cm(404, {}))

        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid-404", 50, buf)
            await writable.write(b"d" * 50)
            with self.assertRaises(SynologyUploadError):
                await writable.flush()

    async def test_exceeds_max_retries_raises_upload_error(self):
        from wcpan.drive.synology.client._writable import _MAX_RETRIES

        total = 50
        client = _make_session("sid-maxretry", total_size=total)
        client.put = MagicMock(
            return_value=_cm_raises(aiohttp.ClientError("drop"))
        )
        client.get = MagicMock(
            return_value=_response_cm(200, {"received": 0, "total": total})
        )

        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _make_writable(client, "sid-maxretry", total, buf)
            await writable.write(b"e" * total)
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with self.assertRaises(SynologyUploadError):
                    await writable.flush()

        self.assertEqual(client.put.call_count, _MAX_RETRIES)


# ---------- create_writable ----------


class TestCreateWritable(IsolatedAsyncioTestCase):
    async def test_size_positive_uses_resumable_path(self):
        client = _make_session("sess-1", total_size=50)
        async with create_writable(
            session=client,
            server_url="http://srv",
            parent_id="par",
            name="f.bin",
            size=50,
            mime_type=None,
        ) as w:
            await w.write(b"a" * 50)
        # session initiation via POST
        client.post.assert_called_once()
        args, _ = client.post.call_args
        self.assertIn("upload-session", args[0])

    async def test_size_zero_uses_empty_path(self):
        client = MagicMock(spec=aiohttp.ClientSession)
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value=_node_dict())
        response.raise_for_status = MagicMock()
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=response)
        cm.__aexit__ = AsyncMock(return_value=False)
        client.post = MagicMock(return_value=cm)

        async with create_writable(
            session=client,
            server_url="http://srv",
            parent_id="par",
            name="empty.bin",
            size=0,
            mime_type=None,
        ) as w:
            pass  # no writes for empty file

        # Must use the non-session upload endpoint
        args, _ = client.post.call_args
        self.assertIn("/upload", args[0])
        self.assertNotIn("upload-session", args[0])

    async def test_exception_cancels_session(self):
        client = _make_session("sess-cancel", total_size=100)

        with self.assertRaises(ValueError):
            async with create_writable(
                session=client,
                server_url="http://srv",
                parent_id="par",
                name="f.bin",
                size=100,
                mime_type=None,
            ) as w:
                raise ValueError("caller error")

        # DELETE must have been called to clean up session
        client.delete.assert_called_once()


# ---------- helpers ----------


def _cm_raises(exc: Exception) -> MagicMock:
    """Build a mock context manager whose __aenter__ raises exc."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=exc)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm
