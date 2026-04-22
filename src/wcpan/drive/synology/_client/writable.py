"""WritableFile that uploads via the wcpan.drive.synology server."""

import asyncio
import tempfile
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager, suppress
from logging import getLogger
from typing import cast, override

from aiohttp import ClientSession, ClientTimeout
from wcpan.drive.core.exceptions import NodeExistsError
from wcpan.drive.core.types import MediaInfo, Node, WritableFile

from .._lib import NodeRecordDict, node_from_record, node_record_from_dict
from ..exceptions import SynologyUploadError
from .http409 import node_from_409


_L = getLogger(__name__)

_MAX_SPOOL = 64 * 1024 * 1024  # 64 MiB before spilling to disk
_MAX_RETRIES = 5
_FINAL_TIMEOUT = ClientTimeout(
    total=None
)  # no timeout — server uploads to Synology before responding


def _media_info_to_params(media_info: MediaInfo | None) -> dict[str, str]:
    if media_info is None:
        return {}
    out: dict[str, str] = {}
    if media_info.width or media_info.height or media_info.ms_duration:
        out["width"] = str(media_info.width)
        out["height"] = str(media_info.height)
        out["ms_duration"] = str(media_info.ms_duration)
    if media_info.is_image:
        out["media_image"] = "1"
    if media_info.is_video:
        out["media_video"] = "1"
    return out


@asynccontextmanager
async def create_writable(
    session: ClientSession,
    server_url: str,
    parent_id: str,
    name: str,
    size: int | None,
    mime_type: str | None,
    media_info: MediaInfo | None = None,
) -> AsyncGenerator[WritableFile]:
    server_url = server_url.rstrip("/")

    if size is not None and size == 0:
        node = await _upload_empty(
            session, server_url, parent_id, name, mime_type, media_info
        )
        yield _EmptyWritableFile(node)
        return

    if size is not None and size > 0:
        session_url = await _initiate_session(
            session, server_url, parent_id, name, size, mime_type, media_info
        )
        with tempfile.SpooledTemporaryFile[bytes](
            max_size=_MAX_SPOOL, mode="w+b"
        ) as buf:
            writable = _ResumableWritableFile(session, session_url, size, name, buf)
            try:
                yield writable
                await writable.flush()
            except BaseException:
                with suppress(Exception):
                    await _cancel_session(session, server_url, session_url)
                raise
        return

    # size is None: fall back to streaming (unknown-size upload)
    queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=8)

    async def _stream() -> AsyncIterator[bytes]:
        while True:
            chunk = await queue.get()
            if chunk is None:
                return
            yield chunk

    upload_task: asyncio.Task[Node] = asyncio.create_task(
        _upload_stream(
            session,
            server_url,
            parent_id,
            name,
            mime_type,
            _stream(),
            media_info,
        )
    )

    writable = _StreamingWritableFile(queue, upload_task)
    try:
        yield writable
        await writable.flush()
    except BaseException:
        upload_task.cancel()
        with suppress(Exception):
            await upload_task
        raise


async def _upload_empty(
    session: ClientSession,
    server_url: str,
    parent_id: str,
    name: str,
    mime_type: str | None,
    media_info: MediaInfo | None,
) -> Node:
    url = f"{server_url}/api/v1/nodes/{parent_id}"
    params = {"name": name}
    if mime_type:
        params["mime_type"] = mime_type
    media_params = _media_info_to_params(media_info)
    params.update(media_params)
    _L.debug(
        "upload empty name=%r parent_id=%r mime_type=%r media_params=%r",
        name,
        parent_id,
        mime_type,
        media_params,
    )
    async with session.post(url, data=b"", params=params) as response:
        if response.status == 409:
            node = await node_from_409(response)
            if node is not None:
                raise NodeExistsError(node)
            raise SynologyUploadError(
                f"Upload conflict for {name!r} without node in response",
                file_name=name,
            )
        response.raise_for_status()
        data = await response.json()
    return node_from_record(node_record_from_dict(cast(NodeRecordDict, data)))


async def _upload_stream(
    session: ClientSession,
    server_url: str,
    parent_id: str,
    name: str,
    mime_type: str | None,
    data: AsyncIterator[bytes],
    media_info: MediaInfo | None,
) -> Node:
    url = f"{server_url}/api/v1/nodes/{parent_id}"
    params = {"name": name}
    if mime_type:
        params["mime_type"] = mime_type
    media_params = _media_info_to_params(media_info)
    params.update(media_params)
    _L.debug(
        "upload stream name=%r parent_id=%r mime_type=%r media_params=%r",
        name,
        parent_id,
        mime_type,
        media_params,
    )
    async with session.post(url, data=data, params=params) as response:
        if response.status == 409:
            node = await node_from_409(response)
            if node is not None:
                raise NodeExistsError(node)
            raise SynologyUploadError(
                f"Upload conflict for {name!r} without node in response",
                file_name=name,
            )
        if response.status == 503:
            raise SynologyUploadError(
                f"Upload failed for {name!r}: Synology returned transient error",
                file_name=name,
            )
        response.raise_for_status()
        result = await response.json()
    return node_from_record(node_record_from_dict(cast(NodeRecordDict, result)))


async def _initiate_session(
    session: ClientSession,
    server_url: str,
    parent_id: str,
    name: str,
    size: int,
    mime_type: str | None,
    media_info: MediaInfo | None,
) -> str:
    url = f"{server_url}/api/v1/nodes/{parent_id}/uploads"
    body: dict[str, object] = {"name": name, "size": size}
    if mime_type:
        body["mime_type"] = mime_type
    media_params = _media_info_to_params(media_info)
    body.update(media_params)
    _L.debug(
        "initiate upload session name=%r parent_id=%r size=%d mime_type=%r media_params=%r",
        name,
        parent_id,
        size,
        mime_type,
        media_params,
    )
    async with session.post(url, json=body) as response:
        if response.status == 409:
            node = await node_from_409(response)
            if node is not None:
                raise NodeExistsError(node)
            raise SynologyUploadError(
                f"Upload session conflict for {name!r} without node in response",
                file_name=name,
            )
        response.raise_for_status()
        location = response.headers.get("Location", "")
    if not location:
        raise SynologyUploadError(
            f"Upload session for {name!r} missing Location header",
            file_name=name,
        )
    return f"{server_url}{location}"


async def _cancel_session(
    session: ClientSession,
    server_url: str,
    session_url: str,
) -> None:
    async with session.delete(session_url):
        pass  # ignore errors; best-effort cleanup


class _OffsetMismatch(Exception):
    def __init__(self, server_received: int) -> None:
        super().__init__(f"server has {server_received} bytes")
        self.server_received = server_received


class _ResumableWritableFile(WritableFile):
    def __init__(
        self,
        session: ClientSession,
        session_url: str,
        total_size: int,
        name: str,
        buf: tempfile.SpooledTemporaryFile[bytes],
    ) -> None:
        self._session = session
        self._session_url = session_url
        self._total_size = total_size
        self._name = name
        self._buf = buf
        self._node: Node | None = None
        self._done = False

    @override
    async def write(self, chunk: bytes) -> int:
        self._buf.write(chunk)
        return len(chunk)

    @override
    async def seek(self, offset: int) -> int:
        self._buf.seek(offset)
        return offset

    @override
    async def tell(self) -> int:
        return self._buf.tell()

    @override
    async def flush(self) -> None:
        if self._done:
            return

        received = await self._query_received()
        network_errors = 0
        delay = 1.0

        while True:
            try:
                node = await self._upload_from(received)
                self._node = node
                self._done = True
                return
            except _OffsetMismatch as e:
                received = e.server_received
                # Offset correction — not a network error, no backoff needed.
                continue
            except SynologyUploadError:
                # Non-retryable (e.g. 503 from Synology, session not found).
                raise
            except Exception as e:
                network_errors += 1
                _L.warning(
                    "Resumable upload connection error (attempt %d/%d) for %r: %s",
                    network_errors,
                    _MAX_RETRIES,
                    self._name,
                    e,
                )
                if network_errors >= _MAX_RETRIES:
                    raise SynologyUploadError(
                        f"Resumable upload failed after {_MAX_RETRIES} attempts"
                        f" for {self._name!r}",
                        file_name=self._name,
                    ) from e
                try:
                    received = await self._query_received()
                except Exception:
                    pass  # keep last known received
                await asyncio.sleep(min(delay, 30.0))
                delay *= 2

    @override
    async def node(self) -> Node:
        if self._node is not None:
            return self._node
        if not self._done:
            await self.flush()
        assert self._node is not None
        return self._node

    async def _query_received(self) -> int:
        async with self._session.head(
            self._session_url,
            timeout=ClientTimeout(total=30),
        ) as response:
            if response.status == 404:
                return 0
            response.raise_for_status()
            offset_str = response.headers.get("Upload-Offset", "0")
        return int(offset_str)

    async def _upload_from(self, start_offset: int) -> Node:
        """Stream the entire buffer from *start_offset* in a single PATCH request."""
        self._buf.seek(start_offset)
        headers = {
            "Upload-Offset": str(start_offset),
            "Content-Type": "application/octet-stream",
        }
        async with self._session.patch(
            self._session_url,
            data=self._buf,
            headers=headers,
            timeout=_FINAL_TIMEOUT,
        ) as response:
            if response.status == 409:
                raise _OffsetMismatch(int(response.headers.get("Upload-Offset", "0")))
            if response.status == 204:
                raise _OffsetMismatch(
                    int(response.headers.get("Upload-Offset", str(start_offset)))
                )
            if response.status == 404:
                raise SynologyUploadError(
                    f"Upload session not found for {self._name!r};"
                    " server may have restarted",
                    file_name=self._name,
                )
            if response.status == 503:
                raise SynologyUploadError(
                    f"Synology upload failed for {self._name!r}",
                    file_name=self._name,
                )
            response.raise_for_status()
            data = await response.json()
        return node_from_record(node_record_from_dict(cast(NodeRecordDict, data)))


class _EmptyWritableFile(WritableFile):
    def __init__(self, node: Node) -> None:
        self._node = node

    @override
    async def write(self, chunk: bytes) -> int:
        raise OSError("cannot write to a zero-byte file placeholder")

    @override
    async def seek(self, offset: int) -> int:
        return 0

    @override
    async def tell(self) -> int:
        return 0

    @override
    async def flush(self) -> None:
        pass

    @override
    async def node(self) -> Node:
        return self._node


class _StreamingWritableFile(WritableFile):
    def __init__(
        self,
        queue: asyncio.Queue[bytes | None],
        upload_task: asyncio.Task[Node],
    ) -> None:
        self._queue = queue
        self._upload_task = upload_task
        self._node: Node | None = None
        self._done = False

    @override
    async def write(self, chunk: bytes) -> int:
        await self._queue.put(chunk)
        return len(chunk)

    @override
    async def seek(self, offset: int) -> int:
        raise NotImplementedError("seek not supported for streaming upload")

    @override
    async def tell(self) -> int:
        raise NotImplementedError("tell not supported for streaming upload")

    @override
    async def flush(self) -> None:
        if self._done:
            return
        self._queue.put_nowait(None)
        self._node = await self._upload_task
        self._done = True

    @override
    async def node(self) -> Node:
        if self._node is not None:
            return self._node
        if not self._done:
            await self.flush()
        assert self._node is not None
        return self._node
