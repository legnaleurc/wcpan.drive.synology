"""WritableFile that uploads via the wcpan.drive.synology server."""

import asyncio
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from logging import getLogger
from typing import override

import aiohttp
from aiohttp import ClientSession, ClientTimeout
from wcpan.drive.core.exceptions import NodeExistsError
from wcpan.drive.core.types import MediaInfo, Node, WritableFile

from ..exceptions import SynologyUploadError
from ..lib import node_from_record, node_record_from_dict


_L = getLogger(__name__)

_MAX_SPOOL = 64 * 1024 * 1024   # 64 MiB before spilling to disk
_MAX_RETRIES = 5
_CHUNK_SIZE = 4 * 1024 * 1024   # 4 MiB upload chunks
_CHUNK_TIMEOUT = ClientTimeout(total=5 * 60)  # 5 min per chunk; stalled → retry


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
) -> AsyncIterator[WritableFile]:
    server_url = server_url.rstrip("/")

    if size is not None and size == 0:
        node = await _upload_empty(
            session, server_url, parent_id, name, mime_type, media_info
        )
        yield _EmptyWritableFile(node)
        return

    if size is not None and size > 0:
        session_id = await _initiate_session(
            session, server_url, parent_id, name, size, mime_type, media_info
        )
        with tempfile.SpooledTemporaryFile(max_size=_MAX_SPOOL, mode="b") as buf:
            writable = _ResumableWritableFile(session, server_url, session_id, size, name, buf)
            try:
                yield writable
                await writable.flush()
            except BaseException:
                with suppress(Exception):
                    await _cancel_session(session, server_url, session_id)
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
    url = f"{server_url}/api/v1/nodes/{parent_id}/upload"
    params = {"name": name}
    if mime_type:
        params["mime_type"] = mime_type
    params.update(_media_info_to_params(media_info))
    async with session.post(url, data=b"", params=params) as response:
        response.raise_for_status()
        data = await response.json()
    return node_from_record(node_record_from_dict(data))


async def _upload_stream(
    session: ClientSession,
    server_url: str,
    parent_id: str,
    name: str,
    mime_type: str | None,
    data: AsyncIterator[bytes],
    media_info: MediaInfo | None,
) -> Node:
    url = f"{server_url}/api/v1/nodes/{parent_id}/upload"
    params = {"name": name}
    if mime_type:
        params["mime_type"] = mime_type
    params.update(_media_info_to_params(media_info))
    async with session.post(url, data=data, params=params) as response:
        if response.status == 409:
            raise NodeExistsError(name)
        if response.status == 503:
            raise SynologyUploadError(
                f"Upload failed for {name!r}: Synology returned transient error",
                file_name=name,
            )
        response.raise_for_status()
        result = await response.json()
    return node_from_record(node_record_from_dict(result))


async def _initiate_session(
    session: ClientSession,
    server_url: str,
    parent_id: str,
    name: str,
    size: int,
    mime_type: str | None,
    media_info: MediaInfo | None,
) -> str:
    url = f"{server_url}/api/v1/nodes/{parent_id}/upload-session"
    params: dict[str, str] = {"name": name, "size": str(size)}
    if mime_type:
        params["mime_type"] = mime_type
    params.update(_media_info_to_params(media_info))
    async with session.post(url, params=params) as response:
        if response.status == 409:
            raise NodeExistsError(name)
        response.raise_for_status()
        body = await response.json()
    return body["session_id"]


async def _cancel_session(
    session: ClientSession,
    server_url: str,
    session_id: str,
) -> None:
    url = f"{server_url}/api/v1/upload-sessions/{session_id}"
    async with session.delete(url) as response:
        pass  # ignore errors; best-effort cleanup


class _OffsetMismatch(Exception):
    def __init__(self, server_received: int) -> None:
        super().__init__(f"server has {server_received} bytes")
        self.server_received = server_received


class _ResumableWritableFile(WritableFile):
    def __init__(
        self,
        session: ClientSession,
        server_url: str,
        session_id: str,
        total_size: int,
        name: str,
        buf: tempfile.SpooledTemporaryFile,
    ) -> None:
        self._session = session
        self._server_url = server_url
        self._session_id = session_id
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
        delay = 1.0

        for attempt in range(_MAX_RETRIES):
            try:
                node = await self._upload_from(received)
                self._node = node
                self._done = True
                return
            except _OffsetMismatch as e:
                received = e.server_received
                # Offset mismatch is a logic correction — no backoff needed.
                continue
            except SynologyUploadError:
                # Non-retryable (e.g. 503 from Synology, session not found).
                raise
            except Exception as e:
                _L.warning(
                    "Resumable upload connection error (attempt %d/%d) for %r: %s",
                    attempt + 1,
                    _MAX_RETRIES,
                    self._name,
                    e,
                )
                if attempt == _MAX_RETRIES - 1:
                    raise SynologyUploadError(
                        f"Resumable upload failed after {_MAX_RETRIES} attempts for {self._name!r}",
                        file_name=self._name,
                    ) from e
                try:
                    received = await self._query_received()
                except Exception:
                    pass  # keep last known received
                await asyncio.sleep(min(delay, 30.0))
                delay *= 2

        raise SynologyUploadError(
            f"Resumable upload failed after {_MAX_RETRIES} attempts for {self._name!r}",
            file_name=self._name,
        )

    @override
    async def node(self) -> Node:
        if self._node is not None:
            return self._node
        if not self._done:
            await self.flush()
        assert self._node is not None
        return self._node

    async def _query_received(self) -> int:
        url = f"{self._server_url}/api/v1/upload-sessions/{self._session_id}"
        async with self._session.get(
            url, timeout=ClientTimeout(total=30)
        ) as response:
            if response.status == 404:
                return 0
            response.raise_for_status()
            body = await response.json()
        return int(body.get("received", 0))

    async def _upload_from(self, start_offset: int) -> Node:
        self._buf.seek(start_offset)
        offset = start_offset

        while offset < self._total_size:
            chunk = self._buf.read(_CHUNK_SIZE)
            if not chunk:
                break
            end = offset + len(chunk) - 1
            headers = {
                "Content-Range": f"bytes {offset}-{end}/{self._total_size}",
            }
            url = f"{self._server_url}/api/v1/upload-sessions/{self._session_id}"
            async with self._session.put(
                url, data=chunk, headers=headers, timeout=_CHUNK_TIMEOUT
            ) as response:
                if response.status == 409:
                    body = await response.json()
                    raise _OffsetMismatch(int(body.get("received", offset)))
                if response.status == 404:
                    raise SynologyUploadError(
                        f"Upload session not found for {self._name!r}; server may have restarted",
                        file_name=self._name,
                    )
                if response.status == 503:
                    raise SynologyUploadError(
                        f"Synology upload failed for {self._name!r}",
                        file_name=self._name,
                    )
                response.raise_for_status()

                if response.status == 201:
                    data = await response.json()
                    return node_from_record(node_record_from_dict(data))

                # 200: chunk accepted, continue
                offset += len(chunk)

        raise SynologyUploadError(
            f"Upload loop ended without completion for {self._name!r}",
            file_name=self._name,
        )


class _EmptyWritableFile(WritableFile):
    def __init__(self, node: Node) -> None:
        self._node = node

    @override
    async def write(self, chunk: bytes) -> int:
        return 0

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
