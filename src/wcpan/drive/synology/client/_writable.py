"""WritableFile that uploads via the wcpan.drive.synology server."""

import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any, override

from aiohttp import ClientSession
from wcpan.drive.core.exceptions import NodeExistsError
from wcpan.drive.core.types import MediaInfo, Node, WritableFile

from ..exceptions import SynologyUploadConflictError, SynologyUploadError
from ..lib import node_from_record, node_record_from_dict


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
