import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import PurePosixPath
from typing import Any, override

from wcpan.drive.core.types import MediaInfo, Node, PrivateDict, WritableFile

from ._api import files
from ._lib import SynologyFileDict, node_from_api
from ._network import Network


@asynccontextmanager
async def create_writable(
    *,
    network: Network,
    parent_path: PurePosixPath,
    name: str,
    size: int | None = None,
    mime_type: str | None = None,
    media_info: MediaInfo | None = None,
    private: PrivateDict | None = None,
) -> AsyncIterator[WritableFile]:
    if size is not None and size == 0:
        yield await _create_empty(
            network=network,
            parent_path=parent_path,
            name=name,
            mime_type=mime_type,
        )
        return

    queue: asyncio.Queue[bytes | None] = asyncio.Queue()
    hasher = hashlib.md5()

    async def _chunk_source() -> AsyncIterator[bytes]:
        while True:
            chunk = await queue.get()
            if chunk is None:
                return
            yield chunk

    upload_task: asyncio.Task[SynologyFileDict] = asyncio.create_task(
        files.upload_file(
            network=network,
            parent_path=parent_path,
            name=name,
            data=_chunk_source(),
            mime_type=mime_type,
        )
    )

    file_path = parent_path / name
    writable = SynologyStreamingWritableFile(
        queue, hasher, upload_task, network, file_path
    )
    try:
        yield writable
        await writable.flush()
    except BaseException:
        upload_task.cancel()
        with suppress(Exception):
            await upload_task
        raise


async def _create_empty(
    *,
    network: Network,
    parent_path: PurePosixPath,
    name: str,
    mime_type: str | None = None,
) -> WritableFile:
    async def _empty() -> AsyncIterator[bytes]:
        return
        yield  # make it an async generator

    file_dict = await files.upload_file(
        network=network,
        parent_path=parent_path,
        name=name,
        data=_empty(),
        mime_type=mime_type,
    )
    file_dict["hash"] = hashlib.md5(b"").hexdigest()
    return SynologyEmptyWritableFile(node_from_api(file_dict))


class SynologyEmptyWritableFile(WritableFile):
    """WritableFile for empty files. All operations are no-ops."""

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


class SynologyStreamingWritableFile(WritableFile):
    """WritableFile that streams data to Synology via a concurrent upload task.

    An upload task starts immediately when this object is created. Chunks
    written via write() are placed on an unbounded asyncio.Queue; the upload
    task consumes them and streams directly into the HTTP body using
    aiohttp's AsyncGeneratorPayload — no full-file buffer is ever held in
    memory.

    MD5 is computed incrementally in write() so that flush() can attach the
    hash to the returned node without an extra pass over the data.
    """

    def __init__(
        self,
        queue: asyncio.Queue[bytes | None],
        hasher: Any,
        upload_task: asyncio.Task[SynologyFileDict],
        network: Network,
        file_path: PurePosixPath,
    ) -> None:
        self._queue = queue
        self._hasher = hasher
        self._upload_task = upload_task
        self._network = network
        self._file_path = file_path
        self._file_dict: SynologyFileDict | None = None
        self._node: Node | None = None
        self._done = False

    @override
    async def write(self, chunk: bytes) -> int:
        self._hasher.update(chunk)
        self._queue.put_nowait(chunk)  # unbounded — never blocks
        return len(chunk)

    @override
    async def seek(self, offset: int) -> int:
        raise NotImplementedError("seek is not supported for streaming upload")

    @override
    async def tell(self) -> int:
        raise NotImplementedError("tell is not supported for streaming upload")

    @override
    async def flush(self) -> None:
        if self._done:
            return
        self._queue.put_nowait(None)  # sentinel: signals end-of-stream
        self._file_dict = await self._upload_task
        self._done = True

    @override
    async def node(self) -> Node:
        if self._node is not None:
            return self._node
        if not self._done:
            await self.flush()
        assert self._file_dict is not None
        self._file_dict["hash"] = await files.compute_md5(
            self._network, self._file_path
        )
        self._node = node_from_api(self._file_dict)
        return self._node
