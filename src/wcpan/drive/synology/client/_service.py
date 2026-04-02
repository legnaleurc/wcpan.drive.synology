"""FileService implementation that talks to the wcpan.drive.synology server."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import override

from aiohttp import ClientSession
from wcpan.drive.core.exceptions import NodeExistsError, NodeNotFoundError
from wcpan.drive.core.types import (
    ChangeAction,
    CreateHasher,
    FileService,
    MediaInfo,
    Node,
    PrivateDict,
    ReadableFile,
    WritableFile,
)

from ..exceptions import SynologyServerError
from ..lib import node_from_record, node_record_from_dict
from ._hasher import create_hasher
from ._readable import ClientReadableFile
from ._writable import create_writable


class ClientFileService(FileService):
    def __init__(self, session: ClientSession, server_url: str) -> None:
        self._session = session
        self._url = server_url.rstrip("/")

    @property
    @override
    def api_version(self) -> int:
        return 5

    @override
    async def get_initial_cursor(self) -> str:
        return "0"

    @override
    async def get_root(self) -> Node:
        async with self._session.get(f"{self._url}/api/v1/root") as response:
            _check(response.status, "get_root")
            data = await response.json()
        return node_from_record(node_record_from_dict(data))

    @override
    async def get_changes(
        self,
        cursor: str,
    ) -> AsyncIterator[tuple[list[ChangeAction], str]]:
        has_more = True
        while has_more:
            async with self._session.get(
                f"{self._url}/api/v1/changes",
                params={"cursor": cursor},
            ) as response:
                _check(response.status, "get_changes")
                data = await response.json()

            cursor = str(data["cursor"])
            has_more = bool(data.get("has_more", False))
            raw_changes = data.get("changes", [])

            actions: list[ChangeAction] = []
            for raw in raw_changes:
                if raw.get("removed"):
                    actions.append((True, raw["node_id"]))
                else:
                    record = node_record_from_dict(raw["node"])
                    actions.append((False, node_from_record(record)))

            yield actions, cursor

    @override
    async def move(
        self,
        node: Node,
        *,
        new_parent: Node | None,
        new_name: str | None,
    ) -> Node:
        if not node.parent_id:
            raise ValueError("Cannot move root node")

        body: dict = {}
        if new_name:
            body["name"] = new_name
        if new_parent:
            body["parent_id"] = new_parent.id

        async with self._session.patch(
            f"{self._url}/api/v1/nodes/{node.id}",
            json=body,
        ) as response:
            _check(response.status, "move")
            data = await response.json()

        return node_from_record(node_record_from_dict(data))

    @override
    async def delete(self, node: Node, *, permanent: bool = False) -> None:
        async with self._session.delete(
            f"{self._url}/api/v1/nodes/{node.id}"
        ) as response:
            if response.status == 404:
                raise NodeNotFoundError(node.id)
            _check(response.status, "delete")

    @override
    async def restore(self, node: Node) -> Node:
        raise NotImplementedError("restore is not supported")

    @override
    async def purge_trash(self) -> None:
        pass

    @override
    async def create_directory(
        self,
        name: str,
        parent: Node,
        *,
        exist_ok: bool,
        private: PrivateDict | None,
    ) -> Node:
        async with self._session.post(
            f"{self._url}/api/v1/nodes",
            json={"name": name, "parent_id": parent.id},
        ) as response:
            if response.status == 409:
                if exist_ok:
                    # Look up existing node via changes (simplification: re-raise)
                    raise NodeExistsError(None)  # caller handles
                raise NodeExistsError(None)
            _check(response.status, "create_directory")
            data = await response.json()

        return node_from_record(node_record_from_dict(data))

    @asynccontextmanager
    @override
    async def download_file(self, node: Node) -> AsyncIterator[ReadableFile]:
        async with ClientReadableFile(self._session, self._url, node) as fin:
            yield fin

    @asynccontextmanager
    @override
    async def upload_file(
        self,
        name: str,
        parent: Node,
        *,
        size: int | None,
        mime_type: str | None,
        media_info: MediaInfo | None,
        private: PrivateDict | None,
    ) -> AsyncIterator[WritableFile]:
        async with create_writable(
            session=self._session,
            server_url=self._url,
            parent_id=parent.id,
            name=name,
            size=size,
            mime_type=mime_type,
            media_info=media_info,
        ) as fout:
            yield fout

    @override
    async def get_hasher_factory(self) -> CreateHasher:
        return create_hasher

    @override
    async def is_authenticated(self) -> bool:
        return True  # session managed by context manager

    @override
    async def authenticate(self) -> None:
        pass


def _check(status: int, operation: str) -> None:
    if status == 404:
        raise NodeNotFoundError(operation)
    if status >= 500:
        raise SynologyServerError(f"{operation} failed", status=status)
    if status >= 400:
        raise SynologyServerError(f"{operation} returned {status}", status=status)
