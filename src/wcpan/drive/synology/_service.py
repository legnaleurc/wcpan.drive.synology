"""
Main FileService implementation for Synology Drive.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from logging import getLogger
from pathlib import PurePosixPath
from typing import Literal, TypedDict, override

from aiohttp import ClientSession
from wcpan.drive.core.exceptions import NodeExistsError
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

from ._auth import AuthManager
from ._hasher import create_hasher
from ._lib import node_from_api, node_path
from ._network import Network
from ._readable import SynologyReadableFile
from ._writable import create_writable


_L = getLogger(__name__)

# The virtual super-root ID used by wcpan.drive.feed.
_SUPER_ROOT_ID = "00000000-0000-0000-0000-000000000000"


@asynccontextmanager
async def create_service(
    *,
    username: str,
    password: str,
    base_url: str,
    feed_url: str,
    path_map: dict[str, str] | None = None,
    otp_code: str | None = None,
) -> AsyncIterator[FileService]:
    auth = AuthManager(username, password, base_url, otp_code)

    async with ClientSession() as session:
        await auth.ensure_authenticated(session)
        network = Network(session, auth)
        service = SynologyDriveFileService(
            network, auth, session, feed_url, path_map or {}
        )
        try:
            yield service
        finally:
            await auth.logout(session)


class SynologyDriveFileService(FileService):
    def __init__(
        self,
        network: Network,
        auth: AuthManager,
        session: ClientSession,
        feed_url: str,
        path_map: dict[str, str],
    ) -> None:
        self._network = network
        self._auth = auth
        self._session = session
        self._feed_url = feed_url.rstrip("/")
        # Sort by key length descending for longest-prefix-first matching.
        self._path_map = sorted(
            path_map.items(), key=lambda kv: len(kv[0]), reverse=True
        )

    @property
    @override
    def api_version(self) -> int:
        return 5

    @override
    async def get_initial_cursor(self) -> str:
        return "0"

    @override
    async def get_root(self) -> Node:
        async with self._session.get(f"{self._feed_url}/api/v1/root") as response:
            record = await response.json()
        return _node_from_feed_record(record, synology_path=None)

    @override
    async def get_changes(
        self,
        cursor: str,
    ) -> AsyncIterator[tuple[list[ChangeAction], str]]:
        async with self._session.get(
            f"{self._feed_url}/api/v1/changes",
            params={"cursor": cursor},
        ) as response:
            data = await response.json()

        new_cursor = str(data["cursor"])
        raw_changes: list[_FeedRemoveChange | _FeedUpdateChange] = data["changes"]

        # path_cache: feed UUID → Synology path (None for the virtual super-root)
        path_cache: dict[str, PurePosixPath | None] = {_SUPER_ROOT_ID: None}
        changes: list[ChangeAction] = []

        for raw in raw_changes:
            match raw:
                case {"removed": True, "node_id": node_id}:
                    changes.append((True, node_id))
                case {"removed": False, "node": record}:
                    synology_path = await self._child_path(
                        record["id"], record["parent_id"], record["name"], path_cache
                    )
                    path_cache[record["id"]] = synology_path
                    changes.append(
                        (
                            False,
                            _node_from_feed_record(record, synology_path=synology_path),
                        )
                    )
                case _:
                    raise ValueError(f"unexpected change record: {raw!r}")

        yield changes, new_cursor

    async def _child_path(
        self,
        node_id: str,
        parent_id: str | None,
        name: str,
        path_cache: dict[str, PurePosixPath | None],
    ) -> PurePosixPath:
        if parent_id is None:
            return await self._feed_path(node_id)

        parent_path = path_cache.get(parent_id)

        if parent_path is None:
            # Parent is super-root or not in current batch → ask feed directly.
            return await self._feed_path(node_id)

        return parent_path / name

    async def _feed_path(self, node_id: str) -> PurePosixPath:
        async with self._session.get(
            f"{self._feed_url}/api/v1/nodes/{node_id}/path"
        ) as resp:
            data = await resp.json()
        raw: str = data["path"]
        for feed_prefix, synology_prefix in self._path_map:
            if PurePosixPath(raw).is_relative_to(feed_prefix):
                return PurePosixPath(synology_prefix) / PurePosixPath(raw).relative_to(
                    feed_prefix
                )
        return PurePosixPath(raw)

    @override
    async def move(
        self,
        node: Node,
        *,
        new_parent: Node | None,
        new_name: str | None,
    ) -> Node:
        from ._api import files

        if not node.parent_id:
            raise ValueError("Cannot move root node")

        new_parent_path = node_path(new_parent) if new_parent else None
        updated_dict = await files.update_file(
            self._network,
            file_path=node_path(node),
            new_name=new_name,
            new_parent_path=new_parent_path,
        )
        return node_from_api(updated_dict)

    @override
    async def delete(self, node: Node, *, permanent: bool = False) -> None:
        from ._api import files

        await files.delete_file(self._network, node_path(node))

    @override
    async def restore(self, node: Node) -> Node:
        raise NotImplementedError("restore is not supported on Synology")

    @override
    async def purge_trash(self) -> None:
        _L.warning("Purge trash not implemented for Synology")

    @override
    async def create_directory(
        self,
        name: str,
        parent: Node,
        *,
        exist_ok: bool,
        private: PrivateDict | None,
    ) -> Node:
        from ._api import folders

        parent_path = node_path(parent)

        try:
            existing = await folders.get_folder_info(self._network, parent_path / name)

            if exist_ok:
                _L.info(f"Skipped (existing) {name}")
                return node_from_api(existing)
            else:
                raise NodeExistsError(node_from_api(existing))

        except NodeExistsError:
            raise
        except Exception:
            pass

        folder_dict = await folders.create_folder(
            self._network,
            name=name,
            parent_path=parent_path,
        )
        return node_from_api(folder_dict)

    @asynccontextmanager
    @override
    async def download_file(self, node: Node) -> AsyncIterator[ReadableFile]:
        async with SynologyReadableFile(self._network, node) as fin:
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
        parent_path = node_path(parent)

        try:
            from ._api import files

            existing = await files.get_file_info(self._network, parent_path / name)
            raise NodeExistsError(node_from_api(existing))

        except Exception as e:
            if isinstance(e, NodeExistsError):
                raise
            pass

        async with create_writable(
            network=self._network,
            parent_path=parent_path,
            name=name,
            size=size,
            mime_type=mime_type,
            media_info=media_info,
            private=private,
        ) as fout:
            yield fout

    @override
    async def get_hasher_factory(self) -> CreateHasher:
        return create_hasher

    @override
    async def is_authenticated(self) -> bool:
        return self._auth.is_authenticated()

    @override
    async def authenticate(self) -> None:
        await self._auth.ensure_authenticated(self._session)

    @property
    def network(self) -> Network:
        return self._network


class _FeedRemoveChange(TypedDict):
    removed: Literal[True]
    node_id: str


class _FeedUpdateChange(TypedDict):
    removed: Literal[False]
    node: "_FeedRecord"


class _FeedRecord(TypedDict):
    id: str
    name: str
    parent_id: str | None
    is_directory: bool
    mime_type: str | None
    hash: str | None
    size: int | None
    is_image: bool
    is_video: bool
    width: int | None
    height: int | None
    ms_duration: int | None
    ctime: str
    mtime: str


def _node_from_feed_record(
    record: _FeedRecord,
    *,
    synology_path: PurePosixPath | None,
) -> Node:
    """Convert a wcpan.drive.feed NodeRecord dict to a wcpan.drive.core Node."""
    ctime = datetime.fromisoformat(record["ctime"])
    mtime = datetime.fromisoformat(record["mtime"])
    if ctime.tzinfo is None:
        ctime = ctime.replace(tzinfo=UTC)
    if mtime.tzinfo is None:
        mtime = mtime.replace(tzinfo=UTC)

    return Node(
        id=record["id"],
        name=record["name"],
        is_trashed=False,
        ctime=ctime,
        mtime=mtime,
        parent_id=record["parent_id"],
        is_directory=record["is_directory"],
        mime_type=record["mime_type"] or "",
        hash=record["hash"] or "",
        size=record["size"] or 0,
        is_image=record["is_image"],
        is_video=record["is_video"],
        width=record["width"] or 0,
        height=record["height"] or 0,
        ms_duration=record["ms_duration"] or 0,
        private={"path": str(synology_path)} if synology_path is not None else None,
    )
