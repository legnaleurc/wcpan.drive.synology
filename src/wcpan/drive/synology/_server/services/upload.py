"""Upload workflow service plus in-memory resumable session store."""

import os
import secrets
import tempfile
from collections.abc import AsyncIterable, Generator
from contextlib import contextmanager
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path

from aiofile import async_open
from wcpan.drive.core.types import MediaInfo

from ...exceptions import (
    SynologyNetworkError,
    SynologyUploadConflictError,
    SynologyUploadError,
)
from ...types import MirrorStableId, NodeRecord
from ..api.drive import SynologyDriveApi
from ..api.lib import convert_file_info
from ..api.types import SynologyFileInfo
from ..lib.names import normalize_name
from ..services.paths import SynologyPathService
from ..services.sync import NodeSyncService


_L = getLogger(__name__)

FILE_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB for both reads and writes to temp file


@dataclass
class UploadSession:
    session_id: str
    parent_id: MirrorStableId
    name: str
    total_size: int
    mime_type: str | None
    media_info: MediaInfo | None
    temp_path: Path
    received: int = 0
    uploading: bool = False


class UploadSessionStore:
    def __init__(self, *, tmp_dir: Path) -> None:
        self._sessions: dict[str, UploadSession] = {}
        self._tmp_dir = tmp_dir

    def create(
        self,
        parent_id: MirrorStableId,
        name: str,
        total_size: int,
        mime_type: str | None,
        media_info: MediaInfo | None,
    ) -> UploadSession:
        session_id = secrets.token_urlsafe(32)
        fd, tmp_str = tempfile.mkstemp(
            prefix="wcpan_upload_",
            suffix=".tmp",
            dir=self._tmp_dir,
        )
        os.close(fd)
        session = UploadSession(
            session_id=session_id,
            parent_id=parent_id,
            name=name,
            total_size=total_size,
            mime_type=mime_type,
            media_info=media_info,
            temp_path=Path(tmp_str),
        )
        self._sessions[session_id] = session
        return session

    def get(self, session_id: str) -> UploadSession | None:
        return self._sessions.get(session_id)

    def remove(self, session_id: str) -> UploadSession | None:
        return self._sessions.pop(session_id, None)

    def close_all(self) -> None:
        self._sessions.clear()


@dataclass
class SessionStatus:
    received: int
    total_size: int


@dataclass
class UploadCreated:
    session_id: str
    total_size: int


@dataclass
class UploadAccepted:
    offset: int


@dataclass
class UploadCreatedNode:
    record: NodeRecord


class UploadSessionNotFoundError(Exception):
    pass


class UploadInvalidChunkError(Exception):
    pass


class UploadTransientError(Exception):
    pass


class UploadConflictError(Exception):
    def __init__(self, record: NodeRecord) -> None:
        super().__init__()
        self.record = record


class UploadOffsetMismatchError(Exception):
    def __init__(self, offset: int) -> None:
        super().__init__()
        self.offset = offset


class UploadService:
    def __init__(
        self,
        *,
        store: UploadSessionStore,
        node_sync: NodeSyncService,
        drive_api: SynologyDriveApi,
        syno_paths: SynologyPathService,
    ) -> None:
        self._store = store
        self._node_sync = node_sync
        self._drive_api = drive_api
        self._syno_paths = syno_paths

    async def create_session(
        self,
        parent_id: MirrorStableId,
        name: str,
        total_size: int,
        mime_type: str | None,
        media_info: MediaInfo | None,
    ) -> UploadCreated:
        name = normalize_name(name)
        existing = await self._syno_paths.find_child_by_name(
            self._drive_api, parent_id, name
        )
        if existing is not None:
            conflict_record = convert_file_info(existing, parent_id)
            if conflict_record is not None:
                raise UploadConflictError(conflict_record)
            _L.warning(
                "Conflict detected for %r under %r but existing node lacks permanent_link; proceeding",
                name,
                parent_id,
            )
        session = self._store.create(parent_id, name, total_size, mime_type, media_info)
        return UploadCreated(session_id=session.session_id, total_size=total_size)

    def get_session_status(self, session_id: str) -> SessionStatus | None:
        session = self._store.get(session_id)
        if session is None:
            return None
        return SessionStatus(received=session.received, total_size=session.total_size)

    def delete_session(self, session_id: str) -> bool:
        session = self._store.remove(session_id)
        if session is None:
            return False
        session.temp_path.unlink(missing_ok=True)
        return True

    async def upload_direct(
        self,
        parent_id: MirrorStableId,
        name: str,
        content: AsyncIterable[bytes],
        mime_type: str | None,
        media_info: MediaInfo | None,
    ) -> UploadCreatedNode:
        name = normalize_name(name)
        existing = await self._syno_paths.find_child_by_name(
            self._drive_api, parent_id, name
        )
        if existing is not None:
            conflict_record = convert_file_info(existing, parent_id)
            if conflict_record is not None:
                raise UploadConflictError(conflict_record)
            _L.warning(
                "Conflict detected for %r under %r but existing node lacks permanent_link; proceeding",
                name,
                parent_id,
            )
        parent_ref = await self._syno_paths.synology_parent_ref(parent_id)
        try:
            info = await self._drive_api.upload_file(
                parent_ref=parent_ref,
                name=name,
                data=content,
                mime_type=mime_type,
            )
        except SynologyUploadConflictError:
            record = await self._resolve_name_conflict(
                parent_id=parent_id,
                name=name,
            )
            raise UploadConflictError(record)
        except (SynologyUploadError, SynologyNetworkError) as e:
            raise UploadTransientError(str(e)) from e
        return UploadCreatedNode(
            record=await self._upsert_uploaded_node(
                info=info,
                parent_id=parent_id,
                media_info=media_info,
            )
        )

    async def append_chunk(
        self,
        session_id: str,
        start: int,
        content: AsyncIterable[bytes],
    ) -> UploadAccepted | UploadCreatedNode:
        session = self._store.get(session_id)
        if session is None:
            raise UploadSessionNotFoundError()

        if session.uploading or start != session.received:
            raise UploadOffsetMismatchError(session.received)

        session.uploading = True
        try:
            async with async_open(session.temp_path, "r+b") as f:
                f.seek(session.received)
                async for chunk in content:
                    if session.received + len(chunk) > session.total_size:
                        raise UploadInvalidChunkError(
                            "Upload-Offset exceeds Upload-Length"
                        )
                    written = await f.write(chunk)
                    if written != len(chunk):
                        raise UploadTransientError(
                            f"Partial write: expected {len(chunk)} bytes, wrote {written}"
                        )
                    session.received += len(chunk)
        finally:
            session.uploading = False

        if session.received != session.total_size:
            return UploadAccepted(offset=session.received)

        return await self._finalize_session(session_id)

    async def _finalize_session(
        self,
        session_id: str,
    ) -> UploadCreatedNode:
        session = self._store.get(session_id)
        if session is None:
            raise UploadSessionNotFoundError()

        _L.debug(
            "begin finalising upload session session_id=%r name=%r parent_id=%r",
            session_id,
            session.name,
            session.parent_id,
        )

        parent_ref = await self._syno_paths.synology_parent_ref(session.parent_id)
        current_size = session.temp_path.stat().st_size
        if current_size != session.received or current_size != session.total_size:
            _L.warning(
                "upload session size mismatch before finalisation session_id=%r name=%r parent_id=%r received=%d file_size=%d total_size=%d",
                session_id,
                session.name,
                session.parent_id,
                session.received,
                current_size,
                session.total_size,
            )
            raise UploadTransientError("Upload session size mismatch")

        try:
            async with async_open(session.temp_path, "rb") as f:
                info = await self._drive_api.upload_file(
                    parent_ref=parent_ref,
                    name=session.name,
                    data=f.iter_chunked(FILE_CHUNK_SIZE),
                    mime_type=session.mime_type,
                )
        except SynologyUploadConflictError:
            _L.debug(
                "upload session conflicted during finalisation session_id=%r name=%r parent_id=%r",
                session_id,
                session.name,
                session.parent_id,
            )
            self._cleanup_session(session_id)
            record = await self._resolve_name_conflict(
                parent_id=session.parent_id,
                name=session.name,
            )
            raise UploadConflictError(record)
        except (SynologyUploadError, SynologyNetworkError) as e:
            _L.warning(
                "upload session failed during finalisation session_id=%r name=%r parent_id=%r error=%s",
                session_id,
                session.name,
                session.parent_id,
                e,
            )
            raise UploadTransientError(str(e)) from e

        record = await self._upsert_uploaded_node(
            info=info,
            parent_id=session.parent_id,
            media_info=session.media_info,
        )
        self._cleanup_session(session_id)

        _L.debug(
            "finalised upload session session_id=%r name=%r parent_id=%r id=%r",
            session_id,
            session.name,
            session.parent_id,
            record.id,
        )
        return UploadCreatedNode(record=record)

    async def _resolve_name_conflict(
        self,
        *,
        parent_id: MirrorStableId,
        name: str,
    ) -> NodeRecord:
        name = normalize_name(name)
        info = await self._syno_paths.find_child_by_name(
            self._drive_api,
            parent_id,
            name,
        )
        if info is None:
            raise SynologyUploadConflictError(
                f"Name conflict for {name!r} but existing node could not be resolved",
                file_name=name,
            )
        record = convert_file_info(info, parent_id)
        if record is None:
            raise SynologyUploadConflictError(
                f"Name conflict for {name!r} but existing node lacks permanent_link",
                file_name=name,
            )
        return record

    async def _upsert_uploaded_node(
        self,
        *,
        info: SynologyFileInfo,
        parent_id: MirrorStableId,
        media_info: MediaInfo | None,
    ) -> NodeRecord:
        record = convert_file_info(info, parent_id)
        if record is None:
            raise SynologyUploadError(
                f"Uploaded node {info.get('name', '?')!r} lacks permanent_link in API response",
                file_name=info.get("name", ""),
            )
        if media_info is not None:
            record = NodeRecord(
                id=record.id,
                parent_id=record.parent_id,
                name=record.name,
                is_directory=record.is_directory,
                created_time=record.created_time,
                modified_time=record.modified_time,
                changed_time=record.changed_time,
                mime_type=record.mime_type,
                hash=record.hash,
                size=record.size,
                is_image=media_info.is_image,
                is_video=media_info.is_video,
                width=media_info.width,
                height=media_info.height,
                ms_duration=media_info.ms_duration,
                mutable_id=record.mutable_id,
            )
        return await self._node_sync.upsert(record)

    def _cleanup_session(self, session_id: str) -> None:
        session = self._store.remove(session_id)
        if session is not None:
            session.temp_path.unlink(missing_ok=True)


@contextmanager
def create_upload_service(
    *,
    tmp_dir: Path | None,
    node_sync: NodeSyncService,
    drive_api: SynologyDriveApi,
    syno_paths: SynologyPathService,
) -> Generator[UploadService, None, None]:
    with tempfile.TemporaryDirectory(
        prefix="wcpan_upload_",
        dir=tmp_dir,
    ) as td:
        store = UploadSessionStore(tmp_dir=Path(td))
        try:
            yield UploadService(
                store=store,
                node_sync=node_sync,
                drive_api=drive_api,
                syno_paths=syno_paths,
            )
        finally:
            store.close_all()
