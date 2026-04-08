"""In-memory resumable upload session management."""

import os
import secrets
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from wcpan.drive.core.types import MediaInfo


@dataclass
class UploadSession:
    session_id: str
    parent_id: str
    name: str
    total_size: int
    mime_type: str | None
    media_info: MediaInfo | None
    temp_path: Path
    received: int = 0
    uploading: bool = False


class UploadSessionService:
    def __init__(self, tmp_dir: Path) -> None:
        self._sessions: dict[str, UploadSession] = {}
        self._tmp_dir = tmp_dir

    def create(
        self,
        parent_id: str,
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


@contextmanager
def create_upload_session_service(
    tmp_dir: Path | None = None,
) -> Generator[UploadSessionService, None, None]:
    with tempfile.TemporaryDirectory(
        prefix="wcpan_upload_",
        dir=tmp_dir,
    ) as td:
        service = UploadSessionService(tmp_dir=Path(td))
        try:
            yield service
        finally:
            service.close_all()
