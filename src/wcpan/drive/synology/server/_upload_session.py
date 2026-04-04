"""In-memory resumable upload session management."""

import asyncio
import os
import secrets
import tempfile
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class UploadSession:
    session_id: str
    parent_id: str
    name: str
    total_size: int
    mime_type: str | None
    client_query: dict[str, str]
    temp_path: Path
    received: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False, compare=False)


class UploadSessionStore:
    def __init__(self, tmp_dir: Path | None = None) -> None:
        self._sessions: dict[str, UploadSession] = {}
        self._tmp_dir = tmp_dir

    def create(
        self,
        parent_id: str,
        name: str,
        total_size: int,
        mime_type: str | None,
        client_query: dict[str, str],
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
            client_query=client_query,
            temp_path=Path(tmp_str),
        )
        self._sessions[session_id] = session
        return session

    def get(self, session_id: str) -> UploadSession | None:
        return self._sessions.get(session_id)

    def remove(self, session_id: str) -> UploadSession | None:
        return self._sessions.pop(session_id, None)

    def close_all(self) -> None:
        for session in list(self._sessions.values()):
            delete_temp_sync(session.temp_path)
        self._sessions.clear()


def parse_content_range(header: str | None) -> tuple[int, int, int] | None:
    """Parse 'bytes {start}-{end}/{total}'. Returns (start, end, total) or None."""
    if not header or not header.startswith("bytes "):
        return None
    rest = header[6:]
    if "/" not in rest:
        return None
    range_part, total_str = rest.split("/", 1)
    if "-" not in range_part:
        return None
    start_str, end_str = range_part.split("-", 1)
    try:
        return int(start_str), int(end_str), int(total_str)
    except ValueError:
        return None


def write_chunk_sync(path: Path, offset: int, data: bytes) -> None:
    """Write data at offset in the temp file (blocking)."""
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)


def read_chunk_sync(path: Path, offset: int, size: int) -> bytes:
    """Read up to size bytes from path at offset (blocking)."""
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(size)


def delete_temp_sync(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
