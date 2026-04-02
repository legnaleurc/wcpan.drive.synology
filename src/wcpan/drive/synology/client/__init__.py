from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiohttp import ClientSession
from wcpan.drive.core.types import FileService

from ._service import ClientFileService


@asynccontextmanager
async def create_service(*, server_url: str) -> AsyncIterator[FileService]:
    """Create a FileService that talks to a wcpan.drive.synology server."""
    async with ClientSession() as session:
        yield ClientFileService(session, server_url)
