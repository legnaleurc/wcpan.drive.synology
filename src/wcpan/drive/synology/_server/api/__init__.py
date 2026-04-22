from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from ..types import ServerConfig
from .drive import SynologyDriveApi
from .webstation import (
    WebStationSynologyDriveApi,
    create_webstation_network_service,
)


@asynccontextmanager
async def create_synology_drive_api(
    config: ServerConfig,
) -> AsyncGenerator[SynologyDriveApi, None]:
    async with create_webstation_network_service(
        base_url=config.synology_url,
        username=config.username,
        password=config.password,
        otp_code=config.otp_code,
    ) as network:
        yield WebStationSynologyDriveApi(network=network)


__all__ = ["SynologyDriveApi", "create_synology_drive_api"]
