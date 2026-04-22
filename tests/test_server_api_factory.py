from pathlib import PurePosixPath
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology._server.api import create_synology_drive_api
from wcpan.drive.synology._server.api.webstation import WebStationSynologyDriveApi
from wcpan.drive.synology._server.types import ServerConfig, SynologyPath


def _config() -> ServerConfig:
    return ServerConfig(
        host="127.0.0.1",
        port=8080,
        database_url="sqlite:///tmp/test.sqlite",
        synology_url="https://nas.example",
        username="user",
        password="secret",
        mounts={"docs": SynologyPath(PurePosixPath("/docs"))},
        public_url="https://public.example",
        webhook_app_id="app-id",
        local_paths={},
    )


def _cm(value: object) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=value)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestCreateSynologyDriveApi(IsolatedAsyncioTestCase):
    async def test_create_webstation_api(self) -> None:
        network = MagicMock()
        with patch(
            "wcpan.drive.synology._server.api.create_webstation_network_service",
            return_value=_cm(network),
        ) as create_network:
            async with create_synology_drive_api(_config()) as api:
                self.assertIsInstance(api, WebStationSynologyDriveApi)

        create_network.assert_called_once_with(
            base_url="https://nas.example",
            username="user",
            password="secret",
            otp_code=None,
        )
