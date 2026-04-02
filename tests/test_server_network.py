"""Tests for server Network client wrapper."""

from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from wcpan.drive.synology.exceptions import SynologyNetworkError
from wcpan.drive.synology.server._auth import AuthManager
from wcpan.drive.synology.server._network import Network


def _make_fetch_cm(response: MagicMock) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestNetworkApiBase(IsolatedAsyncioTestCase):
    def test_api_base_uses_auth_url(self):
        # given
        auth = AuthManager("u", "p", "https://nas.example")
        session = MagicMock()
        network = Network(session, auth)
        # when
        base = network.api_base
        # then
        self.assertEqual(
            base,
            "https://nas.example/api/SynologyDrive/default/v1",
        )


class TestNetworkFetch(IsolatedAsyncioTestCase):
    async def test_adds_session_cookie(self):
        # given
        auth = AuthManager("u", "p", "http://h")
        auth._sid = "abc"  # noqa: SLF001
        session = MagicMock()
        network = Network(session, auth)
        response = MagicMock()
        response.raise_for_status = MagicMock()
        session.request = MagicMock(return_value=_make_fetch_cm(response))
        url = "http://h/api/x"
        # when
        async with network.fetch("GET", url):
            pass
        # then
        session.request.assert_called_once()
        call_kw = session.request.call_args.kwargs
        self.assertEqual(call_kw["headers"]["Cookie"], "id=abc;")

    async def test_wraps_generic_exception(self):
        # given
        auth = AuthManager("u", "p", "http://h")
        auth._sid = "x"  # noqa: SLF001
        session = MagicMock()

        @asynccontextmanager
        async def boom_cm():
            raise ConnectionError("boom")
            yield  # pragma: no cover

        session.request = MagicMock(return_value=boom_cm())
        network = Network(session, auth)
        # when / then
        with self.assertRaises(SynologyNetworkError) as ctx:
            async with network.fetch("GET", "http://u"):
                pass  # pragma: no cover
        self.assertIsInstance(ctx.exception.original_error, ConnectionError)
