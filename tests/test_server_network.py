"""Tests for NetworkService and create_network_service."""

from contextlib import asynccontextmanager
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology._server.services.network import (
    NetworkService,
    create_network_service,
)
from wcpan.drive.synology.exceptions import (
    SynologyAuthenticationError,
    SynologyNetworkError,
    SynologySessionExpiredError,
)


def _make_fetch_cm(response: MagicMock) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


def _post_cm(response: MagicMock) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestNetworkApiBase(IsolatedAsyncioTestCase):
    def test_api_base_uses_base_url(self):
        # given
        session = MagicMock()
        network = NetworkService(session, "https://nas.example", "sid")
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
        session = MagicMock()
        network = NetworkService(session, "http://h", "abc")
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
        session = MagicMock()

        @asynccontextmanager
        async def boom_cm():
            raise ConnectionError("boom")
            yield  # pragma: no cover

        session.request = MagicMock(return_value=boom_cm())
        network = NetworkService(session, "http://h", "x")
        # when / then
        with self.assertRaises(SynologyNetworkError) as ctx:
            async with network.fetch("GET", "http://u"):
                pass  # pragma: no cover
        self.assertIsInstance(ctx.exception.original_error, ConnectionError)

    async def test_no_sid_raises_session_expired(self):
        # given
        session = MagicMock()
        network = NetworkService(session, "http://h", "")
        # when / then
        with self.assertRaises(SynologySessionExpiredError):
            async with network.fetch("GET", "http://u"):
                pass  # pragma: no cover


class TestCreateNetworkService(IsolatedAsyncioTestCase):
    async def test_login_success_yields_service(self):
        # given
        mock_session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(
            return_value={"success": True, "data": {"sid": "token-99"}}
        )
        mock_session.post.return_value = _post_cm(response)
        # when
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            async with create_network_service(
                base_url="http://nas",
                username="user",
                password="secret",
            ) as network:
                # then
                self.assertIsInstance(network, NetworkService)
                self.assertEqual(network._sid, "token-99")  # noqa: SLF001

    async def test_login_non_200_raises(self):
        # given
        mock_session = MagicMock()
        response = MagicMock()
        response.status = 401
        response.json = AsyncMock(return_value={})
        mock_session.post.return_value = _post_cm(response)
        # when / then
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            with self.assertRaises(SynologyAuthenticationError):
                async with create_network_service(
                    base_url="http://nas",
                    username="u",
                    password="p",
                ):
                    pass  # pragma: no cover

    async def test_login_success_false_raises(self):
        # given
        mock_session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(
            return_value={"success": False, "error": {"code": 123}}
        )
        mock_session.post.return_value = _post_cm(response)
        # when / then
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            with self.assertRaises(SynologyAuthenticationError):
                async with create_network_service(
                    base_url="http://nas",
                    username="u",
                    password="p",
                ):
                    pass  # pragma: no cover

    async def test_login_missing_sid_raises(self):
        # given
        mock_session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"success": True, "data": {}})
        mock_session.post.return_value = _post_cm(response)
        # when / then
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            with self.assertRaises(SynologyAuthenticationError):
                async with create_network_service(
                    base_url="http://nas",
                    username="u",
                    password="p",
                ):
                    pass  # pragma: no cover

    async def test_otp_included_when_set(self):
        # given
        mock_session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"success": True, "data": {"sid": "s"}})
        mock_session.post.return_value = _post_cm(response)
        # when
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            async with create_network_service(
                base_url="http://nas",
                username="u",
                password="p",
                otp_code="000000",
            ):
                pass
            # then
            call_kw = mock_session.post.call_args_list[0].kwargs
            self.assertEqual(call_kw["json"]["otp_code"], "000000")

    async def test_logout_called_on_exit(self):
        # given
        mock_session = MagicMock()
        login_response = MagicMock()
        login_response.status = 200
        login_response.json = AsyncMock(
            return_value={"success": True, "data": {"sid": "tok"}}
        )
        logout_response = MagicMock()
        logout_response.status = 200
        mock_session.post.side_effect = [
            _post_cm(login_response),
            _post_cm(logout_response),
        ]
        # when
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            async with create_network_service(
                base_url="http://nas",
                username="u",
                password="p",
            ):
                pass
        # then — two post calls: login + logout
        self.assertEqual(mock_session.post.call_count, 2)
        logout_call = mock_session.post.call_args_list[1]
        self.assertIn("logout", logout_call.args[0])

    async def test_base_url_trailing_slash_stripped(self):
        # given
        mock_session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"success": True, "data": {"sid": "s"}})
        mock_session.post.return_value = _post_cm(response)
        # when
        with patch(
            "wcpan.drive.synology._server.services.network.ClientSession"
        ) as mock_cls:
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_session)
            cm.__aexit__ = AsyncMock(return_value=None)
            mock_cls.return_value = cm

            async with create_network_service(
                base_url="http://nas/",
                username="u",
                password="p",
            ) as network:
                # then
                self.assertEqual(
                    network.api_base,
                    "http://nas/api/SynologyDrive/default/v1",
                )
