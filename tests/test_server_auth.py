"""Tests for AuthManager."""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from wcpan.drive.synology.exceptions import (
    SynologyAuthenticationError,
    SynologySessionExpiredError,
)
from wcpan.drive.synology.server._auth import AuthManager


def _post_cm(response: MagicMock) -> MagicMock:
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestAuthManagerLogin(IsolatedAsyncioTestCase):
    async def test_success_sets_sid(self):
        # given
        auth = AuthManager("user", "secret", "http://nas")
        session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(
            return_value={"success": True, "data": {"sid": "token-99"}}
        )
        session.post.return_value = _post_cm(response)
        # when
        await auth.login(session)
        # then
        self.assertEqual(auth.get_session_token(), "token-99")
        session.post.assert_called()

    async def test_non_200_raises(self):
        # given
        auth = AuthManager("u", "p", "http://nas")
        session = MagicMock()
        response = MagicMock()
        response.status = 401
        response.json = AsyncMock(return_value={})
        session.post.return_value = _post_cm(response)
        # when / then
        with self.assertRaises(SynologyAuthenticationError):
            await auth.login(session)

    async def test_success_false_raises(self):
        # given
        auth = AuthManager("u", "p", "http://nas")
        session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(
            return_value={"success": False, "error": {"code": 123}}
        )
        session.post.return_value = _post_cm(response)
        # when / then
        with self.assertRaises(SynologyAuthenticationError):
            await auth.login(session)

    async def test_missing_sid_raises(self):
        # given
        auth = AuthManager("u", "p", "http://nas")
        session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"success": True, "data": {}})
        session.post.return_value = _post_cm(response)
        # when / then
        with self.assertRaises(SynologyAuthenticationError):
            await auth.login(session)

    async def test_otp_included_when_set(self):
        # given
        auth = AuthManager("u", "p", "http://nas", otp_code="000000")
        session = MagicMock()
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(return_value={"success": True, "data": {"sid": "s"}})
        session.post.return_value = _post_cm(response)
        # when
        await auth.login(session)
        # then
        call_kw = session.post.call_args.kwargs
        self.assertEqual(call_kw["json"]["otp_code"], "000000")


class TestAuthManagerSession(IsolatedAsyncioTestCase):
    def test_get_token_without_login_raises(self):
        # given
        auth = AuthManager("u", "p", "http://nas")
        # when / then
        with self.assertRaises(SynologySessionExpiredError):
            auth.get_session_token()

    async def test_logout_without_sid_skips_post(self):
        # given
        auth = AuthManager("u", "p", "http://nas")
        session = MagicMock()
        # when
        await auth.logout(session)
        # then
        session.post.assert_not_called()

    async def test_logout_clears_sid(self):
        # given
        auth = AuthManager("u", "p", "http://nas")
        session = MagicMock()
        auth._sid = "old"  # noqa: SLF001
        response = MagicMock()
        response.status = 200
        session.post.return_value = _post_cm(response)
        # when
        await auth.logout(session)
        # then
        self.assertFalse(auth.is_authenticated())
