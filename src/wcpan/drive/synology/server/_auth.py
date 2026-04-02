import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from aiohttp import ClientSession

from ..exceptions import SynologyAuthenticationError, SynologySessionExpiredError


class AuthManager:
    def __init__(
        self,
        username: str,
        password: str,
        base_url: str,
        otp_code: str | None = None,
    ) -> None:
        self._username = username
        self._password = password
        self._base_url = base_url.rstrip("/")
        self._otp_code = otp_code
        self._sid: str | None = None
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def authenticated(
        self, session: ClientSession
    ) -> AsyncIterator["AuthManager"]:
        try:
            await self.login(session)
            yield self
        finally:
            with suppress(Exception):
                await self.logout(session)

    async def login(self, session: ClientSession) -> None:
        async with self._lock:
            url = f"{self._base_url}/api/SynologyDrive/default/v1/login"
            body: dict = {
                "format": "sid",
                "account": self._username,
                "passwd": self._password,
            }
            if self._otp_code:
                body["otp_code"] = self._otp_code

            async with session.post(url, json=body) as response:
                if response.status != 200:
                    raise SynologyAuthenticationError(
                        f"Login request failed with status {response.status}"
                    )
                data = await response.json()

            if not data.get("success", False):
                error = data.get("error", {})
                raise SynologyAuthenticationError(
                    f"Login failed with error code: {error.get('code', 'unknown')}"
                )

            sid = data.get("data", {}).get("sid")
            if not sid:
                raise SynologyAuthenticationError("No session token in response")
            self._sid = sid

    async def logout(self, session: ClientSession) -> None:
        async with self._lock:
            if not self._sid:
                return
            sid = self._sid
            self._sid = None

        url = f"{self._base_url}/api/SynologyDrive/default/v1/logout"
        with suppress(Exception):
            async with session.post(url, json={"_sid": sid}):
                pass

    def get_session_token(self) -> str:
        if not self._sid:
            raise SynologySessionExpiredError("No session token available")
        return self._sid

    async def ensure_authenticated(self, session: ClientSession) -> None:
        if not self._sid:
            await self.login(session)

    def is_authenticated(self) -> bool:
        return self._sid is not None

    @property
    def base_url(self) -> str:
        return self._base_url
