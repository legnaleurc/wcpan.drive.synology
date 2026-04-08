from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any, NotRequired, TypedDict

from aiohttp import ClientResponse, ClientSession

from ...exceptions import (
    SynologyAuthenticationError,
    SynologyNetworkError,
    SynologySessionExpiredError,
)


type HeaderDict = dict[str, str]


class _FetchParams(TypedDict):
    url: str
    method: str
    headers: dict[str, str]
    params: NotRequired[dict[str, Any]]
    data: NotRequired[AsyncIterable[bytes] | bytes]
    json: NotRequired[Any]
    timeout: NotRequired[None]


async def _login(
    session: ClientSession,
    base_url: str,
    username: str,
    password: str,
    otp_code: str | None,
) -> str:
    url = f"{base_url}/api/SynologyDrive/default/v1/login"
    body: dict[str, str] = {
        "format": "sid",
        "account": username,
        "passwd": password,
    }
    if otp_code:
        body["otp_code"] = otp_code

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
    return str(sid)


async def _logout(session: ClientSession, base_url: str, sid: str) -> None:
    url = f"{base_url}/api/SynologyDrive/default/v1/logout"
    with suppress(Exception):
        async with session.post(url, json={"_sid": sid}):
            pass


class NetworkService:
    def __init__(self, session: ClientSession, base_url: str, sid: str) -> None:
        self._session = session
        self._base_url = base_url
        self._sid = sid

    @property
    def api_base(self) -> str:
        return f"{self._base_url}/api/SynologyDrive/default/v1"

    @asynccontextmanager
    async def fetch(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: HeaderDict | None = None,
        body: AsyncIterable[bytes] | bytes | None = None,
        json: Any | None = None,
        timeout: bool = True,
    ) -> AsyncIterator[ClientResponse]:
        kwargs: _FetchParams = {
            "method": method,
            "url": url,
            "headers": dict(headers) if headers else {},
        }

        # Add session cookie
        if not self._sid:
            raise SynologySessionExpiredError("No session token available")
        kwargs["headers"]["Cookie"] = f"id={self._sid};"

        if params is not None:
            kwargs["params"] = params

        if body is not None:
            kwargs["data"] = body

        if json is not None:
            kwargs["json"] = json

        if not timeout:
            kwargs["timeout"] = None

        try:
            async with self._session.request(**kwargs) as response:
                response.raise_for_status()
                yield response
        except SynologyNetworkError:
            raise
        except Exception as e:
            raise SynologyNetworkError(str(e), e) from e


@asynccontextmanager
async def create_network_service(
    *,
    base_url: str,
    username: str,
    password: str,
    otp_code: str | None = None,
) -> AsyncIterator[NetworkService]:
    base_url = base_url.rstrip("/")
    async with ClientSession() as session:
        sid = await _login(session, base_url, username, password, otp_code)
        try:
            yield NetworkService(session, base_url, sid)
        finally:
            await _logout(session, base_url, sid)
