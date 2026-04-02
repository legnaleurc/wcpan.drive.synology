from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, NotRequired, TypedDict

from aiohttp import ClientResponse, ClientSession

from ..exceptions import SynologyNetworkError
from ._auth import AuthManager


type HeaderDict = dict[str, str]


class _FetchParams(TypedDict):
    url: str
    method: str
    headers: dict[str, str]
    params: NotRequired[dict[str, Any]]
    data: NotRequired[AsyncIterable[bytes] | bytes]
    json: NotRequired[Any]
    timeout: NotRequired[None]


class Network:
    def __init__(self, session: ClientSession, auth: AuthManager) -> None:
        self._session = session
        self._auth = auth

    @property
    def api_base(self) -> str:
        return f"{self._auth.base_url}/api/SynologyDrive/default/v1"

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
        sid = self._auth.get_session_token()
        kwargs["headers"]["Cookie"] = f"id={sid};"

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
