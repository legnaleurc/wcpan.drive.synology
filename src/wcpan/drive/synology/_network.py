from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from logging import getLogger
from typing import Any, NotRequired, TypedDict

from aiohttp import ClientResponse, ClientSession

from ._auth import AuthManager
from .exceptions import SynologyNetworkError


_L = getLogger(__name__)


type HeaderDict = dict[str, str]
type QueryDict = dict[str, int | bool | str]
type ReadableContent = bytes | AsyncIterable[bytes]


class _FetchParams(TypedDict):
    url: str
    method: str
    headers: dict[str, str]
    params: NotRequired[dict[str, Any]]
    data: NotRequired[ReadableContent]
    timeout: NotRequired[None]


class Network:
    def __init__(self, session: ClientSession, auth_manager: AuthManager) -> None:
        self._session = session
        self._auth = auth_manager

    @property
    def entry_url(self) -> str:
        return f"{self._auth.base_url}/webapi/entry.cgi"

    @asynccontextmanager
    async def fetch(
        self,
        method: str,
        url: str,
        *,
        query: QueryDict | None = None,
        headers: HeaderDict | None = None,
        body: ReadableContent | None = None,
        timeout: bool = True,
    ) -> AsyncIterator[ClientResponse]:
        kwargs = _prepare_kwargs(
            method=method,
            url=url,
            query=query,
            headers=headers,
            body=body,
            timeout=timeout,
        )
        await self._add_session_token(kwargs)

        try:
            async with self._session.request(**kwargs) as response:
                response.raise_for_status()
                yield response
        except SynologyNetworkError:
            raise
        except Exception as e:
            raise SynologyNetworkError(str(e), e) from e

    async def _add_session_token(self, kwargs: _FetchParams) -> None:
        token = self._auth.get_session_token()
        if "params" not in kwargs:
            kwargs["params"] = {}
        kwargs["params"]["_sid"] = token

        syno_token = self._auth.get_syno_token()
        if syno_token:
            kwargs["headers"]["X-SYNO-TOKEN"] = syno_token


def _prepare_kwargs(
    *,
    method: str,
    url: str,
    query: QueryDict | None,
    headers: HeaderDict | None,
    body: ReadableContent | None,
    timeout: bool,
) -> _FetchParams:
    kwargs: _FetchParams = {
        "method": method,
        "url": url,
        "headers": {} if headers is None else headers,
    }

    if query is not None:
        kwargs["params"] = _normalize_query_dict(query)

    if body is not None:
        kwargs["data"] = body

    if not timeout:
        kwargs["timeout"] = None

    return kwargs


def _normalize_query_dict(qs: QueryDict) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in qs.items():
        if isinstance(value, bool):
            result[key] = "true" if value else "false"
        elif isinstance(value, int):
            result[key] = str(value)
        else:
            result[key] = value
    return result
