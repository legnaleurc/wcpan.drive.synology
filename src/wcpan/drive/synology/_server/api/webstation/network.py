import json
import secrets
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

from aiohttp import ClientResponse, ClientSession

from ....exceptions import (
    SynologyApiError,
    SynologyAuthenticationError,
    SynologyNetworkError,
)


async def _login(
    session: ClientSession,
    base_url: str,
    username: str,
    password: str,
    otp_code: str | None,
) -> str:
    params: dict[str, str] = {
        "api": "SYNO.API.Auth",
        "version": "3",
        "method": "login",
        "account": username,
        "passwd": password,
        "session": "Drive",
        "format": "sid",
    }
    if otp_code:
        params["otp_code"] = otp_code

    async with session.get(f"{base_url}/webapi/auth.cgi", params=params) as response:
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
    params = {
        "api": "SYNO.API.Auth",
        "version": "3",
        "method": "logout",
        "session": "Drive",
        "_sid": sid,
    }
    with suppress(Exception):
        async with session.get(f"{base_url}/webapi/auth.cgi", params=params):
            pass


def _encode_value(v: Any) -> str:
    """JSON-stringify every value the way rest.php does for JSON APIs."""
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


class WebStationNetworkService:
    """HTTP transport for the Synology WebStation API (/webapi/entry.cgi).

    Uses DSM session auth (_sid) and query-string RPC calls.
    Values are JSON-stringified automatically for JSON requestFormat APIs.
    """

    def __init__(
        self,
        *,
        session: ClientSession,
        base_url: str,
        sid: str,
    ) -> None:
        self._session = session
        self._base_url = base_url
        self._sid = sid

    async def request(
        self,
        api: str,
        version: int,
        method: str,
        **params: Any,
    ) -> Any:
        """GET /webapi/entry.cgi and return the API response data payload."""
        form = {
            "api": api,
            "version": str(version),
            "method": method,
            "_sid": self._sid,
        }
        for k, v in params.items():
            form[k] = _encode_value(v)

        try:
            async with self._session.get(
                f"{self._base_url}/webapi/entry.cgi",
                params=form,
            ) as response:
                response.raise_for_status()
                result: dict[str, Any] = await response.json(content_type=None)
        except SynologyApiError:
            raise
        except Exception as e:
            raise SynologyNetworkError(str(e), e) from e

        if not result.get("success", False):
            error = result.get("error", {})
            code = error.get("code", 0)
            raise SynologyApiError(
                f"{api}.{method} failed with code {code}",
                error_code=code,
            )
        return result.get("data")

    @asynccontextmanager
    async def download(
        self,
        api: str,
        version: int,
        method: str,
        *,
        extra_headers: dict[str, str] | None = None,
        **params: Any,
    ) -> AsyncGenerator[ClientResponse]:
        """Streaming GET to /webapi/entry.cgi — yields the raw ClientResponse."""
        form = {
            "api": api,
            "version": str(version),
            "method": method,
            "_sid": self._sid,
        }
        for k, v in params.items():
            form[k] = _encode_value(v)

        try:
            async with self._session.get(
                f"{self._base_url}/webapi/entry.cgi",
                params=form,
                headers=extra_headers or {},
            ) as response:
                response.raise_for_status()
                yield response
        except SynologyNetworkError:
            raise
        except Exception as e:
            raise SynologyNetworkError(str(e), e) from e

    async def upload(
        self,
        api: str,
        version: int,
        method: str,
        form_fields: dict[str, Any],
        file_data: Any,
        file_name: str,
        mime_type: str,
    ) -> Any:
        """Multipart upload — API params go in query string, file in form body."""
        query = {
            "api": api,
            "version": str(version),
            "method": method,
            "_sid": self._sid,
        }
        content_type, body = _multipart_body(
            form_fields=form_fields,
            file_name=file_name,
            file_data=file_data,
            file_content_type=mime_type,
        )

        try:
            async with self._session.post(
                f"{self._base_url}/webapi/entry.cgi",
                params=query,
                headers={"Content-Type": content_type},
                data=body,
            ) as response:
                response.raise_for_status()
                result: dict[str, Any] = await response.json(content_type=None)
        except SynologyApiError:
            raise
        except Exception as e:
            raise SynologyNetworkError(str(e), e) from e

        if not result.get("success", False):
            error = result.get("error", {})
            code = error.get("code", 0)
            raise SynologyApiError(
                f"{api}.{method} failed with code {code}",
                error_code=code,
            )
        return result.get("data")


def _multipart_body(
    *,
    form_fields: dict[str, Any],
    file_name: str,
    file_data: Any,
    file_content_type: str,
) -> tuple[str, AsyncIterator[bytes]]:
    boundary = secrets.token_hex(16)
    content_type = f"multipart/form-data; boundary={boundary}"
    bnd = boundary.encode()

    async def _generate() -> AsyncIterator[bytes]:
        for key, value in form_fields.items():
            if isinstance(value, bool):
                encoded = "true" if value else "false"
            elif isinstance(value, (dict, list)):
                encoded = _encode_value(value)
            else:
                encoded = str(value)
            yield (
                b"--" + bnd + b"\r\n"
                b'Content-Disposition: form-data; name="' + key.encode() + b'"\r\n'
                b"\r\n" + encoded.encode() + b"\r\n"
            )
        yield (
            b"--" + bnd + b"\r\n"
            b'Content-Disposition: form-data; name="file"; filename="'
            + file_name.encode()
            + b'"\r\n'
            b"Content-Type: " + file_content_type.encode() + b"\r\n"
            b"\r\n"
        )
        async for chunk in file_data:
            yield chunk
        yield b"\r\n--" + bnd + b"--\r\n"

    return content_type, _generate()


@asynccontextmanager
async def create_webstation_network_service(
    *,
    base_url: str,
    username: str,
    password: str,
    otp_code: str | None = None,
) -> AsyncGenerator[WebStationNetworkService]:
    base_url = base_url.rstrip("/")
    async with ClientSession() as session:
        sid = await _login(session, base_url, username, password, otp_code)
        try:
            yield WebStationNetworkService(
                session=session,
                base_url=base_url,
                sid=sid,
            )
        finally:
            await _logout(session, base_url, sid)
