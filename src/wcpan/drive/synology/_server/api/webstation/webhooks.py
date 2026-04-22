"""Synology WebStation webhook operations (SYNO.SynologyDrive.Webhooks v2)."""

from ..types import (
    SynologyWebhookCreateResponse,
    SynologyWebhookInfo,
    SynologyWebhookListResponse,
)
from .network import WebStationNetworkService


_WEBHOOKS_API = "SYNO.SynologyDrive.Webhooks"
_WEBHOOKS_VERSION = 2


async def create_webhook(
    url: str,
    app_id: str,
    *,
    network: WebStationNetworkService,
) -> str:
    data: SynologyWebhookCreateResponse = await network.request(
        _WEBHOOKS_API,
        _WEBHOOKS_VERSION,
        "create",
        type="url",
        url=url,
        app_id=app_id,
    )
    return data["webhook_id"]


async def delete_webhook(
    webhook_id: str,
    app_id: str,
    *,
    network: WebStationNetworkService,
) -> None:
    await network.request(
        _WEBHOOKS_API,
        _WEBHOOKS_VERSION,
        "delete",
        webhook_id=webhook_id,
        app_id=app_id,
    )


async def list_webhooks(
    app_id: str,
    *,
    network: WebStationNetworkService,
) -> list[SynologyWebhookInfo]:
    data: SynologyWebhookListResponse = await network.request(
        _WEBHOOKS_API,
        _WEBHOOKS_VERSION,
        "list",
        app_id=app_id,
    )
    return data["items"]
