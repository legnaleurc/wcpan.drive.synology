from typing import Any

from ...exceptions import SynologyNetworkError
from ..services.network import NetworkService


async def create_webhook(
    network: NetworkService,
    url: str,
    app_id: str,
) -> str:
    body: dict[str, Any] = {"type": "url", "url": url, "app_id": app_id}
    async with network.fetch(
        "POST",
        f"{network.api_base}/webhooks",
        json=body,
    ) as resp:
        data = await resp.json()
    if "data" not in data:
        raise SynologyNetworkError(
            f"Webhook creation failed: {data.get('error', data)}"
        )
    return str(data["data"]["webhook_id"])


async def delete_webhook(
    network: NetworkService,
    webhook_id: str,
    app_id: str,
) -> None:
    async with network.fetch(
        "DELETE",
        f"{network.api_base}/webhooks/{webhook_id}/{app_id}",
    ):
        pass


async def list_webhooks(
    network: NetworkService,
    app_id: str,
) -> list[dict[str, Any]]:
    async with network.fetch(
        "GET",
        f"{network.api_base}/webhooks/{app_id}",
    ) as resp:
        data = await resp.json()
    return list(data.get("data", {}).get("items", []))
