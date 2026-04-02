from .._network import Network


async def create_webhook(
    network: Network,
    url: str,
    app_id: str,
    token: str | None,
) -> str:
    body: dict = {"type": "url", "url": url, "app_id": app_id}
    if token is not None:
        body["token"] = token
    async with network.fetch(
        "POST",
        f"{network.api_base}/webhooks",
        json=body,
    ) as resp:
        data = await resp.json()
    return str(data["data"]["webhook_id"])


async def delete_webhook(
    network: Network,
    webhook_id: str,
    app_id: str,
) -> None:
    async with network.fetch(
        "DELETE",
        f"{network.api_base}/webhooks/{webhook_id}/{app_id}",
    ):
        pass


async def list_webhooks(
    network: Network,
    app_id: str,
) -> list[dict]:
    async with network.fetch(
        "GET",
        f"{network.api_base}/webhooks/{app_id}",
    ) as resp:
        data = await resp.json()
    return list(data.get("data", {}).get("hooks", []))
