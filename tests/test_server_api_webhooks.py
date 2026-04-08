"""Tests for Synology webhook API helpers."""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from wcpan.drive.synology._server.api.webhooks import (
    create_webhook,
    delete_webhook,
    list_webhooks,
)
from wcpan.drive.synology._server.services.network import NetworkService
from wcpan.drive.synology.exceptions import SynologyNetworkError


def _fetch_cm(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json = AsyncMock(return_value=payload)
    response.raise_for_status = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestCreateWebhook(IsolatedAsyncioTestCase):
    async def test_returns_webhook_id(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        payload = {"data": {"webhook_id": "wh-123"}}
        network.fetch = MagicMock(return_value=_fetch_cm(payload))
        wid = await create_webhook(network, "https://me/webhook", "app-1")
        self.assertEqual(wid, "wh-123")
        args, kwargs = network.fetch.call_args
        self.assertEqual(args[0], "POST")
        self.assertIn("/webhooks", args[1])
        self.assertEqual(kwargs["json"]["url"], "https://me/webhook")
        self.assertEqual(kwargs["json"]["app_id"], "app-1")

    async def test_raises_on_error_response(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        payload = {"success": False, "error": {"code": 1000}}
        network.fetch = MagicMock(return_value=_fetch_cm(payload))
        with self.assertRaises(SynologyNetworkError):
            await create_webhook(network, "https://me/webhook", "app-1")


class TestDeleteWebhook(IsolatedAsyncioTestCase):
    async def test_calls_correct_endpoint(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        network.fetch = MagicMock(return_value=_fetch_cm({}))
        await delete_webhook(network, "wh-123", "app-1")
        args = network.fetch.call_args[0]
        self.assertEqual(args[0], "DELETE")
        self.assertIn("/webhooks/wh-123/app-1", args[1])


class TestListWebhooks(IsolatedAsyncioTestCase):
    async def test_returns_hooks_list(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        payload = {"data": {"items": [{"id": "wh-1"}, {"id": "wh-2"}]}}
        network.fetch = MagicMock(return_value=_fetch_cm(payload))
        hooks = await list_webhooks(network, "app-1")
        self.assertEqual(len(hooks), 2)
        self.assertEqual(hooks[0]["id"], "wh-1")
        args = network.fetch.call_args[0]
        self.assertEqual(args[0], "GET")
        self.assertIn("/webhooks/app-1", args[1])

    async def test_empty_result(self):
        network = MagicMock(spec=NetworkService)
        network.api_base = "http://h/api/SynologyDrive/default/v1"
        payload = {"data": {}}
        network.fetch = MagicMock(return_value=_fetch_cm(payload))
        hooks = await list_webhooks(network, "app-1")
        self.assertEqual(hooks, [])
