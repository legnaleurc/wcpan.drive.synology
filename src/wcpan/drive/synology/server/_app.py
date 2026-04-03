"""aiohttp application factory, routes, and startup/shutdown lifecycle."""

import asyncio
import secrets
from collections.abc import AsyncGenerator, Coroutine, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from logging import getLogger

from aiohttp import ClientSession, web

from ..types import ServerConfig
from ._api.webhooks import create_webhook, delete_webhook, list_webhooks
from ._auth import AuthManager
from ._db import Storage
from ._handlers import (
    create_node,
    delete_node,
    download_node,
    get_changes,
    get_cursor,
    get_node,
    get_root,
    handle_synology_webhook,
    update_node,
    upload_node,
)
from ._keys import (
    config_key,
    folders_key,
    network_key,
    off_main_key,
    ready_key,
    storage_key,
    trigger_event_key,
    volume_map_key,
    webhook_token_key,
    write_queue_key,
)
from ._lib import OffMainThread
from ._network import Network
from ._scanner import run_scanner
from ._workers import checkpoint_worker, create_write_queue, write_worker


_L = getLogger(__name__)


@contextmanager
def _managed_pool() -> Generator[ThreadPoolExecutor, None, None]:
    pool = ThreadPoolExecutor()
    try:
        yield pool
    finally:
        pool.shutdown(wait=False, cancel_futures=True)


@asynccontextmanager
async def _background[T](
    group: asyncio.TaskGroup, c: Coroutine[None, None, T]
) -> AsyncGenerator[None, None]:
    task = group.create_task(c)
    try:
        yield
    finally:
        task.cancel()


@asynccontextmanager
async def _managed_webhook(
    network: Network, config: ServerConfig, webhook_token: str
) -> AsyncGenerator[None, None]:
    stale = await list_webhooks(network, config.webhook_app_id)
    for hook in stale:
        try:
            await delete_webhook(
                network, str(hook["webhook_id"]), config.webhook_app_id
            )
        except Exception:
            _L.warning("Failed to remove stale webhook %s", hook.get("webhook_id"))
    webhook_id = await create_webhook(
        network,
        f"{config.public_url}/api/v1/synology-webhook",
        config.webhook_app_id,
        webhook_token,
        options={"filter_events": ["TrueEvent"]},
    )
    _L.info("Webhook registered: id=%s", webhook_id)
    try:
        yield
    finally:
        try:
            await delete_webhook(network, webhook_id, config.webhook_app_id)
            _L.info("Webhook unregistered: id=%s", webhook_id)
        except Exception:
            _L.warning("Failed to unregister webhook id=%s", webhook_id)


async def _app_lifecycle(app: web.Application) -> AsyncGenerator[None, None]:
    config: ServerConfig = app[config_key]
    async with AsyncExitStack() as stack:
        session = await stack.enter_async_context(ClientSession())
        auth = AuthManager(
            username=config.username,
            password=config.password,
            base_url=config.synology_url,
            otp_code=config.otp_code,
        )
        await stack.enter_async_context(auth.authenticated(session))

        network = Network(session, auth)
        app[network_key] = network

        pool = stack.enter_context(_managed_pool())
        off_main = OffMainThread(pool)
        write_queue = create_write_queue()
        storage = Storage(config.database_url)
        app[storage_key] = storage
        app[off_main_key] = off_main
        app[write_queue_key] = write_queue

        _L.info("initializing database: %s", config.database_url)
        await off_main(storage.ensure_schema)

        trigger_event = asyncio.Event()
        app[trigger_event_key] = trigger_event

        ready_event = asyncio.Event()
        app[ready_key] = False

        async def _flip_ready() -> None:
            await ready_event.wait()
            app[ready_key] = True

        webhook_token = secrets.token_urlsafe(32)
        app[webhook_token_key] = webhook_token
        await stack.enter_async_context(
            _managed_webhook(network, config, webhook_token)
        )

        group = await stack.enter_async_context(asyncio.TaskGroup())

        await stack.enter_async_context(
            _background(group, write_worker(write_queue, off_main))
        )
        await stack.enter_async_context(
            _background(group, checkpoint_worker(write_queue, storage))
        )
        await stack.enter_async_context(
            _background(
                group,
                run_scanner(
                    network=network,
                    storage=storage,
                    folders=config.folders,
                    trigger_event=trigger_event,
                    ready_event=ready_event,
                    volume_map=config.volume_map,
                    off_main=off_main,
                    write_queue=write_queue,
                ),
            )
        )
        await stack.enter_async_context(_background(group, _flip_ready()))

        yield


def create_app(config: ServerConfig) -> web.Application:
    app = web.Application()
    app[folders_key] = config.folders
    app[volume_map_key] = config.volume_map
    app[config_key] = config
    app.cleanup_ctx.append(_app_lifecycle)

    _add_routes(app)
    return app


def _add_routes(app: web.Application) -> None:
    app.router.add_get("/api/v1/cursor", get_cursor)
    app.router.add_get("/api/v1/root", get_root)
    app.router.add_get("/api/v1/changes", get_changes)
    app.router.add_get("/api/v1/nodes/{id}", get_node)
    app.router.add_get("/api/v1/nodes/{id}/download", download_node)
    app.router.add_post("/api/v1/nodes", create_node)
    app.router.add_patch("/api/v1/nodes/{id}", update_node)
    app.router.add_delete("/api/v1/nodes/{id}", delete_node)
    app.router.add_post("/api/v1/nodes/{parent_id}/upload", upload_node)
    app.router.add_post("/api/v1/synology-webhook", handle_synology_webhook)
