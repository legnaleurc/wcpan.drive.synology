import asyncio
import os
from collections.abc import Awaitable, Callable
from logging import getLogger

from .services.storage import StorageService
from .types import MetadataQueue, MetadataWorkItem, WebhookQueue, WriteQueue


_L = getLogger(__name__)

_WAL_CHECKPOINT_INTERVAL = 300.0  # seconds

METADATA_WORKER_COUNT = max(2, os.process_cpu_count() or 1)


def create_write_queue() -> WriteQueue:
    n = os.process_cpu_count() or 1
    return asyncio.Queue(maxsize=max(8, n * 2))


def create_webhook_queue() -> WebhookQueue:
    return asyncio.Queue()


_METADATA_QUEUE_MAXSIZE = max(16, (os.process_cpu_count() or 1) * 2)


def create_metadata_queue() -> MetadataQueue:
    return asyncio.Queue(maxsize=_METADATA_QUEUE_MAXSIZE)


async def metadata_worker(
    metadata_queue: MetadataQueue,
    process_item: Callable[[MetadataWorkItem], Awaitable[None]],
) -> None:
    """Single consumer: async enrichment, then enqueue sync write jobs."""
    while True:
        item = await metadata_queue.get()
        try:
            await process_item(item)
        except Exception:
            _L.exception("metadata item failed: %s", item.record.node_id)
        finally:
            metadata_queue.task_done()


async def checkpoint_worker(storage: StorageService) -> None:
    while True:
        await asyncio.sleep(_WAL_CHECKPOINT_INTERVAL)
        await storage.checkpoint()


# Let-it-crash on write failure by design.
# Write failure in sqlite likely means the system is unstable.
# Discard any in-memory data, and let the next initialization recover the state.
async def write_worker(write_queue: WriteQueue) -> None:
    while True:
        task = await write_queue.get()
        try:
            await task()
        except Exception:
            _L.exception("write task failed")
            raise
        finally:
            write_queue.task_done()
