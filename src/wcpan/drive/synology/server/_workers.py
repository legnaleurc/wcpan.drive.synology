import asyncio
import os
from collections.abc import Callable
from logging import getLogger

from ._db import Storage
from ._lib import OffMainThread
from ._types import WriteQueue


_L = getLogger(__name__)

_WAL_CHECKPOINT_INTERVAL = 300.0  # seconds


async def run_queued_write(write_queue: WriteQueue, op: Callable[[], None]) -> None:
    """Enqueue a nullary DB mutation; await until the write worker has run it."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[None] = loop.create_future()

    def job() -> None:
        try:
            op()
        except BaseException as exc:
            loop.call_soon_threadsafe(fut.set_exception, exc)
        else:
            loop.call_soon_threadsafe(fut.set_result, None)

    await write_queue.put(job)
    await fut


def create_write_queue() -> WriteQueue:
    n = os.process_cpu_count() or 1
    return asyncio.Queue(maxsize=max(8, n * 2))


async def checkpoint_worker(write_queue: WriteQueue, storage: Storage) -> None:
    while True:
        await asyncio.sleep(_WAL_CHECKPOINT_INTERVAL)
        await write_queue.put(storage.checkpoint)


async def write_worker(write_queue: WriteQueue, off_main: OffMainThread) -> None:
    while True:
        task = await write_queue.get()
        try:
            await off_main(task)
        except Exception:
            _L.exception("write task failed")
            raise
        finally:
            write_queue.task_done()
