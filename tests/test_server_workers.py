"""Tests for write queue helpers."""

import asyncio
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology.server._workers import run_queued_write


class TestRunQueuedWrite(IsolatedAsyncioTestCase):
    async def test_runs_op_when_worker_consumes_job(self):
        # given
        q: asyncio.Queue = asyncio.Queue()
        seen: list[int] = []

        async def producer() -> None:
            await run_queued_write(q, lambda: seen.append(42))

        prod = asyncio.create_task(producer())
        # when
        job = await q.get()
        job()
        q.task_done()
        await prod
        # then
        self.assertEqual(seen, [42])

    async def test_propagates_exception_from_op(self):
        # given
        q: asyncio.Queue = asyncio.Queue()

        def boom() -> None:
            raise ValueError("nope")

        async def producer() -> None:
            await run_queued_write(q, boom)

        prod = asyncio.create_task(producer())
        job = await q.get()
        # when — job() catches op errors and forwards them to fut (no re-raise)
        job()
        q.task_done()
        await asyncio.sleep(0)
        # then
        with self.assertRaises(ValueError):
            await prod
