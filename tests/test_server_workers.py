"""Tests for write queue helpers."""

import asyncio
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology.server._workers import enqueue_write


class TestEnqueueWrite(IsolatedAsyncioTestCase):
    async def test_puts_op_on_queue(self):
        # given
        q: asyncio.Queue = asyncio.Queue()
        seen: list[int] = []

        # when
        await enqueue_write(q, lambda: seen.append(42))

        # then — op is on the queue but not yet executed
        self.assertEqual(q.qsize(), 1)
        job = q.get_nowait()
        job()
        self.assertEqual(seen, [42])

    async def test_blocks_when_queue_full(self):
        # given — bounded queue at capacity
        q: asyncio.Queue = asyncio.Queue(maxsize=1)
        await q.put(lambda: None)  # fill it

        enqueued = False

        async def producer() -> None:
            nonlocal enqueued
            await enqueue_write(q, lambda: None)
            enqueued = True

        prod = asyncio.create_task(producer())
        await asyncio.sleep(0)
        # queue still full — producer should be blocked
        self.assertFalse(enqueued)

        # drain one item to unblock
        q.get_nowait()
        q.task_done()
        await prod
        self.assertTrue(enqueued)
