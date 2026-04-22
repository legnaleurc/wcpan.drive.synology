"""Tests for write queue helpers and worker coroutines."""

import asyncio
import logging
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from wcpan.drive.synology._server.types import MetadataWorkItem
from wcpan.drive.synology._server.workers import (
    checkpoint_worker,
    metadata_worker,
    write_worker,
)
from wcpan.drive.synology.types import MirrorMutableId, NodeRecord


logging.getLogger("wcpan.drive.synology._server").setLevel(logging.CRITICAL + 1)


_EPOCH = datetime.fromtimestamp(0, UTC)


def _make_record(node_id: str = "n1") -> NodeRecord:
    return NodeRecord(
        id=node_id,
        parent_id="p1",
        name="x",
        is_directory=False,
        ctime=_EPOCH,
        mtime=_EPOCH,
        mime_type="text/plain",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
        mutable_id=MirrorMutableId(node_id),
    )


class TestWriteQueue(IsolatedAsyncioTestCase):
    async def test_puts_op_on_queue(self):
        # given
        q: asyncio.Queue = asyncio.Queue()
        seen: list[int] = []

        async def op() -> None:
            seen.append(42)

        # when
        await q.put(op)

        # then — op is on the queue but not yet executed
        self.assertEqual(q.qsize(), 1)
        job = q.get_nowait()
        await job()
        self.assertEqual(seen, [42])

    async def test_blocks_when_queue_full(self):
        # given — bounded queue at capacity
        q: asyncio.Queue = asyncio.Queue(maxsize=1)

        async def noop() -> None:
            pass

        await q.put(noop)  # fill it

        enqueued = False

        async def producer() -> None:
            nonlocal enqueued
            await q.put(noop)
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


class TestMetadataWorker(IsolatedAsyncioTestCase):
    async def test_processes_item(self):
        q: asyncio.Queue[MetadataWorkItem] = asyncio.Queue()
        processed: list[str] = []

        async def process(item: MetadataWorkItem) -> None:
            processed.append(item.record.id)

        item = MetadataWorkItem(record=_make_record("n1"), force_refresh=False)
        await q.put(item)

        worker = asyncio.create_task(metadata_worker(q, process))
        await q.join()
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        self.assertEqual(processed, ["n1"])

    async def test_exception_does_not_stop_worker(self):
        q: asyncio.Queue[MetadataWorkItem] = asyncio.Queue()
        call_count = 0

        async def process(item: MetadataWorkItem) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("boom")

        await q.put(MetadataWorkItem(record=_make_record("a"), force_refresh=False))
        await q.put(MetadataWorkItem(record=_make_record("b"), force_refresh=False))

        worker = asyncio.create_task(metadata_worker(q, process))
        await q.join()
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        self.assertEqual(call_count, 2)


class TestWriteWorker(IsolatedAsyncioTestCase):
    async def test_processes_task(self):
        q: asyncio.Queue = asyncio.Queue()
        executed: list[int] = []

        async def op() -> None:
            executed.append(1)

        await q.put(op)

        worker = asyncio.create_task(write_worker(q))
        await q.join()
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        self.assertEqual(executed, [1])

    async def test_exception_propagates(self):
        q: asyncio.Queue = asyncio.Queue()

        async def boom() -> None:
            raise RuntimeError("db error")

        await q.put(boom)

        worker = asyncio.create_task(write_worker(q))
        with self.assertRaises(RuntimeError):
            await worker


class TestCheckpointWorker(IsolatedAsyncioTestCase):
    async def test_calls_checkpoint(self):
        storage = MagicMock()
        storage.checkpoint = AsyncMock()

        import wcpan.drive.synology._server.workers as wmod

        original = wmod._WAL_CHECKPOINT_INTERVAL
        wmod._WAL_CHECKPOINT_INTERVAL = 0.01
        try:
            worker = asyncio.create_task(checkpoint_worker(storage))
            await asyncio.sleep(0.05)
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
        finally:
            wmod._WAL_CHECKPOINT_INTERVAL = original

        self.assertGreater(storage.checkpoint.await_count, 0)
