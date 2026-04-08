"""Tests for _PendingFileScheduler and the _guarded wrapper."""

import asyncio
import logging
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology._server.services.webhook import (
    _guarded,
    _PendingFileScheduler,
)


logging.getLogger("wcpan.drive.synology._server").setLevel(logging.CRITICAL + 1)


class TestGuarded(IsolatedAsyncioTestCase):
    async def test_normal_completion(self):
        called = False

        async def ok():
            nonlocal called
            called = True

        await _guarded("f1", ok())
        self.assertTrue(called)

    async def test_exception_is_logged_not_raised(self):
        async def boom():
            raise ValueError("oops")

        # Should not raise
        await _guarded("f1", boom())

    async def test_cancelled_error_propagates(self):
        async def cancel():
            raise asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await _guarded("f1", cancel())


class TestPendingFileScheduler(IsolatedAsyncioTestCase):
    async def test_schedule_creates_task(self):
        completed = asyncio.Event()

        async def work():
            completed.set()

        async with asyncio.TaskGroup() as tg:
            svc = _PendingFileScheduler(tg)
            svc.schedule("f1", work())
            await completed.wait()

    async def test_schedule_replaces_existing(self):
        first_started = asyncio.Event()
        first_cancelled = False

        async def first():
            nonlocal first_cancelled
            first_started.set()
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                first_cancelled = True
                raise

        second_done = asyncio.Event()

        async def second():
            second_done.set()

        async with asyncio.TaskGroup() as tg:
            svc = _PendingFileScheduler(tg)
            svc.schedule("f1", first())
            await first_started.wait()
            # Scheduling same file_id should cancel the first
            svc.schedule("f1", second())
            await second_done.wait()

        self.assertTrue(first_cancelled)

    async def test_cancel_removes_task(self):
        was_cancelled = False

        async def slow():
            nonlocal was_cancelled
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                was_cancelled = True
                raise

        started = asyncio.Event()

        async def slow_with_signal():
            started.set()
            await slow()

        async with asyncio.TaskGroup() as tg:
            svc = _PendingFileScheduler(tg)
            svc.schedule("f1", slow_with_signal())
            await started.wait()
            svc.cancel("f1")

        self.assertTrue(was_cancelled)

    async def test_cancel_nonexistent_is_noop(self):
        async with asyncio.TaskGroup() as tg:
            svc = _PendingFileScheduler(tg)
            svc.cancel("no-such-file")  # should not raise

    async def test_done_callback_cleans_up(self):
        done = asyncio.Event()

        async def quick():
            done.set()

        async with asyncio.TaskGroup() as tg:
            svc = _PendingFileScheduler(tg)
            svc.schedule("f1", quick())
            await done.wait()
            # Give the done-callback a chance to run
            await asyncio.sleep(0)
            self.assertNotIn("f1", svc._tasks)
