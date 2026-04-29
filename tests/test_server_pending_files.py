"""Tests for debounce helpers and the _guarded wrapper."""

import asyncio
import logging
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology._server.services.webhook import (
    _Debouncer,
    _guarded,
    _TaskIdDebouncer,
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


class TestDebouncer(IsolatedAsyncioTestCase):
    async def test_start_returns_without_blocking(self):
        completed = asyncio.Event()

        async def work():
            completed.set()

        async with asyncio.TaskGroup() as tg:
            debouncer = _Debouncer(tg, 999, lambda: work())
            debouncer.start()
            await asyncio.sleep(0)
            self.assertFalse(completed.is_set())
            debouncer.cancel_pending()

    async def test_start_runs_after_delay(self):
        completed = asyncio.Event()

        async def work():
            completed.set()

        async with asyncio.TaskGroup() as tg:
            debouncer = _Debouncer(tg, 0, lambda: work())
            debouncer.start()
            await completed.wait()

        self.assertTrue(debouncer.started)

    async def test_cancel_pending_cancels_unstarted_task(self):
        async with asyncio.TaskGroup() as tg:
            debouncer = _Debouncer(tg, 999, lambda: asyncio.sleep(0))
            task = debouncer.start()

            self.assertTrue(debouncer.cancel_pending())
            with self.assertRaises(asyncio.CancelledError):
                await task

    async def test_cancel_pending_ignores_started_task(self):
        first_started = asyncio.Event()
        allow_finish = asyncio.Event()
        cancelled = False

        async def work():
            nonlocal cancelled
            first_started.set()
            try:
                await allow_finish.wait()
            except asyncio.CancelledError:
                cancelled = True
                raise

        async with asyncio.TaskGroup() as tg:
            debouncer = _Debouncer(tg, 0, lambda: work())
            debouncer.start()
            await first_started.wait()

            self.assertFalse(debouncer.cancel_pending())
            allow_finish.set()

        self.assertFalse(cancelled)


class TestTaskIdDebouncer(IsolatedAsyncioTestCase):
    async def test_schedule_returns_without_blocking(self):
        completed = asyncio.Event()

        async def work():
            completed.set()

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=999)
            svc.schedule("f1", lambda: work())
            await asyncio.sleep(0)
            self.assertFalse(completed.is_set())
            svc.cancel("f1")

    async def test_schedule_creates_task_after_delay(self):
        completed = asyncio.Event()

        async def work():
            completed.set()

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=0)
            svc.schedule("f1", lambda: work())
            await completed.wait()

    async def test_schedule_replaces_existing_before_delay(self):
        first_called = False

        async def first():
            nonlocal first_called
            first_called = True

        second_done = asyncio.Event()

        async def second():
            second_done.set()

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=0.05)
            svc.schedule("f1", lambda: first())
            await asyncio.sleep(0)
            # Scheduling same file_id during the delay should renew the debounce.
            svc.schedule("f1", lambda: second())
            await second_done.wait()

        self.assertFalse(first_called)

    async def test_schedule_does_not_cancel_started_work(self):
        first_started = asyncio.Event()
        allow_first_finish = asyncio.Event()
        first_cancelled = False
        second_called = False

        async def first():
            nonlocal first_cancelled
            first_started.set()
            try:
                await allow_first_finish.wait()
            except asyncio.CancelledError:
                first_cancelled = True
                raise

        async def second():
            nonlocal second_called
            second_called = True

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=0)
            svc.schedule("f1", lambda: first())
            await first_started.wait()
            svc.schedule("f1", lambda: second())
            await asyncio.sleep(0)
            allow_first_finish.set()

        self.assertFalse(first_cancelled)
        self.assertFalse(second_called)

    async def test_cancel_removes_task(self):
        called = False

        async def work():
            nonlocal called
            called = True

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=999)
            svc.schedule("f1", lambda: work())
            await asyncio.sleep(0)
            svc.cancel("f1")

        self.assertFalse(called)

    async def test_cancel_nonexistent_is_noop(self):
        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg)
            svc.cancel("no-such-file")  # should not raise

    async def test_done_callback_cleans_up(self):
        done = asyncio.Event()

        async def quick():
            done.set()

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=0)
            svc.schedule("f1", lambda: quick())
            await done.wait()
            # Give the done-callback a chance to run
            await asyncio.sleep(0)
            self.assertNotIn("f1", svc._debouncers)

    async def test_old_done_callback_does_not_remove_replacement(self):
        first_called = False
        second_done = asyncio.Event()

        async def first():
            nonlocal first_called
            first_called = True

        async def second():
            second_done.set()

        async with asyncio.TaskGroup() as tg:
            svc = _TaskIdDebouncer(tg, delay=0.05)
            svc.schedule("f1", lambda: first())
            old_task = svc._debouncers["f1"]._task
            self.assertIsNotNone(old_task)
            svc.schedule("f1", lambda: second())
            with self.assertRaises(asyncio.CancelledError):
                await old_task
            self.assertIn("f1", svc._debouncers)
            await second_done.wait()

        self.assertFalse(first_called)
