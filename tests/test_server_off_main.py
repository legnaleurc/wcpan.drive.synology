"""Tests for OffMainThread executor bridge."""

from concurrent.futures import ThreadPoolExecutor
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology.server._lib import OffMainThread


class TestOffMainThread(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._pool = ThreadPoolExecutor(max_workers=2)
        self._off = OffMainThread(self._pool)

    async def asyncTearDown(self) -> None:
        self._pool.shutdown(wait=True)

    async def test_runs_callable_on_executor(self):
        # given
        def triple(x: int) -> int:
            return x * 3

        # when
        result = await self._off(triple, 4)
        # then
        self.assertEqual(result, 12)

    async def test_untimed_runs_on_executor(self):
        # given
        def concat(a: str, b: str) -> str:
            return a + b

        # when
        result = await self._off.untimed(concat, "foo", "bar")
        # then
        self.assertEqual(result, "foobar")
