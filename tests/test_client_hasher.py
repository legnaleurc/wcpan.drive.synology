"""Tests for client MD5 hasher."""

import hashlib
from unittest import IsolatedAsyncioTestCase

from wcpan.drive.synology.client._hasher import Md5Hasher, create_hasher


class TestCreateHasher(IsolatedAsyncioTestCase):
    async def test_produces_working_hasher(self):
        # when
        hasher = await create_hasher()
        # then
        await hasher.update(b"hello")
        self.assertEqual(
            await hasher.hexdigest(),
            hashlib.md5(b"hello").hexdigest(),
        )


class TestMd5Hasher(IsolatedAsyncioTestCase):
    async def test_hexdigest_and_digest(self):
        # given
        inner = hashlib.md5()
        h = Md5Hasher(inner)
        # when
        await h.update(b"ab")
        await h.update(b"c")
        hex_d = await h.hexdigest()
        inner2 = hashlib.md5(b"abc")
        # then
        self.assertEqual(hex_d, inner2.hexdigest())
        self.assertEqual(await Md5Hasher(hashlib.md5(b"abc")).digest(), inner2.digest())

    async def test_copy_independent(self):
        # given
        h = await create_hasher()
        await h.update(b"x")
        # when
        c = await h.copy()
        await h.update(b"y")
        await c.update(b"z")
        # then
        self.assertEqual(await h.hexdigest(), hashlib.md5(b"xy").hexdigest())
        self.assertEqual(await c.hexdigest(), hashlib.md5(b"xz").hexdigest())
