"""Tests for ClientFileService helpers."""

from unittest import TestCase

from wcpan.drive.core.exceptions import NodeNotFoundError

from wcpan.drive.synology.client._service import _check
from wcpan.drive.synology.exceptions import SynologyServerError


class TestCheck(TestCase):
    def test_404_raises_node_not_found(self):
        # given
        status = 404
        # when / then
        with self.assertRaises(NodeNotFoundError):
            _check(status, "get_root")

    def test_500_raises_server_error_with_status(self):
        # given
        status = 500
        # when
        with self.assertRaises(SynologyServerError) as ctx:
            _check(status, "move")
        # then
        self.assertEqual(ctx.exception.status, 500)

    def test_400_raises_server_error(self):
        # given
        status = 400
        # when
        with self.assertRaises(SynologyServerError) as ctx:
            _check(status, "delete")
        # then
        self.assertEqual(ctx.exception.status, 400)

    def test_2xx_ok(self):
        # given
        for status in (200, 201, 204):
            with self.subTest(status=status):
                # when / then — no exception
                _check(status, "op")
