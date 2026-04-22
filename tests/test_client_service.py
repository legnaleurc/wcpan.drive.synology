"""Tests for ClientFileService helpers."""

from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase, TestCase

from wcpan.drive.core.exceptions import NodeExistsError, NodeNotFoundError
from wcpan.drive.core.types import Node

from wcpan.drive.synology._client.service import ClientFileService, _check
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


def _parent_node() -> Node:
    t = datetime.now(UTC)
    return Node(
        id="p",
        parent_id=None,
        name="root",
        is_directory=True,
        is_trashed=False,
        ctime=t,
        mtime=t,
        mime_type="application/x-directory",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
        private=None,
    )


class _Fake409Response:
    def __init__(self, status: int, payload: dict) -> None:
        self.status = status
        self._payload = payload

    async def json(self) -> dict:
        return self._payload

    async def __aenter__(self) -> "_Fake409Response":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None


class TestCreateDirectory409(IsolatedAsyncioTestCase):
    def _node_body(self) -> dict:
        t = datetime.now(UTC).isoformat()
        return {
            "id": "existing-dir",
            "mutable_id": "existing-dir",
            "parent_id": "p",
            "name": "d",
            "is_directory": True,
            "ctime": t,
            "mtime": t,
            "mime_type": "application/x-directory",
            "hash": "",
            "size": 0,
            "is_image": False,
            "is_video": False,
            "width": 0,
            "height": 0,
            "ms_duration": 0,
        }

    async def test_exist_ok_returns_node(self) -> None:
        body = self._node_body()

        class _Sess:
            def post(self, *a, **k):
                return _Fake409Response(409, body)

        svc = ClientFileService(session=_Sess(), server_url="http://srv")  # type: ignore[arg-type]
        node = await svc.create_directory(
            "d", _parent_node(), exist_ok=True, private=None
        )
        self.assertEqual(node.id, "existing-dir")

    async def test_exist_ok_false_raises_node_exists(self) -> None:
        body = self._node_body()

        class _Sess2:
            def post(self, *a, **k):
                return _Fake409Response(409, body)

        svc = ClientFileService(session=_Sess2(), server_url="http://srv")  # type: ignore[arg-type]
        with self.assertRaises(NodeExistsError) as ctx:
            await svc.create_directory(
                "d", _parent_node(), exist_ok=False, private=None
            )
        self.assertEqual(ctx.exception.node.id, "existing-dir")

    async def test_missing_node_body_raises_server_error(self) -> None:
        class _Sess:
            def post(self, *a, **k):
                return _Fake409Response(409, {})

        svc = ClientFileService(session=_Sess(), server_url="http://srv")  # type: ignore[arg-type]
        with self.assertRaises(SynologyServerError) as ctx:
            await svc.create_directory("d", _parent_node(), exist_ok=True, private=None)
        self.assertEqual(ctx.exception.status, 409)
