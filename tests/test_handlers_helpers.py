"""Tests for small pure helpers in server._handlers."""

from datetime import UTC, datetime
from unittest import TestCase

from aiohttp import web

from wcpan.drive.synology.server._handlers import (
    _client_media_overlay,
    _parse_query_bool,
    _query_nonneg_int,
    _record_to_response,
)
from wcpan.drive.synology.types import NodeRecord


def _sample_record() -> NodeRecord:
    now = datetime(2024, 3, 1, tzinfo=UTC)
    return NodeRecord(
        node_id="id1",
        parent_id="p1",
        name="x.txt",
        is_directory=False,
        ctime=now,
        mtime=now,
        mime_type="text/plain",
        hash="",
        size=1,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


class TestParseQueryBool(TestCase):
    def test_true_values(self):
        for raw in ("1", "true", "TRUE", " yes ", "on"):
            with self.subTest(raw=raw):
                # when
                result = _parse_query_bool(raw)
                # then
                self.assertTrue(result)

    def test_false_values(self):
        for raw in ("0", "false", "no", "off", ""):
            with self.subTest(raw=raw):
                result = _parse_query_bool(raw)
                self.assertFalse(result)

    def test_invalid_raises_bad_request(self):
        # given
        raw = "maybe"
        # when / then
        with self.assertRaises(web.HTTPBadRequest):
            _parse_query_bool(raw)


class TestQueryNonnegInt(TestCase):
    def test_missing_returns_zero(self):
        # given
        q: dict[str, str] = {}
        # when
        result = _query_nonneg_int(q, "offset")
        # then
        self.assertEqual(result, 0)

    def test_valid_positive(self):
        # given
        q = {"limit": "100"}
        # when
        result = _query_nonneg_int(q, "limit")
        # then
        self.assertEqual(result, 100)

    def test_invalid_raises(self):
        # given
        q = {"limit": "x"}
        # when / then
        with self.assertRaises(web.HTTPBadRequest):
            _query_nonneg_int(q, "limit")

    def test_negative_raises(self):
        # given
        q = {"limit": "-1"}
        # when / then
        with self.assertRaises(web.HTTPBadRequest):
            _query_nonneg_int(q, "limit")


class TestClientMediaOverlay(TestCase):
    def test_no_query_uses_defaults(self):
        # given
        q: dict[str, str] = {}
        # when
        w, h, ms, im, vid = _client_media_overlay(q, is_image=False, is_video=False)
        # then
        self.assertEqual((w, h, ms, im, vid), (0, 0, 0, False, False))

    def test_dims_from_query(self):
        # given
        q = {"width": "10", "height": "20", "ms_duration": "3000"}
        # when
        w, h, ms, im, vid = _client_media_overlay(q, is_image=True, is_video=True)
        # then
        self.assertEqual((w, h, ms), (10, 20, 3000))

    def test_media_flags_from_query(self):
        # given
        q = {"media_image": "0", "media_video": "1"}
        # when
        w, h, ms, im, vid = _client_media_overlay(q, is_image=True, is_video=False)
        # then
        self.assertFalse(im)
        self.assertTrue(vid)


class TestRecordToResponse(TestCase):
    def test_matches_node_record_to_dict_shape(self):
        # given
        record = _sample_record()
        # when
        body = _record_to_response(record)
        # then
        self.assertEqual(body["node_id"], record.node_id)
        self.assertEqual(body["name"], record.name)
