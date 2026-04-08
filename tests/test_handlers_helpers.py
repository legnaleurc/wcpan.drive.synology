"""Tests for small pure helpers in server._handlers."""

from datetime import UTC, datetime
from unittest import TestCase

from wcpan.drive.synology._server.handlers.lib import (
    media_info_from_query,
    record_to_response,
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


class TestMediaInfoFromQuery(TestCase):
    def test_no_media_keys_returns_none(self):
        self.assertIsNone(media_info_from_query({}))
        self.assertIsNone(media_info_from_query({"name": "f.bin"}))

    def test_dims_parsed(self):
        q = {"width": "1920", "height": "1080", "ms_duration": "3000"}
        result = media_info_from_query(q)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.width, 1920)
        self.assertEqual(result.height, 1080)
        self.assertEqual(result.ms_duration, 3000)

    def test_media_image_flag(self):
        result = media_info_from_query({"media_image": "1"})
        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.is_image)
        self.assertFalse(result.is_video)

    def test_media_video_flag(self):
        result = media_info_from_query({"media_video": "true"})
        self.assertIsNotNone(result)
        assert result is not None
        self.assertFalse(result.is_image)
        self.assertTrue(result.is_video)

    def test_missing_dims_default_to_zero(self):
        result = media_info_from_query({"media_image": "1"})
        assert result is not None
        self.assertEqual(result.width, 0)
        self.assertEqual(result.height, 0)
        self.assertEqual(result.ms_duration, 0)


class TestRecordToResponse(TestCase):
    def test_matches_node_record_to_dict_shape(self):
        # given
        record = _sample_record()
        # when
        body = record_to_response(record)
        # then
        self.assertEqual(body["node_id"], record.node_id)
        self.assertEqual(body["name"], record.name)
