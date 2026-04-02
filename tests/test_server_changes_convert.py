"""Tests for change-scan record conversion."""

from datetime import UTC, datetime
from unittest import TestCase

from wcpan.drive.synology.lib import FOLDER_MIME_TYPE
from wcpan.drive.synology.server._api.changes import convert_file_info as _convert


class TestConvertSynologyFileInfo(TestCase):
    def test_directory_record(self):
        # given
        info = {
            "file_id": "d1",
            "parent_id": "p",
            "name": "folder",
            "type": "dir",
            "content_type": "dir",
            "size": 0,
            "created_time": 1700000000,
            "modified_time": 1700003600,
            "sync_id": 5,
        }
        parent_id = "_mount"
        # when
        record = _convert(info, parent_id)
        # then
        self.assertTrue(record.is_directory)
        self.assertEqual(record.mime_type, FOLDER_MIME_TYPE)
        self.assertEqual(record.node_id, "d1")
        self.assertEqual(record.parent_id, parent_id)
        self.assertEqual(record.ctime, datetime.fromtimestamp(1700000000, UTC))
        self.assertEqual(record.mtime, datetime.fromtimestamp(1700003600, UTC))

    def test_file_with_image_content_type(self):
        # given
        info = {
            "file_id": "f1",
            "parent_id": "p",
            "name": "pic.png",
            "type": "file",
            "content_type": "image",
            "hash": "abc",
            "size": 1024,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 1,
        }
        # when
        record = _convert(info, parent_id="par")
        # then
        self.assertFalse(record.is_directory)
        self.assertTrue(record.is_image)
        self.assertFalse(record.is_video)
        self.assertEqual(record.hash, "abc")
        self.assertEqual(record.size, 1024)

    def test_file_with_video_content_type(self):
        # given
        info = {
            "file_id": "v1",
            "parent_id": "p",
            "name": "clip.mp4",
            "type": "file",
            "content_type": "video",
            "size": 999,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 2,
        }
        # when
        record = _convert(info, parent_id=None)
        # then
        self.assertTrue(record.is_video)
        self.assertIsNone(record.parent_id)
