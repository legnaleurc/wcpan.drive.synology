"""Tests for wcpan.drive.synology.lib.

Each test uses given-when-then sections and unittest assertions.
"""

from datetime import UTC, datetime
from unittest import TestCase

from wcpan.drive.synology.lib import (
    FOLDER_MIME_TYPE,
    guess_mime_type,
    node_from_record,
    node_record_from_dict,
    node_record_to_dict,
)
from wcpan.drive.synology.types import NodeRecord


_NOW = datetime(2024, 1, 1, tzinfo=UTC)


def _make_record(
    node_id: str = "file-id-001",
    name: str = "test.txt",
    parent_id: str | None = "parent-id-001",
    is_directory: bool = False,
) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name=name,
        is_directory=is_directory,
        ctime=_NOW,
        mtime=_NOW,
        mime_type=FOLDER_MIME_TYPE if is_directory else "text/plain",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


class TestNodeFromRecord(TestCase):
    def test_id_is_file_id(self):
        # given
        record = _make_record(node_id="synology-file-id-xyz")
        # when
        node = node_from_record(record)
        # then
        self.assertEqual(node.id, "synology-file-id-xyz")

    def test_parent_id_preserved(self):
        # given
        record = _make_record(parent_id="parent-file-id-abc")
        # when
        node = node_from_record(record)
        # then
        self.assertEqual(node.parent_id, "parent-file-id-abc")

    def test_root_parent_id_is_none(self):
        # given
        record = _make_record(parent_id=None)
        # when
        node = node_from_record(record)
        # then
        self.assertIsNone(node.parent_id)

    def test_no_private(self):
        # given
        record = _make_record()
        # when
        node = node_from_record(record)
        # then
        self.assertIsNone(node.private)

    def test_name_preserved(self):
        # given
        record = _make_record(name="document.pdf")
        # when
        node = node_from_record(record)
        # then
        self.assertEqual(node.name, "document.pdf")

    def test_is_directory(self):
        # given
        record = _make_record(is_directory=True)
        # when
        node = node_from_record(record)
        # then
        self.assertTrue(node.is_directory)
        self.assertFalse(node.is_trashed)


class TestGuessMimeType(TestCase):
    def test_directory(self):
        # given
        name = "anything"
        # when
        result = guess_mime_type(name, is_directory=True)
        # then
        self.assertEqual(result, FOLDER_MIME_TYPE)

    def test_known_extension(self):
        # given
        name = "photo.jpg"
        # when
        result = guess_mime_type(name, is_directory=False)
        # then
        self.assertEqual(result, "image/jpeg")

    def test_unknown_extension(self):
        # given
        name = "file.xyz123"
        # when
        result = guess_mime_type(name, is_directory=False)
        # then
        self.assertEqual(result, "application/octet-stream")

    def test_no_extension(self):
        # given
        name = "noextension"
        # when
        result = guess_mime_type(name, is_directory=False)
        # then
        self.assertEqual(result, "application/octet-stream")


class TestNodeRecordRoundtrip(TestCase):
    def test_to_dict_and_back(self):
        # given
        record = _make_record()
        # when
        d = node_record_to_dict(record)
        restored = node_record_from_dict(d)
        # then
        self.assertEqual(restored.node_id, record.node_id)
        self.assertEqual(restored.parent_id, record.parent_id)
        self.assertEqual(restored.name, record.name)
        self.assertEqual(restored.ctime, record.ctime)
        self.assertEqual(restored.mtime, record.mtime)

    def test_cjk_name(self):
        # given
        record = _make_record(name="照片.jpg")
        # when
        d = node_record_to_dict(record)
        restored = node_record_from_dict(d)
        # then
        self.assertEqual(restored.name, "照片.jpg")
