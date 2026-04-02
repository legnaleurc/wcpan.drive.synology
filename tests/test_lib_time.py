"""Tests for time helpers and naive-datetime handling in lib."""

from datetime import UTC, datetime
from unittest import TestCase
from unittest.mock import patch

from wcpan.drive.synology.lib import node_record_from_dict, utc_from_timestamp, utc_now


class _FixedDateTime:
    """Stub bound to wcpan.drive.synology.lib.datetime for utc_now tests."""

    UTC = UTC

    @staticmethod
    def now(tz=None):
        return datetime(2022, 6, 15, 12, 30, 0, tzinfo=UTC)


class TestUtcFromTimestamp(TestCase):
    def test_epoch_zero(self):
        # given
        ts = 0
        # when
        dt = utc_from_timestamp(ts)
        # then
        self.assertEqual(dt, datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC))


class TestUtcNow(TestCase):
    def test_returns_patched_fixed_time(self):
        # given
        fixed = datetime(2022, 6, 15, 12, 30, 0, tzinfo=UTC)
        # when
        with patch("wcpan.drive.synology.lib.datetime", _FixedDateTime):
            result = utc_now()
        # then
        self.assertEqual(result, fixed)


class TestNodeRecordFromDictNaiveDatetime(TestCase):
    def test_naive_ctime_mtime_get_utc(self):
        # given
        data = {
            "node_id": "n1",
            "parent_id": None,
            "name": "f.txt",
            "is_directory": False,
            "ctime": "2020-05-01T10:00:00",
            "mtime": "2020-05-02T11:00:00",
            "mime_type": "text/plain",
            "hash": "",
            "size": 0,
            "is_image": False,
            "is_video": False,
            "width": 0,
            "height": 0,
            "ms_duration": 0,
        }
        # when
        record = node_record_from_dict(data)
        # then
        self.assertEqual(record.ctime.tzinfo, UTC)
        self.assertEqual(record.mtime.tzinfo, UTC)
