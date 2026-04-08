"""Tests for per-mount max_id tracking in Storage."""

import os
import sqlite3
import tempfile
import unittest

from wcpan.drive.synology.server._db import Storage


class TestGetMountMaxIds(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.storage = Storage(self.db_path)
        self.storage.ensure_schema()

    def tearDown(self) -> None:
        os.unlink(self.db_path)

    def _write_kv(self, key: str, value: str) -> None:
        con = sqlite3.connect(self.db_path)
        con.execute(
            "INSERT INTO server_state (key, value) VALUES (?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        con.commit()
        con.close()

    def _read_kv(self, key: str) -> str | None:
        con = sqlite3.connect(self.db_path)
        row = con.execute(
            "SELECT value FROM server_state WHERE key = ?", (key,)
        ).fetchone()
        con.close()
        return row[0] if row else None

    def test_fresh_db_returns_zero_for_all_mounts(self) -> None:
        folders = {"LV": "/team-folders/video/L", "LG": "/team-folders/gallery/L"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result, {"LV": 0, "LG": 0})

    def test_migration_from_global_key(self) -> None:
        """Old global last_max_id is used for all mounts when no per-mount keys exist."""
        self._write_kv("last_max_id", "59172")
        folders = {"LV": "/team-folders/video/L", "LG": "/team-folders/gallery/L"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result, {"LV": 59172, "LG": 59172})

    def test_per_mount_keys_take_precedence_over_global(self) -> None:
        self._write_kv("last_max_id", "59172")
        self._write_kv("last_max_id:LV", "35000")
        self._write_kv("last_max_id:LG", "28000")
        folders = {"LV": "/team-folders/video/L", "LG": "/team-folders/gallery/L"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result, {"LV": 35000, "LG": 28000})

    def test_new_mount_gets_zero_when_other_mounts_have_per_mount_keys(self) -> None:
        """A new mount added to config gets 0, not the global fallback."""
        self._write_kv("last_max_id", "59172")
        self._write_kv("last_max_id:LV", "35000")
        # LG has no per-mount key; since LV has one, global fallback is NOT used
        folders = {"LV": "/team-folders/video/L", "LG": "/team-folders/gallery/L"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result["LV"], 35000)
        self.assertEqual(result["LG"], 0)

    def test_path_change_resets_to_zero(self) -> None:
        """If the stored syno_path differs from config, that mount resets to 0."""
        self._write_kv("last_max_id:LV", "35000")
        self._write_kv("mount_path:LV", "/old/path")
        folders = {"LV": "/new/path"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result["LV"], 0)

    def test_matching_path_preserves_value(self) -> None:
        self._write_kv("last_max_id:LV", "35000")
        self._write_kv("mount_path:LV", "/team-folders/video/L")
        folders = {"LV": "/team-folders/video/L"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result["LV"], 35000)

    def test_no_stored_path_does_not_reset(self) -> None:
        """Missing mount_path key (e.g. first write after upgrade) does not reset."""
        self._write_kv("last_max_id:LV", "35000")
        # No mount_path:LV written
        folders = {"LV": "/team-folders/video/L"}
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result["LV"], 35000)


class TestSetMountState(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.storage = Storage(self.db_path)
        self.storage.ensure_schema()

    def tearDown(self) -> None:
        os.unlink(self.db_path)

    def _read_kv(self, key: str) -> str | None:
        con = sqlite3.connect(self.db_path)
        row = con.execute(
            "SELECT value FROM server_state WHERE key = ?", (key,)
        ).fetchone()
        con.close()
        return row[0] if row else None

    def test_writes_both_keys(self) -> None:
        self.storage.set_mount_state("LV", "/team-folders/video/L", 35000)
        self.assertEqual(self._read_kv("last_max_id:LV"), "35000")
        self.assertEqual(self._read_kv("mount_path:LV"), "/team-folders/video/L")

    def test_overwrites_existing_values(self) -> None:
        self.storage.set_mount_state("LV", "/old/path", 1000)
        self.storage.set_mount_state("LV", "/team-folders/video/L", 35000)
        self.assertEqual(self._read_kv("last_max_id:LV"), "35000")
        self.assertEqual(self._read_kv("mount_path:LV"), "/team-folders/video/L")

    def test_roundtrip_via_get_mount_max_ids(self) -> None:
        folders = {"LV": "/team-folders/video/L"}
        self.storage.set_mount_state("LV", "/team-folders/video/L", 35000)
        result = self.storage.get_mount_max_ids(folders)
        self.assertEqual(result, {"LV": 35000})
