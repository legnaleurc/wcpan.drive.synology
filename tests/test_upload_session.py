"""Tests for server._upload_session pure helpers and UploadSessionStore."""

import tempfile
from pathlib import Path
from unittest import TestCase

from wcpan.drive.synology.server._upload_session import (
    UploadSessionStore,
    delete_temp_sync,
    parse_content_range,
    read_chunk_sync,
    write_chunk_sync,
)


class TestParseContentRange(TestCase):
    def test_valid_range(self):
        result = parse_content_range("bytes 0-99/1000")
        self.assertEqual(result, (0, 99, 1000))

    def test_mid_file_range(self):
        result = parse_content_range("bytes 100-199/1000")
        self.assertEqual(result, (100, 199, 1000))

    def test_last_chunk(self):
        result = parse_content_range("bytes 900-999/1000")
        self.assertEqual(result, (900, 999, 1000))

    def test_none_header(self):
        self.assertIsNone(parse_content_range(None))

    def test_empty_string(self):
        self.assertIsNone(parse_content_range(""))

    def test_wrong_unit(self):
        self.assertIsNone(parse_content_range("chars 0-99/1000"))

    def test_missing_slash(self):
        self.assertIsNone(parse_content_range("bytes 0-99"))

    def test_missing_dash(self):
        self.assertIsNone(parse_content_range("bytes 099/1000"))

    def test_non_numeric(self):
        self.assertIsNone(parse_content_range("bytes a-b/c"))


class TestWriteReadChunkSync(TestCase):
    def test_roundtrip(self):
        with tempfile.NamedTemporaryFile() as f:
            path = Path(f.name)
            data = b"hello world"
            write_chunk_sync(path, 0, data)
            result = read_chunk_sync(path, 0, len(data))
        self.assertEqual(result, data)

    def test_write_at_offset(self):
        with tempfile.NamedTemporaryFile() as f:
            path = Path(f.name)
            write_chunk_sync(path, 0, b"AAAA")
            write_chunk_sync(path, 2, b"BB")
            result = read_chunk_sync(path, 0, 4)
        self.assertEqual(result, b"AABB")

    def test_read_partial(self):
        with tempfile.NamedTemporaryFile() as f:
            path = Path(f.name)
            write_chunk_sync(path, 0, b"abcdef")
            result = read_chunk_sync(path, 2, 3)
        self.assertEqual(result, b"cde")

    def test_read_returns_empty_at_eof(self):
        with tempfile.NamedTemporaryFile() as f:
            path = Path(f.name)
            write_chunk_sync(path, 0, b"abc")
            result = read_chunk_sync(path, 10, 4)
        self.assertEqual(result, b"")


class TestDeleteTempSync(TestCase):
    def test_deletes_existing_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = Path(f.name)
        self.assertTrue(path.exists())
        delete_temp_sync(path)
        self.assertFalse(path.exists())

    def test_missing_file_does_not_raise(self):
        path = Path("/tmp/does_not_exist_wcpan_test.tmp")
        delete_temp_sync(path)  # must not raise


class TestUploadSessionStore(TestCase):
    def test_create_returns_session_with_correct_fields(self):
        store = UploadSessionStore()
        session = store.create(
            parent_id="p1",
            name="file.bin",
            total_size=1024,
            mime_type="application/octet-stream",
            client_query={"width": "100"},
        )
        self.assertEqual(session.parent_id, "p1")
        self.assertEqual(session.name, "file.bin")
        self.assertEqual(session.total_size, 1024)
        self.assertEqual(session.mime_type, "application/octet-stream")
        self.assertEqual(session.client_query, {"width": "100"})
        self.assertEqual(session.received, 0)
        self.assertTrue(session.temp_path.exists())
        # cleanup
        session.temp_path.unlink(missing_ok=True)

    def test_create_uses_custom_tmp_dir(self):
        with tempfile.TemporaryDirectory() as d:
            store = UploadSessionStore(tmp_dir=Path(d))
            session = store.create("p", "f", 10, None, {})
            self.assertTrue(session.temp_path.is_relative_to(d))
            session.temp_path.unlink(missing_ok=True)

    def test_get_returns_created_session(self):
        store = UploadSessionStore()
        session = store.create("p", "f", 10, None, {})
        result = store.get(session.session_id)
        self.assertIs(result, session)
        session.temp_path.unlink(missing_ok=True)

    def test_get_missing_returns_none(self):
        store = UploadSessionStore()
        self.assertIsNone(store.get("nonexistent"))

    def test_remove_returns_session_and_makes_it_unreachable(self):
        store = UploadSessionStore()
        session = store.create("p", "f", 10, None, {})
        removed = store.remove(session.session_id)
        self.assertIs(removed, session)
        self.assertIsNone(store.get(session.session_id))
        session.temp_path.unlink(missing_ok=True)

    def test_remove_missing_returns_none(self):
        store = UploadSessionStore()
        self.assertIsNone(store.remove("nonexistent"))

    def test_close_all_deletes_temp_files(self):
        store = UploadSessionStore()
        s1 = store.create("p", "a", 10, None, {})
        s2 = store.create("p", "b", 20, None, {})
        paths = [s1.temp_path, s2.temp_path]
        for p in paths:
            self.assertTrue(p.exists())
        store.close_all()
        for p in paths:
            self.assertFalse(p.exists())
        self.assertIsNone(store.get(s1.session_id))
        self.assertIsNone(store.get(s2.session_id))

    def test_session_ids_are_unique(self):
        store = UploadSessionStore()
        sessions = [store.create("p", "f", 10, None, {}) for _ in range(10)]
        ids = {s.session_id for s in sessions}
        self.assertEqual(len(ids), 10)
        for s in sessions:
            s.temp_path.unlink(missing_ok=True)
