from hashlib import sha256
from unittest import TestCase

from wcpan.drive.synology._lib import SynologyFileDict, node_from_api, path_to_id


def _make_dict(
    path: str,
    name: str,
    parent_path: str | None = "/volume1",
    is_folder: bool = False,
) -> SynologyFileDict:
    return SynologyFileDict(
        id=path,
        name=name,
        parent_id=parent_path,
        is_folder=is_folder,
        trashed=False,
        created_time="2024-01-01T00:00:00+00:00",
        modified_time="2024-01-01T00:00:00+00:00",
        mime_type="application/x-folder" if is_folder else "text/plain",
        size=0,
    )


class TestPathToId(TestCase):
    def test_deterministic(self):
        assert path_to_id("/volume1/data") == path_to_id("/volume1/data")

    def test_different_paths_differ(self):
        assert path_to_id("/volume1/a") != path_to_id("/volume1/b")

    def test_sha256_hex_length(self):
        assert len(path_to_id("/some/path")) == 64

    def test_matches_sha256(self):
        path = "/volume1/documents/文件.txt"
        expected = sha256(path.encode()).hexdigest()
        assert path_to_id(path) == expected

    def test_cjk_path(self):
        # CJK paths should hash to the same fixed-length id
        path = "/volume1/照片/假期/IMG_001.jpg"
        result = path_to_id(path)
        assert len(result) == 64
        assert result == sha256(path.encode()).hexdigest()


class TestNodeFromApi(TestCase):
    def test_id_is_sha256_of_path(self):
        data = _make_dict("/volume1/data/file.txt", "file.txt", "/volume1/data")
        node = node_from_api(data)
        assert node.id == path_to_id("/volume1/data/file.txt")

    def test_parent_id_is_sha256_of_parent_path(self):
        data = _make_dict("/volume1/data/file.txt", "file.txt", "/volume1/data")
        node = node_from_api(data)
        assert node.parent_id == path_to_id("/volume1/data")

    def test_root_parent_id_is_none(self):
        data = _make_dict("/volume1/data", "", parent_path=None, is_folder=True)
        node = node_from_api(data)
        assert node.parent_id is None

    def test_private_contains_original_path(self):
        path = "/volume1/data/file.txt"
        data = _make_dict(path, "file.txt", "/volume1/data")
        node = node_from_api(data)
        assert node.private == {"path": path}

    def test_private_contains_path_for_folder(self):
        path = "/volume1/data/photos"
        data = _make_dict(path, "photos", "/volume1/data", is_folder=True)
        node = node_from_api(data)
        assert node.private == {"path": path}

    def test_cjk_path_id(self):
        path = "/volume1/照片/假期"
        data = _make_dict(path, "假期", "/volume1/照片", is_folder=True)
        node = node_from_api(data)
        assert node.id == path_to_id(path)
        assert node.private == {"path": path}

    def test_name_preserved(self):
        data = _make_dict("/volume1/data/file.txt", "file.txt", "/volume1/data")
        node = node_from_api(data)
        assert node.name == "file.txt"

    def test_hash_from_dict(self):
        data = _make_dict("/volume1/data/file.txt", "file.txt", "/volume1/data")
        data["hash"] = "abc123"
        node = node_from_api(data)
        assert node.hash == "abc123"

    def test_hash_defaults_empty(self):
        data = _make_dict("/volume1/data/file.txt", "file.txt", "/volume1/data")
        node = node_from_api(data)
        assert node.hash == ""
