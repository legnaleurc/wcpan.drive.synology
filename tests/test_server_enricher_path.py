"""Tests for _resolve_local_path (no disk / pymediainfo)."""

from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase

from wcpan.drive.synology.server._enricher import _resolve_local_path
from wcpan.drive.synology.types import NodeRecord


def _node(
    node_id: str,
    name: str,
    parent_id: str | None,
    *,
    is_directory: bool = False,
) -> NodeRecord:
    t = datetime(2024, 1, 1, tzinfo=UTC)
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name=name,
        is_directory=is_directory,
        ctime=t,
        mtime=t,
        mime_type="application/octet-stream",
        hash="",
        size=0,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


class TestResolveLocalPath(TestCase):
    def test_maps_synology_path_through_volume_prefix(self):
        # given
        file_rec = _node("fid", "readme.txt", "folder_id")
        folder_rec = _node("folder_id", "Projects", "_docs", is_directory=True)
        cache = {"folder_id": folder_rec}
        folders = {"docs": "/volume1/docs"}
        volume_map = {"/volume1/docs": "/mnt/nas/docs"}
        # when
        path = _resolve_local_path(file_rec, cache, folders, volume_map)
        # then
        self.assertEqual(path, Path("/mnt/nas/docs/Projects/readme.txt"))

    def test_returns_none_when_cache_incomplete(self):
        # given
        file_rec = _node("fid", "a.txt", "missing_parent")
        cache: dict[str, NodeRecord | None] = {}
        folders = {"docs": "/volume1/docs"}
        volume_map = {"/volume1/docs": "/mnt/x"}
        # when
        path = _resolve_local_path(file_rec, cache, folders, volume_map)
        # then
        self.assertIsNone(path)

    def test_returns_none_when_no_volume_prefix_matches(self):
        # given
        file_rec = _node("fid", "a.txt", "folder_id")
        folder_rec = _node("folder_id", "x", "_docs", is_directory=True)
        cache = {"folder_id": folder_rec}
        folders = {"docs": "/volume1/docs"}
        volume_map = {"/other/prefix": "/mnt/other"}
        # when
        path = _resolve_local_path(file_rec, cache, folders, volume_map)
        # then
        self.assertIsNone(path)

    def test_picks_longest_volume_map_prefix(self):
        # given
        file_rec = _node("fid", "f.txt", "folder_id")
        folder_rec = _node("folder_id", "sub", "_share", is_directory=True)
        cache = {"folder_id": folder_rec}
        folders = {"share": "/vol/share"}
        volume_map = {
            "/vol": "/mnt/root",
            "/vol/share": "/mnt/share",
        }
        # when
        path = _resolve_local_path(file_rec, cache, folders, volume_map)
        # then
        self.assertEqual(path, Path("/mnt/share/sub/f.txt"))
