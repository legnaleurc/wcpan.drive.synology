"""Tests for change-scan record conversion."""

import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology._lib import FOLDER_MIME_TYPE
from wcpan.drive.synology._server.lib.nodes import convert_file_info as _convert
from wcpan.drive.synology._server.services.enricher import MediaEnrichService
from wcpan.drive.synology._server.services.off_main import OffMainThreadService
from wcpan.drive.synology._server.services.paths import LocalPathService
from wcpan.drive.synology._server.services.sync import NodeSyncService
from wcpan.drive.synology._server.types import MetadataWorkItem
from wcpan.drive.synology._server.workers import (
    create_metadata_queue,
    create_write_queue,
)


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

    def test_file_with_image_metadata_flat_dimensions(self):
        info = {
            "file_id": "f2",
            "parent_id": "p",
            "name": "wide.png",
            "type": "file",
            "content_type": "image",
            "size": 10,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 1,
            "image_metadata": {"width": 1920, "height": 1080},
        }
        record = _convert(info, parent_id="par")
        self.assertEqual(record.width, 1920)
        self.assertEqual(record.height, 1080)
        self.assertEqual(record.ms_duration, 0)

    def test_file_with_image_metadata_nested_resolution(self):
        info = {
            "file_id": "f3",
            "parent_id": "p",
            "name": "nested.jpg",
            "type": "file",
            "content_type": "image",
            "size": 10,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 1,
            "image_metadata": {"resolution": {"width": 640, "height": 480}},
        }
        record = _convert(info, parent_id="par")
        self.assertEqual(record.width, 640)
        self.assertEqual(record.height, 480)

    def test_video_with_image_metadata_duration(self):
        info = {
            "file_id": "v2",
            "parent_id": "p",
            "name": "clip.mp4",
            "type": "file",
            "content_type": "video",
            "size": 100,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 2,
            "image_metadata": {
                "width": 1280,
                "height": 720,
                "duration": 5000,
            },
        }
        record = _convert(info, parent_id="root")
        self.assertEqual(record.width, 1280)
        self.assertEqual(record.height, 720)
        self.assertEqual(record.ms_duration, 5000)


class TestEnrichPreservesApiDimensions(IsolatedAsyncioTestCase):
    async def test_enrich_without_force_refresh_does_not_probe_when_api_filled(self):
        info = {
            "file_id": "f4",
            "parent_id": "p",
            "name": "api.png",
            "type": "file",
            "content_type": "image",
            "size": 10,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 1,
            "image_metadata": {"width": 800, "height": 600},
        }
        record = _convert(info, parent_id="par")
        local_path_svc = LocalPathService(MagicMock(), {}, {"docs": "/tmp"})
        enricher = MediaEnrichService(local_path_svc)
        with patch(
            "wcpan.drive.synology._server.services.enricher._probe_sync",
            side_effect=AssertionError(
                "probe must not run when API supplied dimensions"
            ),
        ):
            out = await enricher.enrich(record, force_refresh=False)
        self.assertEqual(out.width, 800)
        self.assertEqual(out.height, 600)

    async def test_enrich_force_refresh_invokes_probe_when_api_filled(self) -> None:
        info = {
            "file_id": "f5",
            "parent_id": "p",
            "name": "api2.png",
            "type": "file",
            "content_type": "image",
            "size": 10,
            "created_time": 0,
            "modified_time": 0,
            "sync_id": 1,
            "image_metadata": {"width": 800, "height": 600},
        }
        record = _convert(info, parent_id="par")
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            probe_path = Path(tmp.name)
        try:
            with ThreadPoolExecutor(1) as pool:
                off_main = OffMainThreadService(pool)
                local_path_svc = LocalPathService(MagicMock(), {}, {"docs": "/tmp"})
                enricher = MediaEnrichService(local_path_svc, off_main)
                with (
                    patch.object(
                        local_path_svc,
                        "resolve_local_path",
                        new=AsyncMock(return_value=probe_path),
                    ),
                    patch(
                        "wcpan.drive.synology._server.services.enricher._probe_sync",
                        return_value=(100, 200, 0),
                    ) as probe,
                ):
                    out = await enricher.enrich(record, force_refresh=True)
            probe.assert_called_once()
            self.assertEqual(out.width, 100)
            self.assertEqual(out.height, 200)
        finally:
            probe_path.unlink(missing_ok=True)


class TestNodeSyncServiceMetadataQueue(IsolatedAsyncioTestCase):
    async def test_process_metadata_reconcile_insert_enqueues_write(self) -> None:
        wq = create_write_queue()
        mq = create_metadata_queue()
        with ThreadPoolExecutor(1) as pool:
            off_main = OffMainThreadService(pool)
            storage = MagicMock()
            cs = NodeSyncService(
                storage,
                wq,
                off_main,
                {},
                {"docs": "/tmp"},
                metadata_queue=mq,
            )
            info = {
                "file_id": "n1",
                "parent_id": "p",
                "name": "a.png",
                "type": "file",
                "content_type": "image",
                "size": 1,
                "created_time": 0,
                "modified_time": 0,
                "sync_id": 1,
            }
            record = _convert(info, parent_id="par")
            with patch.object(
                cs._enricher,
                "enrich",
                new=AsyncMock(return_value=record),
            ):
                await cs.process_metadata_item(
                    MetadataWorkItem(
                        record=record,
                        force_refresh=True,
                    )
                )
            job = await wq.get()
            try:
                job()
            finally:
                wq.task_done()
            storage.upsert_node_and_emit_change.assert_called_once_with(record)
