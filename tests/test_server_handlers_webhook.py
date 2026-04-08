"""Tests for webhook processing: path extraction, fetch-and-enrich, item dispatch, batch processing."""

import asyncio
import logging
from datetime import UTC, datetime
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology._server.lib.mounts import MountRegistry
from wcpan.drive.synology._server.services.sync import NodeSyncService
from wcpan.drive.synology._server.services.webhook import (
    _delayed_file_upsert,
    _fetch_and_enrich,
    _process_webhook_item,
)
from wcpan.drive.synology.types import NodeRecord


logging.getLogger("wcpan.drive.synology._server").setLevel(logging.CRITICAL + 1)


_EPOCH = datetime.fromtimestamp(0, UTC)

_FAKE_SYNO_INFO = {
    "file_id": "f1",
    "parent_id": "p1",
    "name": "test.txt",
    "type": "file",
    "content_type": "file",
    "size": 100,
    "created_time": 1000,
    "modified_time": 2000,
    "sync_id": 1,
}


def _make_node(node_id: str = "f1", parent_id: str = "p1") -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        parent_id=parent_id,
        name="test.txt",
        is_directory=False,
        ctime=_EPOCH,
        mtime=_EPOCH,
        mime_type="text/plain",
        hash="abc",
        size=100,
        is_image=False,
        is_video=False,
        width=0,
        height=0,
        ms_duration=0,
    )


# ---------------------------------------------------------------------------
# _fetch_and_enrich
# ---------------------------------------------------------------------------


class TestFetchAndEnrich(IsolatedAsyncioTestCase):
    async def test_success(self):
        network = MagicMock()
        storage = MagicMock()
        storage.get_node_by_id = AsyncMock(return_value=_make_node("p1"))
        node_sync = MagicMock(spec=NodeSyncService)
        node_sync.upsert = AsyncMock(return_value=_make_node())

        with patch(
            "wcpan.drive.synology._server.services.webhook.synology_files.get_file_metadata_by_id",
            new_callable=AsyncMock,
            return_value=_FAKE_SYNO_INFO,
        ):
            ok = await _fetch_and_enrich(
                network,
                storage,
                node_sync,
                "f1",
                "p1",
                "file_created",
                MountRegistry({}, {}),
            )
        self.assertTrue(ok)
        node_sync.upsert.assert_awaited_once()

    async def test_no_parent_id_returns_true(self):
        ok = await _fetch_and_enrich(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            "f1",
            "",
            "file_created",
            MountRegistry({}, {}),
        )
        self.assertTrue(ok)

    async def test_parent_not_in_db_not_mount_returns_true(self):
        storage = MagicMock()
        storage.get_node_by_id = AsyncMock(return_value=None)
        ok = await _fetch_and_enrich(
            MagicMock(),
            storage,
            MagicMock(),
            "f1",
            "unknown-parent",
            "file_created",
            MountRegistry({}, {}),
        )
        self.assertTrue(ok)

    async def test_parent_resolved_via_mount(self):
        storage = MagicMock()
        storage.get_node_by_id = AsyncMock(return_value=None)
        node_sync = MagicMock(spec=NodeSyncService)
        node_sync.upsert = AsyncMock(return_value=_make_node())
        registry = MountRegistry({}, {"real-parent": "_docs"})

        with patch(
            "wcpan.drive.synology._server.services.webhook.synology_files.get_file_metadata_by_id",
            new_callable=AsyncMock,
            return_value=_FAKE_SYNO_INFO,
        ):
            ok = await _fetch_and_enrich(
                MagicMock(),
                storage,
                node_sync,
                "f1",
                "real-parent",
                "file_created",
                registry,
            )
        self.assertTrue(ok)
        # parent_id should have been resolved to _docs
        call_args = node_sync.upsert.call_args[0][0]
        self.assertEqual(call_args.parent_id, "_docs")

    async def test_metadata_not_found_returns_false(self):
        storage = MagicMock()
        storage.get_node_by_id = AsyncMock(return_value=_make_node("p1"))

        with patch(
            "wcpan.drive.synology._server.services.webhook.synology_files.get_file_metadata_by_id",
            new_callable=AsyncMock,
            return_value=None,
        ):
            ok = await _fetch_and_enrich(
                MagicMock(),
                storage,
                MagicMock(),
                "f1",
                "p1",
                "file_created",
                MountRegistry({}, {}),
            )
        self.assertFalse(ok)


# ---------------------------------------------------------------------------
# _process_webhook_item
# ---------------------------------------------------------------------------


class TestProcessWebhookItem(IsolatedAsyncioTestCase):
    def _deps(self):
        network = MagicMock()
        storage = MagicMock()
        storage.get_node_by_id = AsyncMock(return_value=_make_node("p1"))
        node_sync = MagicMock(spec=NodeSyncService)
        node_sync.upsert = AsyncMock(return_value=_make_node())
        registry = MountRegistry({}, {})
        return network, storage, node_sync, registry

    async def test_empty_file_id_returns_noop(self):
        n, s, cs, mr = self._deps()
        delete_id, needs = await _process_webhook_item(
            {"event_type": "file_created", "file_id": ""},
            n,
            s,
            cs,
            mr,
        )
        self.assertIsNone(delete_id)
        self.assertFalse(needs)

    async def test_file_removed_returns_delete_id(self):
        n, s, cs, mr = self._deps()
        delete_id, needs = await _process_webhook_item(
            {"event_type": "file_removed", "file_id": "f1"},
            n,
            s,
            cs,
            mr,
        )
        self.assertEqual(delete_id, "f1")
        self.assertFalse(needs)

    async def test_file_modified_file_schedules_delayed(self):
        n, s, cs, mr = self._deps()
        delete_id, needs = await _process_webhook_item(
            {"event_type": "file_modified", "file_id": "f1", "file_type": "file"},
            n,
            s,
            cs,
            mr,
        )
        self.assertIsNone(delete_id)
        self.assertTrue(needs)

    async def test_file_modified_dir_is_noop(self):
        n, s, cs, mr = self._deps()
        delete_id, needs = await _process_webhook_item(
            {"event_type": "file_modified", "file_id": "f1", "file_type": "dir"},
            n,
            s,
            cs,
            mr,
        )
        self.assertIsNone(delete_id)
        self.assertFalse(needs)

    async def test_file_created_dir_upserts_immediately(self):
        n, s, cs, mr = self._deps()
        with patch(
            "wcpan.drive.synology._server.services.webhook.synology_files.get_file_metadata_by_id",
            new_callable=AsyncMock,
            return_value=_FAKE_SYNO_INFO,
        ):
            delete_id, needs = await _process_webhook_item(
                {
                    "event_type": "file_created",
                    "file_id": "f1",
                    "file_type": "dir",
                    "parent_id": "p1",
                },
                n,
                s,
                cs,
                mr,
            )
        self.assertIsNone(delete_id)
        self.assertFalse(needs)
        cs.upsert.assert_awaited_once()

    async def test_file_moved_upserts_immediately(self):
        n, s, cs, mr = self._deps()
        with patch(
            "wcpan.drive.synology._server.services.webhook.synology_files.get_file_metadata_by_id",
            new_callable=AsyncMock,
            return_value=_FAKE_SYNO_INFO,
        ):
            delete_id, needs = await _process_webhook_item(
                {"event_type": "file_moved", "file_id": "f1", "parent_id": "p1"},
                n,
                s,
                cs,
                mr,
            )
        self.assertIsNone(delete_id)
        self.assertFalse(needs)
        cs.upsert.assert_awaited_once()

    async def test_file_renamed_upserts_immediately(self):
        n, s, cs, mr = self._deps()
        with patch(
            "wcpan.drive.synology._server.services.webhook.synology_files.get_file_metadata_by_id",
            new_callable=AsyncMock,
            return_value=_FAKE_SYNO_INFO,
        ):
            delete_id, needs = await _process_webhook_item(
                {"event_type": "file_renamed", "file_id": "f1", "parent_id": "p1"},
                n,
                s,
                cs,
                mr,
            )
        self.assertIsNone(delete_id)
        self.assertFalse(needs)

    async def test_file_created_file_schedules_delayed(self):
        n, s, cs, mr = self._deps()
        delete_id, needs = await _process_webhook_item(
            {
                "event_type": "file_created",
                "file_id": "f1",
                "file_type": "file",
                "parent_id": "p1",
            },
            n,
            s,
            cs,
            mr,
        )
        self.assertIsNone(delete_id)
        self.assertTrue(needs)

    async def test_unknown_event_type_is_noop(self):
        n, s, cs, mr = self._deps()
        delete_id, needs = await _process_webhook_item(
            {"event_type": "something_else", "file_id": "f1"},
            n,
            s,
            cs,
            mr,
        )
        self.assertIsNone(delete_id)
        self.assertFalse(needs)


# ---------------------------------------------------------------------------
# _delayed_file_upsert
# ---------------------------------------------------------------------------


class TestDelayedFileUpsert(IsolatedAsyncioTestCase):
    async def test_succeeds_on_first_retry(self):
        with (
            patch(
                "wcpan.drive.synology._server.services.webhook._fetch_and_enrich",
                new_callable=AsyncMock,
                return_value=True,
            ) as mock_fetch,
            patch(
                "wcpan.drive.synology._server.services.webhook.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await _delayed_file_upsert(
                "f1",
                "p1",
                "file_created",
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MountRegistry({}, {}),
            )
        # Only one sleep (first retry delay)
        mock_sleep.assert_awaited_once()
        mock_fetch.assert_awaited_once()

    async def test_retries_then_succeeds(self):
        with (
            patch(
                "wcpan.drive.synology._server.services.webhook._fetch_and_enrich",
                new_callable=AsyncMock,
                side_effect=[False, False, True],
            ) as mock_fetch,
            patch(
                "wcpan.drive.synology._server.services.webhook.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
        ):
            await _delayed_file_upsert(
                "f1",
                "p1",
                "file_created",
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MountRegistry({}, {}),
            )
        self.assertEqual(mock_fetch.await_count, 3)
        self.assertEqual(mock_sleep.await_count, 3)

    async def test_exhausts_all_retries(self):
        with (
            patch(
                "wcpan.drive.synology._server.services.webhook._fetch_and_enrich",
                new_callable=AsyncMock,
                return_value=False,
            ) as mock_fetch,
            patch(
                "wcpan.drive.synology._server.services.webhook.asyncio.sleep",
                new_callable=AsyncMock,
            ),
        ):
            await _delayed_file_upsert(
                "f1",
                "p1",
                "file_created",
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MountRegistry({}, {}),
            )
        self.assertEqual(mock_fetch.await_count, 3)
