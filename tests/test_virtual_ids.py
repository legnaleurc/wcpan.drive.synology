"""Tests for virtual node_id grammar and mount registry helpers."""

from pathlib import PurePosixPath
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from wcpan.drive.synology._server.lib.mounts import (
    SERVER_ROOT_ID,
    MountRegistry,
    create_mount_registry,
    is_mount_node_id,
    is_virtual,
    mount_id,
    mount_name,
)
from wcpan.drive.synology._server.services.paths import SynologyPathService
from wcpan.drive.synology._server.types import SynologyPath


class TestIsVirtual(TestCase):
    def test_root_is_virtual(self):
        # given
        node_id = SERVER_ROOT_ID
        # when
        result = is_virtual(node_id)
        # then
        self.assertTrue(result)

    def test_mount_is_virtual(self):
        # given
        node_id = "_photos"
        # when
        result = is_virtual(node_id)
        # then
        self.assertTrue(result)

    def test_synology_id_not_virtual(self):
        # given
        node_id = "12345"
        # when
        result = is_virtual(node_id)
        # then
        self.assertFalse(result)


class TestIsMountNodeId(TestCase):
    def test_bare_root_not_mount(self):
        # given
        node_id = "_"
        # when
        result = is_mount_node_id(node_id)
        # then
        self.assertFalse(result)

    def test_named_mount_is_mount(self):
        # given
        node_id = "_share-a"
        # when
        result = is_mount_node_id(node_id)
        # then
        self.assertTrue(result)


class TestMountId(TestCase):
    def test_prefixes_name(self):
        # given
        name = "my-share"
        # when
        result = mount_id(name)
        # then
        self.assertEqual(result, "_my-share")


class TestMountName(TestCase):
    def test_returns_key_for_mount(self):
        # given
        node_id = "_foo"
        # when
        result = mount_name(node_id)
        # then
        self.assertEqual(result, "foo")

    def test_root_returns_none(self):
        # given
        node_id = "_"
        # when
        result = mount_name(node_id)
        # then
        self.assertIsNone(result)

    def test_real_id_returns_none(self):
        # given
        node_id = "999"
        # when
        result = mount_name(node_id)
        # then
        self.assertIsNone(result)


class TestSynologyParentRef(TestCase):
    def test_mount_maps_to_folder_path(self):
        # given
        svc = SynologyPathService(MountRegistry({"photos": "/volume1/photos"}, {}))
        # when
        result = svc.synology_parent_ref("_photos")
        # then
        self.assertEqual(result, "/volume1/photos")

    def test_root_uses_id_prefix(self):
        # given
        svc = SynologyPathService(MountRegistry({}, {}))
        # when
        result = svc.synology_parent_ref("_")
        # then
        self.assertEqual(result, "id:_")

    def test_real_parent_uses_id_prefix(self):
        # given
        svc = SynologyPathService(MountRegistry({}, {}))
        # when
        result = svc.synology_parent_ref("syno-file-7")
        # then
        self.assertEqual(result, "id:syno-file-7")


class TestCreateMountRegistry(IsolatedAsyncioTestCase):
    async def test_empty_mounts(self):
        registry = await create_mount_registry(MagicMock(), {})
        self.assertEqual(registry.mounts, {})
        self.assertIsNone(registry.lookup_mount_virtual_id("unknown"))

    async def test_resolves_root_ids_for_non_nested_mounts(self):
        mounts = {
            "photos": SynologyPath(PurePosixPath("/volume1/photos")),
            "videos": SynologyPath(PurePosixPath("/volume1/videos")),
        }
        network = MagicMock()
        with patch(
            "wcpan.drive.synology._server.lib.mounts.get_file_metadata_by_path",
            new_callable=AsyncMock,
            side_effect=[
                {"file_id": "id-1"},
                {"file_id": "id-2"},
            ],
        ) as mock_get:
            registry = await create_mount_registry(network, mounts)
        self.assertEqual(registry.mounts, mounts)
        self.assertEqual(registry.lookup_mount_virtual_id("id-1"), "_photos")
        self.assertEqual(registry.lookup_mount_virtual_id("id-2"), "_videos")
        self.assertEqual(mock_get.await_count, 2)

    async def test_nested_mounts_raise_before_metadata_lookup(self):
        mounts = {
            "photos": SynologyPath(PurePosixPath("/volume1/photos")),
            "photos_2024": SynologyPath(PurePosixPath("/volume1/photos/2024")),
        }
        with patch(
            "wcpan.drive.synology._server.lib.mounts.get_file_metadata_by_path",
            new_callable=AsyncMock,
        ) as mock_get:
            with self.assertRaises(ValueError):
                await create_mount_registry(MagicMock(), mounts)
        mock_get.assert_not_awaited()

    async def test_nested_mounts_raise_in_reversed_order(self):
        mounts = {
            "photos_2024": SynologyPath(PurePosixPath("/volume1/photos/2024")),
            "photos": SynologyPath(PurePosixPath("/volume1/photos")),
        }
        with self.assertRaises(ValueError):
            await create_mount_registry(MagicMock(), mounts)

    async def test_prefix_but_not_subdirectory_is_ok(self):
        mounts = {
            "photos": SynologyPath(PurePosixPath("/volume1/photos")),
            "photos_archive": SynologyPath(PurePosixPath("/volume1/photos_archive")),
        }
        with patch(
            "wcpan.drive.synology._server.lib.mounts.get_file_metadata_by_path",
            new_callable=AsyncMock,
            side_effect=[
                {"file_id": "id-1"},
                {"file_id": "id-2"},
            ],
        ):
            registry = await create_mount_registry(MagicMock(), mounts)
        self.assertEqual(registry.lookup_mount_virtual_id("id-1"), "_photos")
        self.assertEqual(registry.lookup_mount_virtual_id("id-2"), "_photos_archive")

    async def test_trailing_slash_is_normalized_for_nested_check(self):
        mounts = {
            "a": SynologyPath(PurePosixPath("/volume1/photos/")),
            "b": SynologyPath(PurePosixPath("/volume1/photos/2024")),
        }
        with self.assertRaises(ValueError):
            await create_mount_registry(MagicMock(), mounts)
