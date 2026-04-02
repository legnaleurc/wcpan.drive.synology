"""Tests for virtual node_id grammar."""

from unittest import TestCase

from wcpan.drive.synology.server._virtual_ids import (
    SERVER_ROOT_ID,
    is_mount_node_id,
    is_virtual,
    mount_id,
    mount_name,
    synology_parent_ref,
)


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
        folders = {"photos": "/volume1/photos"}
        # when
        result = synology_parent_ref("_photos", folders)
        # then
        self.assertEqual(result, "/volume1/photos")

    def test_root_uses_id_prefix(self):
        # given
        folders: dict[str, str] = {}
        # when
        result = synology_parent_ref("_", folders)
        # then
        self.assertEqual(result, "id:_")

    def test_real_parent_uses_id_prefix(self):
        # given
        folders: dict[str, str] = {}
        # when
        result = synology_parent_ref("syno-file-7", folders)
        # then
        self.assertEqual(result, "id:syno-file-7")
