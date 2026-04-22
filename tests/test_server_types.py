from unittest import TestCase

from wcpan.drive.synology._server.types import SynologyFileId, SynologyPermanentLink
from wcpan.drive.synology.types import MirrorMutableId, MirrorStableId


class TestSynologyMirrorIdConversions(TestCase):
    def test_permanent_link_to_mirror_stable_id(self) -> None:
        ref = SynologyPermanentLink(permanent_link="perm-1")

        result = ref.to_mirror_stable_id()

        self.assertEqual(result, MirrorStableId("perm-1"))
        self.assertIsInstance(result, str)

    def test_permanent_link_from_mirror_stable_id(self) -> None:
        result = SynologyPermanentLink.from_mirror_stable_id(MirrorStableId("perm-1"))

        self.assertEqual(result, SynologyPermanentLink(permanent_link="perm-1"))

    def test_file_id_to_mirror_mutable_id(self) -> None:
        ref = SynologyFileId(file_id="file-1")

        result = ref.to_mirror_mutable_id()

        self.assertEqual(result, MirrorMutableId("file-1"))
        self.assertIsInstance(result, str)

    def test_file_id_from_mirror_mutable_id(self) -> None:
        result = SynologyFileId.from_mirror_mutable_id(MirrorMutableId("file-1"))

        self.assertEqual(result, SynologyFileId(file_id="file-1"))
