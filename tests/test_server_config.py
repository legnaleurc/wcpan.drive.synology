"""Tests for server config validation."""

import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

from wcpan.drive.synology._server.main import (
    CONFIG_VERSION,
    ConfigVersionError,
    _check_config_version,
    main,
)


class TestCheckConfigVersion(unittest.TestCase):
    def test_matching_version_is_accepted(self):
        _check_config_version({"version": CONFIG_VERSION})

    def test_missing_version_is_rejected(self):
        with self.assertRaises(ConfigVersionError):
            _check_config_version({})

    def test_wrong_version_is_rejected(self):
        with self.assertRaises(ConfigVersionError):
            _check_config_version({"version": 99})

    def test_non_integer_version_is_rejected(self):
        with self.assertRaises(ConfigVersionError):
            _check_config_version({"version": "1"})


class TestMainConfigVersion(unittest.TestCase):
    def test_gc_rejects_missing_version_before_running_command(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "server.yaml"
            config_path.write_text("database_url: sqlite:////tmp/test.db\n")
            stderr = io.StringIO()
            argv = [
                "wcpan.drive.synology",
                "--config",
                str(config_path),
                "gc",
            ]
            with (
                patch("sys.argv", argv),
                patch(
                    "wcpan.drive.synology._server.main.cleanup_dangling_nodes"
                ) as cleanup,
                redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main()
            self.assertEqual(cm.exception.code, 1)
            cleanup.assert_not_called()
            self.assertIn("config version mismatch", stderr.getvalue())
