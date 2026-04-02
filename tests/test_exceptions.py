"""Tests for Synology exception types."""

from unittest import TestCase

from wcpan.drive.synology.exceptions import (
    SynologyApiError,
    SynologyAuthenticationError,
    SynologyNetworkError,
    SynologyServerError,
    SynologySessionExpiredError,
    SynologyUploadError,
)


class TestSynologyAuthenticationError(TestCase):
    def test_default_message(self):
        # given
        # when
        err = SynologyAuthenticationError()
        # then
        self.assertEqual(str(err), "Authentication failed")

    def test_custom_message(self):
        # given
        msg = "bad creds"
        # when
        err = SynologyAuthenticationError(msg)
        # then
        self.assertEqual(str(err), msg)


class TestSynologySessionExpiredError(TestCase):
    def test_default_message(self):
        # when
        err = SynologySessionExpiredError()
        # then
        self.assertEqual(str(err), "Session expired")


class TestSynologyApiError(TestCase):
    def test_error_code_stored(self):
        # given
        code = 403
        # when
        err = SynologyApiError("api failed", error_code=code)
        # then
        self.assertEqual(err.error_code, code)
        self.assertEqual(str(err), "api failed")


class TestSynologyUploadError(TestCase):
    def test_file_name_stored(self):
        # when
        err = SynologyUploadError("up fail", file_name="a.bin")
        # then
        self.assertEqual(err.file_name, "a.bin")


class TestSynologyNetworkError(TestCase):
    def test_original_error_stored(self):
        # given
        cause = OSError("econnrefused")
        # when
        err = SynologyNetworkError("net", original_error=cause)
        # then
        self.assertIs(err.original_error, cause)


class TestSynologyServerError(TestCase):
    def test_status_stored(self):
        # when
        err = SynologyServerError("srv", status=503)
        # then
        self.assertEqual(err.status, 503)
