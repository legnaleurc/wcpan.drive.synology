from wcpan.drive.core.exceptions import DriveError


__all__ = (
    "SynologyAuthenticationError",
    "SynologySessionExpiredError",
    "SynologyApiError",
    "SynologyUploadError",
    "SynologyNetworkError",
    "SynologyServerError",
)


class SynologyAuthenticationError(DriveError):
    """Raised when authentication with Synology DSM fails."""

    def __init__(self, message: str = "Authentication failed") -> None:
        super().__init__(message)


class SynologySessionExpiredError(DriveError):
    """Raised when the session token has expired."""

    def __init__(self, message: str = "Session expired") -> None:
        super().__init__(message)


class SynologyApiError(DriveError):
    """Raised when Synology API returns an error."""

    def __init__(
        self,
        message: str,
        error_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code


class SynologyUploadError(DriveError):
    """Raised when file upload fails."""

    def __init__(self, message: str, file_name: str | None = None) -> None:
        super().__init__(message)
        self.file_name = file_name


class SynologyNetworkError(DriveError):
    """Raised when a network error occurs."""

    def __init__(self, message: str, original_error: Exception | None = None) -> None:
        super().__init__(message)
        self.original_error = original_error


class SynologyServerError(DriveError):
    """Raised when the wcpan.drive.synology server returns an error."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status
