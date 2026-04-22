"""Synology Drive API data types."""

from typing import NotRequired, TypedDict


class SynologyFileInfo(TypedDict):
    """Used fields from the Synology Drive FileInfo payload."""

    file_id: str
    parent_id: str
    permanent_link: str
    name: str
    type: str  # "file" or "dir"
    content_type: str  # "dir", "document", "image", "audio", "video", "file"
    hash: NotRequired[str]
    size: int
    created_time: int  # Unix timestamp seconds
    modified_time: int  # Unix timestamp seconds
    sync_id: int
    max_id: NotRequired[int]
    removed: NotRequired[bool]


class SynologyWebhookInfo(TypedDict):
    webhook_id: str
    app_id: str
    type: str
    url: str
    so_name: str
    token: str


class SynologyWebhookEvent(TypedDict):
    event_type: str
    file_id: str
    permanent_link: str
    file_type: str
    parent_id: str


class SynologyFileListResponse(TypedDict):
    items: list[SynologyFileInfo]
    total: int


class SynologyWebhookListResponse(TypedDict):
    items: list[SynologyWebhookInfo]
    total: int


class SynologyWebhookCreateResponse(TypedDict):
    webhook_id: str


class SynologyTaskError(TypedDict):
    code: int
    message: NotRequired[str]


class SynologyTaskResult(TypedDict):
    errors: list[SynologyTaskError]


class SynologyTaskInfo(TypedDict):
    status: str
    result: NotRequired[SynologyTaskResult]


class SynologyAsyncTaskResponse(TypedDict):
    async_task_id: str
