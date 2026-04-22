from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterable
from contextlib import AbstractAsyncContextManager

from aiohttp import ClientResponse

from ..types import (
    SynologyFolderRef,
    SynologyLookupRef,
    SynologyNodeRef,
    SynologyParentRef,
    SynologyPath,
)
from .types import SynologyFileInfo, SynologyWebhookInfo


class SynologyDriveApi(metaclass=ABCMeta):
    @abstractmethod
    async def get_node_metadata(
        self,
        node_ref: SynologyLookupRef,
    ) -> SynologyFileInfo | None: ...

    @abstractmethod
    async def get_file_metadata_by_path(
        self,
        syno_path: SynologyPath,
    ) -> SynologyFileInfo | None: ...

    @abstractmethod
    async def list_folder(
        self,
        folder_ref: SynologyFolderRef,
        offset: int = 0,
        limit: int = 1000,
    ) -> tuple[list[SynologyFileInfo], int]: ...

    @abstractmethod
    async def list_folder_all(
        self,
        folder_ref: SynologyFolderRef,
        page_size: int = 1000,
    ) -> list[SynologyFileInfo]: ...

    @abstractmethod
    async def create_folder(
        self,
        parent_ref: SynologyParentRef,
        name: str,
    ) -> SynologyFileInfo: ...

    @abstractmethod
    async def rename_node(
        self,
        node_ref: SynologyNodeRef,
        new_name: str,
    ) -> SynologyFileInfo: ...

    @abstractmethod
    async def move_node(
        self,
        node_ref: SynologyNodeRef,
        new_parent_ref: SynologyParentRef,
    ) -> None: ...

    @abstractmethod
    async def delete_node(self, node_ref: SynologyNodeRef) -> None: ...

    @abstractmethod
    async def upload_file(
        self,
        parent_ref: SynologyParentRef,
        name: str,
        data: AsyncIterable[bytes],
        mime_type: str | None = None,
    ) -> SynologyFileInfo: ...

    @abstractmethod
    def download_file(
        self,
        node_ref: SynologyNodeRef,
        range_: slice | None = None,
    ) -> AbstractAsyncContextManager[ClientResponse]: ...

    @abstractmethod
    async def create_webhook(
        self,
        url: str,
        app_id: str,
    ) -> str: ...

    @abstractmethod
    async def delete_webhook(
        self,
        webhook_id: str,
        app_id: str,
    ) -> None: ...

    @abstractmethod
    async def list_webhooks(
        self,
        app_id: str,
    ) -> list[SynologyWebhookInfo]: ...
