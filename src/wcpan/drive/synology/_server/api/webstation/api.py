from collections.abc import AsyncIterable
from contextlib import AbstractAsyncContextManager

from aiohttp import ClientResponse

from ...types import (
    SynologyFolderRef,
    SynologyLookupRef,
    SynologyNodeRef,
    SynologyParentRef,
    SynologyPath,
)
from ..drive import SynologyDriveApi
from ..types import SynologyFileInfo, SynologyWebhookInfo
from .files import (
    create_folder,
    delete_node,
    download_file,
    get_file_metadata_by_path,
    get_node_metadata,
    list_folder,
    list_folder_all,
    move_node,
    rename_node,
    upload_file,
)
from .network import WebStationNetworkService
from .webhooks import create_webhook, delete_webhook, list_webhooks


class WebStationSynologyDriveApi(SynologyDriveApi):
    def __init__(self, *, network: WebStationNetworkService) -> None:
        self._network = network

    async def get_node_metadata(
        self,
        node_ref: SynologyLookupRef,
    ) -> SynologyFileInfo | None:
        return await get_node_metadata(node_ref, network=self._network)

    async def get_file_metadata_by_path(
        self,
        syno_path: SynologyPath,
    ) -> SynologyFileInfo | None:
        return await get_file_metadata_by_path(syno_path, network=self._network)

    async def list_folder(
        self,
        folder_ref: SynologyFolderRef,
        offset: int = 0,
        limit: int = 1000,
    ) -> tuple[list[SynologyFileInfo], int]:
        return await list_folder(
            folder_ref,
            network=self._network,
            offset=offset,
            limit=limit,
        )

    async def list_folder_all(
        self,
        folder_ref: SynologyFolderRef,
        page_size: int = 1000,
    ) -> list[SynologyFileInfo]:
        return await list_folder_all(
            folder_ref,
            network=self._network,
            page_size=page_size,
        )

    async def create_folder(
        self,
        parent_ref: SynologyParentRef,
        name: str,
    ) -> SynologyFileInfo:
        return await create_folder(parent_ref, name, network=self._network)

    async def rename_node(
        self,
        node_ref: SynologyNodeRef,
        new_name: str,
    ) -> SynologyFileInfo:
        return await rename_node(node_ref, new_name, network=self._network)

    async def move_node(
        self,
        node_ref: SynologyNodeRef,
        new_parent_ref: SynologyParentRef,
    ) -> None:
        await move_node(node_ref, new_parent_ref, network=self._network)

    async def delete_node(self, node_ref: SynologyNodeRef) -> None:
        await delete_node(node_ref, network=self._network)

    async def upload_file(
        self,
        parent_ref: SynologyParentRef,
        name: str,
        data: AsyncIterable[bytes],
        mime_type: str | None = None,
    ) -> SynologyFileInfo:
        return await upload_file(
            parent_ref,
            name,
            data,
            network=self._network,
            mime_type=mime_type,
        )

    def download_file(
        self,
        node_ref: SynologyNodeRef,
        range_: slice | None = None,
    ) -> AbstractAsyncContextManager[ClientResponse]:
        return download_file(node_ref, network=self._network, range_=range_)

    async def create_webhook(
        self,
        url: str,
        app_id: str,
    ) -> str:
        return await create_webhook(url, app_id, network=self._network)

    async def delete_webhook(
        self,
        webhook_id: str,
        app_id: str,
    ) -> None:
        await delete_webhook(webhook_id, app_id, network=self._network)

    async def list_webhooks(
        self,
        app_id: str,
    ) -> list[SynologyWebhookInfo]:
        return await list_webhooks(app_id, network=self._network)
