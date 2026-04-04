import asyncio

from aiohttp.web import AppKey

from ..types import ServerConfig
from ._lib import OffMainThread
from ._types import WriteQueue
from ._upload_session import UploadSessionStore


config_key: AppKey[ServerConfig] = AppKey("server_config")
off_main_key: AppKey[OffMainThread] = AppKey("off_main")
write_queue_key: AppKey[WriteQueue] = AppKey("write_queue")
storage_key: AppKey = AppKey("storage")
network_key: AppKey = AppKey("network")
ready_key: AppKey[bool] = AppKey("ready")
folders_key: AppKey[dict[str, str]] = AppKey("folders")
volume_map_key: AppKey[dict[str, str] | None] = AppKey("volume_map")
trigger_event_key: AppKey[asyncio.Event] = AppKey("trigger_event")
webhook_token_key: AppKey[str] = AppKey("webhook_token")
upload_sessions_key: AppKey[UploadSessionStore] = AppKey("upload_sessions")
