from aiohttp.web import AppKey

from .lib.mounts import MountRegistry
from .services.network import NetworkService
from .services.off_main import OffMainThreadService
from .services.paths import SynologyPathService
from .services.storage import StorageService
from .services.sync import NodeSyncService
from .services.upload import UploadSessionService
from .types import ServerConfig, WebhookQueue, WriteQueue


CONFIG_KEY = AppKey[ServerConfig]("server_config")
MOUNT_REGISTRY_KEY = AppKey[MountRegistry]("mount_registry")
OFF_MAIN_KEY = AppKey[OffMainThreadService]("off_main")
WRITE_QUEUE_KEY = AppKey[WriteQueue]("write_queue")
STORAGE_KEY = AppKey[StorageService]("storage")
NETWORK_KEY = AppKey[NetworkService]("network")
READY_KEY = AppKey[bool]("ready")
SYNOLOGY_PATH_KEY = AppKey[SynologyPathService]("synology_path")
CHANGE_SERVICE_KEY = AppKey[NodeSyncService]("change_service")
UPLOAD_SESSIONS_KEY = AppKey[UploadSessionService]("upload_sessions")
WEBHOOK_QUEUE_KEY = AppKey[WebhookQueue]("webhook_queue")
