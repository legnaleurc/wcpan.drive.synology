"""CLI entry point for the wcpan.drive.synology server."""

import argparse
import asyncio
import logging
import sys
from contextlib import AsyncExitStack, suppress
from logging.config import dictConfig
from pathlib import Path, PurePosixPath

import yaml  # type: ignore[import-untyped]
from aiohttp import web
from wcpan.logging import ConfigBuilder

from .app import create_app, managed_off_main
from .lib.mounts import create_mount_registry
from .services.backfill import BackfillService
from .services.network import create_network_service
from .services.paths import SynologyPathService
from .services.storage import (
    SchemaVersionError,
    cleanup_dangling_nodes,
    create_storage_service,
    reset_change_history,
)
from .services.sync import NodeSyncService
from .types import RawServerConfig, ServerConfig, SynologyPath
from .workers import (
    METADATA_WORKER_COUNT,
    create_metadata_queue,
    create_write_queue,
    metadata_worker,
    write_worker,
)


_L = logging.getLogger(__name__)
CONFIG_VERSION = 1


class ConfigVersionError(ValueError):
    """Raised when the YAML config schema version is missing or unsupported."""


def main() -> None:
    parser = argparse.ArgumentParser(description="wcpan.drive.synology")
    parser.add_argument(
        "--config",
        default="/data/server.yaml",
        help="Path to YAML config file (default: /data/server.yaml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("serve", help="Run the Synology mirror server")
    subparsers.add_parser(
        "gc", help="Remove dangling nodes unreachable from the server root"
    )
    backfill_p = subparsers.add_parser(
        "backfill",
        help="Reconcile node metadata under PATH with Synology Drive API",
    )
    backfill_p.add_argument(
        "path",
        type=str,
        help="Virtual directory path in the mirror (e.g. / for root, /photos/Projects)",
    )
    backfill_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing the database",
    )
    subparsers.add_parser(
        "squash", help="[DANGER] Reset change history to a single update per node"
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        raw: RawServerConfig = yaml.safe_load(f)

    try:
        _check_config_version(raw)

        if args.command == "gc":
            count = cleanup_dangling_nodes(raw["database_url"])
            print(f"Removed {count} dangling node(s).")
            return

        if args.command == "squash":
            print("WARNING: squash resets all consumer cursors.", file=sys.stderr)
            count = reset_change_history(raw["database_url"])
            print(f"Reset change history: {count} update record(s) written.")
            return

        config = _server_config_from_raw(raw)
        dictConfig(
            ConfigBuilder(path=config.log_path)
            .add("wcpan.drive.synology", level=args.log_level)
            .add("aiohttp")
            .to_dict()
        )

        if args.command == "backfill":
            try:
                stats = asyncio.run(
                    run_backfill(
                        config,
                        args.path.strip(),
                        dry_run=args.dry_run,
                    )
                )
            except ValueError as e:
                print(e, file=sys.stderr)
                sys.exit(1)
            print(
                f"Checked {stats['checked']} node(s), "
                f"added {stats['added']}, "
                f"updated {stats['updated']}, "
                f"removed {stats['removed']}, "
                f"list errors {stats['list_errors']}."
            )
            return

        app = create_app(config)
        _L.info("listening on %s:%s", config.host, config.port)
        web.run_app(app, host=config.host, port=config.port, print=None)
    except (ConfigVersionError, SchemaVersionError, ValueError) as e:
        print(e, file=sys.stderr)
        sys.exit(1)


def _check_config_version(raw: RawServerConfig) -> None:
    if "version" not in raw:
        raise ConfigVersionError(
            f"config version mismatch: expected {CONFIG_VERSION}, got missing"
        )
    version = raw["version"]
    if type(version) is not int:
        raise ConfigVersionError(
            f"config version mismatch: expected {CONFIG_VERSION}, got {version!r}"
        )
    if version != CONFIG_VERSION:
        raise ConfigVersionError(
            f"config version mismatch: expected {CONFIG_VERSION}, got {version}"
        )


def _server_config_from_raw(raw: RawServerConfig) -> ServerConfig:
    return ServerConfig(
        host=raw.get("host", "127.0.0.1"),
        port=int(raw.get("port", 8080)),
        database_url=raw["database_url"],
        synology_url=raw["synology_url"],
        username=raw["username"],
        password=raw["password"],
        mounts={k: SynologyPath(PurePosixPath(v)) for k, v in raw["mounts"].items()},
        public_url=raw["public_url"],
        webhook_app_id=raw.get("webhook_app_id", "wcpan-drive-synology"),
        local_paths=raw["local_paths"],
        otp_code=raw.get("otp_code"),
        log_path=raw.get("log_path"),
        upload_tmp_dir=raw.get("upload_tmp_dir"),
    )


async def run_backfill(
    config: ServerConfig,
    virtual_path: str,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Build ``BackfillService`` with the same stack as server startup (no HTTP/webhook)."""
    async with AsyncExitStack() as stack:
        network = await stack.enter_async_context(
            create_network_service(
                base_url=config.synology_url,
                username=config.username,
                password=config.password,
                otp_code=config.otp_code,
            )
        )
        off_main = await stack.enter_async_context(managed_off_main())
        storage = await create_storage_service(config.database_url, off_main)
        write_queue = create_write_queue()
        metadata_queue = create_metadata_queue()
        node_sync = NodeSyncService(
            storage=storage,
            write_queue=write_queue,
            off_main=off_main,
            mounts=config.mounts,
            local_paths=config.local_paths,
            metadata_queue=metadata_queue,
        )
        mount_registry = await create_mount_registry(network, config.mounts)
        syno_paths = SynologyPathService(mount_registry)
        backfill = BackfillService(
            network=network,
            storage=storage,
            syno_paths=syno_paths,
            node_sync=node_sync,
        )
        write_task = asyncio.create_task(write_worker(write_queue))
        meta_tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(
                metadata_worker(metadata_queue, node_sync.process_metadata_item)
            )
            for _ in range(METADATA_WORKER_COUNT)
        ]
        try:
            stats = await backfill.run_virtual_path(virtual_path, dry_run=dry_run)
            await metadata_queue.join()
            await write_queue.join()
            return stats
        finally:
            for t in meta_tasks:
                t.cancel()
                with suppress(asyncio.CancelledError):
                    await t
            write_task.cancel()
            with suppress(asyncio.CancelledError):
                await write_task


if __name__ == "__main__":
    main()
