"""CLI entry point for the wcpan.drive.synology server."""

import argparse
import asyncio
import logging
import sys
from logging.config import dictConfig
from pathlib import Path

import yaml  # type: ignore[import-untyped]
from aiohttp import web
from wcpan.logging import ConfigBuilder

from ..types import ServerConfig
from ._app import create_app
from ._db import Storage, cleanup_dangling_nodes, reset_change_history
from ._enricher import enrich_subtree
from ._paths import virtual_path_to_directory_node_id
from ._reconcile import async_api_backfill


_L = logging.getLogger(__name__)


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
        help="Log level for the serve command (default: INFO)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("serve", help="Run the Synology mirror server")
    subparsers.add_parser(
        "gc", help="Remove dangling nodes unreachable from the server root"
    )
    enrich_p = subparsers.add_parser(
        "enrich",
        help="Enrich media metadata (width/height) from local files under PATH (requires volume_map)",
    )
    enrich_p.add_argument(
        "path",
        type=str,
        help="Virtual directory path in the mirror (e.g. / for root, /photos/Projects)",
    )
    enrich_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing the database",
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
        raw = yaml.safe_load(f)

    if args.command == "gc":
        count = cleanup_dangling_nodes(raw["database_url"])
        print(f"Removed {count} dangling node(s).")
        return

    if args.command == "squash":
        print("WARNING: squash resets all consumer cursors.", file=sys.stderr)
        count = reset_change_history(raw["database_url"])
        print(f"Reset change history: {count} update record(s) written.")
        return

    if args.command == "enrich":
        vm = raw.get("volume_map")
        if not vm:
            print(
                "Config error: non-empty 'volume_map' is required for enrich",
                file=sys.stderr,
            )
            sys.exit(1)
        try:
            storage = Storage(raw["database_url"])
            storage.ensure_schema()
            root_id = virtual_path_to_directory_node_id(storage, args.path.strip())
            stats = enrich_subtree(
                raw["database_url"],
                dict(raw["folders"]),
                dict(vm),
                root_id,
                dry_run=args.dry_run,
            )
        except ValueError as e:
            print(e, file=sys.stderr)
            sys.exit(1)
        print(
            f"Checked {stats['checked']} node(s), "
            f"updated {stats['updated']}, "
            f"skipped {stats['skipped']}."
        )
        return

    if args.command == "backfill":
        config = _server_config_from_raw(raw)
        try:
            stats = asyncio.run(
                async_api_backfill(
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
            f"list errors {stats['list_errors']}."
        )
        return

    config = _server_config_from_raw(raw)
    dictConfig(
        ConfigBuilder(path=config.log_path)
        .add("wcpan.drive.synology", level=args.log_level)
        .add("aiohttp")
        .to_dict()
    )

    app = create_app(config)
    _L.info("listening on %s:%s", config.host, config.port)
    web.run_app(app, host=config.host, port=config.port, print=None)


def _server_config_from_raw(raw: dict) -> ServerConfig:
    return ServerConfig(
        host=raw.get("host", "127.0.0.1"),
        port=int(raw.get("port", 8080)),
        database_url=raw["database_url"],
        synology_url=raw["synology_url"],
        username=raw["username"],
        password=raw["password"],
        folders=raw["folders"],
        public_url=raw["public_url"],
        webhook_app_id=raw.get("webhook_app_id", "wcpan-drive-synology"),
        volume_map=raw.get("volume_map"),
        otp_code=raw.get("otp_code"),
        log_path=raw.get("log_path"),
    )


if __name__ == "__main__":
    main()
