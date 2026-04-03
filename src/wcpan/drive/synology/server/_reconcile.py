"""Reconcile SQLite node rows under a directory with Synology Drive list API."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import replace

from aiohttp import ClientSession

from ..types import NodeRecord, ServerConfig
from ._api.changes import convert_file_info
from ._api.files import SynologyFileInfo, list_children_for_parent
from ._auth import AuthManager
from ._db import Storage
from ._network import Network
from ._paths import virtual_path_to_directory_node_id
from ._virtual_ids import is_virtual


_L = logging.getLogger(__name__)


def _norm_hash(h: str) -> str:
    return h or ""


def _api_fields_differ(db: NodeRecord, merged: NodeRecord) -> bool:
    if db.name != merged.name:
        return True
    if db.is_directory != merged.is_directory:
        return True
    if db.size != merged.size:
        return True
    if int(db.mtime.timestamp()) != int(merged.mtime.timestamp()):
        return True
    if int(db.ctime.timestamp()) != int(merged.ctime.timestamp()):
        return True
    if _norm_hash(db.hash) != _norm_hash(merged.hash):
        return True
    if db.is_image != merged.is_image:
        return True
    if db.is_video != merged.is_video:
        return True
    if db.mime_type != merged.mime_type:
        return True
    return False


async def reconcile_subtree(
    storage: Storage,
    network: Network,
    folders: dict[str, str],
    root_node_id: str,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Compare API metadata to DB for all non-virtual nodes under *root_node_id*."""
    subtree_ids = storage.collect_subtree_node_ids(root_node_id)
    by_parent: dict[str, list[NodeRecord]] = defaultdict(list)
    for nid in subtree_ids:
        if is_virtual(nid):
            continue
        rec = storage.get_node_by_id(nid)
        if rec is None or rec.parent_id is None:
            continue
        by_parent[rec.parent_id].append(rec)

    checked = 0
    updated = 0
    missing = 0
    list_errors = 0

    for parent_id, rows in by_parent.items():
        try:
            items = await list_children_for_parent(network, parent_id, folders)
        except Exception:
            _L.exception("Failed to list parent %r", parent_id)
            list_errors += 1
            continue
        by_fid: dict[str, SynologyFileInfo] = {i["file_id"]: i for i in items}
        for rec in rows:
            checked += 1
            item = by_fid.get(rec.node_id)
            if item is None:
                missing += 1
                _L.warning(
                    "Node %r (%r) not in API listing for parent %r",
                    rec.node_id,
                    rec.name,
                    parent_id,
                )
                continue
            from_api = convert_file_info(item, parent_id=rec.parent_id)
            candidate = replace(
                from_api,
                width=rec.width,
                height=rec.height,
                ms_duration=rec.ms_duration,
            )
            if not _api_fields_differ(rec, candidate):
                continue
            updated += 1
            if dry_run:
                _L.info("dry-run: would update %r (%r)", rec.node_id, rec.name)
            else:
                storage.upsert_node_and_emit_change(candidate)

    return {
        "checked": checked,
        "updated": updated,
        "missing": missing,
        "list_errors": list_errors,
    }


async def async_api_backfill(
    config: ServerConfig,
    virtual_path: str,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Load config auth, resolve *virtual_path*, reconcile subtree."""
    storage = Storage(config.database_url)
    storage.ensure_schema()
    root_id = virtual_path_to_directory_node_id(storage, virtual_path)

    async with ClientSession() as session:
        auth = AuthManager(
            username=config.username,
            password=config.password,
            base_url=config.synology_url,
            otp_code=config.otp_code,
        )
        async with auth.authenticated(session):
            network = Network(session, auth)
            return await reconcile_subtree(
                storage,
                network,
                dict(config.folders),
                root_id,
                dry_run=dry_run,
            )
