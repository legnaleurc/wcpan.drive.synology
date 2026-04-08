"""Reconcile SQLite node rows under a directory with Synology Drive list API."""

from __future__ import annotations

import logging
from dataclasses import replace

from aiohttp import ClientSession

from ..types import NodeRecord, ServerConfig
from ._api.changes import convert_file_info
from ._api.files import list_children_for_parent
from ._auth import AuthManager
from ._db import Storage
from ._enricher import enrich_record_sync
from ._network import Network
from ._paths import virtual_path_to_directory_node_id
from ._virtual_ids import SERVER_ROOT_ID


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


def _full_fields_differ(db: NodeRecord, merged: NodeRecord) -> bool:
    if _api_fields_differ(db, merged):
        return True
    if db.width != merged.width:
        return True
    if db.height != merged.height:
        return True
    if db.ms_duration != merged.ms_duration:
        return True
    return False


async def reconcile_subtree(
    storage: Storage,
    network: Network,
    folders: dict[str, str],
    root_node_id: str,
    *,
    volume_map: dict[str, str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """BFS-walk the API from root_node_id, adding missing nodes and updating changed ones.

    For each folder visited, API children are compared against DB children:
    - Missing from DB: inserted via upsert_node_and_emit_change.
    - Present but metadata differs: updated, preserving width/height/ms_duration.
    - Subdirectories are queued for recursive traversal.

    SERVER_ROOT_ID cannot be listed via the API; its DB children (mount nodes) are
    used as the initial queue instead.
    """
    if root_node_id == SERVER_ROOT_ID:
        queue = [c.node_id for c in storage.get_children(root_node_id)]
    else:
        queue = [root_node_id]

    checked = added = updated = removed = list_errors = 0

    while queue:
        parent_id = queue.pop(0)
        try:
            items = await list_children_for_parent(network, parent_id, folders)
        except Exception:
            _L.exception("Failed to list parent %r", parent_id)
            list_errors += 1
            continue

        db_by_id: dict[str, NodeRecord] = {
            r.node_id: r for r in storage.get_children(parent_id)
        }

        for item in items:
            checked += 1
            fid = item["file_id"]
            from_api = convert_file_info(item, parent_id=parent_id)
            existing = db_by_id.get(fid)

            if existing is None:
                node_to_insert = from_api
                if volume_map:
                    node_to_insert = enrich_record_sync(
                        from_api, storage, folders, volume_map
                    )
                added += 1
                _L.info(
                    "Adding missing node %r (%r) under %r", fid, item["name"], parent_id
                )
                if not dry_run:
                    storage.upsert_node_and_emit_change(node_to_insert)
            else:
                if volume_map:
                    enriched = enrich_record_sync(
                        from_api, storage, folders, volume_map
                    )
                    candidate = replace(
                        enriched,
                        width=enriched.width or existing.width,
                        height=enriched.height or existing.height,
                        ms_duration=enriched.ms_duration or existing.ms_duration,
                    )
                else:
                    candidate = replace(
                        from_api,
                        width=existing.width,
                        height=existing.height,
                        ms_duration=existing.ms_duration,
                    )
                if _full_fields_differ(existing, candidate):
                    updated += 1
                    if dry_run:
                        _L.info("dry-run: would update %r (%r)", fid, item["name"])
                    else:
                        storage.upsert_node_and_emit_change(candidate)

            if item["type"] == "dir":
                queue.append(fid)

        api_ids = {item["file_id"] for item in items}
        for fid, existing in db_by_id.items():
            if fid not in api_ids:
                removed += 1
                _L.info(
                    "Removing node %r (%r) absent from API under %r",
                    fid,
                    existing.name,
                    parent_id,
                )
                if not dry_run:
                    storage.delete_subtree_and_emit_changes(existing.node_id)

    return {
        "checked": checked,
        "added": added,
        "updated": updated,
        "removed": removed,
        "list_errors": list_errors,
    }


async def async_api_backfill(
    config: ServerConfig,
    virtual_path: str,
    *,
    dry_run: bool = False,
    volume_map: dict[str, str] | None = None,
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
                volume_map=volume_map,
                dry_run=dry_run,
            )
