import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime

from ..types import NodeRecord
from ._virtual_ids import SERVER_ROOT_ID, is_virtual


_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA cache_size = -65536;

CREATE TABLE IF NOT EXISTS nodes (
    node_id     TEXT    PRIMARY KEY,
    parent_id   TEXT,
    name        TEXT    NOT NULL,
    is_directory INTEGER NOT NULL DEFAULT 0,
    ctime       INTEGER NOT NULL,
    mtime       INTEGER NOT NULL,
    mime_type   TEXT    NOT NULL DEFAULT '',
    hash        TEXT    NOT NULL DEFAULT '',
    size        INTEGER NOT NULL DEFAULT 0,
    is_image    INTEGER NOT NULL DEFAULT 0,
    is_video    INTEGER NOT NULL DEFAULT 0,
    width       INTEGER NOT NULL DEFAULT 0,
    height      INTEGER NOT NULL DEFAULT 0,
    ms_duration INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_nodes_parent_id ON nodes(parent_id);

CREATE TABLE IF NOT EXISTS changes (
    change_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id    TEXT    NOT NULL,
    is_removed INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_changes_node_id ON changes(node_id);

CREATE TABLE IF NOT EXISTS server_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_LAST_MAX_ID_KEY = "last_max_id"
_MAX_CHANGES_PER_PAGE = 1000

_UPSERT_NODE_SQL = """
            INSERT INTO nodes
                (node_id, parent_id, name, is_directory, ctime, mtime,
                 mime_type, hash, size, is_image, is_video, width, height, ms_duration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                parent_id=excluded.parent_id,
                name=excluded.name,
                is_directory=excluded.is_directory,
                ctime=excluded.ctime,
                mtime=excluded.mtime,
                mime_type=excluded.mime_type,
                hash=excluded.hash,
                size=excluded.size,
                is_image=excluded.is_image,
                is_video=excluded.is_video,
                width=excluded.width,
                height=excluded.height,
                ms_duration=excluded.ms_duration
            """


def _node_row_values(record: NodeRecord) -> tuple:
    return (
        record.node_id,
        record.parent_id,
        record.name,
        1 if record.is_directory else 0,
        int(record.ctime.timestamp()),
        int(record.mtime.timestamp()),
        record.mime_type,
        record.hash,
        record.size,
        1 if record.is_image else 0,
        1 if record.is_video else 0,
        record.width,
        record.height,
        record.ms_duration,
    )


def _open(dsn: str) -> sqlite3.Connection:
    con = sqlite3.connect(dsn, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


@contextmanager
def _read_only(dsn: str) -> Iterator[sqlite3.Connection]:
    """Open for SELECT-only; accidental writes raise at SQLite level."""
    con = _open(dsn)
    try:
        con.execute("PRAGMA query_only = ON")
        yield con
    finally:
        con.close()


@contextmanager
def _read_write(dsn: str) -> Iterator[sqlite3.Connection]:
    con = _open(dsn)
    try:
        yield con
        con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


def _ensure_schema(dsn: str) -> None:
    with _read_write(dsn) as con:
        con.executescript(_DDL)


def _checkpoint(dsn: str) -> None:
    with _read_write(dsn) as con:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def _get_last_max_id(dsn: str) -> int:
    with _read_only(dsn) as con:
        with closing(
            con.execute(
                "SELECT value FROM server_state WHERE key = ?",
                (_LAST_MAX_ID_KEY,),
            )
        ) as cur:
            row = cur.fetchone()
    return int(row["value"]) if row else 0


def _set_last_max_id(dsn: str, value: int) -> None:
    with _read_write(dsn) as con:
        con.execute(
            "INSERT INTO server_state (key, value) VALUES (?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (_LAST_MAX_ID_KEY, str(value)),
        )


def _upsert_node(dsn: str, record: NodeRecord) -> None:
    with _read_write(dsn) as con:
        con.execute(_UPSERT_NODE_SQL, _node_row_values(record))


def _upsert_node_and_emit_change(dsn: str, record: NodeRecord) -> None:
    with _read_write(dsn) as con:
        con.execute(_UPSERT_NODE_SQL, _node_row_values(record))
        con.execute(
            "INSERT INTO changes (node_id, is_removed) VALUES (?, 0)",
            (record.node_id,),
        )


def _delete_subtree_on_connection(con: sqlite3.Connection, node_id: str) -> None:
    """Delete node and all descendants; emit remove-changes for each."""
    to_delete: list[str] = []
    queue = [node_id]
    while queue:
        current = queue.pop(0)
        to_delete.append(current)
        with closing(
            con.execute("SELECT node_id FROM nodes WHERE parent_id = ?", (current,))
        ) as cur:
            for row in cur:
                queue.append(row["node_id"])

    for nid in to_delete:
        con.execute("DELETE FROM nodes WHERE node_id = ?", (nid,))
        con.execute(
            "INSERT INTO changes (node_id, is_removed) VALUES (?, 1)",
            (nid,),
        )


def _delete_subtree_and_emit_changes(dsn: str, node_id: str) -> None:
    with _read_write(dsn) as con:
        _delete_subtree_on_connection(con, node_id)


def _bulk_upsert_nodes(dsn: str, records: list[NodeRecord]) -> None:
    """Upsert nodes only (no change rows) in one transaction."""
    if not records:
        return
    with _read_write(dsn) as con:
        for record in records:
            con.execute(_UPSERT_NODE_SQL, _node_row_values(record))


def _apply_scan_folder_batch(
    dsn: str, delete_roots: list[str], upserts: list[NodeRecord]
) -> None:
    """Apply all subtree removals and upserts for one polled folder in one transaction.

    Deletes run first (same order as *delete_roots), then upserts. API-driven
    writes still interleave on the write queue as separate jobs before/after this
    batch.
    """
    if not delete_roots and not upserts:
        return
    with _read_write(dsn) as con:
        for root in delete_roots:
            _delete_subtree_on_connection(con, root)
        for record in upserts:
            con.execute(_UPSERT_NODE_SQL, _node_row_values(record))
            con.execute(
                "INSERT INTO changes (node_id, is_removed) VALUES (?, 0)",
                (record.node_id,),
            )


def _get_node_by_id(dsn: str, node_id: str) -> NodeRecord | None:
    with _read_only(dsn) as con:
        with closing(
            con.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
        ) as cur:
            row = cur.fetchone()
    return _row_to_record(row) if row else None


def _get_children(dsn: str, parent_id: str) -> list[NodeRecord]:
    with _read_only(dsn) as con:
        with closing(
            con.execute("SELECT * FROM nodes WHERE parent_id = ?", (parent_id,))
        ) as cur:
            rows = cur.fetchall()
    return [_row_to_record(r) for r in rows]


def _collect_subtree_node_ids(dsn: str, root_id: str) -> set[str]:
    """All node_id values in the DB subtree rooted at *root_id* (including *root_id*)."""
    result: set[str] = set()
    with _read_only(dsn) as con:
        with closing(
            con.execute("SELECT 1 FROM nodes WHERE node_id = ?", (root_id,))
        ) as cur:
            if cur.fetchone() is None:
                return result
        queue = [root_id]
        while queue:
            nid = queue.pop(0)
            if nid in result:
                continue
            result.add(nid)
            with closing(
                con.execute("SELECT node_id FROM nodes WHERE parent_id = ?", (nid,))
            ) as cur2:
                for row in cur2:
                    queue.append(row["node_id"])
    return result


def _synology_nodes_under_mounts(dsn: str, mount_ids: set[str]) -> set[str]:
    """Non-virtual node_ids reachable from any mount node (BFS)."""
    syno: set[str] = set()
    seen: set[str] = set()
    queue = list(mount_ids)
    with _read_only(dsn) as con:
        while queue:
            nid = queue.pop(0)
            if nid in seen:
                continue
            seen.add(nid)
            if not is_virtual(nid):
                syno.add(nid)
            with closing(
                con.execute("SELECT node_id FROM nodes WHERE parent_id = ?", (nid,))
            ) as cur:
                for row in cur:
                    queue.append(row["node_id"])
    return syno


def _apply_deferred_scan_removals(
    dsn: str, preserved: set[str], mount_ids: set[str]
) -> None:
    """Remove Synology subtrees under *mount_ids* that are not in *preserved*."""
    mirrored = _synology_nodes_under_mounts(dsn, mount_ids)
    candidates = mirrored - preserved
    if not candidates:
        return

    parent_by_id: dict[str, str | None] = {}
    with _read_only(dsn) as con:
        qmarks = ",".join("?" * len(candidates))
        with closing(
            con.execute(
                f"SELECT node_id, parent_id FROM nodes WHERE node_id IN ({qmarks})",
                tuple(candidates),
            )
        ) as cur:
            for row in cur:
                parent_by_id[row["node_id"]] = row["parent_id"]

    roots = sorted(nid for nid in candidates if parent_by_id.get(nid) not in candidates)
    if not roots:
        return

    with _read_write(dsn) as con:
        for root in roots:
            _delete_subtree_on_connection(con, root)


def _get_cursor(dsn: str) -> int:
    with _read_only(dsn) as con:
        with closing(
            con.execute("SELECT MAX(change_id) AS max_id FROM changes")
        ) as cur:
            row = cur.fetchone()
    return row["max_id"] if row and row["max_id"] is not None else 0


def _get_changes_since(
    dsn: str,
    cursor: int,
    max_size: int = _MAX_CHANGES_PER_PAGE,
) -> tuple[list[tuple[str, bool]], int, bool]:
    """Return (changes, new_cursor, has_more).

    Each change is (node_id, is_removed).
    """
    with _read_only(dsn) as con:
        with closing(
            con.execute(
                """
            SELECT c.change_id, c.node_id, c.is_removed,
                   n.parent_id, n.name, n.is_directory, n.ctime, n.mtime,
                   n.mime_type, n.hash, n.size, n.is_image, n.is_video,
                   n.width, n.height, n.ms_duration
            FROM changes c
            LEFT JOIN nodes n ON c.node_id = n.node_id
            WHERE c.change_id > ?
            ORDER BY c.change_id ASC
            LIMIT ?
            """,
                (cursor, max_size + 1),
            )
        ) as cur:
            rows = cur.fetchall()

    has_more = len(rows) > max_size
    rows = rows[:max_size]

    result: list[tuple[str, bool, NodeRecord | None]] = []
    for row in rows:
        is_removed = bool(row["is_removed"])
        record: NodeRecord | None = None
        if not is_removed and row["name"] is not None:
            record = _row_to_record(row)
        result.append((row["node_id"], is_removed, record))

    new_cursor = rows[-1]["change_id"] if rows else cursor
    return result, new_cursor, has_more


def _row_to_record(row: sqlite3.Row) -> NodeRecord:
    return NodeRecord(
        node_id=row["node_id"],
        parent_id=row["parent_id"],
        name=row["name"],
        is_directory=bool(row["is_directory"]),
        ctime=datetime.fromtimestamp(row["ctime"], UTC),
        mtime=datetime.fromtimestamp(row["mtime"], UTC),
        mime_type=row["mime_type"],
        hash=row["hash"],
        size=row["size"],
        is_image=bool(row["is_image"]),
        is_video=bool(row["is_video"]),
        width=row["width"],
        height=row["height"],
        ms_duration=row["ms_duration"],
    )


_GC_CREATE_SQL = """
    CREATE TEMP TABLE _dangling AS
    WITH RECURSIVE reachable(node_id) AS (
        SELECT ?
        UNION ALL
        SELECT n.node_id FROM nodes n
        JOIN reachable r ON n.parent_id = r.node_id
    )
    SELECT node_id FROM nodes
    WHERE node_id NOT IN (SELECT node_id FROM reachable)
"""


def cleanup_dangling_nodes(dsn: str) -> int:
    """Delete nodes not reachable from the server root; emit removal changes.

    Returns the number of nodes removed.
    """
    _ensure_schema(dsn)
    with _read_write(dsn) as con:
        con.execute(_GC_CREATE_SQL, (SERVER_ROOT_ID,))
        with closing(con.execute("SELECT COUNT(*) AS n FROM temp._dangling")) as cur:
            count = int(cur.fetchone()["n"])
        if count > 0:
            con.execute(
                "INSERT INTO changes (node_id, is_removed)"
                " SELECT node_id, 1 FROM temp._dangling"
            )
            con.execute(
                "DELETE FROM nodes WHERE node_id IN (SELECT node_id FROM temp._dangling)"
            )
        con.execute("DROP TABLE IF EXISTS temp._dangling")
    return count


def reset_change_history(dsn: str) -> int:
    """Clear changes and insert one update row per node except the server root.

    Returns the number of change records inserted.
    """
    _ensure_schema(dsn)
    with _read_write(dsn) as con:
        con.execute("DELETE FROM changes")
        con.execute(
            "INSERT INTO changes (node_id, is_removed)"
            " SELECT node_id, 0 FROM nodes WHERE node_id != ?",
            (SERVER_ROOT_ID,),
        )
        with closing(con.execute("SELECT COUNT(*) AS n FROM changes")) as cur:
            return int(cur.fetchone()["n"])


def list_media_backfill_candidates(dsn: str) -> list[NodeRecord]:
    """File nodes that are image/video and still lack width or height."""
    _ensure_schema(dsn)
    with _read_only(dsn) as con:
        with closing(
            con.execute(
                """
                SELECT * FROM nodes
                WHERE is_directory = 0
                  AND (is_image = 1 OR is_video = 1)
                  AND (width = 0 OR height = 0)
                """
            )
        ) as cur:
            rows = cur.fetchall()
    return [_row_to_record(r) for r in rows]


class Storage:
    """Synchronous SQLite access — run on a thread via OffMainThread / write queue."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def ensure_schema(self) -> None:
        _ensure_schema(self._dsn)

    def checkpoint(self) -> None:
        _checkpoint(self._dsn)

    def get_last_max_id(self) -> int:
        return _get_last_max_id(self._dsn)

    def set_last_max_id(self, value: int) -> None:
        _set_last_max_id(self._dsn, value)

    def upsert_node(self, record: NodeRecord) -> None:
        _upsert_node(self._dsn, record)

    def bulk_upsert_nodes(self, records: list[NodeRecord]) -> None:
        _bulk_upsert_nodes(self._dsn, records)

    def upsert_node_and_emit_change(self, record: NodeRecord) -> None:
        _upsert_node_and_emit_change(self._dsn, record)

    def delete_subtree_and_emit_changes(self, node_id: str) -> None:
        _delete_subtree_and_emit_changes(self._dsn, node_id)

    def apply_scan_folder_batch(
        self, delete_roots: list[str], upserts: list[NodeRecord]
    ) -> None:
        _apply_scan_folder_batch(self._dsn, delete_roots, upserts)

    def get_node_by_id(self, node_id: str) -> NodeRecord | None:
        return _get_node_by_id(self._dsn, node_id)

    def get_children(self, parent_id: str) -> list[NodeRecord]:
        return _get_children(self._dsn, parent_id)

    def collect_subtree_node_ids(self, root_id: str) -> set[str]:
        return _collect_subtree_node_ids(self._dsn, root_id)

    def build_deferred_preserved_set(
        self, seen_ids: set[str], preserve_roots: set[str]
    ) -> set[str]:
        preserved = set(seen_ids)
        for root in preserve_roots:
            preserved |= _collect_subtree_node_ids(self._dsn, root)
        return preserved

    def apply_deferred_scan_removals(
        self, preserved: set[str], mount_ids: set[str]
    ) -> None:
        _apply_deferred_scan_removals(self._dsn, preserved, mount_ids)

    def get_cursor(self) -> int:
        return _get_cursor(self._dsn)

    def get_changes_since(
        self,
        cursor: int,
        max_size: int = _MAX_CHANGES_PER_PAGE,
    ) -> tuple[list[tuple[str, bool, NodeRecord | None]], int, bool]:
        return _get_changes_since(self._dsn, cursor, max_size)

    def cleanup_dangling_nodes(self) -> int:
        return cleanup_dangling_nodes(self._dsn)

    def reset_change_history(self) -> int:
        return reset_change_history(self._dsn)

    def list_media_backfill_candidates(self) -> list[NodeRecord]:
        return list_media_backfill_candidates(self._dsn)
