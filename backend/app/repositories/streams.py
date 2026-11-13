from __future__ import annotations

import sqlite3
from typing import Any

_COLUMNS = "id, name, fixture_path, entity_type, apply_cursor, enabled, note, snapshot_import_id"


def _row(r: sqlite3.Row) -> dict[str, Any]:
    item = dict(r)
    if item.get("snapshot_import_id") is not None:
        item["snapshot_import_id"] = int(item["snapshot_import_id"])
    return item


def list_streams(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(f"SELECT {_COLUMNS} FROM streams ORDER BY id").fetchall()
    return [_row(r) for r in rows]


def get_stream(conn: sqlite3.Connection, stream_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        f"SELECT {_COLUMNS} FROM streams WHERE id=?",
        (stream_id,),
    ).fetchone()
    return _row(row) if row else None


def set_cursor(conn: sqlite3.Connection, stream_id: int, cursor: int) -> None:
    conn.execute("UPDATE streams SET apply_cursor=? WHERE id=?", (cursor, stream_id))


def set_enabled(conn: sqlite3.Connection, stream_id: int, enabled: bool) -> None:
    conn.execute("UPDATE streams SET enabled=? WHERE id=?", (1 if enabled else 0, stream_id))


def set_snapshot_import(conn: sqlite3.Connection, stream_id: int, import_id: int | None) -> None:
    conn.execute("UPDATE streams SET snapshot_import_id=? WHERE id=?", (import_id, stream_id))
