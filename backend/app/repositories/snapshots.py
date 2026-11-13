from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def create_import(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    snapshot_id: str,
    fixture_path: str,
    snapshot_ts: str,
    watermark_seq: int,
    row_count: int,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO snapshot_imports(
            stream_id, snapshot_id, fixture_path, snapshot_ts, watermark_seq,
            status, row_count, live_candidate_count, overlap_skipped_count,
            applied_count, duplicate_skipped_count, cursor_before, cursor_after,
            report_json, imported_at, caught_up_at
        ) VALUES (?,?,?,?,?, 'imported', ?, 0, 0, 0, 0, 0, 0, ?, ?, NULL)
        """,
        (
            stream_id,
            snapshot_id,
            fixture_path,
            snapshot_ts,
            watermark_seq,
            row_count,
            json.dumps({}, ensure_ascii=False),
            _now(),
        ),
    )
    return int(cur.lastrowid)


def get_active_import(conn: sqlite3.Connection, stream_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM snapshot_imports WHERE stream_id=? ORDER BY id DESC LIMIT 1",
        (stream_id,),
    ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["report"] = json.loads(item.pop("report_json") or "{}")
    return item


def get_import(conn: sqlite3.Connection, import_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM snapshot_imports WHERE id=?", (import_id,)).fetchone()
    if not row:
        return None
    item = dict(row)
    item["report"] = json.loads(item.pop("report_json") or "{}")
    return item


def list_imports(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM snapshot_imports ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["report"] = json.loads(item.pop("report_json") or "{}")
        out.append(item)
    return out


def update_catchup_progress(
    conn: sqlite3.Connection,
    *,
    import_id: int,
    status: str,
    live_candidate_count: int,
    overlap_skipped_count: int,
    applied_count: int,
    duplicate_skipped_count: int,
    cursor_after: int,
    caught_up: bool,
    report: dict[str, Any],
) -> None:
    caught_up_at = _now() if caught_up else None
    conn.execute(
        """
        UPDATE snapshot_imports SET
            status=?, live_candidate_count=?, overlap_skipped_count=?,
            applied_count=?, duplicate_skipped_count=?, cursor_after=?,
            report_json=?, caught_up_at=COALESCE(?, caught_up_at)
        WHERE id=?
        """,
        (
            status,
            live_candidate_count,
            overlap_skipped_count,
            applied_count,
            duplicate_skipped_count,
            cursor_after,
            json.dumps(report, ensure_ascii=False),
            caught_up_at,
            import_id,
        ),
    )
