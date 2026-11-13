from __future__ import annotations

from pathlib import Path
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.parse_change import parse_change_fixture
from app.modules.snapshot_catchup_race.engine import (
    OVERLAP_DISPOSITION,
    SNAPSHOT_LOADED_DISPOSITION,
    classify_catchup_event,
    fold_expected_sink,
    is_caught_up,
    parse_snapshot_fixture,
)
from app.repositories import cursors as cursor_repo
from app.repositories import ledger as ledger_repo
from app.repositories import runs as runs_repo
from app.repositories import sink as sink_repo
from app.repositories import snapshots as snapshot_repo
from app.repositories import streams as streams_repo

# 对外公布的重叠区消化规则（同时写进快照记录 report 与界面）
PUBLISHED_RULES = (
    "1) seq <= watermark_seq 的 live 事件已折叠进快照，一律记 "
    "skipped_snapshot_overlap 跳过，不重放（快照中缺席=已删除，重放会造脏行/复活）；"
    "2) watermark 之后且已应用过的 seq 幂等吞掉（skipped_duplicate）；"
    "3) 其余 live 变更按 upsert/delete 幂等落到 UNIQUE(stream_id, entity_key) 的同一实体键，"
    "禁止同键多出脏行；游标推进到 live 最大 seq 才标记 caught_up。"
)


def list_snapshot_fixtures() -> list[str]:
    # 快照夹具以 _snapshot.jsonl 结尾；CDC 夹具（*_cdc.jsonl）不进快照下拉
    return sorted(p.name for p in FIXTURES_DIR.glob("*_snapshot.jsonl"))


def _fixture_path(fixture_name: str) -> Path:
    # 只取文件名，杜绝路径穿越
    return FIXTURES_DIR / Path(fixture_name).name


def import_snapshot(stream_id: int, fixture_name: str) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if stream.get("snapshot_import_id"):
            raise ValueError("该流已有快照导入记录；请先重置该 Lane 再导入新快照")

        snapshot = parse_snapshot_fixture(_fixture_path(fixture_name))
        if snapshot["entity_type"] != stream["entity_type"]:
            raise ValueError(
                f"快照实体类型 {snapshot['entity_type']} 与流类型 {stream['entity_type']} 不一致"
            )

        import_id = snapshot_repo.create_import(
            conn,
            stream_id=stream_id,
            snapshot_id=snapshot["snapshot_id"],
            fixture_path=Path(fixture_name).name,
            snapshot_ts=snapshot["snapshot_ts"],
            watermark_seq=snapshot["watermark_seq"],
            row_count=len(snapshot["rows"]),
        )

        # 快照态直接灌进同一 Sink 底座；UNIQUE(stream_id, entity_key) 兜底防脏行
        loaded_keys: list[str] = []
        for row in snapshot["rows"]:
            sink_repo.upsert(
                conn,
                stream_id=stream_id,
                entity_key=row["entity_key"],
                payload=row["payload"],
                seq=row["last_seq"],
                event_ts=row["last_event_ts"],
            )
            ledger_repo.insert_ledger(
                conn,
                stream_id=stream_id,
                run_id=None,
                event={
                    "seq": row["last_seq"],
                    "op": "upsert",
                    "entity_key": row["entity_key"],
                    "event_ts": row["last_event_ts"],
                    "payload": row["payload"],
                },
                disposition=SNAPSHOT_LOADED_DISPOSITION,
            )
            loaded_keys.append(row["entity_key"])

        # 游标不预支：仍从 0 开始，重叠区由 watermark 规则显式跳过并记账
        streams_repo.set_snapshot_import(conn, stream_id, import_id)
        record = snapshot_repo.get_import(conn, import_id)
        record["report"] = {
            "rules": PUBLISHED_RULES,
            "note": snapshot["note"],
            "loaded_entity_keys": loaded_keys,
            "sink_count_after_import": sink_repo.count_for_stream(conn, stream_id),
        }
        snapshot_repo.update_catchup_progress(
            conn,
            import_id=import_id,
            status="imported",
            live_candidate_count=0,
            overlap_skipped_count=0,
            applied_count=0,
            duplicate_skipped_count=0,
            cursor_after=0,
            caught_up=False,
            report=record["report"],
        )
        conn.commit()
        return snapshot_repo.get_import(conn, import_id) or {}
    finally:
        conn.close()


def catch_up(stream_id: int, limit: int | None = None) -> dict[str, Any]:
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        import_id = stream.get("snapshot_import_id")
        if not import_id:
            raise ValueError("该流没有快照导入，请先导入快照再追 live")
        record = snapshot_repo.get_import(conn, import_id)
        if not record:
            raise ValueError("snapshot import record missing")
        if record["status"] == "caught_up":
            raise ValueError("已追上（caught_up），无需重复追平；如需重跑请先重置该 Lane")

        watermark = int(record["watermark_seq"])
        events = parse_change_fixture(FIXTURES_DIR / stream["fixture_path"])
        if limit is not None and limit >= 0:
            batch = events[:limit]
        else:
            batch = events
        live_max_seq = max(int(e["seq"]) for e in events)

        cursor_before = int(stream["apply_cursor"] or 0)
        run_id = runs_repo.create_run(conn, stream_id, cursor_before)

        run_applied = 0
        run_overlap = 0
        run_duplicate = 0
        cursor = cursor_before
        dispositions: list[str] = []

        for event in batch:
            seq = int(event["seq"])
            already = cursor_repo.is_applied(conn, stream_id, seq)
            decision = classify_catchup_event(
                seq=seq, already_applied=already, watermark_seq=watermark
            )

            if decision == OVERLAP_DISPOSITION:
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                run_overlap += 1
                dispositions.append(decision)
                continue

            if decision == "skipped_duplicate":
                ledger_repo.insert_ledger(
                    conn, stream_id=stream_id, run_id=run_id, event=event, disposition=decision
                )
                run_duplicate += 1
                dispositions.append(decision)
                continue

            if event["op"] == "upsert":
                sink_repo.upsert(
                    conn,
                    stream_id=stream_id,
                    entity_key=event["entity_key"],
                    payload=event.get("payload") or {},
                    seq=seq,
                    event_ts=event["event_ts"],
                )
            else:
                sink_repo.delete(conn, stream_id=stream_id, entity_key=event["entity_key"])

            cursor_repo.mark_applied(conn, stream_id, seq, run_id)
            cursor = seq
            streams_repo.set_cursor(conn, stream_id, cursor)
            ledger_repo.insert_ledger(
                conn, stream_id=stream_id, run_id=run_id, event=event, disposition="applied"
            )
            run_applied += 1
            dispositions.append("applied")

        caught_up = is_caught_up(cursor=cursor, live_max_seq=live_max_seq)
        status = "caught_up" if caught_up else "catching_up"

        run_report = {
            "snapshot_import_id": import_id,
            "watermark_seq": watermark,
            "overlap_skipped": run_overlap,
            "duplicate_skipped": run_duplicate,
            "caught_up": caught_up,
            "live_max_seq": live_max_seq,
            "limit": limit,
            "dispositions": dispositions,
            "rules": PUBLISHED_RULES,
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status=status,
            loaded=len(batch),
            applied=run_applied,
            quarantined=0,
            skipped=run_overlap + run_duplicate,
            cursor_after=cursor,
            report=run_report,
        )

        # 快照记录上的计数一律按账本 distinct seq 重算：分批重扫不会重复计数，
        # 记录数天然能和账本（去重 seq 集合）、applied_seqs 对上号
        distinct_overlap = _ledger_distinct_seqs(conn, stream_id, OVERLAP_DISPOSITION)
        distinct_duplicate = _ledger_distinct_seqs(conn, stream_id, "skipped_duplicate")
        distinct_catchup_seqs = {
            int(r["seq"])
            for r in conn.execute(
                "SELECT DISTINCT seq FROM change_ledger "
                "WHERE stream_id=? AND disposition IN (?,?,?)",
                (stream_id, OVERLAP_DISPOSITION, "skipped_duplicate", "applied"),
            ).fetchall()
        }
        merged_report = dict(record.get("report") or {})
        history = merged_report.get("catchup_runs", [])
        history.append({"run_id": run_id, **run_report})
        merged_report["catchup_runs"] = history
        snapshot_repo.update_catchup_progress(
            conn,
            import_id=import_id,
            status=status,
            live_candidate_count=len(distinct_catchup_seqs),
            overlap_skipped_count=len(distinct_overlap),
            applied_count=len(applied_seqs_for(conn, stream_id)),
            duplicate_skipped_count=len(distinct_duplicate),
            cursor_after=cursor,
            caught_up=caught_up,
            report=merged_report,
        )
        conn.commit()
        return {
            "import": snapshot_repo.get_import(conn, import_id),
            "run": runs_repo.get_run(conn, run_id),
            "reconciliation": reconcile(conn, stream_id),
        }
    finally:
        conn.close()


def reset_lane(stream_id: int) -> dict[str, Any]:
    """清空某路流的快照/Live 痕迹，便于重新演示竞态。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        conn.execute("DELETE FROM change_ledger WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM quarantine WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM applied_seqs WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM sink_records WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM apply_runs WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM snapshot_imports WHERE stream_id=?", (stream_id,))
        streams_repo.set_cursor(conn, stream_id, 0)
        streams_repo.set_snapshot_import(conn, stream_id, None)
        conn.commit()
        return {"stream_id": stream_id, "reset": True}
    finally:
        conn.close()


def _ledger_disposition_counts(conn, stream_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT disposition, COUNT(*) c FROM change_ledger WHERE stream_id=? GROUP BY disposition",
        (stream_id,),
    ).fetchall()
    return {r["disposition"]: int(r["c"]) for r in rows}


def _ledger_distinct_seqs(conn, stream_id: int, disposition: str) -> set[int]:
    rows = conn.execute(
        "SELECT DISTINCT seq FROM change_ledger WHERE stream_id=? AND disposition=?",
        (stream_id, disposition),
    ).fetchall()
    return {int(r["seq"]) for r in rows}


def applied_seqs_for(conn, stream_id: int) -> set[int]:
    return {
        int(r["seq"])
        for r in conn.execute(
            "SELECT seq FROM applied_seqs WHERE stream_id=?", (stream_id,)
        ).fetchall()
    }


def reconcile(conn, stream_id: int) -> dict[str, Any]:
    """四方对账：快照记录 / 账本 / Sink / 游标。"""
    stream = streams_repo.get_stream(conn, stream_id)
    if not stream:
        raise ValueError("stream not found")
    import_id = stream.get("snapshot_import_id")
    record = snapshot_repo.get_import(conn, import_id) if import_id else None
    events = parse_change_fixture(FIXTURES_DIR / stream["fixture_path"])
    live_max_seq = max(int(e["seq"]) for e in events)

    ledger_counts = _ledger_disposition_counts(conn, stream_id)
    applied_seqs = {
        int(r["seq"])
        for r in conn.execute(
            "SELECT seq FROM applied_seqs WHERE stream_id=?", (stream_id,)
        ).fetchall()
    }
    overlap_seqs = _ledger_distinct_seqs(conn, stream_id, OVERLAP_DISPOSITION)
    loaded_seqs = _ledger_distinct_seqs(conn, stream_id, SNAPSHOT_LOADED_DISPOSITION)

    actual_sink = {
        r["entity_key"]: r["payload"]
        for r in sink_repo.list_sink_for_stream(conn, stream_id)
    }
    expected_sink = fold_expected_sink(events)

    checks: list[dict[str, Any]] = []

    def _check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    if record:
        watermark = int(record["watermark_seq"])
        expected_overlap = {int(e["seq"]) for e in events if int(e["seq"]) <= watermark}
        expected_apply = {int(e["seq"]) for e in events if int(e["seq"]) > watermark}
        _check(
            "快照行数 = 账本 snapshot_loaded 行数",
            int(record["row_count"]) == ledger_counts.get(SNAPSHOT_LOADED_DISPOSITION, 0),
            f"记录 {record['row_count']} / 账本 {ledger_counts.get(SNAPSHOT_LOADED_DISPOSITION, 0)}",
        )
        if record["status"] == "caught_up":
            _check(
                "重叠区 seq 全部至少跳过一次",
                overlap_seqs == expected_overlap,
                f"预期 {sorted(expected_overlap)} / 实际 {sorted(overlap_seqs)}",
            )
            _check(
                "真正 applied 的 seq 集合 = watermark 之后的 live seq",
                applied_seqs == expected_apply,
                f"预期 {sorted(expected_apply)} / 实际 {sorted(applied_seqs)}",
            )
            _check(
                "快照记录 applied 累计 = applied_seqs 数",
                int(record["applied_count"]) == len(applied_seqs),
                f"记录 {record['applied_count']} / applied_seqs {len(applied_seqs)}",
            )
            _check(
                "快照记录 overlap 累计 = 账本去重 overlap seq 数",
                int(record["overlap_skipped_count"]) == len(overlap_seqs),
                f"记录 {record['overlap_skipped_count']} / 账本 {len(overlap_seqs)}",
            )
            _check(
                "快照记录 live 候选 = live 夹具 distinct seq 总数",
                int(record["live_candidate_count"])
                == len(expected_overlap | expected_apply),
                f"记录 {record['live_candidate_count']} / 夹具 {len(expected_overlap | expected_apply)}",
            )
        _check(
            "快照记录游标 = 流 apply_cursor",
            int(record["cursor_after"]) == int(stream["apply_cursor"]),
            f"记录 {record['cursor_after']} / 流 {stream['apply_cursor']}",
        )

    _check(
        "Sink 实际态 = 全量 CDC 折叠预期态",
        actual_sink == expected_sink,
        f"实际键 {sorted(actual_sink)} / 预期键 {sorted(expected_sink)}",
    )
    max_applied = max(applied_seqs) if applied_seqs else 0
    _check(
        "游标 = 已应用最大 seq",
        int(stream["apply_cursor"]) == max_applied,
        f"游标 {stream['apply_cursor']} / max_applied {max_applied}",
    )
    if record and record["status"] == "caught_up":
        _check(
            "游标已到 live 末端（追上）",
            int(stream["apply_cursor"]) == live_max_seq,
            f"游标 {stream['apply_cursor']} / live_max {live_max_seq}",
        )

    return {
        "stream_id": stream_id,
        "status": record["status"] if record else "no_snapshot",
        "snapshot_record": {
            "row_count": record["row_count"] if record else 0,
            "watermark_seq": record["watermark_seq"] if record else None,
            "live_candidate_count": record["live_candidate_count"] if record else 0,
            "overlap_skipped_count": record["overlap_skipped_count"] if record else 0,
            "applied_count": record["applied_count"] if record else 0,
            "duplicate_skipped_count": record["duplicate_skipped_count"] if record else 0,
            "cursor_after": record["cursor_after"] if record else 0,
        },
        "ledger": {
            "cumulative_by_disposition": ledger_counts,
            "distinct_overlap_seqs": sorted(overlap_seqs),
            "distinct_applied_seqs": sorted(applied_seqs),
            "snapshot_loaded_seqs": sorted(loaded_seqs),
        },
        "sink": {
            "actual": actual_sink,
            "expected": expected_sink,
            "actual_count": len(actual_sink),
            "expected_count": len(expected_sink),
        },
        "cursor": {
            "apply_cursor": int(stream["apply_cursor"]),
            "max_applied_seq": max_applied,
            "live_max_seq": live_max_seq,
        },
        "checks": checks,
        "all_match": all(c["ok"] for c in checks),
        "rules": PUBLISHED_RULES,
    }
