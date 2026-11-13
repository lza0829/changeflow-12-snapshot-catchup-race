"""snapshot_catchup_race：快照导入与 live 追平竞态的纯函数引擎。

主线只处理「同一流上，一版快照 + 一路 live CDC」的竞态：
- 快照把 watermark_seq（含）之前的变更折叠成实体当前态；
- live 追平时，落在重叠区（seq <= watermark_seq）的事件按公布规则跳过/幂等吞掉；
- 只有 watermark 之后的变更才真正 apply。

不做多源汇入裁决，也不做乱序缓冲超时。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# 重叠区事件的账本处置标记（对外公布的规则名，界面直接展示）
OVERLAP_DISPOSITION = "skipped_snapshot_overlap"
# 快照装载行的账本处置标记
SNAPSHOT_LOADED_DISPOSITION = "snapshot_loaded"


def parse_snapshot_fixture(path: Path) -> dict[str, Any]:
    """解析快照夹具：第一行是快照清单（manifest），其余每行一个实体快照态。"""
    if not path.exists():
        raise FileNotFoundError(f"snapshot fixture missing: {path}")
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if not lines:
        raise ValueError("empty snapshot fixture")
    try:
        manifest = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid snapshot manifest: {exc}") from exc
    for key in ("snapshot_id", "snapshot_ts", "watermark_seq", "entity_type"):
        if key not in manifest:
            raise ValueError(f"snapshot manifest missing {key}")

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line_no, raw in enumerate(lines[1:], start=2):
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid jsonl at snapshot line {line_no}: {exc}") from exc
        for key in ("entity_key", "last_seq", "last_event_ts"):
            if key not in row:
                raise ValueError(f"snapshot row at line {line_no} missing {key}")
        entity_key = str(row["entity_key"])
        if entity_key in seen:
            raise ValueError(f"duplicate entity_key in snapshot: {entity_key}")
        seen.add(entity_key)
        rows.append(
            {
                "entity_key": entity_key,
                "last_seq": int(row["last_seq"]),
                "last_event_ts": str(row["last_event_ts"]),
                "payload": row.get("payload") or {},
            }
        )

    watermark = int(manifest["watermark_seq"])
    if rows and any(r["last_seq"] > watermark for r in rows):
        raise ValueError("snapshot row last_seq cannot exceed watermark_seq")
    return {
        "snapshot_id": str(manifest["snapshot_id"]),
        "snapshot_ts": str(manifest["snapshot_ts"]),
        "watermark_seq": watermark,
        "entity_type": str(manifest["entity_type"]),
        "note": str(manifest.get("note") or ""),
        "rows": rows,
    }


def classify_catchup_event(
    *,
    seq: int,
    already_applied: bool,
    watermark_seq: int,
) -> str:
    """追平态事件裁决。

    公布的重叠消化规则（按优先级）：
      1. ``seq <= watermark_seq``：变更已折叠进快照，一律跳过
         （``skipped_snapshot_overlap``）。哪怕 sink 里没有该键也不重放——
         快照的缺席本身就是结论（例如该键在快照前已被删除），重放旧事件会
         造出脏行/让已删除实体复活；
      2. watermark 之后、但本流已经应用过该 seq（重跑）：幂等跳过
         （``skipped_duplicate``）；
      3. 其余是 live 新变更：``apply``，按 upsert/delete 幂等落到同一实体键，
         sink 的 UNIQUE(stream_id, entity_key) 保证不会悄悄多出脏行。
    """
    if int(seq) <= int(watermark_seq):
        return OVERLAP_DISPOSITION
    if already_applied:
        return "skipped_duplicate"
    return "apply"


def is_caught_up(*, cursor: int, live_max_seq: int) -> bool:
    """游标推进到 live 夹具的最大 seq，才算追上。"""
    return int(cursor) >= int(live_max_seq)


def fold_expected_sink(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """把一路 CDC 从头折叠出预期最终态，供对账时与真实 sink 比对。"""
    expected: dict[str, dict[str, Any]] = {}
    for event in sorted(events, key=lambda e: int(e["seq"])):
        key = str(event["entity_key"])
        if event["op"] == "delete":
            expected.pop(key, None)
        else:
            expected[key] = event.get("payload") or {}
    return expected
