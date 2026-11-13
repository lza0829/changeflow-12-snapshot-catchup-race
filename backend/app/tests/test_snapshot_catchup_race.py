import os
import shutil
import tempfile

# 服务级测试需要独立 DATA_DIR，必须在导入 app.db 之前设置
_TMP_DATA = tempfile.mkdtemp(prefix="changeflow_snapshot_test_")
os.environ.setdefault("DATA_DIR", _TMP_DATA)

from app.config import FIXTURES_DIR  # noqa: E402
from app.db import connect  # noqa: E402
from app.modules.snapshot_catchup_race.engine import (  # noqa: E402
    OVERLAP_DISPOSITION,
    classify_catchup_event,
    fold_expected_sink,
    is_caught_up,
    parse_snapshot_fixture,
)
from app.repositories import streams as streams_repo  # noqa: E402
from app.seed import init_db  # noqa: E402
from app.services import snapshot_service as ss  # noqa: E402


def test_parse_snapshot_fixture():
    snap = parse_snapshot_fixture(FIXTURES_DIR / "orders_snapshot.jsonl")
    assert snap["snapshot_id"] == "ORD-SNAP-001"
    assert snap["watermark_seq"] == 5
    assert [r["entity_key"] for r in snap["rows"]] == ["ORD-1001", "ORD-1003"]
    assert all(r["last_seq"] <= snap["watermark_seq"] for r in snap["rows"])


def test_classify_rules():
    # 重叠区：无论是否应用过都跳过
    assert (
        classify_catchup_event(seq=5, already_applied=False, watermark_seq=5)
        == OVERLAP_DISPOSITION
    )
    assert (
        classify_catchup_event(seq=1, already_applied=True, watermark_seq=5)
        == OVERLAP_DISPOSITION
    )
    # watermark 之后：已应用幂等吞掉，未应用才 apply
    assert classify_catchup_event(seq=6, already_applied=True, watermark_seq=5) == "skipped_duplicate"
    assert classify_catchup_event(seq=6, already_applied=False, watermark_seq=5) == "apply"


def test_fold_and_caught_up():
    events = [
        {"seq": 1, "op": "upsert", "entity_key": "A", "payload": {"v": 1}},
        {"seq": 2, "op": "delete", "entity_key": "A", "payload": {}},
        {"seq": 3, "op": "upsert", "entity_key": "B", "payload": {"v": 2}},
    ]
    assert fold_expected_sink(events) == {"B": {"v": 2}}
    assert is_caught_up(cursor=3, live_max_seq=3) is True
    assert is_caught_up(cursor=2, live_max_seq=3) is False


def _lane_stream_id():
    conn = connect()
    try:
        rows = streams_repo.list_streams(conn)
        return next(s["id"] for s in rows if "快照" in s["name"])
    finally:
        conn.close()


def test_import_then_batched_catchup_reconciles():
    shutil.rmtree(_TMP_DATA, ignore_errors=True)
    os.makedirs(_TMP_DATA, exist_ok=True)
    init_db()
    sid = _lane_stream_id()

    record = ss.import_snapshot(sid, "orders_snapshot.jsonl")
    assert record["status"] == "imported"
    assert record["row_count"] == 2
    assert record["watermark_seq"] == 5

    # 第一批只处理前 3 条（全在重叠区）：必须显式处于 catching_up
    batch1 = ss.catch_up(sid, limit=3)
    assert batch1["import"]["status"] == "catching_up"
    assert batch1["import"]["caught_up_at"] is None
    assert batch1["import"]["overlap_skipped_count"] == 3
    assert batch1["import"]["applied_count"] == 0

    # 追完：重扫不重复计数，四方对账全绿
    done = ss.catch_up(sid)
    imp = done["import"]
    assert imp["status"] == "caught_up"
    assert imp["caught_up_at"] is not None
    assert imp["live_candidate_count"] == 10
    assert imp["overlap_skipped_count"] == 5
    assert imp["applied_count"] == 5
    assert imp["cursor_after"] == 10
    assert done["reconciliation"]["all_match"] is True

    sink_keys = set(done["reconciliation"]["sink"]["actual"])
    assert sink_keys == {"ORD-1001", "ORD-1004", "ORD-1005"}
    assert done["reconciliation"]["sink"]["actual"]["ORD-1001"]["status"] == "shipped"

    # 关掉浏览器再开 = 重新连接，状态仍在
    conn = connect()
    try:
        again = streams_repo.get_stream(conn, sid)
        assert again["apply_cursor"] == 10
        assert again["snapshot_import_id"] == imp["id"]
    finally:
        conn.close()


def test_reset_allows_reimport_and_one_shot_catchup():
    sid = _lane_stream_id()
    ss.reset_lane(sid)
    ss.import_snapshot(sid, "orders_snapshot.jsonl")
    done = ss.catch_up(sid)
    assert done["import"]["status"] == "caught_up"
    assert done["reconciliation"]["all_match"] is True
