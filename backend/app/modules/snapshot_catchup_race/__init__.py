"""snapshot_catchup_race：快照导入与 live 追平竞态处理。

- engine：纯函数，快照夹具解析、追平事件裁决、预期态折叠；
- 服务在 app.services.snapshot_service，路由在 app.routers.snapshots。

重叠消化规则（对外公布）：
  1. seq <= watermark_seq 的 live 事件已折叠进快照 -> skipped_snapshot_overlap，不重放；
  2. watermark 之后但已应用过的 seq -> skipped_duplicate，幂等吞掉；
  3. 其余 -> apply，按 upsert/delete 幂等落 UNIQUE(stream_id, entity_key) 的同一实体键。
"""
from __future__ import annotations

from app.modules.snapshot_catchup_race.engine import (
    OVERLAP_DISPOSITION,
    SNAPSHOT_LOADED_DISPOSITION,
    classify_catchup_event,
    fold_expected_sink,
    is_caught_up,
    parse_snapshot_fixture,
)

__all__ = [
    "OVERLAP_DISPOSITION",
    "SNAPSHOT_LOADED_DISPOSITION",
    "classify_catchup_event",
    "fold_expected_sink",
    "is_caught_up",
    "parse_snapshot_fixture",
]
