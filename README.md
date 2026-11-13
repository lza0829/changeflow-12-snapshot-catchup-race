# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 快照导入与 Live 追平（snapshot_catchup_race）

空 Sink 只靠一条条 live CDC 追太慢，实际先灌一版快照再追增量。本主线只处理
**同一流上「一版快照 + 一路 live CDC」的竞态**，不做多源汇入裁决，也不做乱序缓冲超时。

入口：前端「快照追平」页（`/snapshot`），或 `/api/snapshots/*`。

操作顺序：

1. **导入快照**：选「Orders 快照追平 Lane」与快照夹具 `orders_snapshot.jsonl`，
   实体态直接灌进同一 Sink 底座（`UNIQUE(stream_id, entity_key)` 兜底，同键不会出脏行），
   同时落 `snapshot_imports` 记录（含 snapshot_id、快照时间、`watermark_seq=5`、行数），
   每行在账本记 `snapshot_loaded`，记录可回看且持久化。
2. **追 Live**：只认真处理 `seq > watermark_seq` 的变更；可填「本批上限」分批追，
   追平中状态机显示 `catching_up`、`caught_up_at` 为空，一眼能看出尚未追上；
   游标推进到 live 最大 seq 才置 `caught_up` 并写追上时间。
3. **四方对账**：快照记录 / 账本 / Sink / 游标四处计数与 seq 集合逐项核对，
   全部落 SQLite，关掉浏览器再开状态仍在；页内提供「重置该 Lane」重新演示。

公布的重叠消化规则：

- `seq <= watermark_seq`：变更已折叠进快照 → 账本记 `skipped_snapshot_overlap` 跳过，
  不重放。快照中缺席本身就是结论（实体在快照前已删除），重放旧事件会造脏行/让删除复活；
- watermark 之后但已应用过的 seq（重跑）→ `skipped_duplicate` 幂等吞掉；
- 其余 live 变更 → 按 upsert/delete 幂等落到同一实体键。

样例（`orders_snapshot.jsonl` + `orders_snapshot_lane_cdc.jsonl`）：

- 快照 2 行（ORD-1001、ORD-1003，watermark=5）；live 共 seq 1→10；
- seq1..5 落重叠区跳过（5 个去重 seq），seq6..10 真正 applied（5 个）；
- 追平后 Sink 与全量 CDC 折叠预期一致：ORD-1001=shipped、ORD-1004、ORD-1005，
  ORD-1003 被 seq10 删除而消失，游标到 10，对账全绿。

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。
- **Orders 快照追平 Lane**：live 夹具 `orders_snapshot_lane_cdc.jsonl`（seq 1→10），
  配套快照夹具 `orders_snapshot.jsonl`（watermark=5，与 live 的 seq1..5 重叠），
  用于演示快照导入 → 分批追平 → 四方对账。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致；
快照竞态走「快照追平」页。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链与「快照导入与 live 追平竞态」
（`backend/app/modules/snapshot_catchup_race`）。其余缺口等待/跳过、乱序缓冲、事务整批、
双 sink 部分失败、毒丸选择性跳过等仍在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
