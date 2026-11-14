# 检验结果变更日志服务（wal2json 逻辑复制 + FastAPI）

实验室数据工程师排查“检验结果漏同步”时，需要回答：**源库一次提交到底改过哪些样本？**
本环境用 PostgreSQL 16 逻辑复制（`wal2json`）持续采集源库 `results` 表的增/改/删，
把**源事务号、提交位置、事务内有序的前后值**原子写入独立日志库，并通过 FastAPI 提供
按提交位置翻页的变更查询、采集状态接口，以及面向数据交付的**冲突追溯 API**
（`/conflict-trace`）：输入样本号、检测项目与已归档提交位置，从同一已提交视图重建
截至该位置的 `results.id` 有效行，定位同一样本/项目并存的矛盾有效结果及其首次成立位置。

## 1. 架构

```
源库 source-db (PG16 + wal2json)                独立日志库 log-db (PG16)
┌─────────────────────────────┐   复制协议流     ┌──────────────────────────┐
│ results 表 (REPLICA ID FULL)│ ───────────────▶ │ source_commit            │
│ 持久逻辑复制槽 lab_results_  │  wal2json B/I/U/ │ result_change (有序前后值)│
│ slot（建槽后变更才入流）     │  D/C 消息        │ slot_state / heartbeat   │
└──────────────┬──────────────┘                  └──────────▲───────────────┘
               │ 1.日志库 COMMIT 落盘                        │
               │ 2.再 send_feedback 确认源位置      ┌───────┴────────┐
               └─────────────────────────────────── │ collector 容器  │
                                                    └────────────────┘
             ┌──────────────────────────────────────────────────┐
             │ api 容器（FastAPI，只读日志库 + 源槽状态）         │
             │ /docs  /commits  /commits/{lsn}  /changes  /status│
             │ /conflict-trace（截至提交位置重建有效行、冲突追溯）│
             └──────────────────────────────────────────────────┘
```

## 2. 需求与实现对照

| 要求 | 实现方式 |
| --- | --- |
| 只采集建槽后固定 `results` 表的增改删 | 采集器启动时创建持久逻辑复制槽；wal2json 选项 `add-tables=public.results`、`actions=insert,update,delete` |
| 表含样本号、检测项目、结果 | 源表 `results(sample_no, test_item, result, ...)` |
| 更正、删除保留完整旧值 | `ALTER TABLE results REPLICA IDENTITY FULL` + wal2json `include-old-keys`，旧值整行 JSONB 入库 |
| 保存源事务号、提交位置、有序前后值 | `source_commit(xid, commit_lsn, begin_lsn, commit_time)` + `result_change(change_seq, old_values, new_values)`，同事务同行多次修改各占一行、按 `change_seq` 保序 |
| 回滚不记录 | 逻辑解码只输出已提交事务，ROLLBACK 不产生任何消息 |
| 整笔事务一次可见、顺序与源提交一致 | 每笔源事务在日志库一个本地事务中原子写入；按 `commit_lsn` 全局排序 |
| 先持久提交再确认源位置 | 日志库 `COMMIT` 成功后才 `send_feedback(flush_lsn=end_lsn)`，源槽 `confirmed_flush_lsn/restart_lsn` 随后才推进 |
| 落库后确认前退出/断流/重启，不漏不重 | 重启从持久槽旧位置重新解码；`source_commit(slot_name, commit_lsn)` 唯一约束 + `ON CONFLICT DO NOTHING` 幂等重放 |
| 不能靠扫描当前表补历史 | 唯一数据通道是复制槽 WAL 流；不读取 `results` 当前数据做任何补采 |
| 采集停顿时源库仍可提交 | 复制槽保留所需 WAL（`/status` 的 `retained_wal_bytes` 可见），恢复后自动补采 |
| 查询仅见完整事务 | 写入原子性保证；API 只读已提交数据 |
| 按提交位置翻页 + 采集状态 API | `GET /commits?cursor=<LSN>&limit=`、`GET /commits/{commit_lsn}`、`GET /changes`、`GET /status` |
| 可实际启动的 Compose + /docs + 可控崩溃演示 | `docker compose up -d --build`；`./demo/demo.sh` 一键演示全部场景 |

### 确认时序（正确性核心）

```
wal2json 消息 B,I,U,...,C
        │
        ▼
日志库 BEGIN → insert source_commit/result_change/slot_state → COMMIT（落盘）
        │
        ├─ 若开启崩溃演示：此刻 os._exit（源位置尚未确认）
        ▼
复制连接 send_feedback(flush_lsn = 本事务 end_lsn)
        ▼
源槽 confirmed_flush_lsn 推进，旧 WAL 才允许回收
```

在“日志库已提交、feedback 未生效”之间任何退出：源槽仍停在旧位置 → 重启重放该事务 →
唯一约束幂等吞掉 → 再次确认。因此**不漏记、不重复**，且不依赖扫描当前表。

### 冲突追溯（`/conflict-trace`）语义

数据工程师向数仓交付检验结果前，需要解释**同一样本、同一检测项目为何并存矛盾的有效结果**。
该接口只读取日志库中 `commit_lsn <= 截止位置` 的已提交事务，按整笔事务回放重建每个
`results.id` 的有效行，不扫描源库当前表。关键判定规则：

| 规则 | 实现 |
| --- | --- |
| 只有不同 id 且结果值不同才算冲突 | 同一 `(sample_no, test_item)` 下有效行 `>=2` 个不同 id，且 `result` 去重后仍 `>=2` 个值才置 `conclusion=conflict`；不同 id 但同值不算冲突 |
| 整笔事务最终状态判定 | 同一 `commit_lsn` 内的多次改值、改键（改 sample_no/test_item）、删除先顺序回放，冲突只在事务边界用最终状态评估，事务内中间态绝不外露 |
| 截止点之后不混入 | 候选 id 与历史变更 SQL 均带 `commit_lsn <= %s`；即使采集器已继续采集，截止点之后的事务只用于在 `ingested_after_cutoff` 中声明“已排除”，不参与重建 |
| 断链不得报无冲突 | 同一 id 的 `old_values` 与上一次已知新值逐列不符（或已追踪 id 再次 INSERT）即记 `chain_breaks`，结论强制为 `chain_broken`，并给出期望值与实际旧值 |
| 建槽前基线行 | 某 id 建槽后首条记录就是 UPDATE/DELETE 时，以其完整旧值建立初始状态并标记 `is_pre_slot_baseline=true`；**建槽前从未变更的行没有任何日志、天然不可见**，`coverage.scope_note` 明确声明这一覆盖范围局限 |
| 当前这轮冲突首次成立位置 | 冲突从无到有的事务位置即本轮起点；冲突消解（删除/改键/结果趋同）后再次成立，以重新成立的事务为新一轮起点，返回 `conflict_round_first_commit_lsn/xid/time` |

重建核心是纯标准库模块 `app/app/replay.py`（不依赖数据库/框架），`tests/` 下提供
零第三方依赖的 `unittest` 用例，覆盖同事务多次改值、改键、删除、断链、建槽前基线行、
冲突消解与新轮次等场景，可直接 `python3 -I tests/test_replay.py` 运行。

## 3. 快速启动

```bash
docker compose up -d --build
# 文档与接口
open http://localhost:8000/docs      # Swagger UI
```

端口（见 `.env`）：源库 `5432`、日志库 `5433`、API `8000`。

## 4. 一键演示

```bash
./demo/demo.sh
```

脚本自动完成并断言：

1. **正常事务**：同事务对同一行两次更正（两条都保留且有序）、更正、插入、删除（完整旧值）；
2. **回滚不记录**：ROLLBACK 的内容在日志库中为 0 条，源表数据不变；
3. **停顿续采**：停掉 collector → 源库继续提交（日志库不可见、复制槽 WAL 保留量增长）
   → 重启 collector 自动补采；
4. **崩溃恢复**：在“落库后确认前”窗口杀掉采集器 → 日志已可见、源槽未推进 →
   正常重启后幂等续采，事务数与行数均不重复，确认位置最终追平；
5. **翻页与样本检索**：`next_cursor` 翻页、按样本号查看完整前后值；
6. **冲突追溯**：同值并存不报冲突 → 异值后报冲突并返回本轮首次成立位置 →
   用旧截止位置验证之后的变更不混入 → 建槽前基线行标记与覆盖范围说明 →
   制造日志缺口后接口给出 `chain_broken`（不报无冲突）→ 非法 LSN 400 / 未归档位置 404。

### 手动触发崩溃点（可选）

```bash
docker compose stop collector
docker compose run --rm --no-deps --entrypoint sh collector \
  -c 'touch /var/lib/lab-cdc/crash-once'
CRASH_AFTER_PERSIST=1 docker compose up -d --no-deps collector
# 在源库提交一笔事务后，collector 会在确认前退出一次（标记自动归档，不会反复崩）
docker compose up -d --no-deps collector   # 正常重启续采
```

## 5. API 摘要

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /docs` | Swagger 交互文档 |
| `GET /commits?cursor=0/..&limit=20&direction=forward` | 按提交位置翻页，每页含整笔事务及有序前后值 |
| `GET /commits/{commit_lsn}` | 取单笔事务（源 xid、提交位置、全部行变更） |
| `GET /changes?sample_no=S2026-0002&action=U` | 按样本号/动作检索行级变更，删除也能按旧样本号追溯 |
| `GET /status` | 采集器存活心跳、已持久/已确认 LSN、源槽 active/restart_lsn、WAL 保留字节数 |
| `GET /conflict-trace?sample_no=..&test_item=..&commit_lsn=..` | **冲突追溯**：重建截至该提交位置的有效行；`conclusion` 为 `conflict`/`no_conflict`/`chain_broken`，返回各行结果与最近变更位置、本轮冲突首次成立位置、断链明细及覆盖范围说明 |

`/conflict-trace` 响应要点：

- `rows`：截止位置处仍有效的行（已删除/已改键离开目标键的 id 不出现），含 `result`、
  `last_change_commit_lsn/xid/time`、`is_pre_slot_baseline`；
- `conflict_round_first_*`：当前这轮冲突首次成立的事务位置（无冲突或断链时为 `null`）；
- `chain_breaks`：变更链断裂明细（期望值 vs 实际旧值），此时 `conclusion=chain_broken`；
- `coverage`：建槽后最早归档位置、候选/基线 id、`ingested_after_cutoff_excluded`
  及覆盖范围文字说明（建槽前从未变更的行不可见、回滚不参与）。

## 6. 运维说明

- 采集器**不设自动重启策略**，以便崩溃演示中进程保持退出；生产环境可加 `restart: unless-stopped`，
  重连与幂等逻辑已内置（流中断 3 秒后重连，从槽位置继续）。
- 长期停采会让源库 WAL 持续增长，`/status` 的 `retained_wal_bytes` 用于监控该积压。
- 复制槽为持久对象，容器重建/镜像升级不会丢位置；清理环境用 `docker compose down -v`
  （会删除两个数据卷与崩溃标记卷，再次启动即为全新基线）。
- 初始基线数据在建槽前写入，按设计不进入变更日志；日志只承载建槽之后的变更流。
