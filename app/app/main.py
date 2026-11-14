"""
变更日志查询 API（FastAPI）。

只查询独立日志库中已完整提交的事务；采集器按事务原子写入，
因此本接口永远不会读到“半笔事务”。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query
from psycopg2.errors import InvalidTextRepresentation, UndefinedObject
from pydantic import BaseModel

from .config import get_settings
from .db import log_query, source_query
from .lsn import lsn_to_int, lsn_valid
from .replay import reconstruct

app = FastAPI(
    title="检验结果变更日志 API",
    description=(
        "基于 PostgreSQL 16 逻辑复制（wal2json）的 results 表变更日志。\n\n"
        "- 仅含建槽之后的 INSERT/UPDATE/DELETE；\n"
        "- 更正、删除保留完整旧值，同事务同行多次修改逐条保序；\n"
        "- 按源提交位置（commit_lsn）翻页，顺序与源提交一致；\n"
        "- 回滚事务不会出现；\n"
        "- GET /conflict-trace 从同一已提交视图重建截至指定提交位置的有效行，"
        "追溯同一样本/检测项目的矛盾结果、断链与冲突轮起点。"
    ),
    version="1.1.0",
)
settings = get_settings()


# ----------------------------------------------------------------- 响应模型
class Change(BaseModel):
    change_seq: int
    action: str
    table_name: str
    row_lsn: str | None
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None


class Commit(BaseModel):
    xid: int
    commit_lsn: str
    begin_lsn: str | None
    commit_time: datetime
    change_count: int
    ingested_at: datetime
    changes: list[Change]


class CommitPage(BaseModel):
    items: list[Commit]
    next_cursor: str | None
    has_more: bool
    limit: int


class FlatChange(BaseModel):
    commit_lsn: str
    xid: int
    commit_time: datetime
    change_seq: int
    action: str
    sample_no: str | None
    test_item: str | None
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None


class ChangePage(BaseModel):
    items: list[FlatChange]
    next_cursor: str | None
    has_more: bool
    limit: int


class Status(BaseModel):
    slot_name: str
    collector_alive: bool
    heartbeat_age_seconds: float | None
    persisted_lsn: str | None
    confirmed_lsn: str | None
    last_xid: int | None
    last_change_at: datetime | None
    source_slot_active: bool | None
    source_restart_lsn: str | None
    source_confirmed_flush_lsn: str | None
    source_pg_current_lsn: str | None
    retained_wal_bytes: int | None


# ------------------------------------------------- 冲突追溯（/conflict-trace）模型
class TracedRow(BaseModel):
    row_id: int
    sample_no: str | None
    test_item: str | None
    result: Any
    is_pre_slot_baseline: bool
    last_change_commit_lsn: str
    last_change_xid: int
    last_change_time: datetime
    last_change_action: str


class ChainBreak(BaseModel):
    row_id: int
    reason: str
    detected_at_commit_lsn: str
    detected_at_xid: int
    expected_values: dict[str, Any] | None
    actual_old_values: dict[str, Any] | None


class ConflictTrace(BaseModel):
    sample_no: str
    test_item: str
    cutoff_commit_lsn: str
    conflict: bool
    conclusion: Literal["conflict", "no_conflict", "chain_broken"]
    conflict_round_first_commit_lsn: str | None
    conflict_round_first_xid: int | None
    conflict_round_first_time: datetime | None
    rows: list[TracedRow]
    chain_breaks: list[ChainBreak]
    coverage: dict[str, Any]
    scanned_commit_count: int
    ingested_after_cutoff: bool


@app.get("/", include_in_schema=False)
def root():
    return {"service": "lab-results-cdc", "docs": "/docs"}


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


# ------------------------------------------------------------- 提交位置翻页
@app.get("/commits", response_model=CommitPage, summary="按提交位置翻页查询完整事务")
def list_commits(
    cursor: str | None = Query(None, description="上一页返回的 next_cursor（提交位置 LSN），首页不传"),
    limit: int = Query(20, ge=1, le=200),
    direction: Literal["forward", "backward"] = Query("forward"),
):
    if cursor is not None and not lsn_valid(cursor):
        raise HTTPException(status_code=400, detail=f"invalid cursor LSN: {cursor!r}")

    order = "ASC" if direction == "forward" else "DESC"
    cmp_op = ">" if direction == "forward" else "<"
    params: list[Any] = [settings.slot_name]
    where = "slot_name = %s"
    if cursor:
        where += f" AND commit_lsn {cmp_op} %s::pg_lsn"
        params.append(cursor)

    # 多取一条用于判断 has_more
    sql = f"""
        SELECT xid, commit_lsn::text AS commit_lsn, begin_lsn::text AS begin_lsn,
               commit_time, change_count, ingested_at
          FROM source_commit
         WHERE {where}
      ORDER BY commit_lsn {order}
         LIMIT %s
    """
    try:
        rows = log_query(settings, sql, params + [limit + 1])
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid cursor LSN")

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["commit_lsn"] if has_more and page else None

    if page:
        details = log_query(
            settings,
            """
            SELECT commit_lsn::text AS commit_lsn, change_seq, action, table_name,
                   row_lsn::text AS row_lsn, old_values, new_values
              FROM result_change
             WHERE slot_name = %s AND commit_lsn = ANY(%s::pg_lsn[])
             ORDER BY commit_lsn, change_seq
            """,
            [settings.slot_name, [r["commit_lsn"] for r in page]],
        )
        grouped: dict[str, list] = {}
        for d in details:
            grouped.setdefault(d["commit_lsn"], []).append(d)
        for r in page:
            r["changes"] = grouped.get(r["commit_lsn"], [])

    return CommitPage(items=page, next_cursor=next_cursor, has_more=has_more, limit=limit)


@app.get("/commits/{commit_lsn}", response_model=Commit, summary="按提交位置取单笔事务及全部前后值")
def get_commit(commit_lsn: str):
    if not lsn_valid(commit_lsn):
        raise HTTPException(status_code=400, detail=f"invalid LSN: {commit_lsn!r}")
    try:
        head = log_query(
            settings,
            """
            SELECT xid, commit_lsn::text AS commit_lsn, begin_lsn::text AS begin_lsn,
                   commit_time, change_count, ingested_at
              FROM source_commit
             WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
            """,
            [settings.slot_name, commit_lsn],
        )
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid LSN")
    if not head:
        raise HTTPException(status_code=404, detail="commit not found")

    changes = log_query(
        settings,
        """
        SELECT change_seq, action, table_name, row_lsn::text AS row_lsn,
               old_values, new_values
          FROM result_change
         WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
      ORDER BY change_seq
        """,
        [settings.slot_name, commit_lsn],
    )
    head[0]["changes"] = changes
    return head[0]


# ------------------------------------------------------------- 行变更检索
@app.get("/changes", response_model=ChangePage, summary="按样本号/动作检索行级变更（含前后值）")
def list_changes(
    sample_no: str | None = Query(None, description="样本号，匹配 new/old 两侧，支持更正与删除追溯"),
    action: Literal["I", "U", "D"] | None = Query(None),
    cursor: str | None = Query(None, description="提交位置 LSN 游标"),
    limit: int = Query(50, ge=1, le=500),
):
    if cursor is not None and not lsn_valid(cursor):
        raise HTTPException(status_code=400, detail=f"invalid cursor LSN: {cursor!r}")

    where = ["rc.slot_name = %s"]
    params: list[Any] = [settings.slot_name]
    if sample_no:
        where.append(
            "COALESCE(rc.new_values ->> 'sample_no', rc.old_values ->> 'sample_no') = %s"
        )
        params.append(sample_no)
    if action:
        where.append("rc.action = %s")
        params.append(action)
    if cursor:
        where.append("rc.commit_lsn > %s::pg_lsn")
        params.append(cursor)

    sql = f"""
        SELECT rc.commit_lsn::text AS commit_lsn, sc.xid, sc.commit_time,
               rc.change_seq, rc.action,
               COALESCE(rc.new_values ->> 'sample_no', rc.old_values ->> 'sample_no') AS sample_no,
               COALESCE(rc.new_values ->> 'test_item', rc.old_values ->> 'test_item') AS test_item,
               rc.old_values, rc.new_values
          FROM result_change rc
          JOIN source_commit sc
            ON sc.slot_name = rc.slot_name AND sc.commit_lsn = rc.commit_lsn
         WHERE {' AND '.join(where)}
      ORDER BY rc.commit_lsn ASC, rc.change_seq ASC
         LIMIT %s
    """
    try:
        rows = log_query(settings, sql, params + [limit + 1])
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid cursor LSN")

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["commit_lsn"] if has_more and page else None
    return ChangePage(items=page, next_cursor=next_cursor, has_more=has_more, limit=limit)


# ----------------------------------------------------------------- 采集状态
@app.get("/status", response_model=Status, summary="采集状态、续采位置与源槽 WAL 保留情况")
def get_status():
    state_rows = log_query(
        settings,
        """
        SELECT s.persisted_lsn::text AS persisted_lsn,
               s.confirmed_lsn::text AS confirmed_lsn,
               s.last_xid, s.last_change_at,
               EXTRACT(EPOCH FROM (now() - h.heartbeat_at))::float AS heartbeat_age_seconds
          FROM slot_state s
          LEFT JOIN collector_heartbeat h ON h.slot_name = s.slot_name
         WHERE s.slot_name = %s
        """,
        [settings.slot_name],
    )
    state = state_rows[0] if state_rows else {}
    alive = (state.get("heartbeat_age_seconds") is not None
             and state["heartbeat_age_seconds"] < 30)

    slot = {}
    try:
        slot_rows = source_query(
            settings,
            """
            SELECT active,
                   restart_lsn::text AS restart_lsn,
                   confirmed_flush_lsn::text AS confirmed_flush_lsn
              FROM pg_replication_slots
             WHERE slot_name = %s
            """,
            [settings.slot_name],
        )
        if slot_rows:
            slot = slot_rows[0]
        current = source_query(settings, "SELECT pg_current_wal_lsn()::text AS lsn")
        current_lsn = current[0]["lsn"]
        retained = source_query(
            settings,
            "SELECT pg_wal_lsn_diff(%s, %s)::bigint AS bytes",
            [current_lsn, slot.get("restart_lsn") or current_lsn],
        )[0]["bytes"]
    except UndefinedObject:
        current_lsn, retained = None, None
    except Exception:
        # 源库暂不可达时仍返回日志库侧状态
        current_lsn, retained = None, None

    return Status(
        slot_name=settings.slot_name,
        collector_alive=bool(alive),
        heartbeat_age_seconds=state.get("heartbeat_age_seconds"),
        persisted_lsn=state.get("persisted_lsn"),
        confirmed_lsn=state.get("confirmed_lsn"),
        last_xid=state.get("last_xid"),
        last_change_at=state.get("last_change_at"),
        source_slot_active=slot.get("active"),
        source_restart_lsn=slot.get("restart_lsn"),
        source_confirmed_flush_lsn=slot.get("confirmed_flush_lsn"),
        source_pg_current_lsn=current_lsn,
        retained_wal_bytes=retained,
    )


# =====================================================================
# 冲突追溯：从同一已提交视图重建截至指定提交位置的 results.id 有效行
# =====================================================================
@app.get(
    "/conflict-trace",
    response_model=ConflictTrace,
    summary="追溯同一样本/检测项目在指定提交位置处的有效行冲突",
)
def conflict_trace(
    sample_no: str = Query(..., description="样本号，如 S2026-0002"),
    test_item: str = Query(..., description="检测项目，如 血糖"),
    commit_lsn: str = Query(
        ...,
        alias="commit_lsn",
        description="已归档提交位置（commit_lsn），重建结果严格截止并包含该事务，之后的采集不混入",
    ),
):
    if not lsn_valid(commit_lsn):
        raise HTTPException(status_code=400, detail=f"invalid commit_lsn: {commit_lsn!r}")

    # 截止位置必须是本槽已归档的完整事务，避免对不存在/未采到的位置给出误导性结论
    try:
        hit = log_query(
            settings,
            "SELECT 1 FROM source_commit WHERE slot_name = %s AND commit_lsn = %s::pg_lsn",
            [settings.slot_name, commit_lsn],
        )
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid commit_lsn")
    if not hit:
        raise HTTPException(
            status_code=404,
            detail="该提交位置在本采集槽的已归档事务中不存在（可能尚未采集到，或 LSN 有误）",
        )

    # 1) 找候选 results.id：截至该位置（含），old/new 任一侧命中样本号+检测项目的行。
    #    改键（UPDATE 改 sample_no/test_item）与删除都能经 old_values 侧被追溯到。
    candidates = log_query(
        settings,
        """
        SELECT DISTINCT COALESCE(new_values ->> 'id', old_values ->> 'id')::bigint AS row_id
          FROM result_change
         WHERE slot_name = %s
           AND commit_lsn <= %s::pg_lsn
           AND (
                (new_values ->> 'sample_no' = %s AND new_values ->> 'test_item' = %s)
             OR (old_values ->> 'sample_no' = %s AND old_values ->> 'test_item' = %s)
           )
        """,
        [settings.slot_name, commit_lsn,
         sample_no, test_item, sample_no, test_item],
    )
    candidate_ids = [r["row_id"] for r in candidates]

    changes: list[dict[str, Any]] = []
    if candidate_ids:
        # 2) 拉取这些 id 的完整历史（仍严格 <= 截止位置），按事务与事务内顺序全局有序。
        #    必须取完整历史而非仅目标键片段，才能校验“旧值=上次已知新值”的变更链。
        changes = log_query(
            settings,
            """
            SELECT rc.commit_lsn::text AS commit_lsn, rc.xid, sc.commit_time,
                   rc.change_seq, rc.action, rc.row_lsn::text AS row_lsn,
                   rc.old_values, rc.new_values
              FROM result_change rc
              JOIN source_commit sc
                ON sc.slot_name = rc.slot_name AND sc.commit_lsn = rc.commit_lsn
             WHERE rc.slot_name = %s
               AND rc.commit_lsn <= %s::pg_lsn
               AND COALESCE(rc.new_values ->> 'id', rc.old_values ->> 'id')::bigint
                   = ANY(%s::bigint[])
             ORDER BY rc.commit_lsn ASC, rc.change_seq ASC
            """,
            [settings.slot_name, commit_lsn, candidate_ids],
        )

    # 3) 纯函数重建：整笔事务回放、事务边界判定冲突、断链检测、建槽前基线行标记
    result = reconstruct(changes, sample_no, test_item)
    states = result["states"]

    # 4) 组装截止位置处的“有效行”（已删除或已改键离开目标键的 id 不是有效行）
    traced_rows: list[TracedRow] = []
    pre_slot_ids: list[int] = []
    for rid, st in sorted(states.items()):
        if st["pre_slot"]:
            pre_slot_ids.append(rid)
        cur = st["current"]
        if cur is None:
            continue
        if cur.get("sample_no") != sample_no or cur.get("test_item") != test_item:
            continue
        last = st["last_change"]
        traced_rows.append(TracedRow(
            row_id=rid,
            sample_no=cur.get("sample_no"),
            test_item=cur.get("test_item"),
            result=cur.get("result"),
            is_pre_slot_baseline=st["pre_slot"],
            last_change_commit_lsn=last["commit_lsn"],
            last_change_xid=last["xid"],
            last_change_time=last["commit_time"],
            last_change_action=last["action"].strip(),
        ))

    chain_breaks = [
        ChainBreak(
            row_id=brk["row_id"],
            reason=brk["reason"],
            detected_at_commit_lsn=brk["change"]["commit_lsn"],
            detected_at_xid=brk["change"]["xid"],
            expected_values=brk["expected_values"],
            actual_old_values=brk["actual_old_values"],
        )
        for brk in result["chain_breaks"]
    ]

    # 5) 结论：存在断链时禁止给出“无冲突”，只能提示链路断裂
    if chain_breaks:
        conclusion: str = "chain_broken"
    elif result["has_conflict"]:
        conclusion = "conflict"
    else:
        conclusion = "no_conflict"

    start = result["conflict_round_start"]

    # 6) 覆盖范围：建槽时刻、建槽前不可见行、截止点之后已采集但被排除的事务
    earliest = log_query(
        settings,
        """
        SELECT commit_lsn::text AS commit_lsn, commit_time
          FROM source_commit
         WHERE slot_name = %s
         ORDER BY commit_lsn ASC
         LIMIT 1
        """,
        [settings.slot_name],
    )
    max_row = log_query(
        settings,
        "SELECT max(commit_lsn)::text AS lsn FROM source_commit WHERE slot_name = %s",
        [settings.slot_name],
    )
    max_lsn = max_row[0]["lsn"] if max_row else None
    ingested_after = bool(
        max_lsn and lsn_to_int(max_lsn) > lsn_to_int(commit_lsn)
    )

    coverage = {
        "slot_name": settings.slot_name,
        "earliest_captured_commit_lsn": earliest[0]["commit_lsn"] if earliest else None,
        "earliest_captured_commit_time": earliest[0]["commit_time"] if earliest else None,
        "cutoff_commit_lsn": commit_lsn,
        "candidate_row_ids": candidate_ids,
        "pre_slot_baseline_row_ids": pre_slot_ids,
        "ingested_after_cutoff_excluded": ingested_after,
        "scope_note": (
            "重建仅基于逻辑复制槽建槽之后采集到的已提交变更；"
            "建槽之前从未发生过变更的 results 行没有任何日志记录，因此不可见，"
            "其存在与取值无法从本视图判断。建槽前已存在、建槽后首次被改/删的行"
            "以首条旧值建立基线并在 is_pre_slot_baseline 标记。"
            "回滚事务不产生解码消息，不参与重建。"
        ),
        "cutoff_note": (
            "结果严格截止并包含 commit_lsn 对应事务；该位置之后即使采集器已继续采集，"
            "也不会混入本次重建（ingested_after_cutoff_excluded=true 表示存在被排除的后续事务）。"
        ),
    }

    return ConflictTrace(
        sample_no=sample_no,
        test_item=test_item,
        cutoff_commit_lsn=commit_lsn,
        # 断链时该布尔值只是回放尽力计算的结果，结论以 conclusion=chain_broken 为准
        conflict=result["has_conflict"],
        conclusion=conclusion,
        conflict_round_first_commit_lsn=start["commit_lsn"] if start else None,
        conflict_round_first_xid=start["xid"] if start else None,
        conflict_round_first_time=start["commit_time"] if start else None,
        rows=traced_rows,
        chain_breaks=chain_breaks,
        coverage=coverage,
        scanned_commit_count=result["transactions_replayed"],
        ingested_after_cutoff=ingested_after,
    )

