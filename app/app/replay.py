"""
截至某一已提交位置（commit_lsn）的 results 有效行重建与冲突判定（纯标准库）。

本模块不依赖数据库或 Web 框架，输入是按 (commit_lsn, change_seq) 全局有序的
行变更 dict 列表，输出每个 results.id 在整笔事务回放后的最终状态、断链记录以及
“当前这轮冲突首次成立”的事务位置，便于单元测试与在 API 层复用。

语义约定（与需求逐条对应）：

* 整笔事务最终状态判定：同一 commit_lsn 下的全部 change_seq 先顺序回放，
  冲突只在事务边界用事务后的最终状态评估；同一事务内多次改值的中间态绝不外露，
  改键（sample_no/test_item 变化）与删除同样以最终状态为准。
* 断链：同一 id 再次出现 U/D 时，old_values 必须与上一次已知新值逐列相等；
  若不一致（日志缺口、绕过逻辑解码的直改等），记一条 chain_break。
  出现断链时调用方不得报告“无冲突”，只能提示链路断裂并给出期望/实际值。
* 建槽前基线行：某 id 建槽后首次出现就是 U/D，说明它在建槽前已存在，
  以该条 old_values 建立初始状态并标记 pre_slot_row=True；建槽前从未变更的行
  没有任何日志记录，天然不可见，由 API 层在覆盖范围说明中声明这一局限。
* 冲突：同一 (sample_no, test_item) 下存在 >=2 个不同 id 的有效行，
  且它们的 result 去重后仍 >=2 个不同值才成立；不同 id 但结果相同不算冲突。
* 冲突轮：冲突从无到有的那个事务位置即本轮首次成立位置；冲突消解后再次成立，
  以重新成立的事务作为新一轮的起点。
"""
from __future__ import annotations

from itertools import groupby
from typing import Any


def _row_id(change: dict[str, Any]) -> int:
    """wal2json 的 old/new 两侧都带主键 id；id 为标识列，不会被 UPDATE 修改。"""
    values = change["new_values"] or change["old_values"]
    return int(values["id"])


def _make_break(row_id: int, change: dict[str, Any],
                expected: dict[str, Any] | None,
                actual: dict[str, Any] | None,
                reason: str) -> dict[str, Any]:
    return {
        "row_id": row_id,
        "reason": reason,
        "change": change,
        "expected_values": expected,
        "actual_old_values": actual,
    }


def _group_by_commit(changes: list[dict[str, Any]]):
    """changes 已按 commit_lsn, change_seq 排序，按提交位置切成整笔事务。"""
    for commit_lsn, group in groupby(changes, key=lambda c: c["commit_lsn"]):
        yield commit_lsn, list(group)


def reconstruct(changes: list[dict[str, Any]],
                sample_no: str,
                test_item: str) -> dict[str, Any]:
    """
    回放截至某个提交位置的有序变更，重建目标样本/项目的有效行。

    参数
    ----
    changes: 每行包含 commit_lsn / xid / commit_time / change_seq / action /
             row_lsn / old_values / new_values（后两者为 dict 或 None），
             调用方负责只传入 commit_lsn <= 截止位置的变更。
    sample_no, test_item: 目标样本号与检测项目。

    返回
    ----
    {
      "states": {row_id: {"current": dict|None, "last_change": dict, "pre_slot": bool}},
      "chain_breaks": [断链记录...],
      "conflict_round_start": {"commit_lsn","xid","commit_time"} | None,
      "has_conflict": bool,           # 截止位置处冲突是否成立（尽力计算的结果）
      "transactions_replayed": int,
    }
    """
    # row_id -> 当前状态：current=None 表示已删除
    states: dict[int, dict[str, Any]] = {}
    chain_breaks: list[dict[str, Any]] = []
    conflict_active = False
    conflict_start: dict[str, Any] | None = None
    transactions_replayed = 0

    for commit_lsn, group in _group_by_commit(changes):
        transactions_replayed += 1
        txn_head = group[0]

        # ---- 事务内：按 change_seq 顺序回放，只积累状态、不评估冲突 ----
        for ch in group:
            rid = _row_id(ch)
            action = ch["action"].strip().upper()
            old = ch["old_values"]
            new = ch["new_values"]
            st = states.get(rid)

            if st is None:
                if action == "I":
                    # 建槽后新建的行，新值即初始状态
                    states[rid] = {
                        "current": new, "last_change": ch, "pre_slot": False,
                    }
                    continue
                # 建槽后首条记录就是 U/D：建槽前已存在的基线行，
                # 以完整旧值建立初始状态，并标记其可见性来自旧值而非 INSERT。
                st = {"current": old, "last_change": ch, "pre_slot": True}
                states[rid] = st

            # ---- 已有已知状态：先校验变更链，再应用最终状态 ----
            if action == "I":
                # 已追踪的 id 再次 INSERT：标识复用或中间历史缺失
                chain_breaks.append(_make_break(
                    rid, ch, st["current"], None,
                    "同一 results.id 再次出现 INSERT，行标识被复用或历史变更缺失",
                ))
            elif old != st["current"]:
                # 旧值与上一次已知新值逐列不符：链路断裂，不能当作正常更正
                chain_breaks.append(_make_break(
                    rid, ch, st["current"], old,
                    "旧值与上一次已知新值不一致，变更链断裂（可能存在未采集的历史变更）",
                ))

            if action == "D":
                st["current"] = None
            else:  # I / U：以新值覆盖（同事务多次改值自然只留下最后一次）
                st["current"] = new
            st["last_change"] = ch

        # ---- 事务边界：用整笔事务后的最终状态评估冲突，中间态不外露 ----
        valid_states = [
            s for s in states.values()
            if s["current"] is not None
            and s["current"].get("sample_no") == sample_no
            and s["current"].get("test_item") == test_item
        ]
        distinct_results = {s["current"].get("result") for s in valid_states}
        # states 以 id 为键，valid_states 天然是不同 id；还需结果值不同才算冲突
        conflict_now = len(valid_states) >= 2 and len(distinct_results) >= 2

        if conflict_now and not conflict_active:
            conflict_active = True
            conflict_start = {
                "commit_lsn": commit_lsn,
                "xid": txn_head["xid"],
                "commit_time": txn_head["commit_time"],
            }
        elif not conflict_now:
            # 冲突消解（行数不足、结果趋同或行被改键/删除）：轮次结束
            conflict_active = False
            conflict_start = None

    return {
        "states": states,
        "chain_breaks": chain_breaks,
        "conflict_round_start": conflict_start,
        "has_conflict": conflict_active,
        "transactions_replayed": transactions_replayed,
    }
