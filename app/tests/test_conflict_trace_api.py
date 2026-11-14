"""
conflict_trace 端点级测试（零第三方依赖）。

用 tests/_stubs 下的本地桩替代 fastapi/pydantic/psycopg2，用内存数据集实现
app.main.log_query，驱动真实端点函数，验证：
  - 截止点之后的已采集变更不混入；
  - 不同 id 且结果值不同才报冲突、同值不报；
  - 断链时结论固定为 chain_broken，不能报无冲突；
  - 建槽前基线行标记、删除行不可见；
  - 非法 LSN 400、未归档位置 404。
"""
import os
import sys
import types
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "tests", "_stubs"))  # fastapi/pydantic/...
sys.path.insert(0, ROOT)                                   # app 包

from fastapi import HTTPException  # noqa: E402

import app.main as main  # noqa: E402
from app.lsn import lsn_to_int  # noqa: E402

SAMPLE, ITEM = "S1", "血糖"
SLOT = "lab_results_slot"


def r(rid, result, sample=SAMPLE, item=ITEM):
    return {"id": rid, "sample_no": sample, "test_item": item,
            "result": result, "updated_at": "2026-11-14T10:00:00Z"}


def c(commit, seq, action, old=None, new=None, xid=None):
    return {
        "commit_lsn": commit,
        "xid": xid if xid is not None else int(commit.split("/")[1], 16),
        "commit_time": f"2026-11-14T10:{int(commit.split('/')[1], 16) % 60:02d}:00Z",
        "change_seq": seq,
        "action": action,
        "row_lsn": commit,
        "old_values": old,
        "new_values": new,
    }


# ------------------------------------------------------------- 内存假库引擎
class FakeDB:
    def __init__(self, changes):
        self.changes = changes

    @staticmethod
    def _rid(ch):
        v = ch["new_values"] or ch["old_values"]
        return int(v["id"])

    def _key_match(self, values, sn, item):
        return values is not None and values.get("sample_no") == sn \
            and values.get("test_item") == item

    def query(self, sql, params):
        # 1) 截止位置是否为已归档事务
        if "SELECT 1 FROM source_commit" in sql:
            _, lsn = params
            exists = any(ch["commit_lsn"] == lsn for ch in self.changes)
            return [{"1": 1}] if exists else []

        # 2) 候选 id
        if "SELECT DISTINCT" in sql:
            _, lsn, sn, item, _, _ = params
            ids = sorted({
                self._rid(ch) for ch in self.changes
                if lsn_to_int(ch["commit_lsn"]) <= lsn_to_int(lsn)
                and (self._key_match(ch["new_values"], sn, item)
                     or self._key_match(ch["old_values"], sn, item))
            })
            return [{"row_id": i} for i in ids]

        # 3) 候选 id 的完整历史
        if "= ANY(%s::bigint[])" in sql:
            _, lsn, ids = params
            rows = [
                dict(ch) for ch in self.changes
                if lsn_to_int(ch["commit_lsn"]) <= lsn_to_int(lsn)
                and self._rid(ch) in ids
            ]
            rows.sort(key=lambda x: (lsn_to_int(x["commit_lsn"]), x["change_seq"]))
            return rows

        # 4) 最早归档事务
        if "ORDER BY commit_lsn ASC" in sql and "LIMIT 1" in sql:
            if not self.changes:
                return []
            first = min(self.changes, key=lambda x: lsn_to_int(x["commit_lsn"]))
            return [{"commit_lsn": first["commit_lsn"],
                     "commit_time": first["commit_time"]}]

        # 5) 最大归档位置
        if "max(commit_lsn)" in sql:
            return [{"lsn": max(
                (ch["commit_lsn"] for ch in self.changes),
                key=lsn_to_int, default=None)}]

        raise AssertionError(f"unexpected SQL: {sql}")


class ConflictTraceApiTest(unittest.TestCase):
    def setUp(self):
        main.settings = types.SimpleNamespace(slot_name=SLOT)
        self._orig = main.log_query

    def tearDown(self):
        main.log_query = self._orig

    def use(self, changes):
        db = FakeDB(changes)
        main.log_query = lambda settings, sql, params=None: db.query(sql, params or [])

    def trace(self, lsn):
        return main.conflict_trace(sample_no=SAMPLE, test_item=ITEM, commit_lsn=lsn)

    # ------------------------------------------------------------ 主冲突场景
    def test_conflict_and_cutoff_excludes_later_ingestion(self):
        self.use([
            c("0/10", 1, "I", new=r(1, "5.4")),
            c("0/20", 1, "I", new=r(2, "5.4")),                       # 同值无冲突
            c("0/30", 1, "U", old=r(2, "5.4"), new=r(2, "5.9")),      # 异值冲突
            c("0/40", 1, "U", old=r(1, "5.4"), new=r(1, "9.9")),      # 截止点之后
            c("0/40", 2, "I", new=r(9, "7.7", sample="S2")),          # 其他样本
        ])
        out = self.trace("0/30")
        self.assertEqual(out.conclusion, "conflict")
        self.assertTrue(out.conflict)
        self.assertEqual(out.conflict_round_first_commit_lsn, "0/30")
        values = {row.row_id: row.result for row in out.rows}
        self.assertEqual(values, {1: "5.4", 2: "5.9"})      # id1 的 9.9 未混入
        self.assertTrue(out.ingested_after_cutoff)
        self.assertEqual(out.scanned_commit_count, 3)
        self.assertEqual(out.coverage["candidate_row_ids"], [1, 2])
        self.assertEqual(out.chain_breaks, [])

    def test_same_value_distinct_ids_is_no_conflict(self):
        self.use([
            c("0/10", 1, "I", new=r(1, "5.4")),
            c("0/20", 1, "I", new=r(2, "5.4")),
        ])
        out = self.trace("0/20")
        self.assertEqual(out.conclusion, "no_conflict")
        self.assertFalse(out.conflict)
        self.assertIsNone(out.conflict_round_first_commit_lsn)
        self.assertEqual(len(out.rows), 2)

    # ------------------------------------------------------------ 断链
    def test_chain_break_forbids_no_conflict_conclusion(self):
        self.use([
            c("0/10", 1, "I", new=r(1, "5.4")),
            c("0/30", 1, "U", old=r(1, "9.9"), new=r(1, "5.6")),  # 旧值对不上
        ])
        out = self.trace("0/30")
        self.assertEqual(out.conclusion, "chain_broken")
        self.assertEqual(len(out.chain_breaks), 1)
        brk = out.chain_breaks[0]
        self.assertEqual(brk.row_id, 1)
        self.assertEqual(brk.expected_values["result"], "5.4")
        self.assertEqual(brk.actual_old_values["result"], "9.9")
        self.assertEqual(brk.detected_at_commit_lsn, "0/30")

    # ------------------------------------------------- 建槽前基线行 + 删除
    def test_pre_slot_baseline_and_deleted_row_invisible(self):
        self.use([
            c("0/10", 1, "U", old=r(5, "4.1"), new=r(5, "4.3")),   # 首见即 U
            c("0/20", 1, "I", new=r(6, "4.3")),                    # 同值无冲突
            c("0/30", 1, "D", old=r(6, "4.3")),                    # 删除
        ])
        at_20 = self.trace("0/20")
        self.assertEqual(at_20.conclusion, "no_conflict")
        self.assertEqual({row.row_id for row in at_20.rows}, {5, 6})

        at_30 = self.trace("0/30")
        self.assertEqual({row.row_id for row in at_30.rows}, {5})  # id6 已删除不可见
        self.assertTrue(at_30.rows[0].is_pre_slot_baseline)
        self.assertEqual(at_30.coverage["pre_slot_baseline_row_ids"], [5])

    # ------------------------------------------------------------ 错误处理
    def test_invalid_lsn_returns_400(self):
        self.use([])
        with self.assertRaises(HTTPException) as ctx:
            self.trace("not-an-lsn")
        self.assertEqual(ctx.exception.status_code, 400)

    def test_unknown_archived_position_returns_404(self):
        self.use([c("0/10", 1, "I", new=r(1, "5.4"))])
        with self.assertRaises(HTTPException) as ctx:
            self.trace("0/99")
        self.assertEqual(ctx.exception.status_code, 404)

    def test_empty_history_returns_scope_note_not_crash(self):
        # 截止位置存在但没有任何命中目标键的变更
        self.use([c("0/10", 1, "I", new=r(9, "7.7", sample="S2"))])
        out = self.trace("0/10")
        self.assertEqual(out.conclusion, "no_conflict")
        self.assertEqual(out.rows, [])
        self.assertEqual(out.coverage["candidate_row_ids"], [])
        self.assertIn("建槽", out.coverage["scope_note"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
