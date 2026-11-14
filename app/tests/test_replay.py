"""replay.reconstruct 的零依赖单元测试（标准库 unittest，可直接 python3 运行）。"""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.replay import reconstruct  # noqa: E402

SAMPLE = "S1"
ITEM = "血糖"


def row(rid, result, sample=SAMPLE, item=ITEM, ts="2026-11-14T10:00:00Z"):
    return {"id": rid, "sample_no": sample, "test_item": item,
            "result": result, "updated_at": ts}


def ch(commit, seq, action, rid, old=None, new=None, xid=None):
    return {
        "commit_lsn": commit,
        "xid": xid if xid is not None else int(commit.split("/")[1], 16) % 1000,
        "commit_time": "2026-11-14T10:00:00Z",
        "change_seq": seq,
        "action": action,
        "row_lsn": commit,
        "old_values": old,
        "new_values": new,
    }


def insert(commit, seq, rid, result, **kw):
    return ch(commit, seq, "I", rid, None, row(rid, result, **kw))


def update(commit, seq, rid, old, new, **kw):
    return ch(commit, seq, "U", rid, old, new, **kw)


def delete(commit, seq, rid, old):
    return ch(commit, seq, "D", rid, old, None)


def valid_states(result):
    return {rid: s for rid, s in result["states"].items()
            if s["current"] is not None
            and s["current"]["sample_no"] == SAMPLE
            and s["current"]["test_item"] == ITEM}


class ReconstructTest(unittest.TestCase):

    def test_basic_conflict_only_when_values_differ(self):
        # 不同 id 同值 -> 无冲突；再改出异值 -> 冲突，起点为异值所在事务
        changes = [
            insert("0/10", 1, 1, "5.4"),
            insert("0/20", 1, 2, "5.4"),
            update("0/30", 1, 2, row(2, "5.4"), row(2, "5.9")),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertTrue(r["has_conflict"])
        self.assertEqual(r["conflict_round_start"]["commit_lsn"], "0/30")
        rows = valid_states(r)
        self.assertEqual({rid: s["current"]["result"] for rid, s in rows.items()},
                         {1: "5.4", 2: "5.9"})

    def test_distinct_ids_same_value_is_not_conflict(self):
        changes = [insert("0/10", 1, 1, "5.4"), insert("0/20", 1, 2, "5.4")]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertFalse(r["has_conflict"])
        self.assertIsNone(r["conflict_round_start"])
        self.assertEqual(len(valid_states(r)), 2)

    def test_same_transaction_multiple_updates_only_final_state_counts(self):
        # 同一事务内 id1 连续改两次，最终 5.6；同事务插入 id2=5.9
        changes = [
            update("0/40", 1, 1, row(1, "5.4"), row(1, "5.8")),
            update("0/40", 2, 1, row(1, "5.8"), row(1, "5.6")),
            insert("0/40", 3, 2, "5.9"),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertEqual(r["transactions_replayed"], 1)
        self.assertTrue(r["has_conflict"])
        self.assertEqual(r["conflict_round_start"]["commit_lsn"], "0/40")
        self.assertEqual(valid_states(r)[1]["current"]["result"], "5.6")
        self.assertEqual(valid_states(r)[1]["last_change"]["change_seq"], 2)

    def test_resolution_then_new_round_resets_start(self):
        changes = [
            insert("0/10", 1, 1, "5.4"),
            insert("0/20", 1, 2, "5.9"),          # 冲突成立，起点 0/20
            update("0/30", 1, 2, row(2, "5.9"), row(2, "5.4")),  # 同值，消解
            update("0/40", 1, 2, row(2, "5.4"), row(2, "6.0")),  # 再次冲突
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertTrue(r["has_conflict"])
        self.assertEqual(r["conflict_round_start"]["commit_lsn"], "0/40")

    def test_resolution_by_delete(self):
        changes = [
            insert("0/10", 1, 1, "5.4"),
            insert("0/20", 1, 2, "5.9"),
            delete("0/30", 1, 2, row(2, "5.9")),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertFalse(r["has_conflict"])
        self.assertIsNone(r["conflict_round_start"])
        self.assertIsNone(r["states"][2]["current"])

    def test_key_change_moves_row_out_of_scope(self):
        # id2 改键到另一项目，不再属于目标键；id3 新入目标键
        changes = [
            insert("0/10", 1, 1, "5.4"),
            insert("0/20", 1, 2, "5.9"),          # 冲突
            update("0/30", 1, 2, row(2, "5.9"), row(2, "5.9", item="尿糖")),
            insert("0/40", 1, 3, "7.0"),          # 新冲突 5.4 vs 7.0
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertTrue(r["has_conflict"])
        self.assertEqual(r["conflict_round_start"]["commit_lsn"], "0/40")
        rows = valid_states(r)
        self.assertEqual(set(rows), {1, 3})

    def test_cutoff_excludes_later_changes(self):
        changes = [
            insert("0/10", 1, 1, "5.4"),
            insert("0/20", 1, 2, "5.4"),          # 同值无冲突
            update("0/30", 1, 2, row(2, "5.4"), row(2, "5.9")),  # 截止点之后
        ]
        # 只传 <= 0/20 的变更，模拟截止位置，之后的更正不得混入
        cutoff = [c for c in changes if c["commit_lsn"] <= "0/20"]
        r = reconstruct(cutoff, SAMPLE, ITEM)
        self.assertFalse(r["has_conflict"])
        self.assertEqual(valid_states(r)[2]["current"]["result"], "5.4")

    def test_chain_break_on_mismatched_old_value(self):
        # 日志缺失一笔：已知 id1=5.4，再次出现的旧值却是 9.9
        changes = [
            insert("0/10", 1, 1, "5.4"),
            update("0/30", 1, 1, row(1, "9.9"), row(1, "5.6")),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertEqual(len(r["chain_breaks"]), 1)
        brk = r["chain_breaks"][0]
        self.assertEqual(brk["row_id"], 1)
        self.assertEqual(brk["expected_values"]["result"], "5.4")
        self.assertEqual(brk["actual_old_values"]["result"], "9.9")
        # 断链后状态仍按新值推进，但调用方必须依据 chain_breaks 拒绝“无冲突”结论
        self.assertEqual(valid_states(r)[1]["current"]["result"], "5.6")

    def test_pre_slot_baseline_row_first_seen_as_update(self):
        # 建槽前已存在的行，建槽后首条是 UPDATE：用旧值建立基线并标记
        changes = [
            update("0/10", 1, 7, row(7, "4.1"), row(7, "4.3")),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        st = r["states"][7]
        self.assertTrue(st["pre_slot"])
        self.assertEqual(st["current"]["result"], "4.3")

    def test_pre_slot_baseline_row_first_seen_as_delete(self):
        changes = [delete("0/10", 1, 8, row(8, "4.1"))]
        r = reconstruct(changes, SAMPLE, ITEM)
        st = r["states"][8]
        self.assertTrue(st["pre_slot"])
        self.assertIsNone(st["current"])

    def test_repeated_insert_is_chain_break(self):
        changes = [
            insert("0/10", 1, 1, "5.4"),
            insert("0/20", 1, 1, "5.9"),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertEqual(len(r["chain_breaks"]), 1)

    def test_intra_txn_chain_consistent_when_multiple_updates(self):
        # 同事务多次改值，每次旧值都承接上一条新值 -> 无断链
        changes = [
            update("0/10", 1, 1, row(1, "5.4"), row(1, "5.8")),
            update("0/10", 2, 1, row(1, "5.8"), row(1, "5.6")),
        ]
        r = reconstruct(changes, SAMPLE, ITEM)
        self.assertEqual(r["chain_breaks"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
