-- 断链演示（手工制造日志缺口）。
-- 正常变更链要求：同一 id 再次出现 UPDATE/DELETE 时，old_values 必须等于上一次
-- 已知的新值。这里直接在日志库中“抹掉”某 id 的最新一条已提交变更，使下一次真实
-- 更正的 old_values 与日志中保留的上次新值对不上，触发 chain_broken。
-- 注意：只修改日志库，源库 results 表保持真实连续，用于验证 API 不扫描当前表、
--       只依据变更日志重建，并在缺口处明确提示断链而不是给出“无冲突”。
-- 本脚本由 demo.sh 用 psql 变量 :target_id 执行。
DELETE FROM result_change
 WHERE id = (
     SELECT rc.id
       FROM result_change rc
      WHERE rc.slot_name = :'slot_name'
        AND rc.new_values ->> 'id' = :'target_id'
      ORDER BY rc.commit_lsn DESC, rc.change_seq DESC
      LIMIT 1
 );
