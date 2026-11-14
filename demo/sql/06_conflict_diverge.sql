-- 冲突成立：把重复行的结果改成与原行不同的值，同一样本/项目出现矛盾有效结果。
-- 该事务的提交位置即“当前这轮冲突首次成立”的位置，API 应精确返回它。
BEGIN;

UPDATE results
   SET result = '7.1', updated_at = now()
 WHERE sample_no = 'S2026-0002'
   AND test_item = '血糖'
   AND result = '5.6'
   AND id = (SELECT max(id) FROM results
              WHERE sample_no = 'S2026-0002' AND test_item = '血糖');

COMMIT;
