-- 京东订单 + 财务明细查询（两个订单号）
-- 1) 先查订单表结构
SELECT column_name FROM information_schema.columns WHERE table_name='ods_rpa_jd_order' ORDER BY ordinal_position;
SELECT column_name FROM information_schema.columns WHERE table_name='ods_rpa_jd_finance' ORDER BY ordinal_position;

-- 2) 订单表：模糊匹配两个单号
SELECT * FROM ods_rpa_jd_order WHERE "订单编号" IN ('3432261015423735', '2606087974025494');

-- 3) 财务表：模糊匹配
SELECT * FROM ods_rpa_jd_finance WHERE "到家业务单号" IN ('3432261015423735', '2606087974025494');
