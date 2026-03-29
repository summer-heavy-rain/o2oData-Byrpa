-- =============================================================================
-- 京东小时达/到家：按门店汇总结算金额（ods_rpa_jd_finance）
-- =============================================================================
-- 说明：
--   - 财务表仅有「结算金额」文本列，**无「应结金额」字段**；门店口径 = 该 dt 分区内
--     按「门店编号」对 **SUM(结算金额)**（费用明细多行轧差，与 lib 口径一致）。
--   - 门店名称从 ods_rpa_jd_order 取 DISTINCT ON(门店ID) 最新一条（按 dt 降序）。
-- 修改业务日：改 params
-- =============================================================================

WITH params AS (
  SELECT DATE '2026-03-25' AS d
),

store_names AS (
  SELECT DISTINCT ON (NULLIF(BTRIM(REPLACE(o."门店ID", E'\t', '')), ''))
    NULLIF(BTRIM(REPLACE(o."门店ID", E'\t', '')), '') AS store_id,
    NULLIF(BTRIM(REPLACE(o."门店名称", E'\t', '')), '') AS store_name
  FROM ods_rpa_jd_order o
  WHERE NULLIF(BTRIM(REPLACE(o."门店ID", E'\t', '')), '') IS NOT NULL
  ORDER BY NULLIF(BTRIM(REPLACE(o."门店ID", E'\t', '')), ''), o.dt DESC NULLS LAST
),

fin AS (
  SELECT
    NULLIF(BTRIM(REPLACE(f."门店编号", E'\t', '')), '') AS store_id,
    NULLIF(BTRIM(REPLACE(f."到家业务单号", E'\t', '')), '') AS bill_no,
    CASE WHEN BTRIM(REPLACE(f."结算金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
         THEN BTRIM(REPLACE(f."结算金额", E'\t', ''))::numeric
         ELSE NULL END AS amt
  FROM ods_rpa_jd_finance f
  CROSS JOIN params p
  WHERE f.dt::text = to_char(p.d, 'YYYY-MM-DD')
)

SELECT
  fin.store_id AS 门店编号,
  sn.store_name AS 门店名称,
  COUNT(DISTINCT fin.bill_no) AS 账单业务单数,
  COUNT(*) AS 费用明细行数,
  ROUND(COALESCE(SUM(fin.amt), 0), 2) AS 结算金额合计
FROM fin
LEFT JOIN store_names sn ON sn.store_id = fin.store_id
GROUP BY fin.store_id, sn.store_name
ORDER BY 结算金额合计 DESC;
