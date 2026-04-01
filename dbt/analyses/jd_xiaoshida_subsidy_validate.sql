-- =============================================================================
-- 京东小时达：活动补贴类「京东承担金额」辅助校验
-- =============================================================================
-- 原始文件：sql/o2o_jd_xiaoshida_subsidy_jd_carry_validate.sql（多语句，含 BEGIN/COMMIT）
-- 重构为单语句 dbt analysis，返回汇总 + 失败明细
--
-- 业务规则（与 docs/京东小时达结算计算逻辑.md §6 一致）：
--   凡活动补贴白名单中的费用类型，预期 结算金额 ≈ 京东承担金额，商家承担 ≈ 0
--
-- 用法：dbt compile --select jd_xiaoshida_subsidy_validate --vars '{report_date: "2026-03-25"}'
-- =============================================================================

WITH params AS (
  SELECT DATE '{{ var("report_date", "2026-03-25") }}' AS d
),

hour_bills AS (
  SELECT DISTINCT NULLIF(BTRIM(REPLACE(o."订单编号", E'\t', '')), '') AS bill_no
  FROM ods_rpa_jd_order o
  CROSS JOIN params p
  WHERE o.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND BTRIM(REPLACE(o."订单来源", E'\t', '')) = '小时达'
    AND NULLIF(BTRIM(REPLACE(o."订单编号", E'\t', '')), '') IS NOT NULL
),

activity_subsidy_types(fee_type) AS (
  VALUES
    ('优惠券-平台-小时购'::text),
    ('优惠券-京东平台-小时购'),
    ('京豆-平台-小时达'),
    ('积分-平台-小时购')
),

parsed AS (
  SELECT
    f.dt,
    NULLIF(BTRIM(REPLACE(f."到家业务单号", E'\t', '')), '') AS bill_no,
    NULLIF(BTRIM(REPLACE(f."费用类型", E'\t', '')), '') AS fee_type,
    CASE WHEN BTRIM(REPLACE(f."结算金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
         THEN BTRIM(REPLACE(f."结算金额", E'\t', ''))::numeric END AS amt_settle,
    CASE WHEN BTRIM(REPLACE(f."京东承担金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
         THEN BTRIM(REPLACE(f."京东承担金额", E'\t', ''))::numeric END AS amt_jd,
    CASE WHEN BTRIM(REPLACE(f."商家承担金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
         THEN BTRIM(REPLACE(f."商家承担金额", E'\t', ''))::numeric END AS amt_merchant
  FROM ods_rpa_jd_finance f
  CROSS JOIN params p
  WHERE f.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND NULLIF(BTRIM(REPLACE(f."到家业务单号", E'\t', '')), '') IN (SELECT bill_no FROM hour_bills)
    AND NULLIF(BTRIM(REPLACE(f."费用类型", E'\t', '')), '') IN (SELECT fee_type FROM activity_subsidy_types)
),

checked AS (
  SELECT
    *,
    COALESCE(amt_settle, 0) AS s,
    COALESCE(amt_jd, 0) AS j,
    COALESCE(amt_merchant, 0) AS m,
    (
      amt_settle IS NOT NULL
      AND amt_jd IS NOT NULL
      AND ABS(amt_settle - amt_jd) <= 0.02
      AND ABS(COALESCE(amt_merchant, 0)) <= 0.02
    ) AS is_ok
  FROM parsed
),

summary AS (
  SELECT
    dt::text AS 分区dt,
    COUNT(*) AS 活动补贴行数,
    COUNT(*) FILTER (WHERE is_ok) AS 通过,
    COUNT(*) FILTER (WHERE NOT is_ok) AS 失败或解析异常,
    ROUND(COALESCE(SUM(s), 0), 2) AS 结算金额合计,
    ROUND(COALESCE(SUM(j), 0), 2) AS 京东承担合计
  FROM checked
  GROUP BY dt
)

SELECT
  s.分区dt,
  s.活动补贴行数,
  s.通过,
  s."失败或解析异常",
  s.结算金额合计,
  s.京东承担合计,
  c.bill_no AS 失败_到家业务单号,
  c.fee_type AS 失败_费用类型,
  c.amt_settle AS 失败_结算金额,
  c.amt_jd AS 失败_京东承担金额,
  c.amt_merchant AS 失败_商家承担金额,
  CASE
    WHEN c.amt_settle IS NULL OR c.amt_jd IS NULL THEN '结算或京东承担无法解析为数字'
    WHEN ABS(COALESCE(c.amt_merchant, 0)) > 0.02 THEN '商家承担金额应为0'
    ELSE '结算金额与京东承担金额不一致'
  END AS 失败_原因简述
FROM summary s
LEFT JOIN checked c ON c.dt::text = s.分区dt AND NOT c.is_ok
ORDER BY s.分区dt, c.bill_no, c.fee_type;
