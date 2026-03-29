-- =============================================================================
-- 京东小时达：活动补贴类「京东承担金额」辅助校验
-- =============================================================================
-- 业务规则（与 docs/京东小时达结算计算逻辑.md §6 一致）：
--   凡 §0.1「活动补贴」白名单中的费用类型，平台补贴增收应对应京东承担；
--   预期：结算金额 ≈ 京东承担金额，商家承担金额 ≈ 0（容差 0.02 元，防文本解析误差）。
-- 范围：仅统计「到家业务单号」在当日 ods_rpa_jd_order 中 订单来源 = 小时达 的账单，
--       避免与到家链路混用小时达费用枚举。
-- 用法：改下方 params 中的日期后整段执行；先落临时表，再输出失败明细与汇总。
-- =============================================================================

BEGIN;

DROP TABLE IF EXISTS _jd_xiaoshida_subsidy_validate;

CREATE TEMP TABLE _jd_xiaoshida_subsidy_validate AS
WITH params AS (
  SELECT DATE '2026-03-25' AS d
),

hour_bills AS (
  SELECT DISTINCT NULLIF(BTRIM(REPLACE(o."订单编号", E'\t', '')), '') AS bill_no
  FROM ods_rpa_jd_order o
  CROSS JOIN params p
  WHERE o.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND BTRIM(REPLACE(o."订单来源", E'\t', '')) = '小时达'
    AND NULLIF(BTRIM(REPLACE(o."订单编号", E'\t', '')), '') IS NOT NULL
),

activity_subsidy_types AS (
  SELECT unnest(ARRAY[
    '优惠券-平台-小时购',
    '优惠券-京东平台-小时购',
    '京豆-平台-小时达',
    '积分-平台-小时购'
  ])::text AS fee_type
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
)
SELECT * FROM checked;

-- ① 失败明细（无行时说明当日样本全部通过）
SELECT
  dt::text AS 分区dt,
  bill_no AS 到家业务单号,
  fee_type AS 费用类型,
  amt_settle AS 结算金额,
  amt_jd AS 京东承担金额,
  amt_merchant AS 商家承担金额,
  CASE
    WHEN amt_settle IS NULL OR amt_jd IS NULL THEN '结算或京东承担无法解析为数字'
    WHEN ABS(COALESCE(amt_merchant, 0)) > 0.02 THEN '商家承担金额应为0'
    ELSE '结算金额与京东承担金额不一致'
  END AS 原因简述
FROM _jd_xiaoshida_subsidy_validate
WHERE NOT is_ok
ORDER BY bill_no, fee_type;

-- ② 汇总（应检行数、通过、失败）
SELECT
  dt::text AS 分区dt,
  COUNT(*) AS 活动补贴行数,
  COUNT(*) FILTER (WHERE is_ok) AS 通过,
  COUNT(*) FILTER (WHERE NOT is_ok) AS 失败或解析异常,
  ROUND(COALESCE(SUM(s), 0), 2) AS 结算金额合计,
  ROUND(COALESCE(SUM(j), 0), 2) AS 京东承担合计
FROM _jd_xiaoshida_subsidy_validate
GROUP BY dt;

COMMIT;
