-- =============================================================================
-- 饿了么：按 订单类型_col_9 诊断 — SUM(结算金额_col_11) vs SUM(订单应收_col_13)
-- =============================================================================
-- 用途：发现除「赔偿」外，是否还有其它订单类型在**行级**存在「结算列 ≠ 订单应收」的轧差。
-- 范围：与 o2o_eleme_order_fin_settlement_compare.sql **同一 matched 定义**
--       （业务日 D 订单锚点 ∩ 财务窗 dt>=D-120 有账单）。
-- 解读：
--   - diff_结算减应收 ≈ 0：该类型行上两列一致（与 11 项公式 / 订单应收口径一致）。
--   - diff 显著非 0：需单独核对是否还有「订单应收为 0、仅结算列有值」等新类型（类似赔偿单）。
-- 修改业务日：改 params
-- =============================================================================

WITH params AS (
  SELECT DATE '2026-03-25' AS d, DATE '2026-03-26' AS d_end
),

ele_order_anchor AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), '') AS order_no
  FROM ods_rpa_eleme_order e
  CROSS JOIN params p
  WHERE e.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND NULLIF(TRIM(REPLACE(e."下单时间", E'\t', '')), '') IS NOT NULL
    AND (
      TRIM(REPLACE(e."下单时间", E'\t', '')) LIKE
        to_char(p.d, 'YYYY') || '年' || to_char(p.d, 'MM') || '月' || to_char(p.d, 'DD') || '日%'
      OR (
        TRIM(REPLACE(e."下单时间", E'\t', '')) ~ '^\d{4}-\d{2}-\d{2}'
        AND to_timestamp(TRIM(REPLACE(e."下单时间", E'\t', '')), 'YYYY-MM-DD HH24:MI:SS') >= p.d::timestamptz
        AND to_timestamp(TRIM(REPLACE(e."下单时间", E'\t', '')), 'YYYY-MM-DD HH24:MI:SS') < p.d_end::timestamptz
      )
    )
  ORDER BY NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), ''), e."订单序号"
),

ele_fin_order_exists AS (
  SELECT DISTINCT NULLIF(TRIM(REPLACE(f."订单号_col_6", E'\t', '')), '') AS order_no
  FROM ods_rpa_eleme_fin_sales_detail f
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(f."订单号_col_6", E'\t', '')), '') IS NOT NULL
),

matched_order AS (
  SELECT o.order_no
  FROM ele_order_anchor o
  INNER JOIN ele_fin_order_exists x ON x.order_no = o.order_no
),

by_type AS (
  SELECT
    COALESCE(NULLIF(BTRIM(REPLACE(f."订单类型_col_9", E'\t', '')), ''), '(空)') AS 订单类型_col_9,
    BOOL_OR(BTRIM(REPLACE(f."订单类型_col_9", E'\t', '')) LIKE '%赔偿%') AS 类型含赔偿字样,
    COUNT(*)::bigint AS 行数,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."结算金额_col_11", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."结算金额_col_11", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS sum_结算金额_col11,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."订单应收_col_13", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."订单应收_col_13", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS sum_订单应收_col13
  FROM ods_rpa_eleme_fin_sales_detail f
  CROSS JOIN params p
  INNER JOIN matched_order m
    ON m.order_no = NULLIF(TRIM(REPLACE(f."订单号_col_6", E'\t', '')), '')
  WHERE f.dt::date >= p.d - 120
  GROUP BY 1
)

SELECT
  订单类型_col_9,
  类型含赔偿字样,
  行数,
  sum_结算金额_col11,
  sum_订单应收_col13,
  sum_结算金额_col11 - sum_订单应收_col13 AS diff_结算减应收
FROM by_type
ORDER BY ABS(sum_结算金额_col11 - sum_订单应收_col13) DESC NULLS LAST,
  行数 DESC;
