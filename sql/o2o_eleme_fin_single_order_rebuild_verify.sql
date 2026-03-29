-- =============================================================================
-- 饿了么：单笔订单 — 分项还原结算金额 + 行级展开
-- =============================================================================
-- 已验证恒等式（2026-03-28）：
--   结算金额 = 商品金额 + 应收运费 + 实收佣金 + 实收支付服务费
--            + 活动补贴 + 代金券补贴 + 商家配送费补贴总额
--            + 履约技术服务费合计 + 物流配送费合计 + 骑手消费 + 优惠金额合计
--
-- 所有项直接相加（DB值已带正负号，支出为负、收入为正，无需手动翻转）。
--
-- 列映射（公式表头 → 入仓列名，2026-03-28 最终验证版）：
--   商品金额             → 商家收入_净营业额合计_col_17
--   应收运费             → 商家收入_净营业额合计_物流净营业额
--   实收佣金             → 商家支出_服务费用合计_col_55（非 平台交易技术服务费，后者是汇总）
--   实收支付服务费         → 商家支出_服务费用合计_col_59（非 col_60，后者是计费基数）
--   活动补贴             → 商家收入_净营业额合计_col_20（净营业额子项，非资金来源维度）
--   代金券补贴            → 商家收入_净营业额合计_col_21（净营业额子项，非资金来源维度）
--   商家配送费补贴总额      → 商家收入_净营业额合计_col_28（净营业额子项）
--   履约技术服务费合计      → 商家支出_服务费用合计_平台履约技术服务费
--   物流配送费合计         → 商家支出_服务费用合计_物流网络配送费
--   骑手小费             → 其他支出_col_83（非 服务费用合计_col_74，后者120天全0）
--   优惠金额合计          → 其他支出_col_81（非 服务费用合计_col_75，后者120天全0）
--
-- 易错提醒：
--   col_56 / col_60 两行同号（=用户支付金额），是佣金/支付费率的计费基数，不参与加总
--   col_57 = 佣金费率, col_61 = 支付费率，仅供验算
--   平台交易技术服务费 = 实收佣金 + 实收支付服务费 的汇总，与子项不能同时入公式
--
-- 等价验证：结算金额 = 净营业额合计 + 服务费用合计（行级恒等，已双行验证 ✓）
--
-- 用法：改 params 的 target_order_no；财务窗 dt >= d - 120。
-- =============================================================================

WITH params AS (
  SELECT
    DATE '2026-03-25' AS d,
    '4014576145498364475'::text AS target_order_no
),

lines AS (
  SELECT
    ROW_NUMBER() OVER (
      ORDER BY NULLIF(TRIM(REPLACE(f."账单日期_col_3", E'\t', '')), '')::date NULLS LAST,
        f.dt, f.ctid
    ) AS line_no,
    f.dt::text AS partition_dt,
    NULLIF(TRIM(REPLACE(f."账单日期_col_3", E'\t', '')), '') AS bill_date,
    NULLIF(TRIM(REPLACE(f."订单号_col_6", E'\t', '')), '') AS order_no,
    -- 11 项公式字段
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_17", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_17", E'\t', ''))::numeric END) AS 商品金额,
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额", E'\t', ''))::numeric END) AS 应收运费,
    (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_55", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_55", E'\t', ''))::numeric END) AS 实收佣金,
    (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_59", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_59", E'\t', ''))::numeric END) AS 实收支付服务费,
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_20", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_20", E'\t', ''))::numeric END) AS 活动补贴,
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_21", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_21", E'\t', ''))::numeric END) AS 代金券补贴,
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_28", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_28", E'\t', ''))::numeric END) AS 商家配送费补贴总额,
    (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台履约技术服务费", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台履约技术服务费", E'\t', ''))::numeric END) AS 履约技术服务费合计,
    (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_物流网络配送费", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_物流网络配送费", E'\t', ''))::numeric END) AS 物流配送费合计,
    (CASE WHEN BTRIM(REPLACE(f."其他支出_col_83", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."其他支出_col_83", E'\t', ''))::numeric END) AS 骑手小费,
    (CASE WHEN BTRIM(REPLACE(f."其他支出_col_81", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."其他支出_col_81", E'\t', ''))::numeric END) AS 优惠金额合计,
    -- 对照列
    (CASE WHEN BTRIM(REPLACE(f."结算金额_col_11", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."结算金额_col_11", E'\t', ''))::numeric END) AS 结算金额,
    (CASE WHEN BTRIM(REPLACE(f."订单应收_col_13", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."订单应收_col_13", E'\t', ''))::numeric END) AS 订单应收,
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_净营业额合计", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_净营业额合计", E'\t', ''))::numeric END) AS 净营业额合计,
    (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_服务费用合计", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_服务费用合计", E'\t', ''))::numeric END) AS 服务费用合计
  FROM ods_rpa_eleme_fin_sales_detail f
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(f."订单号_col_6", E'\t', '')), '') = p.target_order_no
),

with_formula AS (
  SELECT
    l.*,
    (
      COALESCE(l.商品金额, 0) + COALESCE(l.应收运费, 0)
      + COALESCE(l.实收佣金, 0) + COALESCE(l.实收支付服务费, 0)
      + COALESCE(l.活动补贴, 0) + COALESCE(l.代金券补贴, 0)
      + COALESCE(l.商家配送费补贴总额, 0)
      + COALESCE(l.履约技术服务费合计, 0) + COALESCE(l.物流配送费合计, 0)
      + COALESCE(l.骑手小费, 0) + COALESCE(l.优惠金额合计, 0)
    ) AS 公式还原_结算金额,
    COALESCE(l.净营业额合计, 0) + COALESCE(l.服务费用合计, 0) AS 等价验证_净营业额加服务费
  FROM lines l
)

/* ① 行级：11项分项 + 还原结果 vs 实际结算金额 */
SELECT
  line_no, partition_dt, bill_date, order_no,
  商品金额, 应收运费, 实收佣金, 实收支付服务费,
  活动补贴, 代金券补贴, 商家配送费补贴总额,
  履约技术服务费合计, 物流配送费合计, 骑手小费, 优惠金额合计,
  公式还原_结算金额,
  结算金额 AS 实际结算金额,
  公式还原_结算金额 - COALESCE(结算金额, 0) AS 差异,
  等价验证_净营业额加服务费,
  订单应收 AS 对照_订单应收,
  净营业额合计, 服务费用合计
FROM with_formula
ORDER BY line_no;

/* ② 整单汇总 */
WITH params AS (
  SELECT DATE '2026-03-25' AS d, '4014576145498364475'::text AS target_order_no
),
lines AS (
  SELECT
    (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_17", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_17", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_55", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_55", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_59", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_col_59", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_20", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_20", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_21", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_21", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_28", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家收入_净营业额合计_col_28", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台履约技术服务费", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台履约技术服务费", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_物流网络配送费", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."商家支出_服务费用合计_物流网络配送费", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."其他支出_col_83", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."其他支出_col_83", E'\t', ''))::numeric ELSE 0 END)
    + (CASE WHEN BTRIM(REPLACE(f."其他支出_col_81", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."其他支出_col_81", E'\t', ''))::numeric ELSE 0 END)
    AS formula_rebuilt,
    (CASE WHEN BTRIM(REPLACE(f."订单应收_col_13", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."订单应收_col_13", E'\t', ''))::numeric ELSE 0 END) AS ddys,
    (CASE WHEN BTRIM(REPLACE(f."结算金额_col_11", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(f."结算金额_col_11", E'\t', ''))::numeric ELSE 0 END) AS jiesuan
  FROM ods_rpa_eleme_fin_sales_detail f
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(f."订单号_col_6", E'\t', '')), '') = p.target_order_no
)
SELECT
  (SELECT target_order_no FROM params) AS order_no,
  SUM(formula_rebuilt) AS sum_公式还原,
  SUM(jiesuan) AS sum_结算金额,
  SUM(ddys) AS sum_订单应收,
  SUM(formula_rebuilt) - SUM(jiesuan) AS diff_公式减结算,
  SUM(formula_rebuilt) - SUM(ddys) AS diff_公式减订单应收
FROM lines;
