-- =============================================================================
-- 饿了么：订单明细 → 财务销售账单 关联后结算对比（仅 ods_rpa_*）
-- =============================================================================
-- 数据源：同一业务日分区 dt=D 的 RPA「订单明细」+ 财务窗 dt>=D-120 的「销售账单明细」即可做分析。
--
-- 主对比（规则口径，与经营对账一致）：
--   **订单侧**：非取消且非全退 → 导出 **商户应收金额**（解析后）。
--   **`已取消` 或 `全部退款`**：导出 **> 0** → 对账 **按 0**（异常正数抹掉）；导出 **< 0** → **保留负值**（成本支出/轧差净应付）；导出 NULL/0 → **0**。
--   **财务侧**：同一 **订单号** 在窗内对 **订单应收** 做 **SUM**（多行轧差如 36.99+(−41.29)）。
--   禁止只用 DISTINCT ON 取一行。
-- 诊断：另输出导出 **原始** 商户应收合计（含全退非零），与规则口径并列，便于排查导出脏数据。
--
-- 逻辑 A（财务取数逻辑 / 结算口径）：**SUM(订单应收)** GROUP BY 订单号（窗 dt>=D-120）
-- 逻辑 B（饿了么结算还原公式，11项全加，DB值已带正负号）：
--   商品金额(col_17) + 应收运费(物流净营业额) + 实收佣金(col_55) + 实收支付服务费(col_59)
--   + 活动补贴(col_20) + 代金券补贴(col_21) + 商家配送费补贴总额(col_28)
--   + 履约技术服务费(平台履约技术服务费) + 物流配送费(物流网络配送费)
--   + 骑手小费(其他支出) + 优惠合计(其他支出)
-- 等价于：净营业额合计 + 服务费用合计（行级恒等，2026-03-28 验证）
--
-- 完整「结算金额列」口径（与平台 SUM(结算金额) 一致）：
--   订单类型 **含「赔偿」字样** 的行：`订单应收` 恒为 0，**仅 `结算金额` 有值**；
--   其余行：`结算金额` = `订单应收`（与 11 项公式还原一致）。
--   故：**按单汇总** `SUM(结算金额) = SUM(订单应收) + SUM(结算金额 WHERE 订单类型 LIKE '%赔偿%')`。
--   例：2026-03-25 匹配单合计 26420.01 = 26396.36 + 23.65（2 笔赔偿单 8.78+14.87）。
--
-- 参考（非主结算）：财务「商家收入_净营业额合计_净营业额合计」
-- 订单导出「商户应收金额」应对齐逻辑 A（订单应收），非净营业额
-- 字段对照：docs/饿了么结算字段对照.md
--
-- 未关联到财务账单的订单：pending_order_cnt，不参与任何金额汇总
--
-- 修改业务日：改 params
-- =============================================================================

WITH params AS (
  SELECT DATE '{{ var("report_date", "2026-03-25") }}' AS d,
         DATE '{{ var("report_date", "2026-03-25") }}'::date + 1 AS d_end
),

/* 订单明细锚点（同日分区 + 业务日下单窗；含取消/全退单，便于与财务交叉） */
ele_order_anchor AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), '') AS order_no,
    (
      NULLIF(TRIM(REPLACE(e."订单状态", E'\t', '')), '') = '已取消'
      OR NULLIF(TRIM(REPLACE(e."退款状态", E'\t', '')), '') = '全部退款'
    ) AS is_cancel_or_full_refund,
    (CASE WHEN BTRIM(REPLACE(e."商户应收金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
          THEN BTRIM(REPLACE(e."商户应收金额", E'\t', ''))::numeric
          ELSE NULL END) AS order_merchant_receivable_raw
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

/* 财务侧：仅用于判断「有无账单」的订单号集合 */
ele_fin_order_exists AS (
  SELECT DISTINCT NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '') AS order_no
  FROM ods_rpa_eleme_fin_sales_detail f
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '') IS NOT NULL
),

matched_order AS (
  SELECT
    o.order_no,
    o.is_cancel_or_full_refund,
    o.order_merchant_receivable_raw
  FROM ele_order_anchor o
  INNER JOIN ele_fin_order_exists x ON x.order_no = o.order_no
),

pending_order AS (
  SELECT o.order_no
  FROM ele_order_anchor o
  LEFT JOIN ele_fin_order_exists x ON x.order_no = o.order_no
  WHERE x.order_no IS NULL
),

/* 已匹配单：财务窗内按订单号汇总（同一单多行金额相加，与平台导出一致） */
ele_fin_pick AS (
  SELECT
    NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '') AS order_no,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_净营业额合计", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_净营业额合计", E'\t', ''))::numeric
           ELSE 0::numeric END
    ) AS fin_jingyingye,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."结算金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."结算金额", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS fin_jiesuan_col11,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."结算金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           AND BTRIM(REPLACE(f."订单类型", E'\t', '')) LIKE '%赔偿%'
           THEN BTRIM(REPLACE(f."结算金额", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS fin_peichang_jiesuan_col11,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_1", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_1", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS spje,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."订单应收", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."订单应收", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS ddys,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额", E'\t', ''))::numeric
           ELSE 0::numeric END
    ) AS yingshou_yunfei,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费_1", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费_1", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS shishou_yongjin,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费_5", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费_5", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS shishou_zhifu,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_4", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_4", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS huodong_butie,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_5", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_5", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS daijinquan_butie,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额_1", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额_1", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS sj_psf_butie,  -- 商家配送费补贴总额（净营业额子项）
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台履约技术服务费", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台履约技术服务费", E'\t', ''))::numeric
           ELSE 0::numeric END
    ) AS lvye_heji,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_物流网络配送费", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家支出_服务费用合计_物流网络配送费", E'\t', ''))::numeric
           ELSE 0::numeric END
    ) AS wuliu_heji,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."其他支出", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."其他支出", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS qita_zhichu  -- 其他支出（骑手小费+优惠合计，重灌后合并为单列）
  FROM ods_rpa_eleme_fin_sales_detail f
  INNER JOIN matched_order k
    ON k.order_no = NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '')
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
  GROUP BY 1
),

joined AS (
  SELECT
    m.order_no,
    m.is_cancel_or_full_refund,
    m.order_merchant_receivable_raw,
    (
      CASE
        WHEN NOT m.is_cancel_or_full_refund THEN m.order_merchant_receivable_raw
        WHEN COALESCE(m.order_merchant_receivable_raw, 0) > 0 THEN 0::numeric
        WHEN COALESCE(m.order_merchant_receivable_raw, 0) < 0 THEN m.order_merchant_receivable_raw
        ELSE 0::numeric
      END
    ) AS order_settlement_amt,
    COALESCE(p.ddys, 0) AS fin_order_receivable_amt,
    p.fin_jingyingye,
    p.fin_jiesuan_col11,
    p.fin_peichang_jiesuan_col11,
    p.spje,
    p.ddys,
    p.yingshou_yunfei,
    p.shishou_yongjin,
    p.shishou_zhifu,
    p.huodong_butie,
    p.daijinquan_butie,
    p.sj_psf_butie,
    p.lvye_heji,
    p.wuliu_heji,
    p.qita_zhichu,
    (
      COALESCE(p.spje, 0) + COALESCE(p.yingshou_yunfei, 0)
      + COALESCE(p.shishou_yongjin, 0)
      + COALESCE(p.shishou_zhifu, 0)
      + COALESCE(p.huodong_butie, 0)
      + COALESCE(p.daijinquan_butie, 0)
      + COALESCE(p.sj_psf_butie, 0)
      + COALESCE(p.lvye_heji, 0)
      + COALESCE(p.wuliu_heji, 0)
      + COALESCE(p.qita_zhichu, 0)
    ) AS formula_rebuilt
  FROM matched_order m
  INNER JOIN ele_fin_pick p ON p.order_no = m.order_no
),

agg AS (
  SELECT
    COUNT(*)::bigint AS matched_cnt,
    (SELECT COUNT(*) FROM pending_order)::bigint AS pending_cnt,
    (SELECT COUNT(*) FROM ele_order_anchor)::bigint AS anchor_order_cnt,
    (SELECT COUNT(*) FILTER (WHERE is_cancel_or_full_refund) FROM ele_order_anchor)::bigint AS anchor_cancel_or_full_refund_cnt,
    COALESCE(SUM(COALESCE(order_settlement_amt, 0)), 0) AS sum_订单明细_规则口径_全退正0负保留,
    COALESCE(SUM(COALESCE(order_merchant_receivable_raw, 0)), 0) AS sum_订单明细_导出原始_含全退列值,
    COALESCE(SUM(fin_order_receivable_amt), 0) AS sum_财务_订单应收_按订单汇总,
    COALESCE(SUM(fin_jingyingye), 0) AS sum_ref_净营业额合计_非结算,
    COALESCE(SUM(fin_jiesuan_col11), 0) AS sum_ref_财务结算金额列,
    COALESCE(SUM(fin_peichang_jiesuan_col11), 0) AS sum_财务_赔偿单_结算金额col11,
    COALESCE(SUM(fin_order_receivable_amt + COALESCE(fin_peichang_jiesuan_col11, 0)), 0)
      AS sum_订单应收_加_赔偿结算_应等于结算列,
    COALESCE(SUM(fin_order_receivable_amt + COALESCE(fin_peichang_jiesuan_col11, 0)), 0)
      - COALESCE(SUM(fin_jiesuan_col11), 0) AS diff_应收加赔偿减结算列,
    COALESCE(SUM(formula_rebuilt), 0) AS sum_logic_b_公式还原,
    COALESCE(SUM(fin_order_receivable_amt), 0) - COALESCE(SUM(formula_rebuilt), 0) AS diff_a_minus_b,
    COALESCE(SUM(fin_order_receivable_amt), 0) - COALESCE(SUM(fin_jiesuan_col11), 0) AS diff_a_minus_结算列,
    COALESCE(SUM(formula_rebuilt), 0) - COALESCE(SUM(fin_jiesuan_col11), 0) AS diff_b_minus_结算列,
    COUNT(*) FILTER (WHERE ABS(COALESCE(formula_rebuilt, 0) - COALESCE(fin_jiesuan_col11, 0)) > 0.02)
      AS cnt_orders_公式ne结算列,
    COUNT(*) FILTER (WHERE ABS(COALESCE(fin_order_receivable_amt, 0) - COALESCE(fin_jiesuan_col11, 0)) > 0.02)
      AS cnt_orders_a_ne_结算列,
    /* 主对齐：规则口径（全退/取消：正→0、负保留）vs 财务 SUM(订单应收) */
    COUNT(*) FILTER (WHERE ABS(COALESCE(order_settlement_amt, 0) - COALESCE(fin_order_receivable_amt, 0)) > 0.02)
      AS cnt_orders_主对比_规则口径_ne_财务汇总,
    COALESCE(SUM(COALESCE(order_settlement_amt, 0)), 0) - COALESCE(SUM(fin_order_receivable_amt), 0)
      AS diff_sum_规则口径减财务汇总,
    /* 诊断：导出原始列 vs 财务（不受全退按 0 规则） */
    COUNT(*) FILTER (WHERE ABS(COALESCE(order_merchant_receivable_raw, 0) - COALESCE(fin_order_receivable_amt, 0)) > 0.02)
      AS cnt_orders_诊断_导出原始_ne_财务汇总,
    /* 取消/全退：规则口径 vs 财务汇总仍不一致 */
    COUNT(*) FILTER (
      WHERE is_cancel_or_full_refund
        AND ABS(COALESCE(order_settlement_amt, 0) - COALESCE(fin_order_receivable_amt, 0)) > 0.02
    ) AS cnt_取消或全退_规则与财务仍不一致
  FROM joined
)

SELECT
  p.d AS report_date,
  a.anchor_order_cnt,
  a.anchor_cancel_or_full_refund_cnt,
  a.pending_cnt AS pending_fin_not_matched_cnt,
  a.matched_cnt AS matched_fin_cnt,
  a.sum_订单明细_规则口径_全退正0负保留,
  a.sum_订单明细_导出原始_含全退列值,
  a.sum_财务_订单应收_按订单汇总,
  a.sum_ref_净营业额合计_非结算 AS sum_参考_净营业额合计_非结算口径,
  a.sum_ref_财务结算金额列 AS sum_参考_财务结算金额col11,
  a.sum_财务_赔偿单_结算金额col11,
  a.sum_订单应收_加_赔偿结算_应等于结算列,
  a.diff_应收加赔偿减结算列,
  a.diff_sum_规则口径减财务汇总 AS diff_合计_规则口径减财务汇总,
  a.cnt_orders_主对比_规则口径_ne_财务汇总,
  a.cnt_orders_诊断_导出原始_ne_财务汇总,
  a.cnt_取消或全退_规则与财务仍不一致,
  a.sum_logic_b_公式还原 AS sum_逻辑B_11项公式还原,
  a.diff_a_minus_b AS diff_逻辑A减逻辑B,
  a.diff_a_minus_结算列 AS diff_订单应收减结算列col11,
  a.diff_b_minus_结算列 AS diff_公式还原减结算列col11,
  a.cnt_orders_公式ne结算列,
  a.cnt_orders_a_ne_结算列
FROM params p
CROSS JOIN agg a;

-- -----------------------------------------------------------------------------
-- 行级差异样例（按需取消注释执行）：
--
-- SELECT j.order_no,
--        j.is_cancel_or_full_refund,
--        j.order_settlement_amt AS 规则口径_全退取消为0,
--        j.order_merchant_receivable_raw,
--        j.fin_order_receivable_amt AS 财务_订单应收,
--        j.fin_jiesuan_col11 AS 结算列,
--        j.formula_rebuilt AS 公式还原,
--        j.formula_rebuilt - j.fin_jiesuan_col11 AS diff_公式vs结算
-- FROM joined j
-- ORDER BY ABS(COALESCE(j.formula_rebuilt,0) - COALESCE(j.fin_jiesuan_col11,0)) DESC
-- LIMIT 50;
-- -----------------------------------------------------------------------------
-- 按订单类型诊断（独立文件，与上文同 params.d）：
--   sql/o2o_eleme_fin_by_order_type_diag.sql
--   → 按 订单类型 汇总 sum(结算金额) − sum(订单应收)，排查除赔偿外是否有新类型轧差。
-- -----------------------------------------------------------------------------
