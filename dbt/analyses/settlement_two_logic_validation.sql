-- =============================================================================
-- O2O 结算双逻辑校验（仅 ods_rpa_*，与 o2o_daily_store_aggregate 同锚点+财务窗）
-- =============================================================================
-- 逻辑一（现行聚合）：美团=财务「商家应收款」；饿了么=财务「订单应收」
-- 逻辑二（分项重算）：
--   美团 ≈ union-knowledgebase/数据字典/O2O数据源字段清单.md 「美团结算公式」
--   饿了么 ≈ 你提供的分项式 + 同文档「饿了么财务单」列映射（列名以当前 PG 为准）
--   说明：导出里费用类多为负数表示扣款；你写的「减实收佣金」在 Excel 里若显示为正数，
--   则等价于在 PG 中对「佣金」列做**加法**（把已带符号的数并入总和）。本 SQL 按后者计算。
--   饿了么列中偶发「计费基数」等非数字占位，用正则过滤后再转 numeric。
-- 未匹配财务的订单：计入 pending_*，不参与两逻辑金额合计
--
-- 修改业务日：改 params 中 d / d_end
-- =============================================================================

WITH params AS (
  SELECT DATE '{{ var("report_date", "2026-03-25") }}' AS d,
         DATE '{{ var("report_date", "2026-03-25") }}'::date + 1 AS d_end
),

/* ---------- 美团：财务窗 + 商品明细锚点 ---------- */
mt_fin_global AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), '') AS order_no
  FROM ods_rpa_meituan_fin_order m
  CROSS JOIN params p
  WHERE m.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(m."基础信息_订单状态", E'\t', '')), '') IS DISTINCT FROM '订单取消'
  ORDER BY NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), ''),
    NULLIF(TRIM(REPLACE(m."基础信息_账单日期", E'\t', '')), '') DESC NULLS LAST
),

mt_anchor AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(pd."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(pd."订单编号", E'\t', '')), '') AS order_no
  FROM ods_rpa_meituan_product_detail pd
  CROSS JOIN params p
  WHERE NULLIF(TRIM(REPLACE(pd."订单状态", E'\t', '')), '') IS DISTINCT FROM '已取消'
    AND NULLIF(TRIM(REPLACE(pd."下单时间", E'\t', '')), '') IS NOT NULL
    AND TRIM(REPLACE(pd."下单时间", E'\t', '')) >= to_char(p.d, 'YYYY-MM-DD')
    AND TRIM(REPLACE(pd."下单时间", E'\t', '')) < to_char(p.d_end, 'YYYY-MM-DD')
  ORDER BY NULLIF(TRIM(REPLACE(pd."订单编号", E'\t', '')), ''),
    NULLIF(TRIM(REPLACE(pd."订单完成时间", E'\t', '')), '') DESC NULLS LAST
),

mt_matched_order AS (
  SELECT a.order_no FROM mt_anchor a
  INNER JOIN mt_fin_global f ON f.order_no = a.order_no
),

mt_pending_order AS (
  SELECT a.order_no FROM mt_anchor a
  LEFT JOIN mt_fin_global f ON f.order_no = a.order_no
  WHERE f.order_no IS NULL
),

/* 每个匹配单取最新账单行，带齐字段做分项 */
mt_fin_pick AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), '') AS order_no,
    NULLIF(TRIM(REPLACE(m."商家应收款", E'\t', '')), '')::numeric AS shoukuan,
    NULLIF(TRIM(REPLACE(m."商家收入_商品总价", E'\t', '')), '')::numeric AS spzj,
    NULLIF(TRIM(REPLACE(m."商家收入_用户支付配送费", E'\t', '')), '')::numeric AS yhpsf,
    NULLIF(TRIM(REPLACE(m."商家收入_餐盒费", E'\t', '')), '')::numeric AS chf,
    NULLIF(TRIM(REPLACE(m."商家收入_打包袋", E'\t', '')), '')::numeric AS dbd,
    NULLIF(TRIM(REPLACE(m."平台活动支出_平台承担_美团商品补贴", E'\t', '')), '')::numeric AS mt_spbt,
    NULLIF(TRIM(REPLACE(m."平台活动支出_平台承担_美团配送费补贴", E'\t', '')), '')::numeric AS mt_psfbt,
    NULLIF(TRIM(REPLACE(m."平台活动支出_平台承担_美团打包袋补贴", E'\t', '')), '')::numeric AS mt_dbdbt,
    NULLIF(TRIM(REPLACE(m."商家活动支出_商家承担_商品补贴", E'\t', '')), '')::numeric AS sj_spbt,
    NULLIF(TRIM(REPLACE(m."商家活动支出_商家承担_配送费补贴", E'\t', '')), '')::numeric AS sj_psfbt,
    NULLIF(TRIM(REPLACE(m."商家活动支出_商家承担_打包袋补贴", E'\t', '')), '')::numeric AS sj_dbdbt,
    NULLIF(TRIM(REPLACE(m."佣金", E'\t', '')), '')::numeric AS yongjin,
    NULLIF(TRIM(REPLACE(m."配送服务费", E'\t', '')), '')::numeric AS psffwf,
    NULLIF(NULLIF(TRIM(REPLACE(m."商品券业务_商家代金券美团补贴", E'\t', '')), ''), '-')::numeric AS quan_mt,
    NULLIF(NULLIF(TRIM(REPLACE(m."商品券业务_商家代金券商家补贴", E'\t', '')), ''), '-')::numeric AS quan_sj,
    NULLIF(NULLIF(TRIM(REPLACE(m."商品券业务_商家代金券用户支付", E'\t', '')), ''), '-')::numeric AS quan_yh
  FROM ods_rpa_meituan_fin_order m
  INNER JOIN mt_matched_order k
    ON k.order_no = NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), '')
  CROSS JOIN params p
  WHERE m.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(m."基础信息_订单状态", E'\t', '')), '') IS DISTINCT FROM '订单取消'
  ORDER BY NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), ''),
    NULLIF(TRIM(REPLACE(m."基础信息_账单日期", E'\t', '')), '') DESC NULLS LAST
),

mt_agg AS (
  SELECT
    COUNT(*)::bigint AS matched_cnt,
    COALESCE(SUM(shoukuan), 0) AS logic1_sum_shoukuan,
    COALESCE(SUM(
      COALESCE(spzj, 0) + COALESCE(yhpsf, 0) + COALESCE(chf, 0) + COALESCE(dbd, 0)
      + COALESCE(mt_spbt, 0) + COALESCE(mt_psfbt, 0) + COALESCE(mt_dbdbt, 0)
      + COALESCE(sj_spbt, 0) + COALESCE(sj_psfbt, 0) + COALESCE(sj_dbdbt, 0)
      + COALESCE(yongjin, 0) + COALESCE(psffwf, 0)
      + COALESCE(quan_mt, 0) + COALESCE(quan_sj, 0) + COALESCE(quan_yh, 0)
    ), 0) AS logic2_sum_formula_kb,
    COALESCE(SUM(ABS(
      COALESCE(shoukuan, 0) - (
        COALESCE(spzj, 0) + COALESCE(yhpsf, 0) + COALESCE(chf, 0) + COALESCE(dbd, 0)
        + COALESCE(mt_spbt, 0) + COALESCE(mt_psfbt, 0) + COALESCE(mt_dbdbt, 0)
        + COALESCE(sj_spbt, 0) + COALESCE(sj_psfbt, 0) + COALESCE(sj_dbdbt, 0)
        + COALESCE(yongjin, 0) + COALESCE(psffwf, 0)
        + COALESCE(quan_mt, 0) + COALESCE(quan_sj, 0) + COALESCE(quan_yh, 0)
      )
    )), 0) AS sum_abs_line_diff,
    COUNT(*) FILTER (WHERE ABS(
      COALESCE(shoukuan, 0) - (
        COALESCE(spzj, 0) + COALESCE(yhpsf, 0) + COALESCE(chf, 0) + COALESCE(dbd, 0)
        + COALESCE(mt_spbt, 0) + COALESCE(mt_psfbt, 0) + COALESCE(mt_dbdbt, 0)
        + COALESCE(sj_spbt, 0) + COALESCE(sj_psfbt, 0) + COALESCE(sj_dbdbt, 0)
        + COALESCE(yongjin, 0) + COALESCE(psffwf, 0)
        + COALESCE(quan_mt, 0) + COALESCE(quan_sj, 0) + COALESCE(quan_yh, 0)
      )
    ) > 0.02) AS cnt_line_mismatch_gt_2fen
  FROM mt_fin_pick
),

/* ---------- 饿了么：财务窗 + 订单导出锚点 ---------- */
ele_fin_global AS (
  SELECT DISTINCT NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '') AS order_no
  FROM ods_rpa_eleme_fin_sales_detail f
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
    AND NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '') IS NOT NULL
),

ele_anchor AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), '') AS order_no
  FROM ods_rpa_eleme_order e
  CROSS JOIN params p
  WHERE e.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND NOT (
      NULLIF(TRIM(REPLACE(e."订单状态", E'\t', '')), '') = '已取消'
      OR NULLIF(TRIM(REPLACE(e."退款状态", E'\t', '')), '') = '全部退款'
    )
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

ele_matched_order AS (
  SELECT a.order_no FROM ele_anchor a
  INNER JOIN ele_fin_global g ON g.order_no = a.order_no
),

ele_pending_order AS (
  SELECT a.order_no FROM ele_anchor a
  LEFT JOIN ele_fin_global g ON g.order_no = a.order_no
  WHERE g.order_no IS NULL
),

ele_fin_pick AS (
  SELECT
    NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '') AS order_no,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_净营业额合计", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_净营业额合计", E'\t', ''))::numeric
           ELSE 0::numeric END
    ) AS jing,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."结算金额", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."结算金额", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS jiesuan_col,
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
      CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费", E'\t', ''))::numeric
           ELSE 0::numeric END
    ) AS shishou_yongjin,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费_5", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家支出_服务费用合计_平台交易技术服务费_5", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS shishou_zhifu_fuwufei,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入资金来源_其中：平台承担", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入资金来源_其中：平台承担", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS huodong_butie,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_5", E'\t', '')) ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_商品净营业额_5", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS daijinquan_butie,
    SUM(
      CASE WHEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额_1", E'\t', ''))
                ~ '^-?[0-9]+(\.[0-9]*)?$'
           THEN BTRIM(REPLACE(f."商家收入_净营业额合计_物流净营业额_1", E'\t', ''))::numeric ELSE 0::numeric END
    ) AS sj_psf_butie,
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
    ) AS qita_zhichu  -- 重灌后骑手小费+优惠金额合计合并为单列
  FROM ods_rpa_eleme_fin_sales_detail f
  INNER JOIN ele_matched_order k
    ON k.order_no = NULLIF(TRIM(REPLACE(f."订单号", E'\t', '')), '')
  CROSS JOIN params p
  WHERE f.dt::date >= p.d - 120
  GROUP BY 1
),

/* ---------- 京东：仅校验「已匹配」结算金额合计（SKU 对账单无你给出的同一套分项式） ---------- */
jd_fin_global AS (
  SELECT
    NULLIF(TRIM(REPLACE(jf."到家业务单号", E'\t', '')), '') AS order_no,
    SUM(NULLIF(TRIM(REPLACE(jf."结算金额", E'\t', '')), '')::numeric) AS settlement_amt
  FROM ods_rpa_jd_finance jf
  CROSS JOIN params p
  WHERE jf.dt::date >= p.d - 120
  GROUP BY 1
  HAVING NULLIF(TRIM(REPLACE(jf."到家业务单号", E'\t', '')), '') IS NOT NULL
),

jd_orders_base AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(jo."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(jo."订单编号", E'\t', '')), '') AS order_no,
    NULLIF(TRIM(REPLACE(jo."订单状态", E'\t', '')), '') AS order_status,
    COALESCE(
      NULLIF(TRIM(REPLACE(jo."成交时间", E'\t', '')), ''),
      NULLIF(TRIM(REPLACE(jo."门店收单时间", E'\t', '')), '')
    ) AS place_time_raw
  FROM ods_rpa_jd_order jo
  CROSS JOIN params p
  WHERE jo.dt::text = to_char(p.d, 'YYYY-MM-DD')
  ORDER BY NULLIF(TRIM(REPLACE(jo."订单编号", E'\t', '')), '')
),

jd_anchor AS (
  SELECT b.order_no
  FROM jd_orders_base b
  CROSS JOIN params p
  WHERE b.place_time_raw IS NOT NULL
    AND b.order_status IS DISTINCT FROM '已取消'
    AND to_timestamp(b.place_time_raw, 'YYYY-MM-DD HH24:MI:SS') >= p.d::timestamptz
    AND to_timestamp(b.place_time_raw, 'YYYY-MM-DD HH24:MI:SS') < p.d_end::timestamptz
),

jd_matched_order AS (
  SELECT a.order_no FROM jd_anchor a
  INNER JOIN jd_fin_global g ON g.order_no = a.order_no
),

jd_pending_order AS (
  SELECT a.order_no FROM jd_anchor a
  LEFT JOIN jd_fin_global g ON g.order_no = a.order_no
  WHERE g.order_no IS NULL
),

jd_agg AS (
  SELECT
    COUNT(*)::bigint AS matched_cnt,
    COALESCE(SUM(g.settlement_amt), 0) AS jd_sum_settlement_matched
  FROM jd_matched_order k
  INNER JOIN jd_fin_global g ON g.order_no = k.order_no
),

ele_agg AS (
  SELECT
    COUNT(*)::bigint AS matched_cnt,
    COALESCE(SUM(ddys), 0) AS logic1_sum_dingdan_yingshou,
    COALESCE(SUM(jing), 0) AS ref_sum_jingyingye,
    COALESCE(SUM(jiesuan_col), 0) AS logic_ref_jiesuan_col11,
    /* 分项全部按「表内已带符号」相加（含佣金为负即扣款） */
    COALESCE(SUM(
      COALESCE(spje, 0) + COALESCE(ddys, 0)
      + COALESCE(shishou_yongjin, 0)
      + COALESCE(shishou_zhifu_fuwufei, 0)
      + COALESCE(huodong_butie, 0)
      + COALESCE(daijinquan_butie, 0)
      + COALESCE(sj_psf_butie, 0)
      + COALESCE(lvye_heji, 0)
      + COALESCE(wuliu_heji, 0)
      + COALESCE(qita_zhichu, 0)
    ), 0) AS logic2_user_formula,
    COALESCE(SUM(
      COALESCE(ddys, 0)
      + COALESCE(shishou_yongjin, 0)
      + COALESCE(shishou_zhifu_fuwufei, 0)
      + COALESCE(huodong_butie, 0)
      + COALESCE(daijinquan_butie, 0)
      + COALESCE(sj_psf_butie, 0)
      + COALESCE(lvye_heji, 0)
      + COALESCE(wuliu_heji, 0)
      + COALESCE(qita_zhichu, 0)
    ), 0) AS logic2_user_formula_no_spje,
    COALESCE(SUM(ABS(COALESCE(ddys, 0) - COALESCE(jiesuan_col, 0))), 0) AS sum_abs_ddys_vs_jiesuan,
    COUNT(*) FILTER (WHERE ABS(COALESCE(ddys, 0) - COALESCE(jiesuan_col, 0)) > 0.02) AS cnt_ddys_ne_jiesuan,
    COALESCE(SUM(ABS(COALESCE(jing, 0) - COALESCE(jiesuan_col, 0))), 0) AS sum_abs_jing_vs_jiesuan,
    COUNT(*) FILTER (WHERE ABS(COALESCE(jing, 0) - COALESCE(jiesuan_col, 0)) > 0.02) AS cnt_jing_ne_jiesuan,
    COALESCE(SUM(ABS(
      COALESCE(ddys, 0) - (
        COALESCE(spje, 0) + COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
      )
    )), 0) AS sum_abs_ddys_vs_formula_full,
    COUNT(*) FILTER (WHERE ABS(COALESCE(ddys, 0) - (
        COALESCE(spje, 0) + COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
    )) > 0.02) AS cnt_ddys_ne_formula_full,
    COALESCE(SUM(ABS(
      COALESCE(ddys, 0) - (
        COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
      )
    )), 0) AS sum_abs_ddys_vs_formula_no_spje,
    COUNT(*) FILTER (WHERE ABS(COALESCE(ddys, 0) - (
        COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
    )) > 0.02) AS cnt_ddys_ne_formula_no_spje,
    COALESCE(SUM(ABS(
      COALESCE(jiesuan_col, 0) - (
        COALESCE(spje, 0) + COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
      )
    )), 0) AS sum_abs_user_vs_jiesuan,
    COUNT(*) FILTER (WHERE ABS(COALESCE(jiesuan_col, 0) - (
        COALESCE(spje, 0) + COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
    )) > 0.02) AS cnt_user_ne_jiesuan,
    COALESCE(SUM(ABS(
      COALESCE(jiesuan_col, 0) - (
        COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
      )
    )), 0) AS sum_abs_no_spje_vs_jiesuan,
    COUNT(*) FILTER (WHERE ABS(COALESCE(jiesuan_col, 0) - (
        COALESCE(ddys, 0)
        + COALESCE(shishou_yongjin, 0)
        + COALESCE(shishou_zhifu_fuwufei, 0)
        + COALESCE(huodong_butie, 0)
        + COALESCE(daijinquan_butie, 0)
        + COALESCE(sj_psf_butie, 0)
        + COALESCE(lvye_heji, 0)
        + COALESCE(wuliu_heji, 0)
        + COALESCE(qita_zhichu, 0)
    )) > 0.02) AS cnt_no_spje_ne_jiesuan
  FROM ele_fin_pick
)

SELECT
  p.d AS report_date,

  (SELECT COUNT(*) FROM mt_pending_order) AS mt_pending_fin_cnt,
  (SELECT COUNT(*) FROM ele_pending_order) AS ele_pending_fin_cnt,
  (SELECT COUNT(*) FROM jd_pending_order) AS jd_pending_fin_cnt,

  m.matched_cnt AS mt_matched_orders,
  m.logic1_sum_shoukuan AS mt_logic1_sum_商家应收款,
  m.logic2_sum_formula_kb AS mt_logic2_sum_知识库展开式,
  m.logic1_sum_shoukuan - m.logic2_sum_formula_kb AS mt_diff_logic1_minus_logic2,
  m.cnt_line_mismatch_gt_2fen AS mt_orders_abs_diff_gt_2fen,

  e.matched_cnt AS ele_matched_orders,
  e.logic1_sum_dingdan_yingshou AS ele_logic1_sum_订单应收_结算口径,
  e.ref_sum_jingyingye AS ele_ref_sum_净营业额合计_非结算,
  e.logic_ref_jiesuan_col11 AS ele_ref_sum_结算金额列,
  e.logic2_user_formula AS ele_logic2_sum_用户分项_含商品金额,
  e.logic2_user_formula_no_spje AS ele_logic2_sum_用户分项_不含商品金额,
  e.logic1_sum_dingdan_yingshou - e.logic2_user_formula AS ele_diff_订单应收_minus_用户公式_含spje,
  e.logic1_sum_dingdan_yingshou - e.logic2_user_formula_no_spje AS ele_diff_订单应收_minus_用户公式_无spje,
  e.logic1_sum_dingdan_yingshou - e.logic_ref_jiesuan_col11 AS ele_diff_订单应收_minus_结算列,
  e.logic_ref_jiesuan_col11 - e.logic2_user_formula AS ele_diff_结算列_minus_用户公式_含spje,
  e.logic_ref_jiesuan_col11 - e.logic2_user_formula_no_spje AS ele_diff_结算列_minus_用户公式_无spje,
  e.cnt_ddys_ne_jiesuan AS ele_orders_订单应收_ne_结算列,
  e.cnt_jing_ne_jiesuan AS ele_orders_净营业_ne_结算列_诊断,
  e.cnt_ddys_ne_formula_full AS ele_orders_订单应收_ne_用户公式_含spje,
  e.cnt_ddys_ne_formula_no_spje AS ele_orders_订单应收_ne_用户公式_无spje,
  e.cnt_user_ne_jiesuan AS ele_orders_用户公式_ne_结算列,
  e.cnt_no_spje_ne_jiesuan AS ele_orders_无spje公式_ne_结算列,
  e.sum_abs_ddys_vs_formula_full AS ele_sum_abs_订单应收_vs_用户公式_含spje,
  e.sum_abs_no_spje_vs_jiesuan AS ele_sum_abs差_无spje_vs_结算列,

  j.matched_cnt AS jd_matched_orders,
  j.jd_sum_settlement_matched AS jd_logic1_sum_财务结算匹配

FROM params p
CROSS JOIN mt_agg m
CROSS JOIN ele_agg e
CROSS JOIN jd_agg j;
