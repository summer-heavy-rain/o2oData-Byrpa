-- =============================================================================
-- O2O 单日门店聚合（即席查询）
-- =============================================================================
-- 仓库：o2oData-Byrpa
-- 口径全文：docs/O2O单日门店聚合口径.md
--
-- 请勿在 union-agent 中维护本文件副本；友联主后端与 O2O RPA 计算职责分离。
--
-- 用途：
--   * 在 PostgreSQL 中直接执行（或粘贴到客户端），按「业务约定口径」出门店日粒度宽表。
--   * 与 dbt/models/marts/ads_o2o_daily_report.sql 并存：实现路径不同，详见文档第六节。
--
-- 修改业务日时：只改下方 WITH params 中的两个 DATE。
--
-- 固定成本：依赖维表 dim_ods_store_operating_cost。若尚未创建，先执行
--   sql/ddl_dim_ods_store_operating_cost.sql
-- 再跑本查询（不影响 ods_rpa_* 贴源层）。
-- =============================================================================

WITH params AS (
  /* 业务日 D；半开区间 [D, D+1) 用于下单/成交时间过滤 */
  SELECT DATE '2026-03-25' AS d, DATE '2026-03-26' AS d_end
),

/* ---------------------------------------------------------------------------
   固定成本：dim_ods_store_operating_cost（DDL 见 sql/ddl_dim_ods_store_operating_cost.sql）
   --------------------------------------------------------------------------- */
fixed_enriched AS (
  SELECT
    c.store_label,
    COALESCE(ds.province, NULLIF(TRIM(c.province), '')) AS province,
    COALESCE(ds.city, NULLIF(TRIM(c.city), ''))         AS city,
    COALESCE(ds.store_nature::text, '未匹配')            AS store_type,
    c.daily_fixed_cost                                  AS rent_util_labor_fixed
  FROM dim_ods_store_operating_cost c
  LEFT JOIN dim_store ds
    ON ds.store_short_name = c.store_label
    OR ds.store_short_name LIKE c.store_label || '%'
),

/* ---------------------------------------------------------------------------
   美团：财务订单明细 ods_rpa_meituan_fin_order
   商家应收：商家应收款；时间：下单时间；按订单号 DISTINCT ON 防重复行
   企客返利：dim_store.meituan_enterprise_rebate（元/单，F06 企客报价导入）× 当日美团订单数
   美团实收（含企客）：mt_receivable_amt + mt_qike_rebate_amt
   剔除：基础信息_订单状态 = 订单取消（不计入结算与票数；与饿了么「已取消/全部退款」对称）
   关联 dim：platform = '美团'，platform_shop_id / shop_short_name 对齐导出门店
   默认 dim_store(..., meituan_enterprise_rebate)
        dim_store_o2o(store_id, platform, platform_shop_id, shop_short_name)
   --------------------------------------------------------------------------- */
mt_fin_dedup AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), '') AS order_no,
    NULLIF(TRIM(REPLACE(m."基础信息_门店id", E'\t', '')), '') AS platform_shop_id,
    NULLIF(TRIM(REPLACE(m."基础信息_门店名称", E'\t', '')), '') AS platform_shop_name,
    NULLIF(TRIM(REPLACE(m."商家应收款", E'\t', '')), '')::numeric AS settlement_amt
  FROM ods_rpa_meituan_fin_order m
  CROSS JOIN params p
  WHERE m.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND NULLIF(TRIM(REPLACE(m."基础信息_订单状态", E'\t', '')), '') IS DISTINCT FROM '订单取消'
    AND NULLIF(TRIM(REPLACE(m."基础信息_下单时间", E'\t', '')), '') IS NOT NULL
    AND TRIM(REPLACE(m."基础信息_下单时间", E'\t', '')) >= to_char(p.d, 'YYYY-MM-DD')
    AND TRIM(REPLACE(m."基础信息_下单时间", E'\t', '')) < to_char(p.d_end, 'YYYY-MM-DD')
  ORDER BY NULLIF(TRIM(REPLACE(m."基础信息_订单号", E'\t', '')), ''),
    NULLIF(TRIM(REPLACE(m."基础信息_账单日期", E'\t', '')), '') DESC NULLS LAST
),

mt_by_store AS (
  SELECT
    COALESCE(ds.store_short_name, mf.platform_shop_name) AS store_label,
    COALESCE(ds.province, '') AS province,
    COALESCE(ds.city, '')     AS city,
    COALESCE(ds.store_nature::text, '未匹配') AS store_type,
    COUNT(*)::bigint AS mt_order_cnt,
    COALESCE(SUM(mf.settlement_amt), 0) AS mt_receivable_amt,
    (COUNT(*)::numeric * COALESCE(MAX(ds.meituan_enterprise_rebate), 0::numeric)) AS mt_qike_rebate_amt,
    COALESCE(SUM(mf.settlement_amt), 0)
      + (COUNT(*)::numeric * COALESCE(MAX(ds.meituan_enterprise_rebate), 0::numeric)) AS mt_receipt_amt,
    COALESCE(MAX(ds.meituan_enterprise_rebate), 0::numeric) AS mt_qike_rebate_per_order
  FROM mt_fin_dedup mf
  LEFT JOIN dim_store_o2o o2o
    ON o2o.platform = '美团买药'
   AND (
     o2o.platform_store_id::text = mf.platform_shop_id
     OR o2o.store_name = mf.platform_shop_name
     OR REPLACE(REPLACE(o2o.store_name, '（', '('), '）', ')')
      = REPLACE(REPLACE(mf.platform_shop_name, '（', '('), '）', ')')
   )
  LEFT JOIN dim_store ds ON ds.store_short_name = o2o.store_short_name
  GROUP BY 1, 2, 3, 4
),

mt_by_store_one AS (
  SELECT
    store_label,
    MAX(province)   AS province,
    MAX(city)       AS city,
    MAX(store_type) AS store_type,
    SUM(mt_order_cnt) AS mt_order_cnt,
    SUM(mt_receivable_amt) AS mt_receivable_amt,
    SUM(mt_qike_rebate_amt) AS mt_qike_rebate_amt,
    SUM(mt_receipt_amt) AS mt_receipt_amt,
    MAX(mt_qike_rebate_per_order) AS mt_qike_rebate_per_order
  FROM mt_by_store
  GROUP BY store_label
),

/* ---------------------------------------------------------------------------
   饿了么：订单导出；结算字段可改为 ods_rpa_eleme_fin_sales_detail（多主体合并后）
   剔除：已取消、全部退款（导出里「商户应收金额」可能仍为正值，不能参与结算/票数）
   --------------------------------------------------------------------------- */
ele_dedup AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(e."订单编号", E'\t', '')), '') AS order_no,
    NULLIF(TRIM(REPLACE(e."门店ID", E'\t', '')), '') AS platform_shop_id,
    NULLIF(TRIM(REPLACE(e."商户名称", E'\t', '')), '') AS platform_shop_name,
    NULLIF(TRIM(REPLACE(e."商户应收金额", E'\t', '')), '')::numeric AS settlement_amt
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

ele_by_store AS (
  SELECT
    COALESCE(ds.store_short_name, ed.platform_shop_name) AS store_label,
    COALESCE(ds.province, '') AS province,
    COALESCE(ds.city, '')     AS city,
    COALESCE(ds.store_nature::text, '未匹配') AS store_type,
    COUNT(*)::bigint AS ele_order_cnt,
    COALESCE(SUM(ed.settlement_amt), 0) AS ele_settlement_amt
  FROM ele_dedup ed
  LEFT JOIN dim_store_o2o o2o
    ON o2o.platform IN ('饿了么', '淘宝闪购')
   AND (
     o2o.platform_store_id::text = ed.platform_shop_id
     OR o2o.store_name = ed.platform_shop_name
     OR REPLACE(REPLACE(o2o.store_name, '（', '('), '）', ')')
      = REPLACE(REPLACE(ed.platform_shop_name, '（', '('), '）', ')')
   )
  LEFT JOIN dim_store ds ON ds.store_short_name = o2o.store_short_name
  GROUP BY 1, 2, 3, 4
),

ele_by_store_one AS (
  SELECT
    store_label,
    MAX(province)   AS province,
    MAX(city)       AS city,
    MAX(store_type) AS store_type,
    SUM(ele_order_cnt) AS ele_order_cnt,
    SUM(ele_settlement_amt) AS ele_settlement_amt
  FROM ele_by_store
  GROUP BY store_label
),

/* ---------------------------------------------------------------------------
   京东：财务 ods_rpa_jd_finance 按「到家业务单号」汇总结算金额（与订单表「订单编号」一致）。
   说明：RPA 落表时「秒送单号」常为空，不能用作关联键；到家业务单号 = 前台订单号。
   时间：无「下单时间」时用成交时间 → 门店收单时间
   --------------------------------------------------------------------------- */
jd_settlement AS (
  SELECT
    NULLIF(TRIM(REPLACE(jf."到家业务单号", E'\t', '')), '') AS order_no,
    SUM(NULLIF(TRIM(REPLACE(jf."结算金额", E'\t', '')), '')::numeric) AS settlement_amt
  FROM ods_rpa_jd_finance jf
  CROSS JOIN params p
  WHERE jf.dt::text = to_char(p.d, 'YYYY-MM-DD')
  GROUP BY 1
  HAVING NULLIF(TRIM(REPLACE(jf."到家业务单号", E'\t', '')), '') IS NOT NULL
),

jd_orders AS (
  SELECT DISTINCT ON (NULLIF(TRIM(REPLACE(jo."订单编号", E'\t', '')), ''))
    NULLIF(TRIM(REPLACE(jo."订单编号", E'\t', '')), '') AS order_no,
    NULLIF(TRIM(REPLACE(jo."门店ID", E'\t', '')), '') AS platform_shop_id,
    NULLIF(TRIM(REPLACE(jo."门店名称", E'\t', '')), '') AS platform_shop_name,
    COALESCE(
      NULLIF(TRIM(REPLACE(jo."成交时间", E'\t', '')), ''),
      NULLIF(TRIM(REPLACE(jo."门店收单时间", E'\t', '')), '')
    ) AS place_time_raw
  FROM ods_rpa_jd_order jo
  CROSS JOIN params p
  WHERE jo.dt::text = to_char(p.d, 'YYYY-MM-DD')
  ORDER BY NULLIF(TRIM(REPLACE(jo."订单编号", E'\t', '')), '')
),

jd_joined AS (
  SELECT
    o.order_no,
    o.platform_shop_id,
    o.platform_shop_name,
    js.settlement_amt,
    o.place_time_raw
  FROM jd_orders o
  JOIN jd_settlement js ON js.order_no = o.order_no
  CROSS JOIN params p
  WHERE o.place_time_raw IS NOT NULL
    AND to_timestamp(o.place_time_raw, 'YYYY-MM-DD HH24:MI:SS') >= p.d::timestamptz
    AND to_timestamp(o.place_time_raw, 'YYYY-MM-DD HH24:MI:SS') < p.d_end::timestamptz
),

jd_by_store AS (
  SELECT
    COALESCE(ds.store_short_name, jj.platform_shop_name) AS store_label,
    COALESCE(ds.province, '') AS province,
    COALESCE(ds.city, '')     AS city,
    COALESCE(ds.store_nature::text, '未匹配') AS store_type,
    COUNT(*)::bigint AS jd_order_cnt,
    COALESCE(SUM(jj.settlement_amt), 0) AS jd_settlement_amt
  FROM jd_joined jj
  LEFT JOIN dim_store_o2o o2o
    ON o2o.platform IN ('京东买药', '京东小时达', '京东')
   AND (
     o2o.platform_store_id::text = jj.platform_shop_id
     OR o2o.store_name = jj.platform_shop_name
     OR REPLACE(REPLACE(o2o.store_name, '（', '('), '）', ')')
      = REPLACE(REPLACE(jj.platform_shop_name, '（', '('), '）', ')')
   )
  LEFT JOIN dim_store ds ON ds.store_short_name = o2o.store_short_name
  GROUP BY 1, 2, 3, 4
),

jd_by_store_one AS (
  SELECT
    store_label,
    MAX(province)   AS province,
    MAX(city)       AS city,
    MAX(store_type) AS store_type,
    SUM(jd_order_cnt) AS jd_order_cnt,
    SUM(jd_settlement_amt) AS jd_settlement_amt
  FROM jd_by_store
  GROUP BY store_label
),

/* 三平台按门店主键对齐：一行一店，分列美团/饿了么/京东；全渠道结算含「美团实收+企客」 */
order_store_spine AS (
  SELECT store_label FROM mt_by_store_one
  UNION
  SELECT store_label FROM ele_by_store_one
  UNION
  SELECT store_label FROM jd_by_store_one
),

orders_by_store AS (
  SELECT
    s.store_label,
    COALESCE(mt.province, el.province, jd.province, '') AS province,
    COALESCE(mt.city, el.city, jd.city, '')             AS city,
    COALESCE(mt.store_type, el.store_type, jd.store_type, '未匹配') AS store_type,
    COALESCE(mt.mt_order_cnt, 0) + COALESCE(el.ele_order_cnt, 0) + COALESCE(jd.jd_order_cnt, 0) AS order_cnt,
    COALESCE(mt.mt_receipt_amt, 0) + COALESCE(el.ele_settlement_amt, 0) + COALESCE(jd.jd_settlement_amt, 0) AS settlement_amt,
    COALESCE(mt.mt_order_cnt, 0)       AS mt_order_cnt,
    COALESCE(mt.mt_receivable_amt, 0)  AS mt_receivable_amt,
    COALESCE(mt.mt_qike_rebate_amt, 0) AS mt_qike_rebate_amt,
    COALESCE(mt.mt_receipt_amt, 0)     AS mt_receipt_amt,
    COALESCE(mt.mt_qike_rebate_per_order, 0) AS mt_qike_rebate_per_order,
    COALESCE(el.ele_order_cnt, 0)      AS ele_order_cnt,
    COALESCE(el.ele_settlement_amt, 0) AS ele_settlement_amt,
    COALESCE(jd.jd_order_cnt, 0)      AS jd_order_cnt,
    COALESCE(jd.jd_settlement_amt, 0) AS jd_settlement_amt
  FROM order_store_spine s
  LEFT JOIN mt_by_store_one mt ON mt.store_label = s.store_label
  LEFT JOIN ele_by_store_one el ON el.store_label = s.store_label
  LEFT JOIN jd_by_store_one jd ON jd.store_label = s.store_label
),

/* ---------------------------------------------------------------------------
   麦芽田：按下单日期 = D 取数；总运费与平均距离为「当日全局」
   运费分摊到门店：按该门店订单量 / 当日全渠道订单总量（近似，见口径文档）
   --------------------------------------------------------------------------- */
myt AS (
  SELECT
    NULLIF(TRIM(m."订单编号"), '') AS order_no,
    NULLIF(TRIM(m."总配送费"), '')::numeric AS freight_amt,
    NULLIF(TRIM(m."距离"), '')::numeric AS dist_km
  FROM ods_rpa_maiyatian_delivery m
  CROSS JOIN params p
  WHERE m.dt::text = to_char(p.d, 'YYYY-MM-DD')
    AND NULLIF(TRIM(m."下单日期"), '') IS NOT NULL
    AND to_date(m."下单日期", 'YYYY-MM-DD') = p.d
),

freight_day AS (
  SELECT
    COALESCE(SUM(freight_amt), 0) AS total_freight,
    COALESCE(AVG(dist_km), 0)     AS avg_dist_km,
    COUNT(DISTINCT order_no)      AS myt_order_cnt
  FROM (
    SELECT order_no, MAX(freight_amt) AS freight_amt, MAX(dist_km) AS dist_km
    FROM myt
    GROUP BY order_no
  ) t
),

orders_tot AS (
  SELECT COALESCE(SUM(order_cnt), 0) AS total_order_cnt
  FROM orders_by_store
),

freight_alloc_by_store AS (
  SELECT
    ob.store_label,
    CASE
      WHEN ot.total_order_cnt > 0 AND fd.total_freight IS NOT NULL
      THEN fd.total_freight * ob.order_cnt::numeric / ot.total_order_cnt
      ELSE 0::numeric
    END AS freight_alloc
  FROM orders_by_store ob
  CROSS JOIN freight_day fd
  CROSS JOIN orders_tot ot
),

/* ---------------------------------------------------------------------------
   饿了么推广：列名可能是「推广现金消费_元」或「推广消费_元」，按实际表结构调整
   --------------------------------------------------------------------------- */
ele_promo_by_store AS (
  SELECT
    t.store_label,
    SUM(t.promo_line) AS promo_spend
  FROM (
    SELECT
      COALESCE(ds.store_short_name, NULLIF(TRIM(pr."门店名称"), '')) AS store_label,
      NULLIF(TRIM(pr."推广现金消费_元"), '')::numeric AS promo_line
    FROM ods_rpa_eleme_promotion pr
    CROSS JOIN params p
    LEFT JOIN dim_store_o2o o2o
      ON o2o.platform IN ('饿了么', '淘宝闪购')
     AND (
       o2o.platform_store_id::text = NULLIF(TRIM(pr."门店ID"), '')
       OR REPLACE(REPLACE(o2o.store_name, '（', '('), '）', ')')
        = REPLACE(REPLACE(NULLIF(TRIM(pr."门店名称"), ''), '（', '('), '）', ')')
     )
    LEFT JOIN dim_store ds ON ds.store_short_name = o2o.store_short_name
    WHERE pr.dt::text = to_char(p.d, 'YYYY-MM-DD')
      AND to_date(NULLIF(TRIM(pr."日期"), ''), 'YYYY-MM-DD') = p.d
  ) t
  WHERE t.store_label IS NOT NULL
  GROUP BY t.store_label
),

promo_by_store AS (
  SELECT store_label, promo_spend FROM ele_promo_by_store
),

/* 招商成本：待行级映射 dim_o2o_sku_mapping 后替换本占位 */
zh_by_store AS (
  SELECT NULL::text AS store_label, 0::numeric AS zh_cost_total WHERE FALSE
),

all_stores AS (
  SELECT store_label FROM fixed_enriched
  UNION
  SELECT store_label FROM orders_by_store
  UNION
  SELECT store_label FROM promo_by_store
)

SELECT
  p.d AS report_date,
  COALESCE(fe.province, ob.province, '') AS province,
  COALESCE(fe.city, ob.city, '')         AS city,
  COALESCE(fe.store_type, ob.store_type, '未匹配') AS store_type,
  s.store_label,

  /* 全渠道结算：美团侧为「商家应收 + 企客返利」，饿了么/京东为导出结算字段 */
  COALESCE(ob.settlement_amt, 0)  AS settlement_amt,
  COALESCE(ob.mt_order_cnt, 0)       AS mt_order_cnt,
  COALESCE(ob.mt_receivable_amt, 0)  AS mt_receivable_amt,
  COALESCE(ob.mt_qike_rebate_amt, 0) AS mt_qike_rebate_amt,
  COALESCE(ob.mt_receipt_amt, 0)     AS mt_receipt_amt,
  COALESCE(ob.mt_qike_rebate_per_order, 0) AS mt_qike_rebate_per_order,
  COALESCE(ob.ele_order_cnt, 0)      AS ele_order_cnt,
  COALESCE(ob.ele_settlement_amt, 0) AS ele_settlement_amt,
  COALESCE(ob.jd_order_cnt, 0)       AS jd_order_cnt,
  COALESCE(ob.jd_settlement_amt, 0)  AS jd_settlement_amt,

  COALESCE(zh.zh_cost_total, 0)   AS zh_cost_total,
  COALESCE(fa.freight_alloc, 0)   AS freight_total_alloc,
  CASE WHEN COALESCE(ob.order_cnt, 0) > 0
       THEN COALESCE(fa.freight_alloc, 0) / ob.order_cnt
       END AS freight_per_order,
  COALESCE(ob.order_cnt, 0)       AS order_cnt,
  fd.avg_dist_km                  AS avg_order_distance_km_global,

  COALESCE(fe.rent_util_labor_fixed, 0) AS rent_util_labor_fixed,
  COALESCE(pm.promo_spend, 0)           AS promo_spend

FROM all_stores s
CROSS JOIN params p
CROSS JOIN freight_day fd
LEFT JOIN fixed_enriched fe ON fe.store_label = s.store_label
LEFT JOIN orders_by_store ob ON ob.store_label = s.store_label
LEFT JOIN freight_alloc_by_store fa ON fa.store_label = s.store_label
LEFT JOIN promo_by_store pm ON pm.store_label = s.store_label
LEFT JOIN zh_by_store zh ON zh.store_label = s.store_label
ORDER BY province, city, store_type, store_label;

-- -----------------------------------------------------------------------------
-- 按 省 / 市 / 直营加盟 汇总示例（去掉 store_label，对金额与票数 SUM）：
--
-- SELECT report_date, province, city, store_type,
--        SUM(settlement_amt), SUM(zh_cost_total), SUM(freight_total_alloc),
--        SUM(order_cnt), SUM(rent_util_labor_fixed), SUM(promo_spend)
-- FROM ( <将上面整段 WITH...SELECT 包成子查询> ) x
-- GROUP BY 1, 2, 3, 4;
-- -----------------------------------------------------------------------------
