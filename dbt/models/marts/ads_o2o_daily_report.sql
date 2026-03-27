{{
  config(
    materialized='table',
    schema='public'
  )
}}

/*
  O2O 业务日报 ADS 层
  按 (日期, 平台, 门店) 粒度聚合订单/运费/推广数据
  对标 o2o业务看板模板(ads层).xlsx 的 14 列指标
*/

WITH store_name_extract AS (
    -- 辅助函数：从 "小猴快跑医疗器械（余杭店）" 提取 "余杭店"
    SELECT 1
),

-- ============ 饿了么 ============
eleme_parsed AS (
    SELECT
        dt,
        '饿了么' AS platform,
        CASE
            WHEN "商户名称" ~ '[（(].+?[）)]'
            THEN regexp_replace("商户名称", '.*[（(](.+?)[）)].*', '\1')
            ELSE "商户名称"
        END AS store_name,
        "订单编号",
        NULLIF(TRIM("商户应收金额"), '')::numeric AS order_revenue,
        NULLIF(TRIM("平台佣金"), '')::numeric AS platform_fee_item,
        NULLIF(TRIM("订单状态"), '') AS order_status
    FROM {{ source('rpa_o2o', 'ods_rpa_eleme_order') }}
),

eleme_agg AS (
    SELECT
        dt, platform, store_name,
        SUM(DISTINCT_REVENUE) AS revenue,
        SUM(platform_fee_item) AS platform_fee,
        COUNT(DISTINCT "订单编号") AS order_count
    FROM (
        SELECT
            dt, platform, store_name, "订单编号",
            -- 订单级金额取 DISTINCT，避免商品行重复累加
            FIRST_VALUE(order_revenue) OVER (
                PARTITION BY dt, store_name, "订单编号"
                ORDER BY order_revenue DESC NULLS LAST
            ) AS DISTINCT_REVENUE,
            platform_fee_item
        FROM eleme_parsed
        WHERE order_status IS DISTINCT FROM '已取消'
    ) sub
    GROUP BY dt, platform, store_name
),

-- ============ 京东 ============
jd_parsed AS (
    SELECT
        dt,
        '京东' AS platform,
        CASE
            WHEN "门店名称" ~ '[（(].+?[）)]'
            THEN regexp_replace("门店名称", '.*[（(](.+?)[）)].*', '\1')
            ELSE "门店名称"
        END AS store_name,
        "订单编号",
        NULLIF(TRIM("门店应收金额"), '')::numeric AS order_revenue,
        NULLIF(TRIM("订单状态"), '') AS order_status
    FROM {{ source('rpa_o2o', 'ods_rpa_jd_order') }}
),

jd_order_dedup AS (
    SELECT DISTINCT dt, platform, store_name, "订单编号", order_revenue
    FROM jd_parsed
    WHERE order_status = '送货完成'
),

jd_agg AS (
    SELECT
        dt, platform, store_name,
        SUM(order_revenue) AS revenue,
        0::numeric AS platform_fee,
        COUNT(DISTINCT "订单编号") AS order_count
    FROM jd_order_dedup
    GROUP BY dt, platform, store_name
),

-- ============ 美团 ============
meituan_parsed AS (
    SELECT
        dt,
        '美团' AS platform,
        CASE
            WHEN "商家名称" ~ '[（(].+?[）)]'
            THEN regexp_replace("商家名称", '.*[（(](.+?)[）)].*', '\1')
            ELSE "商家名称"
        END AS store_name,
        "订单编号",
        NULLIF(TRIM("订单实付交易额_元"), '')::numeric AS order_revenue,
        NULLIF(TRIM("订单状态"), '') AS order_status
    FROM {{ source('rpa_o2o', 'ods_rpa_meituan_product_detail') }}
),

meituan_order_dedup AS (
    SELECT DISTINCT dt, platform, store_name, "订单编号", order_revenue
    FROM meituan_parsed
    WHERE order_status = '已完成'
),

meituan_agg AS (
    SELECT
        dt, platform, store_name,
        SUM(order_revenue) AS revenue,
        0::numeric AS platform_fee,
        COUNT(DISTINCT "订单编号") AS order_count
    FROM meituan_order_dedup
    GROUP BY dt, platform, store_name
),

-- ============ 麦芽田（配送运费 + 距离，跨平台共享）============
maiyatian AS (
    SELECT
        dt,
        CASE
            WHEN "配送门店" ~ '[（(].+?[）)]'
            THEN regexp_replace("配送门店", '.*[（(](.+?)[）)].*', '\1')
            ELSE "配送门店"
        END AS store_name,
        SUM(NULLIF(TRIM("总配送费"), '')::numeric) AS shipping_cost,
        SUM(NULLIF(TRIM("配送距离"), '')::numeric) AS delivery_distance,
        COUNT(*) AS delivery_count
    FROM {{ source('rpa_o2o', 'ods_rpa_maiyatian_delivery') }}
    WHERE NULLIF(TRIM("状态"), '') IS DISTINCT FROM '已取消'
    GROUP BY 1, 2
),

-- ============ 饿了么推广费 ============
eleme_promo AS (
    SELECT
        dt,
        CASE
            WHEN "门店名称" ~ '[（(].+?[）)]'
            THEN regexp_replace("门店名称", '.*[（(](.+?)[）)].*', '\1')
            ELSE "门店名称"
        END AS store_name,
        SUM(NULLIF(TRIM("推广消费_元"), '')::numeric) AS promotion_cost
    FROM {{ source('rpa_o2o', 'ods_rpa_eleme_promotion') }}
    GROUP BY 1, 2
),

-- ============ 合并三平台 ============
all_platforms AS (
    SELECT * FROM eleme_agg
    UNION ALL
    SELECT * FROM jd_agg
    UNION ALL
    SELECT * FROM meituan_agg
)

SELECT
    p.dt,
    p.platform,
    p.store_name,
    COALESCE(p.revenue, 0)::numeric(14,2)     AS revenue,
    0::numeric(14,2)                           AS cost,
    COALESCE(p.revenue, 0)::numeric(14,2)      AS gross_profit,
    COALESCE(m.shipping_cost, 0)::numeric(14,2) AS shipping_cost,
    CASE WHEN p.order_count > 0
         THEN ROUND(COALESCE(m.shipping_cost, 0) / p.order_count, 2)
         ELSE 0 END::numeric(10,2)             AS avg_shipping,
    COALESCE(p.platform_fee, 0)::numeric(14,2) AS platform_fee,
    COALESCE(m.delivery_distance, 0)::numeric(14,2) AS delivery_distance,
    COALESCE(p.order_count, 0)                 AS order_count,
    CASE WHEN p.order_count > 0
         THEN ROUND(COALESCE(m.delivery_distance, 0) / p.order_count, 2)
         ELSE 0 END::numeric(10,2)             AS avg_distance,
    0::numeric(14,2)                           AS rent_labor,
    COALESCE(ep.promotion_cost, 0)::numeric(14,2) AS promotion_cost,
    (COALESCE(p.revenue, 0) - COALESCE(ep.promotion_cost, 0))::numeric(14,2) AS net_profit,
    NULL::text                                 AS store_type
FROM all_platforms p
LEFT JOIN maiyatian m
    ON p.dt = m.dt AND p.store_name = m.store_name
LEFT JOIN eleme_promo ep
    ON p.dt = ep.dt AND p.store_name = ep.store_name AND p.platform = '饿了么'
