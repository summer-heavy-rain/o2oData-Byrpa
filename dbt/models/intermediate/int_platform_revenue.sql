{{
  config(materialized='view')
}}

/*
  三平台经营指标汇总（UNION ALL）
  粒度：dt × platform × store_name
  输出：revenue / platform_fee / shipping / order_count
  ──────────────────────────────────────────────────
  修改本文件 = 修改「各平台如何聚合成统一口径」
  修改某平台的清洗规则 → 改 stg_*_revenue.sql
*/

WITH meituan_agg AS (
    SELECT
        dt, platform, store_name,
        SUM(revenue)               AS revenue,
        ABS(SUM(platform_fee_raw)) AS platform_fee,
        ABS(SUM(shipping_raw))     AS shipping,
        COUNT(DISTINCT order_no)   AS order_count
    FROM {{ ref('stg_meituan_revenue') }}
    GROUP BY dt, platform, store_name
),

eleme_agg AS (
    SELECT
        dt, platform, store_name,
        SUM(revenue)                                            AS revenue,
        ABS(SUM(platform_fee_raw))                              AS platform_fee,
        ABS(SUM(shipping_platform_raw))
          + ABS(SUM(COALESCE(shipping_logistics_raw, 0)))       AS shipping,
        COUNT(DISTINCT order_no)                                AS order_count
    FROM {{ ref('stg_eleme_revenue') }}
    GROUP BY dt, platform, store_name
),

jd_agg AS (
    SELECT
        dt, platform, store_name,
        SUM(CASE WHEN "费用类型" NOT IN ('代收到家佣金','基础配送费','商家承担远距离运费')
                  AND amount > 0 THEN amount ELSE 0 END)       AS revenue,
        ABS(SUM(CASE WHEN "费用类型" = '代收到家佣金'
                     THEN amount ELSE 0 END))                  AS platform_fee,
        ABS(SUM(CASE WHEN "费用类型" IN ('基础配送费','商家承担远距离运费')
                     THEN amount ELSE 0 END))                  AS shipping,
        COUNT(DISTINCT order_no)                               AS order_count
    FROM {{ ref('stg_jd_revenue') }}
    GROUP BY dt, platform, store_name
)

SELECT dt, platform, store_name, revenue, platform_fee, shipping, order_count
FROM meituan_agg

UNION ALL

SELECT dt, platform, store_name, revenue, platform_fee, shipping, order_count
FROM eleme_agg

UNION ALL

SELECT dt, platform, store_name, revenue, platform_fee, shipping, order_count
FROM jd_agg
