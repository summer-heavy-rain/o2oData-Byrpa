{{
  config(
    materialized='table',
    schema='public'
  )
}}

/*
  O2O 业务日报 ADS 层 — 最终宽表
  ═══════════════════════════════════════════════════════════════════
  本文件只做 JOIN + 利润公式，不做任何清洗/聚合逻辑。
  每个经营指标的计算在独立的 intermediate 模型中：

    int_platform_revenue  → 收入 / 平台费 / 配送费 / 订单数
    int_item_cost         → 货品成本
    int_maiyatian_shipping→ 麦芽田第三方配送费
    int_promotion_cost    → 推广费

  利润公式（与校验表对齐）：
    gross_profit = revenue - cost - platform_fee  （招商利润）
    net_profit   = gross_profit - rent_labor       （利润扣房租人工）
    注：shipping / promotion 为展示参考列，不扣在利润里
  ═══════════════════════════════════════════════════════════════════
*/

WITH base AS (
    SELECT
        p.dt,
        p.platform,
        p.store_name,
        sr.province,
        sr.city,
        sr.store_short_name IS NOT NULL                              AS has_store_ref,
        CASE WHEN sr.daily_rent_labor IS NOT NULL THEN '直营' END    AS store_type,

        COALESCE(p.revenue,      0)::numeric(14,2)                   AS revenue,
        COALESCE(p.platform_fee, 0)::numeric(14,2)                   AS platform_fee,
        COALESCE(p.shipping,     0)::numeric(14,2)                   AS platform_shipping,
        COALESCE(ma.maiyatian_shipping, 0)::numeric(14,2)            AS maiyatian_shipping,
        (COALESCE(p.shipping, 0)
         + COALESCE(ma.maiyatian_shipping, 0))::numeric(14,2)        AS shipping,
        CASE WHEN p.order_count > 0
             THEN ROUND((COALESCE(p.shipping, 0)
                         + COALESCE(ma.maiyatian_shipping, 0))
                        / p.order_count, 2)
             ELSE 0 END::numeric(10,2)                               AS avg_shipping,
        COALESCE(pr.promotion_cost, 0)::numeric(14,2)                AS promotion_cost,
        COALESCE(p.order_count,  0)                                  AS order_count,
        COALESCE(ic.cost,        0)::numeric(14,2)                   AS cost,
        COALESCE(sr.daily_rent_labor, 0)::numeric(14,2)              AS rent_labor,

        (COALESCE(p.revenue, 0)
         - COALESCE(ic.cost, 0)
         - COALESCE(p.platform_fee, 0))::numeric(14,2)              AS gross_profit

    FROM {{ ref('int_platform_revenue') }} p

    LEFT JOIN {{ ref('int_item_cost') }} ic
        ON p.dt = ic.dt AND p.store_name = ic.store_name AND p.platform = ic.platform

    LEFT JOIN {{ ref('int_maiyatian_shipping') }} ma
        ON p.dt = ma.dt AND p.store_name = ma.store_name

    LEFT JOIN {{ ref('int_promotion_cost') }} pr
        ON p.dt = pr.dt AND p.store_name = pr.store_name AND p.platform = pr.platform

    LEFT JOIN {{ source('o2o_ref', 'dim_o2o_store_ref') }} sr
        ON p.store_name = sr.store_short_name
)

SELECT
    dt, platform, store_name,
    province, city, has_store_ref,

    revenue, platform_fee,
    platform_shipping, maiyatian_shipping, shipping, avg_shipping,
    promotion_cost, order_count, cost,

    rent_labor,

    CASE WHEN ROW_NUMBER() OVER (
        PARTITION BY dt, store_name
        ORDER BY order_count DESC, platform ASC
    ) = 1 THEN rent_labor ELSE 0 END::numeric(14,2)     AS rent_labor_allocated,

    gross_profit,

    (gross_profit
     - CASE WHEN ROW_NUMBER() OVER (
         PARTITION BY dt, store_name
         ORDER BY order_count DESC, platform ASC
       ) = 1 THEN rent_labor ELSE 0 END
    )::numeric(14,2)                                     AS net_profit,

    store_type

FROM base
