{{
  config(materialized='view')
}}

/*
  货品成本：mini橙订单明细 × 编码映射 × 总部招商价 × 换算比
  粒度：dt × platform × store_name
  ──────────────────────────────────────────────────
  修改本文件 = 修改「成本怎么算」
  公式：SUM(总部招商价 × 商品数量 × 金蝶换算比/mini橙换算比)
*/

SELECT
    NULLIF(TRIM(r.dt), '')::date AS dt,
    CASE
        WHEN r."门店名称" ~ '[（(].+?[）)]'
        THEN regexp_replace(r."门店名称", '.*[（(](.+?)[）)].*', '\1')
        ELSE r."门店名称"
    END AS store_name,
    CASE r."渠道"
        WHEN '淘宝闪购' THEN '饿了么'
        WHEN '京东秒送' THEN '京东'
        WHEN '美团外卖' THEN '美团'
        ELSE r."渠道"
    END AS platform,
    SUM(
        COALESCE(sk.headquarters_price, 0)
        * NULLIF(TRIM(r."商品数量"), '')::numeric
        * COALESCE(sk.kingdee_qty::numeric / NULLIF(sk.mini_qty::numeric, 0), 1)
    ) AS cost
FROM {{ source('rpa_o2o', 'ods_rpa_miniorange_order_detail') }} r
LEFT JOIN {{ source('o2o_ref', 'dim_o2o_sku_mapping') }} sk
    ON r."商品编码" = sk.mini_code
WHERE NULLIF(TRIM(r."订单状态"), '') IS DISTINCT FROM '已取消'
GROUP BY 1, 2, 3
