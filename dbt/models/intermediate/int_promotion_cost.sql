{{
  config(materialized='view')
}}

/*
  推广费：美团 + 饿了么（京东暂无推广数据）
  粒度：dt × platform × store_name
  ──────────────────────────────────────────────────
  修改本文件 = 修改「推广费怎么算」
  美团来源：推广美团自营.xlsx → ods_rpa_meituan_promotion
  饿了么来源：ods_rpa_eleme_promotion（推广消费_元）
*/

SELECT dt, '美团' AS platform, store_name, SUM(amount) AS promotion_cost
FROM {{ source('rpa_o2o', 'ods_rpa_meituan_promotion') }}
GROUP BY dt, store_name

UNION ALL

SELECT
    NULLIF(TRIM("日期"), '')::date AS dt,
    '饿了么' AS platform,
    CASE
        WHEN "门店名称" ~ '[（(].+?[）)]'
        THEN regexp_replace("门店名称", '.*[（(](.+?)[）)].*', '\1')
        ELSE "门店名称"
    END AS store_name,
    SUM(ABS(NULLIF(TRIM("推广消费_元"), '')::numeric)) AS promotion_cost
FROM {{ source('rpa_o2o', 'ods_rpa_eleme_promotion') }}
GROUP BY 1, 3
