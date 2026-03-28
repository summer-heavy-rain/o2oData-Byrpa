{{
  config(materialized='view')
}}

/*
  麦芽田第三方配送费（所有门店、所有平台共用）
  粒度：dt × store_name（跨平台共用，不按平台拆）
  ──────────────────────────────────────────────────
  修改本文件 = 修改「麦芽田运费怎么算」
*/

SELECT
    NULLIF(TRIM(m.dt), '')::date AS dt,
    CASE
        WHEN m."配送门店" ~ '[（(].+?[）)]'
        THEN regexp_replace(m."配送门店", '.*[（(](.+?)[）)].*', '\1')
        ELSE m."配送门店"
    END AS store_name,
    SUM(NULLIF(TRIM(m."总配送费"), '')::numeric) AS maiyatian_shipping
FROM {{ source('rpa_o2o', 'ods_rpa_maiyatian_delivery') }} m
WHERE NULLIF(TRIM(m."状态"), '') IS DISTINCT FROM '已取消'
GROUP BY m.dt, 2
