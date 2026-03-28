{{
  config(materialized='view')
}}

/*
  京东财务清洗：ods_rpa_jd_finance JOIN ods_rpa_jd_order（取门店名映射）
  粒度：一行 = 一笔费用项（同一订单可能多行：结算金额/佣金/配送费）
  输出：dt / platform / store_name / order_no / 费用类型 / amount
*/

WITH jd_store_map AS (
    SELECT DISTINCT ON ("门店ID")
        "门店ID",
        CASE
            WHEN "门店名称" ~ '[（(].+?[）)]'
            THEN regexp_replace("门店名称", '.*[（(](.+?)[）)].*', '\1')
            ELSE "门店名称"
        END AS store_name
    FROM {{ source('rpa_o2o', 'ods_rpa_jd_order') }}
)

SELECT
    NULLIF(TRIM(f.dt), '')::date    AS dt,
    '京东'                          AS platform,
    sm.store_name,
    f."到家业务单号"                AS order_no,
    f."费用类型",
    NULLIF(TRIM(f."结算金额"), '')::numeric AS amount
FROM {{ source('rpa_o2o', 'ods_rpa_jd_finance') }} f
LEFT JOIN jd_store_map sm ON f."门店编号" = sm."门店ID"
WHERE NULLIF(TRIM(f."结算金额"), '') IS NOT NULL
