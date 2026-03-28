{{
  config(materialized='view')
}}

/*
  美团财务清洗：ods_rpa_meituan_fin_order → 门店名标准化 + 字段提取
  粒度：一行 = 一笔订单
  输出：dt / platform / store_name / order_no / revenue / platform_fee_raw / shipping_raw
*/

SELECT
    NULLIF(TRIM("基础信息_账单日期"), '')::date AS dt,
    '美团'                                       AS platform,
    CASE
        WHEN "基础信息_门店名称" ~ '[（(].+?[）)]'
        THEN regexp_replace("基础信息_门店名称", '.*[（(](.+?)[）)].*', '\1')
        ELSE "基础信息_门店名称"
    END                                          AS store_name,
    "基础信息_订单号"                             AS order_no,
    NULLIF(TRIM("商家应收款"),  '')::numeric      AS revenue,
    NULLIF(TRIM("佣金"),        '')::numeric      AS platform_fee_raw,
    NULLIF(TRIM("配送服务费"),  '')::numeric      AS shipping_raw
FROM {{ source('rpa_o2o', 'ods_rpa_meituan_fin_order') }}
WHERE NULLIF(TRIM("基础信息_订单状态"), '') IS DISTINCT FROM '订单取消'
  AND NULLIF(TRIM("基础信息_账单日期"), '') IS NOT NULL
