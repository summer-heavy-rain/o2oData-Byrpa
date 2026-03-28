{{
  config(materialized='view')
}}

/*
  饿了么（淘宝闪购）财务清洗：ods_rpa_eleme_fin_sales_detail
  粒度：一行 = 一笔订单
  平台费 = 平台交易技术服务费（纯佣金，不含履约费）
  运费 = 平台履约技术服务费 + 物流网络配送费
*/

SELECT
    NULLIF(TRIM("账单日期_col_3"), '')::date AS dt,
    '饿了么'                                  AS platform,
    CASE
        WHEN "门店名称_col_1" ~ '[（(].+?[）)]'
        THEN regexp_replace("门店名称_col_1", '.*[（(](.+?)[）)].*', '\1')
        ELSE "门店名称_col_1"
    END                                       AS store_name,
    "订单号_col_6"                            AS order_no,
    NULLIF(TRIM("商家收入_净营业额合计_净营业额合计"),              '')::numeric AS revenue,
    NULLIF(TRIM("商家支出_服务费用合计_平台交易技术服务费"),        '')::numeric AS platform_fee_raw,
    NULLIF(TRIM("商家支出_服务费用合计_平台履约技术服务费"),        '')::numeric AS shipping_platform_raw,
    NULLIF(TRIM("商家支出_服务费用合计_物流网络配送费"),            '')::numeric AS shipping_logistics_raw
FROM {{ source('rpa_o2o', 'ods_rpa_eleme_fin_sales_detail') }}
WHERE NULLIF(TRIM("账单日期_col_3"), '') IS NOT NULL
