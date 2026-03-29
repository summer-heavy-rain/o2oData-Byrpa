# FTP 文件 → ODS 表映射文档

> 数据来源：`\\192.168.1.49\数仓rpa文件\数据组文件`
> 每日按 `YYYY-MM-DD` 日期文件夹组织，由 `scripts/ingest.py` 统一导入
> 配置文件：`config/sources.yaml`

---

## 迷你橙（`【数据组】mini橙数据导出`）


| FTP 文件名                 | ODS 表                             | 格式   | 备注                        |
| ----------------------- | --------------------------------- | ---- | ------------------------- |
| `mini橙-订单商品明细.xlsx`     | `ods_rpa_miniorange_order_detail` | xlsx | `_store_type='直营'`        |
| `mini橙-订单商品明细（加盟）.xlsx` | `ods_rpa_miniorange_order_detail` | xlsx | `_store_type='加盟'`，写入同一张表 |


---

## 饿了么（`【数据组】饿了么数据导出`）

3 个账号后缀：`(无后缀)=默认` / `(善培臣)` / `(自营)`，自动提取写入 `_account_type`

### 订单


| FTP 文件名        | ODS 表                 | 格式   | Sheet  |
| -------------- | --------------------- | ---- | ------ |
| `饿了么-订单*.xlsx` | `ods_rpa_eleme_order` | xlsx | `订单导出` |


### 推广


| FTP 文件名          | ODS 表                     | 格式   | Sheet   |
| ---------------- | ------------------------- | ---- | ------- |
| `饿了么-推广数据*.xlsx` | `ods_rpa_eleme_promotion` | xlsx | Sheet 0 |


### 财务账单（多 Sheet，每个 Sheet → 独立 ODS 表）


| FTP 文件名          | Sheet         | ODS 表                               |
| ---------------- | ------------- | ----------------------------------- |
| `饿了么-财务账单*.xlsx` | `全部账单汇总`      | `ods_rpa_eleme_fin_summary`         |
|                  | `销售账单汇总`      | `ods_rpa_eleme_fin_sales_summary`   |
|                  | `销售账单明细`      | `ods_rpa_eleme_fin_sales_detail`    |
|                  | `代运营账单明细`     | `ods_rpa_eleme_fin_operation`       |
|                  | `CPS账单明细`     | `ods_rpa_eleme_fin_cps`             |
|                  | `卡券账单汇总`      | `ods_rpa_eleme_fin_coupon_summary`  |
|                  | `卡券账单明细`      | `ods_rpa_eleme_fin_coupon_detail`   |
|                  | `激励账单`        | `ods_rpa_eleme_fin_incentive`       |
|                  | `全能半托管销售账单汇总` | `ods_rpa_eleme_fin_semi_summary`    |
|                  | `全能半托管销售账单明细` | `ods_rpa_eleme_fin_semi_detail`     |
|                  | `全能半托管销售商品账单` | `ods_rpa_eleme_fin_semi_product`    |
|                  | `付费问诊账单明细`    | `ods_rpa_eleme_fin_inquiry`         |
|                  | `渠道费用账单汇总`    | `ods_rpa_eleme_fin_channel_summary` |
|                  | `渠道费用账单明细`    | `ods_rpa_eleme_fin_channel_detail`  |
|                  | `退货运费账单汇总`    | `ods_rpa_eleme_fin_return_summary`  |
|                  | `退货运费账单明细`    | `ods_rpa_eleme_fin_return_detail`   |
|                  | `账单字段说明`      | *跳过*                                |


---

## 京东（`【数据组】京东数据导出`）


| FTP 文件名     | ODS 表                | 格式           | Sheet / 备注                |
| ----------- | -------------------- | ------------ | ------------------------- |
| `订单查询.xlsx` | `ods_rpa_jd_order`   | html_as_xlsx | 京东导出的 xlsx 实际是 HTML table |
| `账单下载.xlsx` | `ods_rpa_jd_finance` | xlsx         | Sheet: `sku对账单下载`         |


> 目录中还有 `warn.docx`，已配置跳过

---

## 麦芽田（`【数据组】麦芽田数据导出`）

按门店分文件，门店名从文件名自动提取（正则：`麦芽田-报表中心（(.+?)）`）


| FTP 文件名                   | ODS 表                        | 格式          | 备注                         |
| ------------------------- | ---------------------------- | ----------- | -------------------------- |
| `麦芽田-报表中心（XX门店）.xlsx` × N | `ods_rpa_maiyatian_delivery` | xml_as_xlsx | 门店名→`_store_name`，当前 5 个文件 |


---

## 美团（`【数据组】美团数据导出`）

按门店分文件，门店名从文件名括号中提取（正则：`[（(](.+?)[)）]`）

### 商品明细


| FTP 文件名                 | ODS 表                            | 格式        | 备注                |
| ----------------------- | -------------------------------- | --------- | ----------------- |
| `美团-商品明细（XX门店）.csv` × N | `ods_rpa_meituan_product_detail` | csv (GBK) | 门店名→`_store_name` |


### 账单（多 Sheet）


| FTP 文件名                | Sheet       | ODS 表                             |
| ---------------------- | ----------- | --------------------------------- |
| `美团-账单（XX门店）.xlsx` × N | `账单明细`      | `ods_rpa_meituan_fin_bill`        |
|                        | `订单明细`      | `ods_rpa_meituan_fin_order`       |
|                        | `处罚&赔付`     | `ods_rpa_meituan_fin_penalty`     |
|                        | `健康卡费用商家部分` | `ods_rpa_meituan_fin_health_card` |
|                        | `汇总信息`      | *跳过*                              |
|                        | `账单字段说明`    | *跳过*                              |


---

## 独立导入脚本（不走 ingest.py）


| 源文件           | 目标表                         | 脚本                                    | 备注                       |
| ------------- | --------------------------- | ------------------------------------- | ------------------------ |
| `房租水电成本.xls`  | `dim_o2o_store_ref`         | `scripts/import_store_ref.py`         | 门店简称/省份/城市/日均房租人工        |
| `推广美团自营.xlsx` | `ods_rpa_meituan_promotion` | `scripts/import_meituan_promotion.py` | 美团推广费流水，来源：`【数据组】美团数据导出` |
| `编码匹配.xls`    | `dim_o2o_sku_mapping`       | *(已有表，另行导入)*                          | O2O编码→金蝶编码映射+换算比+总部招商价   |


---

## 所有 ODS 表的公共列

每张 ODS 表都会自动添加以下三列：


| 列名             | 类型          | 说明                |
| -------------- | ----------- | ----------------- |
| `dt`           | DATE        | 分区日期，从 FTP 文件夹名提取 |
| `_source_file` | TEXT        | 来源文件名             |
| `_load_time`   | TIMESTAMPTZ | 入库时间              |


部分表还会添加 `_store_name`（门店名，从文件名提取）或 `_account_type`（账号类型）。