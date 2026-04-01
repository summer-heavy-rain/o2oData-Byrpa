/*
  ⚠️ 已弃用 — DDL 统一由 config/table_schemas.yaml 管理
  新建表/加列请编辑 config/table_schemas.yaml，然后运行:
    python -m scripts.sync_schema

  此文件保留作为历史参考，不再是表结构的权威来源。
  ---------------------------------------------------------------
  原用法: dbt run-operation create_ods_tables
  原则: 业务列全 TEXT，零转换；元数据列带类型
*/

{% macro create_ods_tables() %}

  /* ── 迷你橙 ── */
  {% call statement('create_miniorange', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_miniorange_order_detail (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_type     TEXT,
      "渠道"          TEXT,
      "门店编码"      TEXT,
      "门店名称"      TEXT,
      "渠道订单号"    TEXT,
      "订单状态"      TEXT,
      "一级分类"      TEXT,
      "二级分类"      TEXT,
      "商品编码"      TEXT,
      "商品条码"      TEXT,
      "商品名称"      TEXT,
      "商品数量"      TEXT,
      "出库数量"      TEXT,
      "商品售价"      TEXT,
      "时间"          TEXT,
      "商家补贴"      TEXT,
      "平台补贴"      TEXT,
      "总成本"        TEXT,
      "金额"          TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 饿了么-订单 ── */
  {% call statement('create_eleme_order', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_eleme_order (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _account_type   TEXT,
      "订单序号"      TEXT,
      "订单编号"      TEXT,
      "订单来源"      TEXT,
      "淘宝闪购订单编号" TEXT,
      "三方订单id"    TEXT,
      "城市"          TEXT,
      "商户名称"      TEXT,
      "门店ID"        TEXT,
      "淘宝闪购商家ID" TEXT,
      "配送方式"      TEXT,
      "订单类型"      TEXT,
      "订单状态"      TEXT,
      "退款状态"      TEXT,
      "退款时间"      TEXT,
      "退款金额"      TEXT,
      "订单无效理由"  TEXT,
      "下单时间"      TEXT,
      "预计送达时间"  TEXT,
      "商户接单时间"  TEXT,
      "订单完成时间"  TEXT,
      "订单总金额"    TEXT,
      "用户实付金额"  TEXT,
      "商户应收金额"  TEXT,
      "商家购物金"    TEXT,
      "平台佣金"      TEXT,
      "索赔状态"      TEXT,
      "商品费用"      TEXT,
      "商品名称"      TEXT,
      "商品分类"      TEXT,
      "商品ID"        TEXT,
      "upc"           TEXT,
      "店内码_货号"   TEXT,
      "商品售价"      TEXT,
      "商品数量"      TEXT,
      "商品优惠金额"  TEXT,
      "商品原始总价"  TEXT,
      "商品成交总价"  TEXT,
      "商品退款金额"  TEXT,
      "商品退款数量"  TEXT,
      "商品退款总价"  TEXT,
      "活动ID"        TEXT,
      "活动名称"      TEXT,
      "活动优惠"      TEXT,
      "活动类型"      TEXT,
      "订单优惠金额"  TEXT,
      "配送费"        TEXT,
      "打包费"        TEXT,
      "打包袋费"      TEXT,
      "顾客联系电话"  TEXT,
      "收货地址"      TEXT,
      "备注"          TEXT,
      "订单取消原因"  TEXT,
      "退款原因"      TEXT,
      "骑手姓名"      TEXT,
      "骑手电话"      TEXT,
      "预计送达开始时间" TEXT,
      "预计送达结束时间" TEXT,
      "商家实收金额"  TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 饿了么-推广 ── */
  {% call statement('create_eleme_promotion', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_eleme_promotion (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _account_type   TEXT,
      "日期"          TEXT,
      "城市"          TEXT,
      "省份"          TEXT,
      "门店ID"        TEXT,
      "门店名称"      TEXT,
      "计划ID"        TEXT,
      "计划名称"      TEXT,
      "资金来源"      TEXT,
      "计划归属"      TEXT,
      "推广产品"      TEXT,
      "推广消费_元"   TEXT,
      "推广现金消费_元" TEXT,
      "曝光提升数"    TEXT,
      "进店提升数"    TEXT,
      "全站交易额_元" TEXT,
      "全站曝光量_次" TEXT,
      "全站进店量_次" TEXT,
      "全站订单量_单" TEXT,
      "推广费比"      TEXT,
      "订单成本_元_单" TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 京东-订单 ── */
  {% call statement('create_jd_order', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_jd_order (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      "订单小号"      TEXT,
      "订单编号"      TEXT,
      "预计送达结束时间" TEXT,
      "订单总金额"    TEXT,
      "用户应付金额"  TEXT,
      "门店应收金额"  TEXT,
      "优惠金额"      TEXT,
      "门店收单时间"  TEXT,
      "妥投时间"      TEXT,
      "门店ID"        TEXT,
      "门店名称"      TEXT,
      "订单取消原因"  TEXT,
      "拣货完成时间"  TEXT,
      "承运商"        TEXT,
      "运单号"        TEXT,
      "订单备注"      TEXT,
      "京东到家sku"   TEXT,
      "商家sku"       TEXT,
      "UPC"           TEXT,
      "商品名称"      TEXT,
      "商品数量"      TEXT,
      "商品一级分类"  TEXT,
      "商品二级分类"  TEXT,
      "商品三级分类"  TEXT,
      "商品价格"      TEXT,
      "IMEI"          TEXT,
      "收货人姓名"    TEXT,
      "成交时间"      TEXT,
      "预计送达开始时间" TEXT,
      "订单状态"      TEXT,
      "支付方式"      TEXT,
      "商品优惠金额"  TEXT,
      "运费优惠金额"  TEXT,
      "拣货人"        TEXT,
      "接单时间"      TEXT,
      "是否是商家会员" TEXT,
      "订单类型"      TEXT,
      "支付渠道"      TEXT,
      "是否拼团订单"  TEXT,
      "是否处方药订单" TEXT,
      "处方药来源"    TEXT,
      "电子处方图片"  TEXT,
      "患者姓名"      TEXT,
      "患者性别"      TEXT,
      "患者身份证号"  TEXT,
      "联系方式_手机号" TEXT,
      "监护人姓名"    TEXT,
      "监护人身份证号" TEXT,
      "患者现住址"    TEXT,
      "当前体温"      TEXT,
      "健康码截图url" TEXT,
      "核酸证明报告截图url" TEXT,
      "行程码截图url" TEXT,
      "用药人两周内旅居史及病症情况" TEXT,
      "接触过来自海外及其他高危地区的发热患者" TEXT,
      "不清楚"        TEXT,
      "发热"          TEXT,
      "咳嗽"          TEXT,
      "胸闷"          TEXT,
      "其他"          TEXT,
      "来源单号"      TEXT,
      "是否医保订单"  TEXT,
      "处方单图片url" TEXT,
      "订单来源"      TEXT,
      "咨果订单号"    TEXT,
      "医保渠道支付单号" TEXT,
      "医保自付金额"  TEXT,
      "个账支付金额"  TEXT,
      "医保统筹支付金额" TEXT,
      "是否国家补贴"  TEXT,
      "是否上门激活"  TEXT,
      "SN码"          TEXT,
      "国补审核照片链接" TEXT,
      "国补审核结果"  TEXT,
      "IMEI_1"        TEXT,
      "IMEI_2"        TEXT,
      "国补UPC码"     TEXT,
      "sn上传"        TEXT,
      "撞库结果"      TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 京东-账单 ── */
  {% call statement('create_jd_finance', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_jd_finance (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      "到家业务单号"  TEXT,
      "业务类型"      TEXT,
      "商家编号"      TEXT,
      "门店编号"      TEXT,
      "skuId"         TEXT,
      "sku名称"       TEXT,
      "sku数量"       TEXT,
      "商品售价"      TEXT,
      "费用类型"      TEXT,
      "结算金额"      TEXT,
      "京东承担金额"  TEXT,
      "商家承担金额"  TEXT,
      "商家承担比例"  TEXT,
      "优惠ID"        TEXT,
      "优惠名称"      TEXT,
      "upc"           TEXT,
      "下单时间"      TEXT,
      "完成时间"      TEXT,
      "账期时间"      TEXT,
      "结算单id"      TEXT,
      "钱包结算状态"  TEXT,
      "钱包"          TEXT,
      "秒送单号"      TEXT,
      "秒送skuid"     TEXT,
      "秒送sku名称"   TEXT,
      "秒送upc"       TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 麦芽田 ── */
  {% call statement('create_maiyatian', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_maiyatian_delivery (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_name     TEXT,
      "流水号"        TEXT,
      "来源平台"      TEXT,
      "平台店铺"      TEXT,
      "状态"          TEXT,
      "原流水号"      TEXT,
      "订单编号"      TEXT,
      "是否预约"      TEXT,
      "下单日期"      TEXT,
      "期望送达"      TEXT,
      "预计发货时间"  TEXT,
      "完成时间"      TEXT,
      "备注"          TEXT,
      "收货人"        TEXT,
      "收货人电话"    TEXT,
      "地址"          TEXT,
      "订单总金额"    TEXT,
      "商家实收金额"  TEXT,
      "配送门店"      TEXT,
      "距离"          TEXT,
      "配送平台"      TEXT,
      "配送单号"      TEXT,
      "骑手姓名"      TEXT,
      "骑手电话"      TEXT,
      "配送费"        TEXT,
      "小费"          TEXT,
      "总配送费"      TEXT,
      "配送距离"      TEXT,
      "配送状态"      TEXT,
      "实际发货时间"  TEXT,
      "用户实付配送金额" TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 美团-商品明细 ── */
  {% call statement('create_meituan_product', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_meituan_product_detail (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_name     TEXT,
      "订单编号"      TEXT,
      "下单时间"      TEXT,
      "订单完成时间"  TEXT,
      "商家名称"      TEXT,
      "商家ID"        TEXT,
      "商家所在城市"  TEXT,
      "订单支付类型"  TEXT,
      "订单状态"      TEXT,
      "订单配送状态"  TEXT,
      "是否预订单"    TEXT,
      "商品分类"      TEXT,
      "商品名称"      TEXT,
      "UPC码"         TEXT,
      "平台商品SKU码" TEXT,
      "店内码_货号"   TEXT,
      "是否部分退款商品" TEXT,
      "是否活动订单"  TEXT,
      "优惠活动"      TEXT,
      "是否催单"      TEXT,
      "接单时长_s"    TEXT,
      "订单原价交易额_元" TEXT,
      "订单实付交易额_元" TEXT,
      "平台承担活动金额_元" TEXT,
      "商家承担活动金额_元" TEXT,
      "商品销售数量"  TEXT,
      "商品原价交易额_元" TEXT,
      "商品实付交易额_元" TEXT,
      "商品总补贴金额_元" TEXT,
      "商品商家补贴金额_元" TEXT,
      "商品平台补贴金额_元" TEXT,
      "部分退款商品数量" TEXT,
      "部分退款商品金额_元" TEXT,
      "配送费_元"     TEXT,
      "配送时长_min"  TEXT,
      "打包袋费"      TEXT,
      "回复状态"      TEXT,
      "商家回复内容"  TEXT,
      "订单取消原因_仅取消订单" TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 美团-账单明细 ── */
  {% call statement('create_meituan_bill', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_meituan_fin_bill (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_name     TEXT,
      "结算id"        TEXT,
      "入账结算ID"    TEXT,
      "门店id"        TEXT,
      "门店名称"      TEXT,
      "账单日期"      TEXT,
      "账单金额"      TEXT,
      "应开发票金额"  TEXT,
      "结算状态"      TEXT,
      "归属账期"      TEXT,
      "结算日期"      TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 美团-订单明细 ── */
  {% call statement('create_meituan_order', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_meituan_fin_order (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_name     TEXT,
      "门店id"                      TEXT,
      "门店名称"                    TEXT,
      "物理城市"                    TEXT,
      "是否代理订单"                TEXT,
      "是否医保订单"                TEXT,
      "客户id"                      TEXT,
      "结算id"                      TEXT,
      "入账结算id"                  TEXT,
      "交易类型"                    TEXT,
      "交易描述"                    TEXT,
      "用户支付方式"                TEXT,
      "订单序号"                    TEXT,
      "订单号"                      TEXT,
      "下单时间"                    TEXT,
      "完成时间"                    TEXT,
      "退款时间"                    TEXT,
      "订单状态"                    TEXT,
      "结算状态"                    TEXT,
      "账单日期"                    TEXT,
      "归属账期"                    TEXT,
      "应开佣金发票金额"            TEXT,
      "应开配送发票金额"            TEXT,
      "商家应收款"                  TEXT,
      "商品总价"                    TEXT,
      "用户支付配送费"              TEXT,
      "餐盒费"                      TEXT,
      "打包袋"                      TEXT,
      "商品补贴"                    TEXT,
      "配送费补贴"                  TEXT,
      "打包袋补贴"                  TEXT,
      "商家活动总支出"              TEXT,
      "公益捐款"                    TEXT,
      "美团商品补贴"                TEXT,
      "美团配送费补贴"              TEXT,
      "美团打包袋补贴"              TEXT,
      "美团补贴总支出"              TEXT,
      "代理商商品补贴"              TEXT,
      "代理商配送费补贴"            TEXT,
      "代理商打包袋补贴"            TEXT,
      "代理商补贴总支出"            TEXT,
      "佣金"                        TEXT,
      "佣金2"                       TEXT,
      "健康卡费用商家部分"          TEXT,
      "企业版费率"                  TEXT,
      "企业版佣金"                  TEXT,
      "配送方式"                    TEXT,
      "配送服务费"                  TEXT,
      "配送费返利"                  TEXT,
      "基础价格"                    TEXT,
      "距离收费"                    TEXT,
      "时段收费"                    TEXT,
      "品类收费"                    TEXT,
      "重量收费"                    TEXT,
      "特殊日期收费"                TEXT,
      "爆单加价"                    TEXT,
      "用户线上支付金额"            TEXT,
      "用户医保报销金额"            TEXT,
      "用户自付金额"                TEXT,
      "统筹支付金额"                TEXT,
      "商家代金券美团补贴"          TEXT,
      "商家代金券商家补贴"          TEXT,
      "商家代金券用户支付"          TEXT,
      "商家代金券佣金比例"          TEXT,
      "用户支付_医事服务费"         TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 美团-处罚赔付 ── */
  {% call statement('create_meituan_penalty', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_meituan_fin_penalty (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_name     TEXT,
      "门店id"        TEXT,
      "门店名称"      TEXT,
      "物理城市"      TEXT,
      "客户id"        TEXT,
      "结算id"        TEXT,
      "入账结算id"    TEXT,
      "账单日期"      TEXT,
      "归属账期"      TEXT,
      "结算状态"      TEXT,
      "处罚赔付类型"  TEXT,
      "违规原因"      TEXT,
      "订单号"        TEXT,
      "违规单id"      TEXT,
      "应开发票金额"  TEXT,
      "商家应收款"    TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /* ── 美团-健康卡 ── */
  {% call statement('create_meituan_health_card', fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS ods_rpa_meituan_fin_health_card (
      dt              DATE         NOT NULL,
      _source_file    TEXT         NOT NULL,
      _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      _store_name     TEXT,
      "商家ID"        TEXT,
      "商家名称"      TEXT,
      "交易类型"      TEXT,
      "返现完成时间"  TEXT,
      "返现订单号"    TEXT,
      "订单商品实付总金额" TEXT,
      "可报销商品金额" TEXT,
      "不可报销商品金额" TEXT,
      "用户返现金额"  TEXT,
      "健康卡费用商家部分" TEXT
    ) PARTITION BY RANGE (dt);
  {% endcall %}

  /*
    饿了么财务 sheet 表数量多（16张），合并表头复杂
    初次运行 discover_schema.py 后生成完整 DDL 填入此处
    模板:
    CREATE TABLE IF NOT EXISTS ods_rpa_eleme_fin_xxx (
      dt DATE NOT NULL, _source_file TEXT NOT NULL,
      _load_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      _account_type TEXT,
      -- 业务列由 discover_schema.py 生成
    ) PARTITION BY RANGE (dt);
  */

  {{ log("ODS tables created/verified.", info=True) }}

{% endmacro %}


/* 按日创建分区的辅助宏 */
{% macro create_daily_partition(table_name, dt) %}
  {% set partition_name = table_name ~ '_' ~ dt | replace('-', '') %}
  {% call statement('create_partition_' ~ partition_name, fetch_result=False) %}
    CREATE TABLE IF NOT EXISTS {{ partition_name }}
      PARTITION OF {{ table_name }}
      FOR VALUES FROM ('{{ dt }}') TO ('{{ dt }}'::date + 1);
  {% endcall %}
{% endmacro %}
