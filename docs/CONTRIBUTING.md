# O2O 数据仓库协作规范

## 项目结构 & 责任矩阵

```
o2oData-Byrpa/
│
├── config/sources.yaml                      # [骆铭源] FTP→ODS 映射配置
│
├── scripts/                                 # [骆铭源] 数据导入管道
│   ├── ingest.py                            #   主导入（读 sources.yaml → ODS）
│   ├── config_loader.py / file_reader.py    #   配置解析
│   ├── import_store_ref.py                  #   房租水电 → dim_o2o_store_ref
│   ├── import_meituan_promotion.py          #   美团推广费
│   ├── import_dim_o2o_sku_mapping_from_xls.py  # [同事] 编码匹配 → dim_o2o_sku_mapping
│   └── run_daily.py                         #   日常调度
│
├── sql/                                     # [同事] 即席查询 & 维表 DDL
│   ├── o2o_daily_store_aggregate.sql        #   ⭐ 同事的即席验证查询（422行，含企客/分摊）
│   ├── ddl_dim_ods_store_operating_cost.sql #   固定成本表 DDL
│   └── seed_dim_store_o2o_*.sql             #   门店补录 SQL
│
├── dbt/models/
│   ├── staging/                             # [骆铭源] 原始数据清洗
│   │   ├── _sources.yml                     #   数据源声明
│   │   ├── stg_meituan_revenue.sql          #   美团财务清洗
│   │   ├── stg_eleme_revenue.sql            #   饿了么财务清洗
│   │   └── stg_jd_revenue.sql               #   京东财务清洗
│   │
│   ├── intermediate/                        # [同事可改] 每个经营指标一个文件
│   │   ├── int_platform_revenue.sql         #   收入/平台费/配送费/订单数
│   │   ├── int_item_cost.sql                #   ⭐ 货品成本计算
│   │   ├── int_item_tickets.sql             #   ⭐ 票数计算
│   │   ├── int_maiyatian_shipping.sql       #   ⭐ 麦芽田运费
│   │   └── int_promotion_cost.sql           #   ⭐ 推广费
│   │
│   └── marts/                               # [骆铭源] 最终宽表
│       ├── ads_o2o_daily_report.sql          #   只做 JOIN + 利润公式
│       └── schema.yml                       #   字段文档
│
├── FTP_TO_ODS_MAPPING.md                    # FTP → ODS 映射文档
├── CONTRIBUTING.md                          # 本文件
└── 经营指标核对.md                            # 指标校验记录
```

---

## 谁可以改什么

### 同事的工作范围（4 个 dbt 文件 + 2 个目录）

| 文件 | 改什么 | 影响什么 |
|------|--------|---------|
| `dbt/models/intermediate/int_item_cost.sql` | 成本怎么算（编码映射 × 总部招商价 × 换算比） | ads 表的 cost 列 |
| `dbt/models/intermediate/int_item_tickets.sql` | 票数怎么算（哪些行项目计数） | ads 表的 item_count 列 |
| `dbt/models/intermediate/int_maiyatian_shipping.sql` | 麦芽田运费怎么算 | ads 表的 shipping 列 |
| `dbt/models/intermediate/int_promotion_cost.sql` | 推广费怎么算 | ads 表的 promotion_cost 列 |
| `sql/` 目录 | 即席查询、维表 DDL、数据补录 | 不影响 dbt 管道 |
| 在线指标审计表格 | 记录改动原因和校验结果 | 文档（表格由团队维护） |

### 同事不要碰的文件

| 文件 | 原因 |
|------|------|
| `dbt/models/staging/stg_*.sql` | 改字段清洗规则会影响所有下游 |
| `dbt/models/staging/_sources.yml` | 改数据源声明会影响整个管道 |
| `dbt/models/marts/ads_o2o_daily_report.sql` | 利润公式，改了全盘数字变 |
| `scripts/ingest.py` | 改了 ODS 入库会全面影响 |
| `config/sources.yaml` | 改了 FTP 解析规则所有数据受影响 |

### 骆铭源的工作范围

所有文件都可以改，重点维护：
- `staging/` — ODS 原始字段清洗
- `marts/` — 利润公式和最终 JOIN
- `scripts/` — 数据导入管道
- `_sources.yml` / `schema.yml` — 表声明

---

## 同事修改指标的标准流程

### Step 1：改对应的 intermediate 文件
每个文件 20-40 行 SQL，打开就能看懂。

### Step 2：本地验证
```bash
cd dbt
dbt run --profiles-dir .
```

### Step 3：查验
```sql
SELECT SUM(revenue)::float, SUM(cost)::float, SUM(net_profit)::float
FROM ads_o2o_daily_report WHERE dt = '2026-03-25';
```

### Step 4：提交
```bash
git checkout -b feature/fix-ticket-count
git add .
git commit -m "修复(票数): 退款订单不计入票数"
git push origin feature/fix-ticket-count
```
然后通知骆铭源 review。

---

## 利润公式（不要改）

```
招商利润 = 收入 - 成本 - 平台费
净利润   = 招商利润 - 房租人工
运费和推广费 = 展示参考列，不参与利润计算
```

## 校验基准（3.25）

| 指标 | 校验值 | 允许误差 |
|------|--------|---------|
| 收入 | ¥81,152 | ≤1% |
| 成本 | ¥53,324 | ≤1% |
| 招商利润 | ¥21,216 | ≤1% |
| 房租人工 | ¥18,623 | 0% |
| 净利润 | ¥2,592 | ≤5% |
