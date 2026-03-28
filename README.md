# O2O 数据仓库（o2oData-Byrpa）

O2O 门店日报的数据管道：从各平台 RPA 导出文件 → 入库 → 指标计算 → 日报宽表。

## 快速了解

```
RPA 每天导出各平台数据到共享文件夹
       ↓
scripts/ingest.py 读取文件，写入数据库 ODS 表
       ↓
dbt 计算经营指标，生成日报表 ads_o2o_daily_report
       ↓
union-dashboard 前端页面展示（/o2o-daily）
```

## 项目结构

```
o2oData-Byrpa/
│
│── 数据导入 ──────────────────────────────────────────
├── config/sources.yaml           数据源配置（哪些文件、怎么读）
├── scripts/
│   ├── ingest.py                 主导入脚本（FTP → 数据库）
│   ├── run_daily.py              每日定时执行
│   ├── import_store_ref.py       导入房租水电成本
│   ├── import_meituan_promotion.py  导入美团推广费
│   └── import_dim_o2o_sku_mapping_from_xls.py  导入编码匹配表
│
│── 指标计算（dbt）─────────────────────────────────────
├── dbt/models/
│   ├── staging/                  第一步：原始数据清洗
│   │   ├── stg_meituan_revenue   美团财务清洗
│   │   ├── stg_eleme_revenue     饿了么财务清洗
│   │   └── stg_jd_revenue        京东财务清洗
│   │
│   ├── intermediate/             第二步：各项指标计算 ← ⭐ 同事在这里改
│   │   ├── int_platform_revenue  收入/平台费/配送费/订单数
│   │   ├── int_item_cost         货品成本
│   │   ├── int_item_tickets      票数
│   │   ├── int_maiyatian_shipping 麦芽田运费
│   │   └── int_promotion_cost    推广费
│   │
│   └── marts/                    第三步：汇总出最终日报表
│       └── ads_o2o_daily_report  → 前端页面直接读这张表
│
│── 即席查询 & 维表 ───────────────────────────────────
├── sql/
│   ├── o2o_daily_store_aggregate.sql   手动跑的门店汇总查询
│   ├── ddl_dim_ods_store_operating_cost.sql  固定成本表建表
│   └── seed_dim_store_o2o_*.sql        门店数据补录
│
│── 文档 ─────────────────────────────────────────────
├── docs/
│   ├── 给同事的操作指南.md        ← 同事看这个！
│   ├── FTP_TO_ODS_MAPPING.md     FTP 文件 → 数据库表映射
│   └── CONTRIBUTING.md           协作规范（技术版）
│
│── 配置 ─────────────────────────────────────────────
├── .env / .env.example           数据库连接
├── pyproject.toml                Python 依赖
├── Dockerfile / railway.toml     部署配置
└── .gitignore
```

## 数据覆盖的平台

| 平台 | 数据来源 | ODS 表数量 |
|------|---------|-----------|
| 迷你橙 | mini橙订单明细（直营+加盟） | 1 张 |
| 饿了么 | 订单 + 推广 + 财务（16个Sheet） | 18 张 |
| 京东 | 订单 + SKU对账单 | 2 张 |
| 麦芽田 | 配送报表（按门店分文件） | 1 张 |
| 美团 | 商品明细 + 账单（4个Sheet） | 5 张 + 推广费 1 张 |

详见 `docs/FTP_TO_ODS_MAPPING.md`。

## 核心指标

```
收入 = 各平台财务结算金额
成本 = mini橙商品编码 × 编码匹配表的总部招商价 × 换算比
招商利润 = 收入 - 成本 - 平台费
净利润 = 招商利润 - 房租人工
运费 / 推广费 = 展示参考，不参与利润扣减
```

## 跑指标计算

```bash
cd dbt
dbt run --profiles-dir .
```

## 分工

- **骆铭源**：数据导入管道 + 原始清洗 + 利润公式
- **同事**：各项指标的计算逻辑（`intermediate/` 目录下的文件）

详见 `docs/给同事的操作指南.md`。
