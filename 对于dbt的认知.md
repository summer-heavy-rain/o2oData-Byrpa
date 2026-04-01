# 对于 dbt 的认知

> 阿潘的学习笔记，基于 O2O 日报项目的实际使用整理。

---

## 一、dbt 是什么？一句话说清楚

**dbt 是一个"SQL 管理器"**——你写好一堆 SQL 文件，它帮你按正确的顺序发到数据库去执行。

它不导数据、不建原始表、不连 API，**只管"加工"这一步**。

---

## 二、dbt 和数据库的关系

dbt 本身不是数据库，它只是一个"遥控器"。

我们的项目连的是 **Supabase**（云端 PostgreSQL，服务器在韩国首尔），配置在 `dbt/profiles.yml` 里。每次跑 `dbt run`，本地电脑把 SQL 发过去，数据库执行完返回结果，所以你不需要在本地装数据库。

**为什么感觉很快？**

- 数据量小（每天几百条订单）
- 用了连接池（Pooler），省去建立连接的开销
- 大部分模型是视图（View），创建视图几乎瞬间完成

---

## 三、dbt 在 ETL 里的位置：只做 T

ETL = Extract（提取）+ Transform（转换）+ Load（加载）

dbt 走的是 **ELT** 模式——先把数据加载进数据库，再在数据库里做转换：


| 步骤         | 谁做的       | 在我们项目里                                   |
| ---------- | --------- | ---------------------------------------- |
| **E** - 提取 | RPA 机器人   | 从美团/饿了么/京东后台导出 Excel                     |
| **L** - 加载 | Python 脚本 | `scripts/ingest.py` 把 Excel 写入 `ods_`* 表 |
| **T** - 转换 | **dbt**   | staging → intermediate → marts 三层加工      |


**T 的本质就是写 SQL**，dbt 只是帮你管理这些 SQL。

---

## 四、dbt 的底层逻辑：翻译 + 按顺序执行

你写的每个 `.sql` 文件，dbt 会：

1. **翻译**：把 `{{ source(...) }}` 和 `{{ ref(...) }}` 替换成真实的表名
2. **分析依赖**：通过 `{{ ref() }}` 知道谁依赖谁
3. **按顺序执行**：先建被依赖的，再建依赖别人的
4. **包一层**：根据配置包成 `CREATE VIEW AS (...)` 或 `CREATE TABLE AS (...)`

比如你写了：

```sql
FROM {{ source('rpa_o2o', 'ods_rpa_meituan_fin_order') }}
```

dbt 会翻译成：

```sql
FROM "public"."ods_rpa_meituan_fin_order"
```

**就这么简单。新建一个 SQL 文件放进 `models/` 文件夹，不需要注册，`dbt run` 自动识别。**

---

## 五、dbt 分层 vs 传统数仓分层（ODS/DWD/DWS/ADS）

**结论：完全不冲突，只是叫法不同。**

dbt 社区有一套自己的分层惯例，和国内数仓经典分层本质上是同一件事：


| 国内数仓分层             | dbt 社区叫法                               | 职责               | 我们项目里对应的                           |
| ------------------ | -------------------------------------- | ---------------- | ---------------------------------- |
| **ODS**（贴源层）       | **sources**（只声明，不建模）                   | 原始数据原封不动         | `_sources.yml` 声明的 `ods_rpa_`* 表   |
| **DWD**（明细清洗层）     | **staging**（`stg_`*）                   | 类型转换、字段重命名、过滤脏数据 | `stg_meituan_revenue.sql` 等 3 个文件  |
| **DWM/DWS**（中间汇总层） | **intermediate**（`int_`*）              | 业务逻辑聚合、指标计算      | `int_platform_revenue.sql` 等 4 个文件 |
| **ADS**（应用层）       | **marts**（`ads_`* / `fct_*` / `dim_*`） | 最终报表、宽表          | `ads_o2o_daily_report.sql`         |


**DWD 和 staging 有什么区别？** 没有本质区别。都是"把原始数据洗干净"——字段类型转换、空值处理、门店名提取。叫 staging 还是叫 DWD，取决于你跟哪个圈子的人说话。

**如果想改成国内叫法？** 完全可以，只需要在 `dbt_project.yml` 里把文件夹名从 `staging/intermediate/marts` 改成 `dwd/dws/ads`，然后重命名对应文件夹。**不影响任何 SQL 逻辑。** 但目前阿潘已经在 intermediate 下做了不少工作，建议等模型稳定后再统一。

---

## 六、我们项目的三层加工（T 的具体体现）

```
ods_*（原始表，中文字段名，有脏数据）
  ↓
staging/（清洗：统一字段名、去脏数据、类型转换）
  ↓
intermediate/（计算业务指标：收入、成本、推广费、运费）
  ↓
marts/（拼成最终日报宽表，JOIN + 利润公式）
  ↓
ads_o2o_daily_report（阿潘看的就是这张表）
```

---

## 七、`dbt run` 到底发生了什么（三步走）

### 第一步：解析（Parse）—— 画出依赖图

dbt 扫描 `models/` 下所有 `.sql` 文件，通过 `{{ ref() }}` 和 `{{ source() }}` 分析出谁依赖谁，画出一张 DAG（有向无环图）：

```
ods_rpa_meituan_fin_order ──┐
ods_rpa_eleme_fin_*  ───────┤
ods_rpa_jd_finance ─────────┤     (ODS 原始表，dbt 只读不写)
                            ▼
        stg_meituan_revenue (view)  ─┐
        stg_eleme_revenue   (view)  ─┤  staging 层：3 个文件
        stg_jd_revenue      (view)  ─┤
                                     ▼
        int_platform_revenue (view)  ──┐
        int_item_cost        (view)  ──┤  intermediate 层：4 个文件
        int_maiyatian_shipping(view) ──┤
        int_promotion_cost   (view)  ──┤
                                       ▼
              ads_o2o_daily_report (table)   marts 层：1 个文件
```

dbt 自动按拓扑排序：先建没有上游依赖的，再建依赖别人的。你不用操心顺序。

### 第二步：逐个执行 SQL

按 DAG 顺序，dbt 对每个模型在数据库上执行：

- **view 模型** → `CREATE OR REPLACE VIEW 模型名 AS (你写的 SELECT ...)`
- **table 模型** → 先 `DROP TABLE`，再 `CREATE TABLE 模型名 AS (你写的 SELECT ...)`

### 第三步：输出执行结果

终端会打印类似这样的日志：

```
1 of 8 START view model public.stg_meituan_revenue .......... [RUN]
1 of 8 OK   view model public.stg_meituan_revenue .......... [CREATE VIEW in 0.3s]
...
8 of 8 START table model public.ads_o2o_daily_report ........ [RUN]
8 of 8 OK   table model public.ads_o2o_daily_report ........ [SELECT 156 in 1.2s]

Completed successfully. PASS=8 ERROR=0
```

### 对数据库的实际影响（速查）


| 会做的                                    | 不会做的                 |
| -------------------------------------- | -------------------- |
| 创建/替换 7 个 view（staging + intermediate） | 不碰任何 `ods_rpa_*` 原始表 |
| 重建 1 张物理表（ads_o2o_daily_report）        | 不会插入重复数据             |
| **幂等安全**：跑多少次结果一样                      | 不会删除原始数据             |


---

## 八、View 和 Table 的区别


| 配置                     | 数据库里是什么 | 特点                      |
| ---------------------- | ------- | ----------------------- |
| `materialized='view'`  | 视图      | 不存数据，每次查询时实时计算。像"保存的查询" |
| `materialized='table'` | 真实的表    | 数据写入磁盘，查询直接读，速度快        |


我们的 staging 和 intermediate 都是 **view**（不占空间），只有最终报表 `ads_o2o_daily_report` 是 **table**（给下游查询用，更快）。

---

## 九、Table 模式不会重名吗？

不会。`dbt run` 对 table 模式的处理是**先删后建（覆盖式更新）**：

```
每次跑：DROP TABLE → CREATE TABLE → 写入全量结果
```

所以表里永远是最新一次运行的结果。历史数据不会丢，因为 SQL 里没有限制日期——每次都是全量重算所有天的数据。

---

## 十、增量模式（以后再说）

当数据量大到全量重算很慢时（比如跑超过 5 分钟），可以改成 `materialized='incremental'`，只算新增数据，INSERT 进去。

**但增量模式要额外操心很多事：**

- 历史数据被修改了怎么办（退款冲正）
- 数据迟到了怎么办
- 改了计算逻辑后历史数据还是旧的
- 边界条件写错会漏数据或重复

**我们现在数据量小，全量重算几秒搞定，不需要增量。等跑不动了再改。**

---

## 十一、dbt 不能做的事

- **不能 ALTER TABLE**（改表结构）—— 要直接连数据库用 SQL 改
- **不能导入数据**（E 和 L）—— 那是 Python 脚本的事
- **不带调度功能**—— 定时执行要靠 Windows 计划任务 / cron / Airflow
- **不适合写临时查询**—— 查数据直接用 DBeaver 或 psql 连数据库

---

## 十二、dbt 真正的价值（三个字：省心）


| 优势       | 说人话                                                 |
| -------- | --------------------------------------------------- |
| **依赖管理** | 8 个 SQL 文件自动按正确顺序跑，不用你记                             |
| **版本控制** | SQL 都在 git 里，改了什么、谁改的、能回滚                           |
| **一键重建** | `dbt run` 一条命令，从清洗到日报全部更新                           |
| **逻辑集中** | 每个指标一个文件，想知道"收入怎么算"去 `int_platform_revenue.sql` 看就行 |


如果没有 dbt，你每天出日报要手动按顺序跑 8 个 SQL，顺序错了就报错或算错。8 个还能记住，以后 30 个、50 个呢？

---

## 十三、和 Python 建数仓的对比

同事在 `union-agent` 项目里用 Python 建数仓，主要问题是**业务逻辑散落在四五个地方**：


| 逻辑藏在哪               | 风险        |
| ------------------- | --------- |
| sync 脚本里的 Python 代码 | 改了这里忘了那里  |
| API 路由里的 SQL 字符串    | 同一个指标多处定义 |
| 物化视图里的 SQL          | 口径不一致     |
| 计算脚本里的 Python 循环    | 数字对不上     |


**dbt 的好处：一个指标 = 一个 SQL 文件 = 一个地方。改了就是改了，不会遗漏。**

---

## 十四、常用命令速查

```bash
# 跑全部模型
dbt run --profiles-dir .

# 只跑某一个模型
dbt run --select int_platform_revenue --profiles-dir .

# 编译 SQL（不执行，只看翻译结果）
dbt compile --select 模型名 --profiles-dir .

# 验证日报数字
# 在数据库客户端执行：
SELECT SUM(revenue), SUM(cost), SUM(gross_profit), SUM(net_profit)
FROM ads_o2o_daily_report WHERE dt = '2026-03-25';
```

---

## 十五、快速上手路径（新人三步走）

```bash
# 1. 检查数据库连接是否通
dbt debug --profiles-dir .

# 2. 跑全部模型（编译 + 执行）
dbt run --profiles-dir .

# 3. 生成交互式血缘图（浏览器自动打开）
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

第三步会在浏览器打开一个可交互的 **Lineage Graph（血缘链路图）**，能看到每个模型的上下游依赖关系。这是理解整个项目最快的方式。

---

## 十六、阿潘能改什么、不能改什么

**能改：** `dbt/models/intermediate/` 下的文件（业务指标计算）

**不能碰：** staging（清洗层）、marts（最终报表）、scripts（导入脚本）、config（配置）

**需要骆铭源来改的：** ODS 表结构变更、利润公式修改、新增数据源、定时调度配置

---

## 十七、dbt 项目的文件夹结构（`dbt_project.yml` 字段详解）

`dbt_project.yml` 是项目的"户口本"，告诉 dbt 项目叫什么、东西放在哪。

```yaml
name: o2o_data_warehouse        # 项目名称
version: "1.0.0"                # 版本号，纯标记
config-version: 2               # 配置语法版本，固定写 2
profile: o2o_dw                 # 去 profiles.yml 找这个名字的连接配置
```

下面这些字段定义了**各类文件放在哪个文件夹**：


| 字段               | 指向的文件夹      | 放什么                  | 我们项目有没有 |
| ---------------- | ----------- | -------------------- | ------- |
| `model-paths`    | `models/`   | SQL 模型文件（核心）         | ✅ 有，主力  |
| `macro-paths`    | `macros/`   | 可复用的 SQL 片段          | ✅ 有     |
| `test-paths`     | `tests/`    | 自定义数据测试              | ❌ 还没建   |
| `seed-paths`     | `seeds/`    | 手工维护的 CSV 小表         | ❌ 还没建   |
| `analysis-paths` | `analyses/` | 分析用 SQL（只编译不执行）      | ❌ 还没建   |
| `target-path`    | `target/`   | 编译产物输出目录             | ✅ 自动生成  |
| `clean-targets`  | —           | `dbt clean` 时会删哪些文件夹 | —       |


**没建的文件夹不影响运行**，dbt 找不到就跳过，等需要用的时候再建。

最后的 `models:` 部分是**按文件夹统一设置模型属性**：

```yaml
models:
  o2o_data_warehouse:
    staging:
      +materialized: view      # staging 层：建成视图
      +schema: public
    intermediate:
      +materialized: view      # intermediate 层：建成视图
      +schema: public
    marts:
      +materialized: table     # marts 层：建成物理表（最终报表要落地存储）
      +schema: public
```

---

## 十八、数据源声明 `_sources.yml` 字段详解

`_sources.yml` 告诉 dbt："数据库里已经有这些表了，不是 dbt 建的，但 dbt 要引用它们。"

```yaml
sources:
  - name: rpa_o2o                    # 数据源名称，SQL 里 {{ source('rpa_o2o', '表名') }} 的第一个参数
    description: "..."               # 给人看的说明，显示在 dbt docs 文档里
    schema: public                   # 这些源表在数据库的哪个 schema
    loader: "scripts/ingest.py"      # 谁负责灌数据的，纯文档标注，不影响运行
    loaded_at_field: "_load_time"    # 用哪个字段判断数据加载时间
    freshness:
      warn_after: { count: 2, period: day }   # 超过 2 天没更新 → 警告
      error_after: { count: 3, period: day }  # 超过 3 天没更新 → 报错
```

声明了 source 之后，dbt 就能追踪**从源表到最终报表的完整数据血缘链路**。

---

## 十九、Jinja 模板语言

你在 dbt 的 SQL 文件里看到的 `{{ }}` 和 `{% %}` 不是 SQL 语法，而是 **Jinja**——Python 生态里的模板语言，专门在文本里"挖坑填值"。

```sql
-- {{ }} 输出表达式，编译时替换成实际值
SELECT * FROM {{ source('rpa_o2o', 'ods_rpa_meituan_fin_order') }}
-- 编译后 → SELECT * FROM "public"."ods_rpa_meituan_fin_order"

-- {% %} 逻辑控制，可以写 if/else、循环
{% if target.name == 'prod' %}
  WHERE dt >= '2026-01-01'
{% endif %}
```

**一句话：Jinja 让 SQL 变得可编程。** dbt 先用 Jinja 引擎把模板翻译成纯 SQL，再发给数据库执行。

---

## 二十、SQL 的"编译"与"执行"有什么区别

在 dbt 的语境下：


| 阶段              | 做了什么                                            | 对数据库有影响吗     |
| --------------- | ----------------------------------------------- | ------------ |
| **编译（Compile）** | 把 Jinja 语法（`{{ ref() }}`、`{% if %}`）替换成纯 SQL 文本 | ❌ 不碰数据库      |
| **执行（Run）**     | 把编译好的 SQL 发给数据库，创建视图或表                          | ✅ 数据库里会建/改东西 |


```bash
# 只编译，看翻译后的 SQL 长什么样（安全，不执行）
dbt compile --select int_platform_revenue --profiles-dir .

# 编译 + 执行（会真正建视图/表）
dbt run --profiles-dir .
```

编译产物在 `target/compiled/` 文件夹里，你可以打开看 dbt 到底生成了什么 SQL。

这和高级语言的编译/执行概念类似：**编译 = 翻译成机器能懂的语言，执行 = 真正跑起来。** 只不过 SQL 的"编译"是把 Jinja 模板翻译成纯 SQL，"执行"是把纯 SQL 发给数据库。

---

## 二十一、`target/` 目录里有什么

`target/` 是 dbt 自动管理的输出目录，你不需要手动碰里面的文件：


| 文件/文件夹             | 作用                                   |
| ------------------ | ------------------------------------ |
| `compiled/`        | 编译后的纯 SQL（Jinja 已替换，但还没发给数据库）        |
| `run/`             | 实际执行的 SQL（包含 CREATE VIEW/TABLE 等语句）  |
| `manifest.json`    | 项目"全景图"——所有模型、依赖关系、配置的完整描述           |
| `run_results.json` | 上次 `dbt run` 的执行结果（每个模型耗时、成功/失败）     |
| `catalog.json`     | `dbt docs generate` 产生的数据目录（字段名、类型等） |
| `index.html`       | `dbt docs serve` 的网页入口               |


`dbt clean --profiles-dir .` 会清空整个 `target/` 文件夹。

---

## 二十二、`--profiles-dir .` 与 `profiles.yml` 的位置

dbt 默认去**用户主目录**找配置文件：

```
Windows 默认位置：C:\Users\你的用户名\.dbt\profiles.yml
```

但我们的 `profiles.yml` 放在了**项目目录** `E:\o2oData-Byrpa\dbt\` 里（好处：配置跟着项目走，团队协作更方便），所以每次跑命令都要加 `--profiles-dir .`，告诉 dbt "配置文件在当前目录"。

```
不加 --profiles-dir .  →  dbt 去 C:\Users\Lenovo\.dbt\ 找  →  找不到  →  报错
加了 --profiles-dir .  →  dbt 在当前目录找  →  找到了  →  正常运行
```

**嫌每次打太麻烦？** 可以设置环境变量 `DBT_PROFILES_DIR=E:\o2oData-Byrpa\dbt`，之后就不用加了。

---

## 二十三、Schema（模式）是什么

Schema 翻译为"模式"或"架构"，但最直观的理解是**数据库里的"文件夹"**：

```
PostgreSQL 服务器
  └── 数据库 (Database)     ← postgres（我们的）
        └── 模式 (Schema)   ← public（我们的，默认就有）
              ├── 表 (Table)   ← ods_rpa_meituan_fin_order
              ├── 表 (Table)   ← ads_o2o_daily_report
              └── 视图 (View)  ← stg_meituan_revenue
```

- 每个数据库默认有一个 `public` schema
- 一张表的完整地址是 `postgres.public.ods_rpa_meituan_fin_order`（数据库.模式.表名），因为 `public` 是默认的，平时可以省略
- 可以建多个 schema 来隔离数据（比如 `raw`、`analytics`、`archive`），我们目前全放 `public`，够用

---

## 二十四、Seeds、Tests、Macros、Analyses 四个辅助功能

### Seeds（种子数据）

把 CSV 文件放到 `seeds/` 文件夹，运行 `dbt seed --profiles-dir .`，dbt 自动在数据库里建表并灌入数据。

适合：门店对照表、SKU 编码映射表等**变化不频繁、由人手工维护的小数据**。

**重复运行不会重复插入**——每次都是清空再全量写入，安全。

我们项目里的 `dim_o2o_sku_mapping`（编码匹配表）和 `dim_o2o_store_ref`（门店房租表）就适合用 seed 管理。

### Tests（数据测试）

检查数据质量的断言，比如"主键不能重复"、"金额不能为负"。

```yaml
# 在 schema.yml 里写（内置测试）
columns:
  - name: order_id
    tests:
      - unique        # 不能重复
      - not_null      # 不能为空
```

```sql
-- 或者在 tests/ 文件夹里写自定义测试 SQL
-- 返回有结果 = 测试失败
SELECT * FROM {{ ref('ads_o2o_daily_report') }}
WHERE revenue < 0
```

运行 `dbt test --profiles-dir .` 执行所有测试。

### Macros（宏）

可复用的 SQL 片段，类似"函数"。写一次，到处调用。

```sql
-- macros/cents_to_yuan.sql
{% macro cents_to_yuan(column_name) %}
  ROUND({{ column_name }}::numeric / 100, 2)
{% endmacro %}

-- 在模型里调用
SELECT {{ cents_to_yuan('amount') }} AS amount_yuan
```

我们项目的 `macros/create_ods_tables.sql` 就是用宏来批量生成建表语句的。

### Analyses（分析查询）

放在 `analyses/` 文件夹里的 SQL，dbt 只编译不执行。适合临时分析、数据探查，编译后的 SQL 可以拿去数据库客户端手动跑。

---

## 二十五、`dbt debug` 输出解读

`dbt debug` 是 dbt 的"体检命令"，检查配置和连接是否正常。输出关键看这几项：

```
profiles.yml file [OK found and valid]     ← 配置文件找到了，格式正确
dbt_project.yml file [OK found and valid]  ← 项目文件找到了，格式正确
Connection test: [OK connection ok]        ← 数据库连上了
All checks passed!                         ← 全部通过，可以正常使用
```

**改了配置后第一件事就是跑 `dbt debug --profiles-dir .`**，确认连接没断。

---

## 二十六、dbt 比"纯 SQL + GitHub"强在哪

你可能会想：纯 SQL 文件也能上传 GitHub 做版本管理啊，为什么还要用 dbt？

答案是：**git 管的是"代码的版本"，dbt 管的是"代码之间的关系和执行方式"。两者结合才完整。**

### 纯 SQL 的痛点

假设不用 dbt，你自己写了 8 个 SQL 文件算日报：

```
01_清洗美团.sql → 02_清洗饿了么.sql → ... → 08_拼日报.sql
```

推到 GitHub 了。新同事接手后会问：


| 他的问题                 | 纯 SQL 的答案             | dbt 的答案                               |
| -------------------- | --------------------- | ------------------------------------- |
| 这 8 个文件按什么顺序跑？       | 看文件名编号猜？问你？           | `{{ ref() }}` 自动算依赖，`dbt run` 一条命令全跑完 |
| 04 依赖 01、02、03，怎么知道？ | 打开文件看 FROM 后面写了什么，自己推 | dbt 自动解析出依赖图（DAG）                     |
| 跑之前要建表/视图吗？          | 要（见下面解释）              | 不用，dbt 自动 CREATE                      |
| 改了 01，哪些下游会受影响？      | 全局搜索表名，可能漏            | `dbt ls --select 模型名+` 一键看所有下游        |
| 换数据库环境怎么办？           | 手动改 SQL 里的 schema 名   | `profiles.yml` 切换 target，SQL 不用动      |


### "跑之前要手动建表"是什么意思

一个纯 SQL 文件通常只有 `SELECT`——查出来显示在屏幕上就没了。如果你想让结果**保存下来**给下游用，你得自己手动包一层：

```sql
-- 纯 SQL：你必须自己写 CREATE
CREATE VIEW revenue_summary AS (
  SELECT store_name, SUM(revenue) FROM ...
);
```

而且你得自己判断：该用 VIEW 还是 TABLE？第二次跑要不要先 DROP？忘了 DROP 就报"已存在"错误。

**dbt 帮你省了这些。** 你只写 SELECT 逻辑，dbt 根据 `materialized` 配置自动包成 `CREATE VIEW AS (...)` 或 `CREATE TABLE AS (...)`，还自动处理先删后建。

### 一句话总结

纯 SQL + GitHub = 存了一堆**独立的文本文件**。dbt + GitHub = 存了一套**可一键执行的数据加工流水线**（含逻辑、依赖、执行方式、数据源声明、测试规则）。新同事 clone 下来，`dbt run` 一条命令就能跑通。

---

## 二十七、`config/sources.yaml` 不是 dbt 的文件

项目根目录下的 `config/sources.yaml` 容易和 dbt 的 `_sources.yml` 搞混，但它们完全不同：


| 文件                                | 归谁管                | 干什么的                             |
| --------------------------------- | ------------------ | -------------------------------- |
| `config/sources.yaml`             | Python 入库脚本（骆铭源写的） | 告诉 `ingest.py` 去哪读 Excel、怎么写入数据库 |
| `dbt/models/staging/_sources.yml` | dbt                | 告诉 dbt 数据库里已有哪些源表可以引用            |


```
config/sources.yaml    →  scripts/ingest.py  →  ods_* 表（数据进数据库）
                                                      ↓
dbt/_sources.yml       →  dbt models/        →  加工后的表（数据在数据库里变形）
```

前者管"数据怎么进来"，后者管"数据进来后怎么加工"。`config/sources.yaml` 属于骆铭源负责的范围。

---

## 二十八、dbt 的 YAML 文件是固定语法，不是随便写的

你在 `_sources.yml`、`dbt_project.yml`、`profiles.yml` 里看到的字段名，**全部是 dbt 官方规定好的**，不能自己编。

类比：就像填一张**官方表格**——栏目名称是固定的，你只负责填内容。

```yaml
sources:
  - name: rpa_o2o            # "name" 是 dbt 规定的字段名，"rpa_o2o" 是你填的值
    schema: public           # "schema" 是 dbt 规定的字段名，"public" 是你填的值
    freshness:               # "freshness" 是 dbt 规定的字段名
      warn_after:            # "warn_after" 是 dbt 规定的字段名
        count: 2             # "count" 是 dbt 规定的字段名，"2" 是你填的值
        period: day          # "period" 是 dbt 规定的字段名，"day" 是你填的值
```

写了 dbt 不认识的字段名会报错或被忽略。想知道有哪些字段可用，查 dbt 官方文档。

---

## 二十九、`freshness` 是自动报警规则，不是每天要维护的东西

```yaml
freshness:
  warn_after: { count: 2, period: day }   # 数据超过 2 天没更新 → 黄灯警告
  error_after: { count: 3, period: day }  # 数据超过 3 天没更新 → 红灯报错
```

### 它怎么工作

1. 每张源表入库时自动打了时间戳（`_load_time` 字段）
2. 运行 `dbt source freshness --profiles-dir .` 时，dbt 自动查每张表 `_load_time` 的最大值
3. 用当前时间减去最大值 = "数据有多久没更新了"
4. 超过阈值就在终端里显示 WARN 或 ERROR

### 三个关键点

- **写一次就不用管了**，不需要每天维护，就像设闹钟一样
- **不会主动通知你**（不会发微信、不会弹窗），只有你手动跑命令才能看到结果
- **可选功能**，删掉这三行也不影响 `dbt run`，现阶段可以先不管

### 想要自动通知怎么办？

那不是 dbt 的事了，需要额外搭定时脚本或调度平台（Airflow / dbt Cloud），等项目稳定后让骆铭源接。

---

## 三十、YAML 的行内写法 vs 展开写法

你在 `_sources.yml` 里看到的 `{ count: 2, period: day }` 不是注释，是**真正生效的代码**。

YAML 有两种写法，效果完全一样：

```yaml
# 行内写法（紧凑，一行搞定）
warn_after: { count: 2, period: day }

# 展开写法（更清晰）
warn_after:
  count: 2
  period: day
```

YAML 里真正的注释是 `#` 号开头的内容，井号后面的文字才会被忽略。

---

## 三十一、dbt 里什么是自动生成的、什么是人写的


| 类型                | 例子                                              | 谁写的          |
| ----------------- | ----------------------------------------------- | ------------ |
| 模型 SQL 文件         | `stg_meituan_revenue.sql`、`int_item_cost.sql`   | 人手动写         |
| 配置 YAML 文件        | `_sources.yml`、`dbt_project.yml`、`profiles.yml` | 人手动写         |
| `target/` 文件夹里的一切 | `compiled/`、`manifest.json`、`run_results.json`  | **dbt 自动生成** |


**简单记：你写的东西在 `models/`、`macros/` 和项目根目录，dbt 生成的东西全在 `target/` 里。**

---

## 三十二、`scripts/` 文件夹和 dbt 没有关系

`scripts/` 是骆铭源写的 Python 脚本，负责 dbt **之前**的工作（把 Excel 数据灌进数据库）：

| 脚本 | 干什么的 |
|---|---|
| `ingest.py` | 核心入库脚本，读 Excel 写入 `ods_*` 表 |
| `file_reader.py` | 读取文件的辅助工具 |
| `config_loader.py` | 读取 `config/sources.yaml` 配置 |
| `import_dim_o2o_sku_mapping_from_xls.py` | 导入 SKU 编码匹配表 |
| `import_store_ref.py` | 导入门店信息表 |
| `import_rent_labor.py` | 导入房租人工数据 |
| `import_meituan_promotion.py` | 导入美团推广费数据 |
| `discover_schema.py` | 探测 Excel 的列结构，辅助生成建表语句 |
| `run_daily.py` / `run_daily.bat` | 每日批量运行脚本 |

```
scripts/（Python）              dbt/（SQL）
────────────────                ──────────
Excel → ingest.py → ods_* 表 → staging → intermediate → marts
     数据进数据库                      数据在数据库里加工
```

这些脚本属于骆铭源的维护范围，阿潘不需要改。

---

## 三十三、`dbt run-operation`——手动触发宏

### 和 `dbt run` 的区别

| | `dbt run` | `dbt run-operation` |
|---|---|---|
| 执行什么 | `models/` 里的 SQL 模型 | `macros/` 里的某个宏 |
| 自动管依赖 | 是（按 DAG 顺序） | 否（直接执行） |
| 自动包 CREATE VIEW/TABLE | 是 | 否（你写什么就执行什么） |
| 什么时候用 | 每天跑数据加工 | 一次性操作（建表、数据迁移等） |

### 我们项目的实际例子

`macros/create_ods_tables.sql` 里打包了 10 条 `CREATE TABLE IF NOT EXISTS` 建表语句。

结构其实很简单：

```sql
{% macro create_ods_tables() %}         -- 宏的名字

  {% call statement(...) %}             -- 告诉 dbt "把这段 SQL 发给数据库执行"
    CREATE TABLE IF NOT EXISTS ...      -- 普通建表 SQL
  {% endcall %}

  ... 重复 10 次，每张 ODS 表一段 ...

{% endmacro %}
```

运行方式：

```bash
dbt run-operation create_ods_tables --profiles-dir .
```

**相当于一个"初始化按钮"**——新环境跑一次，10 张表全部建好，之后就不用管了。`IF NOT EXISTS` 保证重复跑也不会出错。

---

## 三十四、ODS 建表语句由谁维护最合理

### 核心原则：谁负责数据入库，谁维护建表语句

因为表结构必须和入库数据对得上。在我们团队里就是**骆铭源**。

### 方案从低到高

| 等级 | 方案 | 做法 | 问题 |
|---|---|---|---|
| 最差 | Python 里直接建表 | `ingest.py` 读 Excel 列名自动 `CREATE TABLE` | 建表逻辑藏在代码里，不翻代码看不到表结构 |
| 基础 | 独立 SQL 文件 | 手写 `CREATE TABLE`，手动到数据库执行 | 能 git 管理了，但要手动跑，容易忘 |
| **✅ 现在** | **dbt 宏建表** | DDL 写在 `macros/create_ods_tables.sql`，一键执行 | 有版本管理、可重复执行、和项目在一起 |
| 更好 | 配置驱动建表 | 表结构定义在 YAML 里，程序自动生成 DDL | 数据源配置和表结构不会脱节 |
| 最高 | 专业加载工具 | 用 dlt / Meltano 等工具，自动推断列、自动建表 | 引入新工具有学习成本 |

我们已经升级到了**配置驱动建表**方案（见三十五章）。

---

## 三十五、配置驱动建表——我们的新方案

### 改了什么

之前表结构散落在三个地方，现在统一到一个 YAML 文件：

| 之前（散落） | 问题 |
|---|---|
| `ingest.py` 自动建表 | 建表逻辑藏在 Python 代码里，不翻代码看不到表结构 |
| `create_ods_tables.sql` dbt 宏建表 | 写了但被 Python 抢先，相当于文档 |
| `_sources.yml` dbt 声明 | 还要手动再写一遍 |

| 现在（统一） | 作用 |
|---|---|
| `config/table_schemas.yaml` | 唯一权威来源（编辑这里） |
| `scripts/sync_schema.py` | 读 YAML → 自动建表/加列 + 生成 _sources.yml |
| `ingest.py` | 不再自动建表，表不存在直接报错 |

### 常用命令

```bash
python -m scripts.sync_schema              # 同步所有表到数据库 + 更新 _sources.yml
python -m scripts.sync_schema --dry-run    # 只打印 SQL，不执行
python -m scripts.sync_schema --discover   # 从数据库发现 columns: [] 的表的列结构
```

### YAML 格式速查

```yaml
# 普通 ODS 表（业务列全 TEXT，元数据列自动添加）
ods_rpa_xxx:
  description: "表的说明"
  dbt_source: rpa_o2o
  extra_meta: [_store_name]    # 额外的元数据列（可选）
  columns:
    - 订单编号
    - 门店名称

# 自定义类型表（手动指定每列的类型）
dim_xxx:
  description: "维度表"
  dbt_source: o2o_ref
  custom_ddl: true
  primary_key: [id_column]
  columns:
    - { name: id_column, type: TEXT }
    - { name: amount, type: "NUMERIC(10,2)" }
```

### 安全设计

- `CREATE TABLE IF NOT EXISTS` — 表已存在就跳过
- `ALTER TABLE ADD COLUMN` — 只加列，不改/不删
- 删列不自动做，只警告（防止误删数据）
- `ingest.py` 不再自动建表，表不存在会报错并提示先跑 sync_schema

---

## 三十六、表结构变更的实操速查表

| 操作 | 怎么做 | 需要手动 SQL 吗 |
|---|---|---|
| **加列** | YAML 里加一行列名 → `python -m scripts.sync_schema` | 不需要，全自动 |
| **加表** | YAML 里加一段表定义 → `python -m scripts.sync_schema` | 不需要，全自动 |
| **改列名** | 数据库手动 `RENAME COLUMN` + 改 YAML | 需要（三处必须同步改） |
| **删列** | 数据库手动 `DROP COLUMN` + 改 YAML | 需要（sync_schema 只警告不执行） |
| **删表** | 数据库手动 `DROP TABLE` + 删 YAML → 跑 sync_schema 更新 _sources.yml | 需要 |

**设计原则："加"的操作全自动，"删/改"的操作需要人工确认。** 因为加是安全的，删/改可能丢数据。

### 改列名为什么很少做

ODS 层的列名跟着 Excel 表头走——RPA 导出什么名字就存什么名字。不会主动去改它。如果确实要改列名，必须三个地方同步：数据库（RENAME COLUMN）、table_schemas.yaml、RPA 导出的 Excel 表头。漏了任何一个都会导致数据写入报错。

---

## 三十七、项目文件的依赖关系与执行顺序

### 配置文件之间的依赖关系

```
                    ┌─────────────────────────────┐
                    │  config/table_schemas.yaml   │ ← 表结构定义（编辑这里）
                    └──────────┬──────────────────┘
                               │
                    sync_schema.py 读取它
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
      远程数据库建表      _sources.yml        ingest.py
    CREATE TABLE /      （自动生成，         （依赖表已存在，
      ADD COLUMN         告诉 dbt            表不存在报错）
                         有哪些源表）
              │                │                │
              ▼                ▼                ▼
         表结构就绪       dbt 能引用源表     Excel 数据写入表
```

与 `config/sources.yaml` 是**平行关系**——两个文件分工不同：

| 文件 | 管什么 |
|---|---|
| `config/table_schemas.yaml` | 数据**存到哪**（表结构、列定义） |
| `config/sources.yaml` | 数据**从哪来**（文件路径、解析规则） |

### 日常操作的先后顺序

```
1. 编辑 table_schemas.yaml         ← 只在表结构变更时做
2. python -m scripts.sync_schema   ← 只在表结构变更时做
─────────────────────────────────────────────────
3. python -m scripts.ingest --date 2026-03-25   ← 每天：入库
4. cd dbt && dbt run --profiles-dir .           ← 每天：加工
```

第 1、2 步只在新建表或加列时才需要。日常跑数据只用第 3、4 步。

### `generate_schema_name.sql` 的作用

`macros/generate_schema_name.sql` 是一个 9 行的"修正补丁"。dbt 默认会把 schema 名拼接成 `public_public`，这个宏覆盖了默认行为，让所有模型都直接建在 `public` schema 下。正在生效，不要动它。