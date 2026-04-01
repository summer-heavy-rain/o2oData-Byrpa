1.跑通
cd dbt
dbt debug --profiles-dir .    # 检查数据库连接
dbt run --profiles-dir .      # 编译+运行所有模型

>加了 --profiles-dir . 表示当前目录招配置文件 . 表示当前目录

2. 看 DAG 图（理解依赖）
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .

3.只运行某一个 dbt 模型文件
cd dbt
dbt run --select int_test --profiles-dir .

4.直接改 ODS 表结构（ALTER TABLE 之类的）
dbt 不适合干这个。 dbt 的每个 SQL 文件都会被包成 CREATE VIEW AS (...) 或 CREATE TABLE AS (...)，你没法在里面写 ALTER TABLE。