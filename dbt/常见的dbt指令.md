1.跑通
cd dbt
dbt debug --profiles-dir .    # 检查数据库连接
dbt run --profiles-dir .      # 编译+运行所有模型

>加了 --profiles-dir . 表示当前目录招配置文件 . 表示当前目录

2. 看 DAG 图（理解依赖）
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .