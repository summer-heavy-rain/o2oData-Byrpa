@echo off
REM ============================================
REM O2O 数据管道 - Windows 计划任务入口
REM 每天 11:00 AM 运行（Task Scheduler 配置）
REM ============================================

cd /d "%~dp0.."
echo [%date% %time%] 开始 O2O 数据管道

REM 加载环境变量
if exist .env (
    for /f "usebackq tokens=1,* delims==" %%a in (".env") do (
        if not "%%a"=="" if not "%%a:~0,1%"=="#" set "%%a=%%b"
    )
)

REM 计算昨天的日期
for /f "tokens=1-3 delims=/" %%a in ('powershell -command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"') do set YESTERDAY=%%a

echo [%date% %time%] 处理日期: %YESTERDAY%

REM 阶段 1: 数据入库 (ODS)
echo [%date% %time%] 阶段 1/2: 数据入库
python -m scripts.ingest --date %YESTERDAY%
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] 入库失败！
    exit /b 1
)

REM 阶段 2: dbt 刷新 (ADS)
echo [%date% %time%] 阶段 2/2: dbt 刷新
cd dbt
dbt run --select ads_o2o_daily_report
if %ERRORLEVEL% neq 0 (
    echo [%date% %time%] dbt 刷新失败！
    exit /b 2
)
cd ..

echo [%date% %time%] 日调度完成
