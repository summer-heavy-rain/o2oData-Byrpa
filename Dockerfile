FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-postgres==1.9.0

COPY dbt/ /app/dbt/

WORKDIR /app/dbt

CMD ["dbt", "run", "--select", "ads_o2o_daily_report"]
