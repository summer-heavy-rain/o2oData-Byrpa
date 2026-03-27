"""
O2O 数据管道日调度入口
流程: SMB 数据入库(ODS) → dbt 刷新(ADS)

用法:
  python -m scripts.run_daily                    # 默认昨天
  python -m scripts.run_daily --date 2026-03-25  # 指定日期
  python -m scripts.run_daily --skip-dbt         # 跳过 dbt（仅入库）
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("daily")

PROJECT_ROOT = Path(__file__).parent.parent
DBT_DIR = PROJECT_ROOT / "dbt"


def run_ingest(dt: date) -> bool:
    log.info(f"===== 阶段 1/2: 数据入库 (ODS) — {dt} =====")
    cmd = [sys.executable, "-m", "scripts.ingest", "--date", str(dt)]
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if result.returncode != 0:
        log.error(f"入库失败 (exit {result.returncode})")
        return False
    log.info("入库完成")
    return True


def run_dbt() -> bool:
    log.info("===== 阶段 2/2: dbt 刷新 (ADS) =====")
    dbt_cmd = ["dbt", "run", "--select", "ads_o2o_daily_report"]
    result = subprocess.run(dbt_cmd, cwd=str(DBT_DIR))
    if result.returncode != 0:
        log.error(f"dbt run 失败 (exit {result.returncode})")
        return False
    log.info("dbt 刷新完成")
    return True


def main():
    parser = argparse.ArgumentParser(description="O2O 日调度")
    parser.add_argument("--date", help="日期 YYYY-MM-DD（默认昨天）")
    parser.add_argument("--skip-dbt", action="store_true", help="跳过 dbt 刷新")
    args = parser.parse_args()

    dt = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)

    ok = run_ingest(dt)
    if not ok:
        sys.exit(1)

    if not args.skip_dbt:
        ok = run_dbt()
        if not ok:
            sys.exit(2)

    log.info("===== 日调度全部完成 =====")


if __name__ == "__main__":
    main()
