"""
Schema 发现工具
读取实际文件 → 生成 dbt 兼容的 ODS DDL SQL

用法:
  python -m scripts.discover_schema --date 2026-03-25
  python -m scripts.discover_schema --date 2026-03-25 --source eleme --output dbt/macros/ods_eleme.sql
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.ingest import process_source
from scripts.config_loader import load_sources


def generate_ddl(table: str, columns: list[str], meta_columns: dict[str, str]) -> str:
    """生成单张 ODS 表的 CREATE TABLE DDL"""
    lines = [f"CREATE TABLE IF NOT EXISTS {table} ("]
    lines.append(f"  dt              DATE         NOT NULL,")
    lines.append(f"  _source_file    TEXT         NOT NULL,")
    lines.append(f"  _load_time      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),")

    for col, col_type in meta_columns.items():
        lines.append(f"  {col:<16s} {col_type},")

    for col in columns:
        safe = f'"{col}"' if not col.startswith('"') else col
        lines.append(f"  {safe:<40s} TEXT,")

    last = lines[-1]
    lines[-1] = last.rstrip(",")
    lines.append(") PARTITION BY RANGE (dt);")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="ODS Schema 发现 → DDL 生成")
    parser.add_argument("--date", required=True, help="采样日期 YYYY-MM-DD")
    parser.add_argument("--source", help="仅处理指定数据源")
    parser.add_argument("--output", help="输出文件路径（默认打印到 stdout）")
    parser.add_argument("--config", help="配置文件路径")
    args = parser.parse_args()

    dt = date.fromisoformat(args.date)
    manifest = load_sources(args.config)

    all_ddl = []
    all_ddl.append("-- Auto-generated ODS DDL by discover_schema.py")
    all_ddl.append(f"-- Sample date: {dt}\n")

    for source in manifest.sources:
        if args.source and source.key != args.source:
            continue

        try:
            results = process_source(source, dt, manifest, discover_only=True)
        except Exception as e:
            all_ddl.append(f"-- ERROR [{source.display_name}]: {e}\n")
            continue

        if not results:
            continue

        for table, dfs in results.items():
            import pandas as pd

            merged = pd.concat(dfs, ignore_index=True)
            biz_cols = [
                c for c in merged.columns if not c.startswith("_") and c != "dt"
            ]

            meta = {}
            if "_store_type" in merged.columns:
                meta["_store_type"] = "TEXT"
            if "_account_type" in merged.columns:
                meta["_account_type"] = "TEXT"
            if "_store_name" in merged.columns:
                meta["_store_name"] = "TEXT"

            ddl = generate_ddl(table, biz_cols, meta)
            all_ddl.append(f"/* {source.display_name} */")
            all_ddl.append(ddl)
            all_ddl.append("")

    output = "\n".join(all_ddl)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"DDL 已写入: {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
