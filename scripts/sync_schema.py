"""
配置驱动的表结构同步工具
从 config/table_schemas.yaml 读取表定义 → 自动建表/加列 + 生成 dbt _sources.yml

用法:
  python -m scripts.sync_schema                     # 同步所有表到数据库
  python -m scripts.sync_schema --dry-run            # 只打印 SQL，不执行
  python -m scripts.sync_schema --discover           # 从数据库发现未声明列的表结构
  python -m scripts.sync_schema --generate-sources   # 重新生成 _sources.yml
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sync_schema")

SCHEMAS_PATH = Path(__file__).parent.parent / "config" / "table_schemas.yaml"
SOURCES_YML_PATH = (
    Path(__file__).parent.parent / "dbt" / "models" / "staging" / "_sources.yml"
)


def load_table_schemas(path: Path | None = None) -> dict:
    path = path or SCHEMAS_PATH
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_column_list(table_def: dict, defaults: dict) -> list[dict]:
    """Build the full column list for a table definition."""
    if table_def.get("custom_ddl"):
        cols = table_def.get("columns", [])
        if not cols:
            return []
        return [c if isinstance(c, dict) else {"name": c, "type": "TEXT"} for c in cols]

    biz_columns = table_def.get("columns", [])
    if not biz_columns:
        return []

    columns = []
    for meta in defaults.get("meta_columns", []):
        columns.append(dict(meta))

    for extra in table_def.get("extra_meta", []):
        columns.append({"name": extra, "type": "TEXT"})

    for col in biz_columns:
        if isinstance(col, str):
            columns.append({"name": col, "type": "TEXT"})
        else:
            columns.append(dict(col))

    return columns


def build_create_table_sql(table_name: str, columns: list[dict], table_def: dict) -> str:
    lines = [f'CREATE TABLE IF NOT EXISTS "{table_name}" (']
    col_lines = []
    for col in columns:
        col_lines.append(f'  "{col["name"]}" {col["type"]}')

    pk = table_def.get("primary_key")
    if pk:
        pk_cols = ", ".join(f'"{c}"' for c in pk)
        col_lines.append(f"  PRIMARY KEY ({pk_cols})")

    lines.append(",\n".join(col_lines))
    lines.append(");")
    return "\n".join(lines)


def get_engine():
    import sqlalchemy

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL 未设置")
        sys.exit(1)
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    return sqlalchemy.create_engine(db_url)


def table_exists(conn, table_name: str) -> bool:
    import sqlalchemy

    result = conn.execute(
        sqlalchemy.text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = :t"
        ),
        {"t": table_name},
    ).fetchone()
    return result is not None


def get_existing_columns(conn, table_name: str) -> set[str]:
    import sqlalchemy

    rows = conn.execute(
        sqlalchemy.text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t"
        ),
        {"t": table_name},
    ).fetchall()
    return {r[0] for r in rows}


def sync_tables(schemas: dict, *, dry_run: bool = False):
    import sqlalchemy

    defaults = schemas.get("defaults", {})
    tables = schemas.get("tables", {})
    engine = get_engine()

    created, altered, skipped = 0, 0, 0

    with engine.begin() as conn:
        for table_name, table_def in tables.items():
            columns = build_column_list(table_def, defaults)
            if not columns:
                log.info(f"  跳过 {table_name}: 未定义列（运行 --discover 填充）")
                skipped += 1
                continue

            exists = table_exists(conn, table_name)

            if not exists:
                sql = build_create_table_sql(table_name, columns, table_def)
                if dry_run:
                    log.info(f"  [DRY-RUN] 将创建表 {table_name}:")
                    for line in sql.split("\n"):
                        log.info(f"    {line}")
                else:
                    conn.execute(sqlalchemy.text(sql))
                    conn.execute(
                        sqlalchemy.text(
                            f'CREATE INDEX IF NOT EXISTS idx_{table_name}_dt '
                            f'ON "{table_name}" (dt)'
                        )
                    )
                    log.info(f"  ✅ 创建表 {table_name} ({len(columns)} 列)")
                created += 1
            else:
                existing_cols = get_existing_columns(conn, table_name)
                yaml_col_names = {c["name"] for c in columns}
                new_cols = []

                for col in columns:
                    if col["name"] not in existing_cols:
                        col_type = "TEXT" if not table_def.get("custom_ddl") else col["type"]
                        alter_sql = (
                            f'ALTER TABLE "{table_name}" '
                            f'ADD COLUMN "{col["name"]}" {col_type}'
                        )
                        if dry_run:
                            log.info(f"  [DRY-RUN] {alter_sql}")
                        else:
                            conn.execute(sqlalchemy.text(alter_sql))
                        new_cols.append(col["name"])

                if new_cols:
                    log.info(f"  ✅ {table_name}: 新增 {len(new_cols)} 列 → {new_cols}")
                    altered += 1
                else:
                    log.info(f"  ✔ {table_name}: 已是最新")

                extra_in_db = existing_cols - yaml_col_names
                if extra_in_db:
                    log.warning(
                        f"  ⚠️ {table_name}: 数据库中有 {len(extra_in_db)} 列未在 YAML 中声明: "
                        f"{sorted(extra_in_db)}"
                    )

    log.info(f"\n{'=' * 50}")
    log.info(
        f"同步完成: 新建 {created} 表, 更新 {altered} 表, "
        f"跳过 {skipped} 表（未定义列）"
    )


def discover_from_db(schemas: dict, *, discover_all: bool = False):
    """从数据库发现表的实际列结构，输出 YAML 片段供粘贴。

    默认只处理 columns: [] 的表。加 --discover-all 处理所有表。
    """
    import sqlalchemy

    defaults = schemas.get("defaults", {})
    tables = schemas.get("tables", {})
    engine = get_engine()

    meta_names = {m["name"] for m in defaults.get("meta_columns", [])}

    with engine.connect() as conn:
        for table_name, table_def in tables.items():
            if not discover_all and table_def.get("columns"):
                continue
            if table_def.get("custom_ddl"):
                continue

            if not table_exists(conn, table_name):
                log.info(f"  {table_name}: 数据库中不存在，跳过")
                continue

            extra_meta = set(table_def.get("extra_meta", []))
            skip = meta_names | extra_meta

            rows = conn.execute(
                sqlalchemy.text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = :t "
                    "ORDER BY ordinal_position"
                ),
                {"t": table_name},
            ).fetchall()
            ordered_biz = [r[0] for r in rows if r[0] not in skip]

            if not ordered_biz:
                log.info(f"  {table_name}: 无业务列")
                continue

            print(f"\n  # --- {table_name} ({len(ordered_biz)} 业务列) ---")
            print(f"    columns:")
            for c in ordered_biz:
                print(f'      - "{c}"')

    log.info("\n将上方输出粘贴到 config/table_schemas.yaml 对应的表定义中。")


def generate_sources_yml(schemas: dict):
    """从 table_schemas.yaml 生成 dbt _sources.yml。"""
    tables = schemas.get("tables", {})

    rpa_tables = []
    ref_tables = []
    for table_name, table_def in tables.items():
        group = table_def.get("dbt_source", "rpa_o2o")
        entry = {"name": table_name, "description": table_def.get("description", "")}
        if group == "rpa_o2o":
            rpa_tables.append(entry)
        elif group == "o2o_ref":
            ref_tables.append(entry)

    yml = {
        "version": 2,
        "sources": [
            {
                "name": "rpa_o2o",
                "description": "O2O 平台 RPA 导出数据（贴源层，全 TEXT，按日分区）",
                "schema": "public",
                "loader": "scripts/ingest.py",
                "loaded_at_field": "_load_time",
                "freshness": {
                    "warn_after": {"count": 2, "period": "day"},
                    "error_after": {"count": 3, "period": "day"},
                },
                "tables": [
                    {"name": t["name"], "description": t["description"]}
                    for t in rpa_tables
                ],
            },
            {
                "name": "o2o_ref",
                "description": "O2O 日报自有参考维度表（由 o2oData-Byrpa 脚本独立维护）",
                "schema": "public",
                "tables": [
                    {"name": t["name"], "description": t["description"]}
                    for t in ref_tables
                ],
            },
            {
                "name": "union_dim",
                "description": "union-agent 管理的维度表（同 schema，外部维护）",
                "schema": "public",
                "tables": [
                    {"name": "dim_store", "description": "O2O门店基础信息（40家）"},
                    {
                        "name": "ods_miniorange_order_item",
                        "description": "mini橙订单商品明细（~183万行）",
                    },
                    {
                        "name": "ods_miniorange_store",
                        "description": "mini橙门店列表（53家）",
                    },
                ],
            },
        ],
    }

    class FlowStyleDumper(yaml.SafeDumper):
        pass

    def represent_freshness_value(dumper, data):
        return dumper.represent_mapping("tag:yaml.org,2002:map", data.items(), flow_style=True)

    output = yaml.dump(
        yml,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )

    SOURCES_YML_PATH.write_text(output, encoding="utf-8")
    log.info(f"✅ 已生成 {SOURCES_YML_PATH}")


def main():
    parser = argparse.ArgumentParser(description="配置驱动的表结构同步工具")
    parser.add_argument("--dry-run", action="store_true", help="只打印 SQL，不执行")
    parser.add_argument(
        "--discover",
        action="store_true",
        help="从数据库发现 columns: [] 的表的列结构",
    )
    parser.add_argument(
        "--discover-all",
        action="store_true",
        help="从数据库发现所有表的列结构（含已有定义的表，用于校验）",
    )
    parser.add_argument(
        "--generate-sources",
        action="store_true",
        help="从 table_schemas.yaml 重新生成 _sources.yml",
    )
    parser.add_argument("--config", help="table_schemas.yaml 路径")
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else None
    schemas = load_table_schemas(config_path)
    log.info(f"已加载 {len(schemas.get('tables', {}))} 张表定义")

    if args.discover or args.discover_all:
        discover_from_db(schemas, discover_all=args.discover_all)
        return

    if args.generate_sources:
        generate_sources_yml(schemas)
        return

    sync_tables(schemas, dry_run=args.dry_run)

    if not args.dry_run:
        generate_sources_yml(schemas)


if __name__ == "__main__":
    main()
