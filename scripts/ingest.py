"""
O2O RPA 数据入库主入口
从共享存储读取 Excel/CSV → 解析 → 写入 Supabase ODS 表（按日分区）

用法:
  python -m scripts.ingest --date 2026-03-25
  python -m scripts.ingest --date 2026-03-25 --source mini_orange
  python -m scripts.ingest --date 2026-03-25 --discover-only
"""
from __future__ import annotations

import argparse
import fnmatch
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

from scripts.config_loader import (
    FileRule,
    SourceConfig,
    extract_tag_from_filename,
    load_sources,
    match_file_rule,
)
from scripts.file_reader import read_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest")


def discover_files(source: SourceConfig, folder: Path) -> list[tuple[Path, FileRule, dict]]:
    """
    发现指定日期目录下的待处理文件
    返回: [(文件路径, 匹配的规则, 附加列), ...]
    """
    if not folder.exists():
        log.warning(f"[{source.display_name}] 目录不存在: {folder}")
        return []

    results = []

    if source.discovery_mode == "explicit":
        for file_def in source.files:
            fpath = folder / file_def["filename"]
            if not fpath.exists():
                log.error(f"[{source.display_name}] 指定文件缺失: {file_def['filename']}")
                raise FileNotFoundError(f"指定文件缺失: {fpath}")
            rule = FileRule(
                match=file_def["filename"],
                format=file_def.get("format", "xlsx"),
                sheet=file_def.get("sheet", 0),
                header_row=file_def.get("header_row", 1),
                target_table=file_def["target_table"],
            )
            extra = file_def.get("extra_columns", {})
            results.append((fpath, rule, extra))
    else:
        all_files = sorted(
            [f for f in folder.iterdir() if f.is_file()],
            key=lambda f: f.name,
        )
        skip = set(source.skip_non_data_files)
        data_files = [f for f in all_files if f.name not in skip]

        if source.min_files and len(data_files) < source.min_files:
            raise RuntimeError(
                f"[{source.display_name}] 文件数 {len(data_files)} < 最少要求 {source.min_files}，"
                f"缺失文件请检查 RPA 是否正常运行。"
                f"已发现: {[f.name for f in data_files]}"
            )

        for fpath in data_files:
            rule = match_file_rule(fpath.name, source.file_rules)
            if rule is None:
                log.warning(f"[{source.display_name}] 文件无匹配规则，跳过: {fpath.name}")
                continue

            extra: dict = {}
            if source.account_type_extract:
                tag = extract_tag_from_filename(fpath.name, source.account_type_extract)
                extra["_account_type"] = tag
            if source.store_name_extract:
                tag = extract_tag_from_filename(fpath.name, source.store_name_extract)
                if tag:
                    extra["_store_name"] = tag

            results.append((fpath, rule, extra))

    return results


_COL_I_RE = re.compile(r"^col_(\d+)$")


def _align_columns_across_files(
    dfs: list[pd.DataFrame], table: str
) -> list[pd.DataFrame]:
    """
    当多个 DataFrame 写入同一张 ODS 表但列数不同时，用命名列做锚点
    重新对齐 col_i 列，消除因某个文件多/少列导致的全局错位。

    思路：取列数最少的 DataFrame 作为参考（canonical），逐段比对：
    两个相邻命名列之间的 col_i 数量不一致 → 多出的列标记为 _extra_*，
    其余 col_i 按参考顺序重编号。
    """
    if len(dfs) <= 1:
        return dfs

    col_counts = [len(df.columns) for df in dfs]
    if len(set(col_counts)) == 1:
        return dfs

    ref_idx = col_counts.index(min(col_counts))
    ref_cols = dfs[ref_idx].columns.tolist()

    ref_anchors = [
        (i, c) for i, c in enumerate(ref_cols)
        if not _COL_I_RE.match(c) and not c.startswith("_")
    ]

    result = []
    for df_i, df in enumerate(dfs):
        if df_i == ref_idx or list(df.columns) == ref_cols:
            result.append(df)
            continue

        cur_cols = df.columns.tolist()
        cur_anchors = [
            (i, c) for i, c in enumerate(cur_cols)
            if not _COL_I_RE.match(c) and not c.startswith("_")
        ]

        ref_anchor_names = [c for _, c in ref_anchors]
        cur_anchor_names = [c for _, c in cur_anchors]
        if ref_anchor_names != cur_anchor_names:
            log.warning(
                f"  [{table}] 命名列不匹配，跳过列对齐: "
                f"ref={len(ref_anchor_names)} vs cur={len(cur_anchor_names)}"
            )
            result.append(df)
            continue

        col_rename: dict[str, str] = {}
        extra_cols: list[int] = []

        boundaries_ref = [0] + [pos for pos, _ in ref_anchors] + [len(ref_cols)]
        boundaries_cur = [0] + [pos for pos, _ in cur_anchors] + [len(cur_cols)]

        for seg in range(len(boundaries_ref) - 1):
            r_start, r_end = boundaries_ref[seg], boundaries_ref[seg + 1]
            c_start, c_end = boundaries_cur[seg], boundaries_cur[seg + 1]

            r_unnamed = [
                (j, ref_cols[j]) for j in range(r_start, r_end)
                if _COL_I_RE.match(ref_cols[j])
            ]
            c_unnamed = [
                (j, cur_cols[j]) for j in range(c_start, c_end)
                if _COL_I_RE.match(cur_cols[j])
            ]

            if len(c_unnamed) == len(r_unnamed):
                for (c_pos, c_name), (_, r_name) in zip(c_unnamed, r_unnamed):
                    if c_name != r_name:
                        col_rename[c_name] = r_name
            elif len(c_unnamed) > len(r_unnamed):
                n_extra = len(c_unnamed) - len(r_unnamed)
                for k in range(n_extra):
                    extra_pos = c_unnamed[k][0]
                    extra_cols.append(extra_pos)
                remaining = c_unnamed[n_extra:]
                for (c_pos, c_name), (_, r_name) in zip(remaining, r_unnamed):
                    if c_name != r_name:
                        col_rename[c_name] = r_name

        if extra_cols or col_rename:
            extra_names = [cur_cols[p] for p in extra_cols]
            log.info(
                f"  [{table}] 列对齐: 移除多余列 {extra_names}, "
                f"重映射 {len(col_rename)} 列"
            )
            df = df.drop(columns=extra_names, errors="ignore")
            if col_rename:
                df = df.rename(columns=col_rename)

        result.append(df)

    return result


def process_source(source: SourceConfig, dt: date, manifest, *, discover_only: bool = False):
    """处理单个数据源的完整流程"""
    folder = manifest.resolve_path(source, dt)
    log.info(f"{'=' * 60}")
    log.info(f"[{source.display_name}] 处理日期 {dt}，目录: {folder}")

    file_tasks = discover_files(source, folder)
    log.info(f"[{source.display_name}] 发现 {len(file_tasks)} 个文件")

    all_results: dict[str, list[pd.DataFrame]] = {}

    for fpath, rule, extra_cols in file_tasks:
        log.info(f"  读取: {fpath.name} (格式: {rule.format})")

        try:
            table_dfs = read_file(fpath, rule)
        except Exception as e:
            log.error(f"  读取失败: {e}")
            raise

        for target_table, df in table_dfs:
            df["dt"] = str(dt)
            df["_source_file"] = fpath.name
            df["_load_time"] = datetime.utcnow().isoformat()
            for col, val in extra_cols.items():
                df[col] = val

            if target_table not in all_results:
                all_results[target_table] = []
            all_results[target_table].append(df)

            log.info(f"    → {target_table}: {len(df)} 行, {len(df.columns)} 列")

    if discover_only:
        log.info(f"\n[{source.display_name}] Schema 发现结果:")
        for table, dfs in all_results.items():
            merged = pd.concat(dfs, ignore_index=True)
            cols = [c for c in merged.columns if not c.startswith("_") and c != "dt"]
            log.info(f"  {table}: {len(cols)} 业务列")
            for c in cols:
                log.info(f"    - \"{c}\"  TEXT")
        return all_results

    for table, dfs in all_results.items():
        dfs = _align_columns_across_files(dfs, table)
        merged = pd.concat(dfs, ignore_index=True)
        upload_to_supabase(table, merged, dt)

    return all_results


def upload_to_supabase(table: str, df: pd.DataFrame, dt: date):
    """上传 DataFrame 到 Supabase ODS 表（普通表 + dt 列实现逻辑日分区）"""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL 未设置，跳过上传")
        return
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    try:
        import sqlalchemy
    except ImportError:
        log.error("需要安装 sqlalchemy: pip install sqlalchemy psycopg2-binary")
        return

    engine = sqlalchemy.create_engine(db_url)

    with engine.begin() as conn:
        has_table = conn.execute(
            sqlalchemy.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name=:t"
            ),
            {"t": table},
        ).fetchone()

        if has_table:
            conn.execute(
                sqlalchemy.text(f'DELETE FROM "{table}" WHERE dt = :dt'),
                {"dt": str(dt)},
            )
            existing_cols = {
                r[0]
                for r in conn.execute(
                    sqlalchemy.text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name=:t"
                    ),
                    {"t": table},
                ).fetchall()
            }
            for c in df.columns:
                if c not in existing_cols:
                    conn.execute(sqlalchemy.text(
                        f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT'
                    ))
        else:
            cols_sql = ", ".join(
                f'"{c}" TEXT' for c in df.columns
            )
            conn.execute(sqlalchemy.text(
                f'CREATE TABLE "{table}" ({cols_sql})'
            ))
            conn.execute(sqlalchemy.text(
                f'CREATE INDEX IF NOT EXISTS idx_{table}_dt ON "{table}" (dt)'
            ))

    df.to_sql(
        table,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    log.info(f"  ✓ {table} 已上传 {len(df)} 行")


def main():
    parser = argparse.ArgumentParser(description="O2O RPA 数据入库")
    parser.add_argument("--date", required=True, help="日期 YYYY-MM-DD")
    parser.add_argument("--source", help="仅处理指定数据源 (mini_orange/eleme/jd/maiyatian/meituan)")
    parser.add_argument("--discover-only", action="store_true", help="仅发现 schema，不上传")
    parser.add_argument("--config", help="配置文件路径（默认 config/sources.yaml）")
    args = parser.parse_args()

    dt = date.fromisoformat(args.date)
    manifest = load_sources(args.config)

    for source in manifest.sources:
        if args.source and source.key != args.source:
            continue
        try:
            process_source(source, dt, manifest, discover_only=args.discover_only)
        except Exception as e:
            log.error(f"[{source.display_name}] 处理失败: {e}")
            raise

    log.info(f"\n{'=' * 60}")
    log.info("全部完成")


if __name__ == "__main__":
    main()
