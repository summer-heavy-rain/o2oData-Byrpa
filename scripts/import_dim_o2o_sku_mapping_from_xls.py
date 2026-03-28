#!/usr/bin/env python3
"""
从《编码匹配.xls》导入 / 更新 dim_o2o_sku_mapping。
列：小猴迷你橙(门店)、友联商品编码、供应链（金蝶）数量、迷你橙数量、总部招商价
同一 (mini_code, kingdee_code) 多行时以后出现的行覆盖（与 Excel 顺序一致）。

用法：
  DATABASE_URL=... python scripts/import_dim_o2o_sku_mapping_from_xls.py /path/to/编码匹配.xls
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

try:
    import xlrd
except ImportError as e:
    raise SystemExit("请安装: pip install xlrd psycopg2-binary") from e


def norm_code(row: int, col: int, sh) -> str:
    t = sh.cell_type(row, col)
    v = sh.cell_value(row, col)
    if t == xlrd.XL_CELL_EMPTY:
        return ""
    if t == xlrd.XL_CELL_NUMBER:
        if v == int(v):
            return str(int(v))
        return str(v).rstrip("0").rstrip(".") if "." in str(v) else str(v)
    s = str(v).strip().replace("\t", "")
    if not s or s == "-":
        return ""
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except ValueError:
        pass
    return s


def norm_qty_price(row: int, col: int, sh) -> float | None:
    t = sh.cell_type(row, col)
    v = sh.cell_value(row, col)
    if t == xlrd.XL_CELL_EMPTY:
        return None
    if t == xlrd.XL_CELL_NUMBER:
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s or s in ("-", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    uri = os.environ.get("DATABASE_URL")
    if not uri:
        raise SystemExit("缺少环境变量 DATABASE_URL")

    xls = Path(sys.argv[1] if len(sys.argv) > 1 else "").expanduser()
    if not xls.is_file():
        raise SystemExit("用法: python import_dim_o2o_sku_mapping_from_xls.py <编码匹配.xls>")

    book = xlrd.open_workbook(str(xls))
    sh = book.sheet_by_index(0)
    # 去重：同键保留最后一行
    merged: dict[tuple[str, str], tuple[float, float, float]] = {}
    for r in range(1, sh.nrows):
        mini = norm_code(r, 0, sh)
        kd = norm_code(r, 1, sh)
        if not mini or not kd:
            continue
        kq = norm_qty_price(r, 2, sh)
        mq = norm_qty_price(r, 3, sh)
        hp = norm_qty_price(r, 4, sh)
        if kq is None or mq is None or hp is None:
            continue
        merged[(mini, kd)] = (kq, mq, hp)

    rows = [(a, b, c[0], c[1], c[2]) for (a, b), c in merged.items()]
    if not rows:
        raise SystemExit("无有效行")

    sql = """
    INSERT INTO dim_o2o_sku_mapping (mini_code, kingdee_code, kingdee_qty, mini_qty, headquarters_price)
    VALUES %s
    ON CONFLICT (mini_code, kingdee_code) DO UPDATE SET
      kingdee_qty = EXCLUDED.kingdee_qty,
      mini_qty = EXCLUDED.mini_qty,
      headquarters_price = EXCLUDED.headquarters_price,
      updated_at = NOW()
    """

    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    batch = 800
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        execute_values(
            cur,
            sql,
            chunk,
            template="(%s, %s, %s::numeric, %s::numeric, %s::numeric)",
            page_size=len(chunk),
        )
    conn.commit()
    cur.close()
    conn.close()
    print(f"完成：写入 {len(rows)} 条 (mini_code, kingdee_code) 键（源表 {sh.nrows - 1} 行，去重后）。")


if __name__ == "__main__":
    main()
