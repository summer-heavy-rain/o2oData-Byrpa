"""
从 房租水电成本.xls 导入 dim_o2o_store_ref 表
列: store_short_name, province, city, daily_rent_labor
"""
import asyncio, ssl, xlrd, sys, os
from pathlib import Path
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv(Path(__file__).parent.parent / ".env")

DATABASE_URL = os.environ["DATABASE_URL"]
XLS_PATH = Path(__file__).parent.parent / "房租水电成本.xls"


CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS dim_o2o_store_ref (
    store_short_name TEXT PRIMARY KEY,
    province         TEXT,
    city             TEXT,
    daily_rent_labor NUMERIC(10,4)
);
"""


def read_xls(path: Path) -> list[tuple]:
    wb = xlrd.open_workbook(str(path))
    sh = wb.sheet_by_index(0)
    # row 1 = header, data from row 2
    rows = []
    for i in range(2, sh.nrows):
        r = sh.row_values(i)
        name = str(r[0]).strip() if r[0] else ""
        if not name:
            continue
        province = str(r[1]).strip() if r[1] else None
        city = str(r[2]).strip() if r[2] else None
        daily = float(r[13]) if r[13] else 0.0
        rows.append((name, province or None, city or None, daily))
    return rows


async def main():
    import asyncpg

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    rows = read_xls(XLS_PATH)
    print(f"读取到 {len(rows)} 个门店")

    conn = await asyncpg.connect(DATABASE_URL, ssl=ctx)
    try:
        await conn.execute(CREATE_TABLE)
        await conn.execute("TRUNCATE TABLE dim_o2o_store_ref")
        await conn.executemany(
            """
            INSERT INTO dim_o2o_store_ref (store_short_name, province, city, daily_rent_labor)
            VALUES ($1, $2, $3, $4)
            """,
            rows,
        )
        count = await conn.fetchval("SELECT COUNT(*) FROM dim_o2o_store_ref")
        print(f"✅ dim_o2o_store_ref 写入完成：{count} 行")

        # 验证几条
        sample = await conn.fetch(
            "SELECT * FROM dim_o2o_store_ref LIMIT 5"
        )
        for r in sample:
            print(f"  {r['store_short_name']} | {r['province']} {r['city']} | 日均:{r['daily_rent_labor']:.2f}")
    finally:
        await conn.close()


asyncio.run(main())
