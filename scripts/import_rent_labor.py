"""导入房租水电成本.xls 到 dim_store.daily_rent_labor"""
import asyncio
import ssl
import sys

import asyncpg
import xlrd

import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["DATABASE_URL"]
XLS_PATH = r"d:\3.25\房租水电成本.xls"


async def main():
    sys.stdout.reconfigure(encoding="utf-8")

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    conn = await asyncpg.connect(DB_URL, ssl=ctx)

    await conn.execute(
        "ALTER TABLE dim_store ADD COLUMN IF NOT EXISTS daily_rent_labor NUMERIC(10,2)"
    )
    print("Column daily_rent_labor added/verified")

    wb = xlrd.open_workbook(XLS_PATH)
    sheet = wb.sheet_by_index(0)

    updates = []
    for i in range(2, sheet.nrows):
        store = sheet.cell_value(i, 0)
        cost = sheet.cell_value(i, 13)
        if store and isinstance(cost, (int, float)) and cost > 0:
            updates.append((store, round(cost, 2)))

    updated = 0
    not_found = []
    for store_name, cost in updates:
        result = await conn.execute(
            "UPDATE dim_store SET daily_rent_labor = $1 WHERE store_short_name = $2",
            cost,
            store_name,
        )
        count = int(result.split(" ")[1])
        if count > 0:
            updated += 1
        else:
            not_found.append(store_name)

    print(f"Updated: {updated}/{len(updates)} stores")
    if not_found:
        print(f"Not found in dim_store: {not_found}")

    rows = await conn.fetch(
        "SELECT store_short_name, daily_rent_labor FROM dim_store "
        "WHERE daily_rent_labor IS NOT NULL ORDER BY store_short_name"
    )
    print(f"Verification: {len(rows)} stores with daily_rent_labor")
    for r in rows[:5]:
        name = r["store_short_name"]
        cost = r["daily_rent_labor"]
        print(f"  {name}: {cost}")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
