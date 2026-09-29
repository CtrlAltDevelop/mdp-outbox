"""Storage: one month of 1m candles for one symbol, before and after compression.

Writes 30 days of seeded random-walk candles (43,200 rows) through the store,
rolls them up into 5m, 1h and 1d, and compares the ``candles`` hypertable's
size before and after compressing every chunk. Uses its own database,
``<DATABASE_URL's database>_bench``, created and dropped around the run.

    uv run python bench/storage.py
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import sys
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql

from mdp.schema import Candle
from mdp.storage.timescale import TimescaleStore, migrate
from mdp.timeframes import TIMEFRAMES, Timeframe, to_ms

DAYS = 30
START = datetime(2026, 8, 1, tzinfo=UTC)


def candles(symbol: str) -> list[Candle]:
    rng = random.Random(5)
    price = 65_000.0
    out: list[Candle] = []
    for minute in range(DAYS * 1440):
        path = [price := price * math.exp(rng.gauss(0, 0.0004)) for _ in range(4)]
        o, c = Decimal(f"{path[0]:.2f}"), Decimal(f"{path[-1]:.2f}")
        volume = Decimal(f"{rng.expovariate(0.2):.5f}")
        out.append(
            Candle(
                symbol=symbol,
                tf=Timeframe.M1,
                time=to_ms(START) + minute * 60_000,
                open=o,
                high=Decimal(f"{max(path):.2f}"),
                low=Decimal(f"{min(path):.2f}"),
                close=c,
                volume=volume,
                quote_volume=(volume * c).quantize(Decimal("0.00000001")),
                trades=rng.randint(20, 400),
                closed=True,
            )
        )
    return out


async def size(conn: psycopg.AsyncConnection[Any]) -> int:
    cur = await conn.execute("SELECT hypertable_size('candles')")
    row = await cur.fetchone()
    assert row is not None
    return int(row[0])


async def main() -> None:
    admin_url = os.environ["DATABASE_URL"]
    parts = urlsplit(admin_url)
    name = f"{parts.path.lstrip('/')}_bench"
    bench_url = urlunsplit(parts._replace(path=f"/{name}"))
    async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
        await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        await migrate(bench_url)
        store = await TimescaleStore.connect(bench_url)
        rows = candles("BTC-USDT")
        for i in range(0, len(rows), 5_000):
            async with store.transaction() as tx:
                await tx.upsert_candles(rows[i : i + 5_000])
        end = rows[-1].time + 60_000
        for tf in TIMEFRAMES[1:]:
            await store.rollup("BTC-USDT", tf, rows[0].time, end)
        await store.close()

        async with await psycopg.AsyncConnection.connect(bench_url, autocommit=True) as conn:
            await conn.execute("VACUUM ANALYZE candles")
            cur = await conn.execute("SELECT tf, count(*) FROM candles GROUP BY tf ORDER BY 2 DESC")
            counts: dict[str, int] = dict(await cur.fetchall())
            before = await size(conn)
            await conn.execute("SELECT compress_chunk(c) FROM show_chunks('candles') c")
            after = await size(conn)
    finally:
        async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
            await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))

    print(f"{DAYS} days, 1 symbol: " + ", ".join(f"{n:,} x {tf}" for tf, n in counts.items()))
    print(f"uncompressed  {before / 1024 / 1024:6.2f} MiB")
    print(f"compressed    {after / 1024 / 1024:6.2f} MiB  ({before / after:.1f}x smaller)")


if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(main(), loop_factory=loop)
