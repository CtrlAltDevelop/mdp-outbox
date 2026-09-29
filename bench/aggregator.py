"""Throughput: how many trades per second one aggregator folds into candles.

Three stages, each on the same seeded synthetic trades:

* ``core``    — ``Aggregator.process`` on parsed trades: the candle maths alone;
* ``decode``  — the same, starting from the JSON the stream carries;
* ``service`` — the full aggregation service: XREADGROUP from Redis, the
  TimescaleDB transaction (trade log, candles, checkpoint), publish and XACK.
  Needs ``REDIS_URL`` and ``DATABASE_URL``; skipped without them.

    uv run python bench/aggregator.py [--trades 200000] [--symbols 4]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import sys
import time
from datetime import UTC, datetime, timedelta

import psycopg
import redis.asyncio as aioredis

from mdp.aggregation import AggregationService
from mdp.aggregator import Aggregator
from mdp.schema import Trade
from mdp.sources.synthetic import SyntheticSource
from mdp.storage.timescale import TimescaleStore, migrate
from mdp.streams import encode_trade, trades_stream

# 200 trades/s of event time: realistic density, so candles fill and close.
EVENT_RATE = 200
SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "ADA-USDT", "DOT-USDT"]


def trades(count: int, symbols: list[str]) -> list[Trade]:
    # Stamped so the last trade lands about now. Trades from months ago would
    # fall in chunks that the retention and compression policies act on.
    start = datetime.now(UTC) - timedelta(seconds=count / EVENT_RATE + 60)
    source = SyntheticSource(symbols, seed=1, rate=EVENT_RATE, start=start, limit=count)
    return list(source.trades())


def report(stage: str, count: int, seconds: float) -> None:
    print(f"{stage:<8} {count:>9,} trades  {seconds:7.2f} s  {count / seconds:>10,.0f} trades/s")


def bench_core(batch: list[Trade], batch_size: int) -> None:
    agg = Aggregator()
    began = time.perf_counter()
    for i in range(0, len(batch), batch_size):
        agg.process(batch[i : i + batch_size])
    report("core", len(batch), time.perf_counter() - began)


def bench_decode(batch: list[Trade], batch_size: int) -> None:
    wire = [t.model_dump_json() for t in batch]
    agg = Aggregator()
    began = time.perf_counter()
    for i in range(0, len(wire), batch_size):
        agg.process([Trade.model_validate_json(raw) for raw in wire[i : i + batch_size]])
    report("decode", len(wire), time.perf_counter() - began)


async def bench_service(batch: list[Trade], symbols: list[str], batch_size: int) -> None:
    redis_url, database_url = os.environ.get("REDIS_URL"), os.environ.get("DATABASE_URL")
    if not (redis_url and database_url):
        print("service  skipped: set REDIS_URL and DATABASE_URL")
        return
    redis = aioredis.Redis.from_url(redis_url)
    await redis.flushdb()
    await migrate(database_url)
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        await conn.execute("TRUNCATE trades, candles, aggregator_state")

    for i in range(0, len(batch), 5_000):
        pipe = redis.pipeline(transaction=False)
        for trade in batch[i : i + 5_000]:
            pipe.xadd(trades_stream(trade.symbol), encode_trade(trade))  # type: ignore[arg-type]
        await pipe.execute()

    store = await TimescaleStore.connect(database_url)
    service = AggregationService(redis, store, symbols, idle_grace_ms=None, batch_size=batch_size)
    await service.start()
    began = time.perf_counter()
    while await service.poll():
        pass
    report("service", len(batch), time.perf_counter() - began)
    await store.close()
    await redis.aclose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trades", type=int, default=200_000)
    parser.add_argument("--symbols", type=int, default=4, choices=range(1, len(SYMBOLS) + 1))
    parser.add_argument("--batch", type=int, default=1_000)
    args = parser.parse_args()
    symbols = SYMBOLS[: args.symbols]

    print(f"Python {platform.python_version()} on {platform.system()} {platform.machine()}")
    print(f"{args.trades:,} synthetic trades, {len(symbols)} symbols, batches of {args.batch}\n")
    batch = trades(args.trades, symbols)
    bench_core(batch, args.batch)
    bench_decode(batch, args.batch)
    loop = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(bench_service(batch, symbols, args.batch), loop_factory=loop)


if __name__ == "__main__":
    main()
