"""End-to-end latency: a trade's exchange timestamp to its candle on a WebSocket.

Runs the real path in one process — synthetic live source, ingest into Redis,
the aggregation service with its TimescaleDB transaction, pub/sub, the API's
fan-out hub and a WebSocket client — and times every live 1m update the client
receives.

A candle frame says which trade it includes last only implicitly: in live mode
trades arrive in event-time order, so the update of bucket ``b`` with
``trades == n`` is the one that folded in the ``n``-th trade of ``b``. The
source is wrapped to record each trade's timestamp under that key.

    uv run python bench/latency.py [--seconds 60] [--rate 50]

Needs ``REDIS_URL`` and ``DATABASE_URL``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import sys
import time
from collections import Counter
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager

import psycopg
import redis.asyncio as aioredis
import uvicorn
from websockets.asyncio.client import connect

from mdp.aggregation import AggregationService
from mdp.api import Backend, create_app
from mdp.ingest import IngestService
from mdp.normalizer import Normalizer, SourceEvent
from mdp.schema import Trade
from mdp.sources.synthetic import SyntheticSource
from mdp.storage.timescale import TimescaleStore, migrate
from mdp.timeframes import Timeframe

SYMBOL = "BTC-USDT"
PORT = 18765


class RecordingSource:
    """Wraps a source, remembering each trade's timestamp by (bucket, n-th trade)."""

    def __init__(self, inner: SyntheticSource) -> None:
        self._inner = inner
        self.stamps: dict[tuple[int, int], int] = {}
        self._counts: Counter[int] = Counter()

    @property
    def name(self) -> str:
        return self._inner.name

    async def events(self) -> AsyncIterator[SourceEvent]:
        async for event in self._inner.events():
            if isinstance(event, Trade):
                bucket = Timeframe.M1.floor(event.ts_ms)
                self._counts[bucket] += 1
                self.stamps[(bucket, self._counts[bucket])] = event.ts_ms
            yield event


def percentile(values: Sequence[float], q: float) -> float:
    return statistics.quantiles(values, n=1000, method="inclusive")[round(q * 1000) - 1]


async def main(seconds: float, rate: float) -> None:
    redis_url, database_url = os.environ["REDIS_URL"], os.environ["DATABASE_URL"]
    redis = aioredis.Redis.from_url(redis_url)
    await redis.flushdb()
    await migrate(database_url)
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        await conn.execute("TRUNCATE trades, candles, aggregator_state")
    store = await TimescaleStore.connect(database_url)

    source = RecordingSource(SyntheticSource([SYMBOL], seed=3, rate=rate))
    ingest = IngestService(source, Normalizer([SYMBOL]), redis)
    aggregate = AggregationService(redis, store, [SYMBOL])

    @asynccontextmanager
    async def resources() -> AsyncIterator[tuple[Backend, aioredis.Redis]]:
        yield store, redis

    server = uvicorn.Server(
        uvicorn.Config(create_app(resources, [SYMBOL]), port=PORT, log_level="warning")
    )
    lags_ms: list[float] = []
    serving = asyncio.create_task(server.serve())
    tasks = [asyncio.create_task(aggregate.run(block_ms=100))]
    while not server.started:  # noqa: ASYNC110 - uvicorn exposes a flag, not an event
        await asyncio.sleep(0.05)

    async with connect(f"ws://localhost:{PORT}/v1/ws") as ws:
        await ws.send(json.dumps({"op": "subscribe", "channel": f"candles:{SYMBOL}:1m"}))
        await ws.recv()  # the ack
        tasks.append(asyncio.create_task(ingest.run()))
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            frame = json.loads(await ws.recv())
            received = time.time_ns() / 1e6
            candle = frame["data"]
            stamp = source.stamps.get((candle["time"], candle["trades"]))
            if not candle["closed"] and stamp is not None:
                lags_ms.append(received - stamp)

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    server.should_exit = True
    await serving
    await store.close()
    await redis.aclose()

    print(f"Python {platform.python_version()} on {platform.system()} {platform.machine()}")
    print(f"{rate:g} trades/s for {seconds:g}s, {len(lags_ms)} live 1m updates received\n")
    for name, q in (("p50", 0.5), ("p90", 0.9), ("p99", 0.99)):
        print(f"{name}  {percentile(lags_ms, q):7.1f} ms")
    print(f"max  {max(lags_ms):7.1f} ms")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--rate", type=float, default=50)
    args = parser.parse_args()
    loop = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(main(args.seconds, args.rate), loop_factory=loop)
