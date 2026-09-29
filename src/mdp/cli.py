"""``mdp``: one entry point, one subcommand per process.

    mdp migrate                      create or update the database schema
    mdp ingest                       run the configured source into Redis
    mdp aggregate                    build candles from the trade streams
    mdp api                          serve REST, WebSocket and /metrics
    mdp repair [--once]              find missing minutes and fetch them
    mdp backfill SYMBOL FROM TO      load exchange history for a range

Every process reads the same ``MDP_*`` settings (see ``mdp.config``), so the
docker-compose file runs one image with a different command per service.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from collections.abc import AsyncIterator, Coroutine, Sequence
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import redis.asyncio as aioredis
import uvicorn
from prometheus_client import start_http_server

from mdp.aggregation import AggregationService
from mdp.api import Backend, create_app, parse_time
from mdp.backfill import Backfiller, BinanceKlines, KlineSource, KrakenKlines, repair_forever
from mdp.config import Settings
from mdp.ingest import IngestService
from mdp.normalizer import Normalizer
from mdp.sources import TradeSource
from mdp.sources.binance import BinanceSource
from mdp.sources.engine import EngineFormat, EngineSource
from mdp.sources.kraken import KrakenSource
from mdp.sources.synthetic import SyntheticSource
from mdp.storage.timescale import TimescaleStore, migrate

log = logging.getLogger("mdp")


def build_source(settings: Settings) -> TradeSource:
    symbols = settings.symbols
    match settings.source:
        case "synthetic":
            return SyntheticSource(
                symbols, seed=settings.synthetic_seed, rate=settings.synthetic_rate
            )
        case "binance":
            return BinanceSource(symbols)
        case "kraken":
            return KrakenSource(symbols)
        case "engine":
            fmt = EngineFormat(
                price_scale=settings.engine_price_scale, qty_scale=settings.engine_qty_scale
            )
            url = urlsplit(settings.engine_url)
            if url.scheme in ("ws", "wss"):
                return EngineSource.over_websocket(settings.engine_url, symbols, fmt=fmt)
            if url.scheme in ("redis", "rediss") and url.fragment:
                engine_redis = aioredis.Redis.from_url(urlunsplit(url._replace(fragment="")))
                return EngineSource.over_redis(engine_redis, url.fragment, symbols, fmt=fmt)
            raise SystemExit("MDP_ENGINE_URL must be ws(s)://... or redis://host/db#stream-key")


async def ingest(settings: Settings) -> None:
    start_http_server(settings.metrics_port)
    redis = aioredis.Redis.from_url(settings.redis_url)
    source = build_source(settings)
    log.info("ingesting %s from %s", ",".join(settings.symbols), source.name)
    await IngestService(source, Normalizer(settings.symbols), redis).run()


async def aggregate(settings: Settings) -> None:
    start_http_server(settings.metrics_port)
    redis = aioredis.Redis.from_url(settings.redis_url)
    store = await TimescaleStore.connect(settings.database_url)
    service = AggregationService(
        redis,
        store,
        settings.symbols,
        consumer=settings.consumer,
        allowed_lateness_ms=settings.allowed_lateness_ms,
        idle_grace_ms=settings.idle_grace_ms,
        batch_size=settings.batch_size,
    )
    log.info("aggregating %s as %s", ",".join(settings.symbols), settings.consumer)
    try:
        await service.run()
    finally:
        await store.close()


async def api(settings: Settings) -> None:
    @asynccontextmanager
    async def resources() -> AsyncIterator[tuple[Backend, aioredis.Redis]]:
        store = await TimescaleStore.connect(settings.database_url)
        redis = aioredis.Redis.from_url(settings.redis_url)
        try:
            yield store, redis
        finally:
            await redis.aclose()
            await store.close()

    app = create_app(resources, settings.symbols)
    config = uvicorn.Config(
        app, host=settings.api_host, port=settings.api_port, log_level=settings.log_level.lower()
    )
    await uvicorn.Server(config).serve()


def kline_source(settings: Settings, http: httpx.AsyncClient) -> KlineSource:
    return BinanceKlines(http) if settings.repair_source == "binance" else KrakenKlines(http)


async def repair(settings: Settings, *, once: bool) -> None:
    store = await TimescaleStore.connect(settings.database_url)
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            backfiller = Backfiller(store, kline_source(settings, http))
            lookback_ms = settings.repair_lookback_minutes * 60_000
            if once:
                for symbol in settings.symbols:
                    report = await backfiller.repair(symbol, lookback_ms)
                    print(f"{symbol}: {report.missing} missing, {report.repaired} repaired")
                return
            start_http_server(settings.metrics_port)
            await repair_forever(
                backfiller,
                settings.symbols,
                lookback_ms=lookback_ms,
                interval_s=settings.repair_interval_s,
            )
    finally:
        await store.close()


async def backfill(settings: Settings, symbol: str, start: str, end: str) -> None:
    store = await TimescaleStore.connect(settings.database_url)
    try:
        async with httpx.AsyncClient(timeout=15) as http:
            backfiller = Backfiller(store, kline_source(settings, http))
            written = await backfiller.backfill(symbol, parse_time(start), parse_time(end))
            print(f"{symbol}: {written} minutes written")
    finally:
        await store.close()


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="mdp", description="Market Data Pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="create or update the database schema")
    commands.add_parser("ingest", help="run the configured source into Redis")
    commands.add_parser("aggregate", help="build candles from the trade streams")
    commands.add_parser("api", help="serve REST, WebSocket and /metrics")
    repair_cmd = commands.add_parser("repair", help="find missing minutes and fetch them")
    repair_cmd.add_argument("--once", action="store_true", help="one pass, then exit")
    backfill_cmd = commands.add_parser("backfill", help="load exchange history for a range")
    backfill_cmd.add_argument("symbol", help="canonical symbol, e.g. BTC-USDT")
    backfill_cmd.add_argument("start", help="epoch ms or ISO 8601")
    backfill_cmd.add_argument("end", help="epoch ms or ISO 8601")
    return parser.parse_args(argv)


def command(args: argparse.Namespace, settings: Settings) -> Coroutine[Any, Any, None]:
    match args.command:
        case "migrate":
            return migrate(settings.database_url)
        case "ingest":
            return ingest(settings)
        case "aggregate":
            return aggregate(settings)
        case "api":
            return api(settings)
        case "repair":
            return repair(settings, once=args.once)
        case _:
            return backfill(settings, args.symbol.upper(), args.start, args.end)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # psycopg's async driver needs a selector loop; it is the default outside Windows.
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(command(args, settings), loop_factory=loop_factory)


if __name__ == "__main__":
    main()
