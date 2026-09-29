"""History from exchange REST APIs: backfill, gap detection and repair.

The live path only ever sees what arrives on the WebSocket. When the feed drops
for ten minutes, those minutes have no candles: the aggregator never makes up a
candle it has no trades for, so a missing row always means *we did not see*,
never *nothing happened* — which is what makes gaps detectable at all.

Repair asks the exchange for its own 1m klines over exactly the missing minutes
and writes them as closed candles, then rebuilds the higher timeframes over the
same span from the 1m rows (see ADR 0004). Only buckets the aggregator's
watermark has already passed are touched; everything newer is still the live
path's to write.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

import httpx

from mdp.schema import Candle
from mdp.storage import Store
from mdp.timeframes import BASE_TIMEFRAME, TIMEFRAMES, from_ms

log = logging.getLogger(__name__)

M1 = BASE_TIMEFRAME.ms
# With no aggregator checkpoint to go by, treat minutes this old as settled.
SETTLE_MS = 2 * M1


class KlineSource(Protocol):
    @property
    def name(self) -> str: ...

    async def klines(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        """The venue's closed 1m candles with ``start_ms <= time < end_ms``."""
        ...


class BinanceKlines:
    """``GET /api/v3/klines``: up to 1000 minutes per request, paged forwards."""

    PAGE = 1_000

    def __init__(self, http: httpx.AsyncClient, base_url: str = "https://api.binance.com") -> None:
        self._http = http
        self._base = base_url

    @property
    def name(self) -> str:
        return "binance"

    async def klines(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        out: list[Candle] = []
        cursor = start_ms
        while cursor < end_ms:
            response = await self._http.get(
                f"{self._base}/api/v3/klines",
                params={
                    "symbol": symbol.replace("-", ""),
                    "interval": "1m",
                    "startTime": cursor,
                    "endTime": end_ms - 1,
                    "limit": self.PAGE,
                },
            )
            response.raise_for_status()
            rows: list[list[Any]] = response.json()
            out.extend(parse_binance_klines(symbol, rows, end_ms))
            if len(rows) < self.PAGE:
                break
            cursor = int(rows[-1][0]) + M1
        return out


def parse_binance_klines(symbol: str, rows: list[list[Any]], end_ms: int) -> list[Candle]:
    # [open time, open, high, low, close, volume, close time, quote volume, trades, ...]
    return [
        Candle(
            symbol=symbol,
            tf=BASE_TIMEFRAME,
            time=int(row[0]),
            open=Decimal(row[1]),
            high=Decimal(row[2]),
            low=Decimal(row[3]),
            close=Decimal(row[4]),
            volume=Decimal(row[5]),
            quote_volume=Decimal(row[7]),
            trades=int(row[8]),
            closed=True,
        )
        for row in rows
        if int(row[0]) < end_ms
    ]


class KrakenKlines:
    """``GET /0/public/OHLC``. Kraken only serves the most recent 720 candles."""

    def __init__(self, http: httpx.AsyncClient, base_url: str = "https://api.kraken.com") -> None:
        self._http = http
        self._base = base_url

    @property
    def name(self) -> str:
        return "kraken"

    async def klines(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        response = await self._http.get(
            f"{self._base}/0/public/OHLC",
            params={"pair": symbol.replace("-", ""), "interval": 1, "since": start_ms // 1000 - 1},
        )
        response.raise_for_status()
        body = response.json()
        if body.get("error"):
            raise RuntimeError(f"kraken: {body['error']}")
        return parse_kraken_ohlc(symbol, body["result"], start_ms, end_ms)


def parse_kraken_ohlc(
    symbol: str, result: dict[str, Any], start_ms: int, end_ms: int
) -> list[Candle]:
    # Keyed by Kraken's own pair name (XXBTZUSD for BTCUSD), next to a "last" cursor.
    [rows] = [v for k, v in result.items() if k != "last"]
    candles: list[Candle] = []
    # [time (s), open, high, low, close, vwap, volume, count]
    for t, o, h, lo, c, vwap, volume, count in rows:
        time_ms = int(t) * 1000
        if start_ms <= time_ms < end_ms:
            candles.append(
                Candle(
                    symbol=symbol,
                    tf=BASE_TIMEFRAME,
                    time=time_ms,
                    open=Decimal(o),
                    high=Decimal(h),
                    low=Decimal(lo),
                    close=Decimal(c),
                    volume=Decimal(volume),
                    # Kraken publishes no quote volume; vwap * volume is its best reconstruction.
                    quote_volume=Decimal(vwap) * Decimal(volume),
                    trades=int(count),
                    closed=True,
                )
            )
    return candles


def contiguous(minutes: Sequence[int]) -> list[tuple[int, int]]:
    """Group sorted minute starts into half-open ``[start, end)`` runs."""
    runs: list[tuple[int, int]] = []
    for minute in minutes:
        if runs and runs[-1][1] == minute:
            runs[-1] = (runs[-1][0], minute + M1)
        else:
            runs.append((minute, minute + M1))
    return runs


@dataclass(frozen=True, slots=True)
class RepairReport:
    symbol: str
    missing: int
    repaired: int


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


class Backfiller:
    def __init__(
        self,
        store: Store,
        source: KlineSource,
        *,
        clock_ms: Callable[[], int] = _wall_clock_ms,
    ) -> None:
        self._store = store
        self._source = source
        self._clock_ms = clock_ms

    async def settled_before(self, symbol: str) -> int:
        """Minutes before this are closed: the aggregator's watermark, or a margin behind now."""
        states = await self._store.load_states([symbol])
        state = states.get(symbol)
        limit = state["watermark"] if state else self._clock_ms() - SETTLE_MS
        return BASE_TIMEFRAME.floor(limit)

    async def backfill(self, symbol: str, start_ms: int, end_ms: int) -> int:
        """Load the venue's 1m history for a range, overwriting what is there."""
        settled = await self.settled_before(symbol)
        start, end = BASE_TIMEFRAME.floor(start_ms), min(BASE_TIMEFRAME.floor(end_ms), settled)
        if start >= end:
            return 0
        candles = await self._source.klines(symbol, start, end)
        await self._write(symbol, candles, start, settled)
        log.info("backfilled %d minutes of %s from %s", len(candles), symbol, self._source.name)
        return len(candles)

    async def repair(self, symbol: str, lookback_ms: int) -> RepairReport:
        """Find minutes with no candle inside the lookback window and fetch just those."""
        settled = await self.settled_before(symbol)
        missing = await self._store.missing_minutes(symbol, settled - lookback_ms, settled)
        if not missing:
            return RepairReport(symbol, 0, 0)
        fetched: list[Candle] = []
        for start, end in contiguous(missing):
            fetched.extend(await self._source.klines(symbol, start, end))
        await self._write(symbol, fetched, missing[0], settled)
        log.info(
            "%s: %d minutes missing since %s, %d recovered from %s",
            symbol,
            len(missing),
            from_ms(missing[0]).isoformat(),
            len(fetched),
            self._source.name,
        )
        return RepairReport(symbol, len(missing), len(fetched))

    async def _write(self, symbol: str, candles: list[Candle], start_ms: int, settled: int) -> None:
        if candles:
            async with self._store.transaction() as tx:
                await tx.upsert_candles(candles)
        for tf in TIMEFRAMES[1:]:
            await self._store.rollup(symbol, tf, start_ms, settled)


async def repair_forever(
    backfiller: Backfiller,
    symbols: Sequence[str],
    *,
    lookback_ms: int,
    interval_s: float,
    on_report: Callable[[RepairReport], None] = lambda report: None,
) -> None:
    while True:
        for symbol in symbols:
            try:
                on_report(await backfiller.repair(symbol, lookback_ms))
            except (httpx.HTTPError, RuntimeError) as exc:
                log.warning("repairing %s failed, will retry: %r", symbol, exc)
        await asyncio.sleep(interval_s)
