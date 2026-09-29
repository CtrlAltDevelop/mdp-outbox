"""Backfill and gap repair, against recorded REST responses and both stores."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest

from mdp.backfill import (
    Backfiller,
    BinanceKlines,
    KrakenKlines,
    contiguous,
    parse_binance_klines,
    parse_kraken_ohlc,
)
from mdp.schema import Candle
from mdp.storage import Store
from mdp.timeframes import Timeframe, to_ms
from tests.helpers import at

FIXTURES = Path(__file__).parent / "fixtures"
M1 = Timeframe.M1


def fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def minute(m: int, price: str = "100", trades: int = 2) -> Candle:
    p = Decimal(price)
    return Candle(
        symbol="BTC-USDT",
        tf=M1,
        time=to_ms(at(m)),
        open=p,
        high=p + 1,
        low=p - 1,
        close=p,
        volume=Decimal(1),
        quote_volume=p,
        trades=trades,
        closed=True,
    )


class FakeVenue:
    """A kline source that knows every minute and records what it was asked for."""

    def __init__(self) -> None:
        self.requests: list[tuple[int, int]] = []

    @property
    def name(self) -> str:
        return "fake"

    async def klines(self, symbol: str, start_ms: int, end_ms: int) -> list[Candle]:
        self.requests.append((start_ms, end_ms))
        return [minute(m, "200", 9) for m in range(-60, 60) if start_ms <= to_ms(at(m)) < end_ms]


async def seed(store: Store, minutes: list[int], watermark: int) -> None:
    async with store.transaction() as tx:
        await tx.upsert_candles([minute(m) for m in minutes])
        await tx.save_state(
            "BTC-USDT", {"watermark": watermark, "max_event": watermark, "open": []}
        )


def test_binance_klines_from_a_recorded_response() -> None:
    rows = fixture("binance_klines.json")

    candles = parse_binance_klines("BTC-USDT", rows, end_ms=rows[-1][0])

    assert len(candles) == 4  # end is exclusive
    first = candles[0]
    assert first.time == 1_790_445_600_000
    assert (first.open, first.high, first.low, first.close) == (
        Decimal("84132.40000000"),
        Decimal("84132.41000000"),
        Decimal("84132.21000000"),
        Decimal("84132.21000000"),
    )
    assert (first.volume, first.quote_volume, first.trades) == (
        Decimal("3.46100000"),
        Decimal("291182.10406440"),
        443,
    )
    assert first.closed


def test_kraken_ohlc_from_a_recorded_response() -> None:
    result = fixture("kraken_ohlc.json")["result"]

    candles = parse_kraken_ohlc("BTC-USD", result, 1_790_445_600_000, 1_790_445_780_000)

    assert [c.time for c in candles] == [1_790_445_600_000, 1_790_445_660_000, 1_790_445_720_000]
    assert candles[0].open == Decimal("84125.9")
    assert candles[0].quote_volume == Decimal("84117.2") * Decimal("0.37798365")
    assert candles[0].trades == 71


async def test_binance_client_pages_through_long_ranges() -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        seen.append(params)
        start = int(params["startTime"])
        count = min(int(params["limit"]), (int(params["endTime"]) + 1 - start) // 60_000)
        rows = [
            [start + i * 60_000, "1", "1", "1", "1", "1", 0, "1", 1, "0", "0", "0"]
            for i in range(count)
        ]
        return httpx.Response(200, json=rows)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = BinanceKlines(http)
        client.PAGE = 3
        candles = await client.klines("BTC-USDT", to_ms(at(0)), to_ms(at(7)))

    assert [c.time for c in candles] == [to_ms(at(m)) for m in range(7)]
    assert [int(p["startTime"]) for p in seen] == [to_ms(at(0)), to_ms(at(3)), to_ms(at(6))]
    assert seen[0]["symbol"] == "BTCUSDT"


async def test_kraken_client_reports_api_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": ["EQuery:Unknown asset pair"]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(RuntimeError, match="Unknown asset pair"):
            await KrakenKlines(http).klines("NOPE-USD", 0, 60_000)


def test_contiguous_groups_minutes_into_runs() -> None:
    minutes = [to_ms(at(m)) for m in (3, 4, 7, 9, 10, 11)]

    assert contiguous(minutes) == [
        (to_ms(at(3)), to_ms(at(5))),
        (to_ms(at(7)), to_ms(at(8))),
        (to_ms(at(9)), to_ms(at(12))),
    ]


async def test_repair_fetches_only_the_missing_minutes(store: Store) -> None:
    await seed(store, [0, 1, 2, 5, 6, 8, 9], watermark=to_ms(at(10, 5)))
    venue = FakeVenue()

    report = await Backfiller(store, venue).repair("BTC-USDT", lookback_ms=60 * 60_000)

    assert (report.missing, report.repaired) == (3, 3)
    assert venue.requests == [(to_ms(at(3)), to_ms(at(5))), (to_ms(at(7)), to_ms(at(8)))]
    minutes = await store.get_candles("BTC-USDT", M1, to_ms(at(0)), to_ms(at(10)), 100)
    assert [c.trades for c in minutes] == [2, 2, 2, 9, 9, 2, 2, 9, 2, 2]


async def test_repair_rebuilds_closed_higher_timeframes(store: Store) -> None:
    await seed(store, [0, 1, 2, 5, 6, 8, 9], watermark=to_ms(at(10, 5)))

    await Backfiller(store, FakeVenue()).repair("BTC-USDT", lookback_ms=60 * 60_000)

    fives = await store.get_candles("BTC-USDT", Timeframe.M5, to_ms(at(0)), to_ms(at(10)), 10)
    assert [(c.time, c.trades, c.closed) for c in fives] == [
        (to_ms(at(0)), 2 + 2 + 2 + 9 + 9, True),
        (to_ms(at(5)), 2 + 2 + 9 + 2 + 2, True),
    ]
    # The hour and the day are still open: the watermark has not passed them.
    assert await store.get_candles("BTC-USDT", Timeframe.H1, 0, to_ms(at(60)), 10) == []


async def test_repair_leaves_minutes_after_the_watermark_to_the_live_path(store: Store) -> None:
    await seed(store, [0, 1], watermark=to_ms(at(2, 30)))
    venue = FakeVenue()

    report = await Backfiller(store, venue).repair("BTC-USDT", lookback_ms=60 * 60_000)

    assert report.missing == 0
    assert venue.requests == []


async def test_backfill_overwrites_a_range_up_to_the_settled_minute(store: Store) -> None:
    await seed(store, [0, 1, 2], watermark=to_ms(at(4, 10)))

    written = await Backfiller(store, FakeVenue()).backfill("BTC-USDT", to_ms(at(0)), to_ms(at(30)))

    assert written == 4  # minutes 0-3; minute 4 is not settled yet
    minutes = await store.get_candles("BTC-USDT", M1, 0, to_ms(at(60)), 100)
    assert [(c.time, c.close) for c in minutes] == [(to_ms(at(m)), Decimal(200)) for m in range(4)]


async def test_backfill_without_a_checkpoint_uses_a_margin_behind_now(store: Store) -> None:
    backfiller = Backfiller(store, FakeVenue(), clock_ms=lambda: to_ms(at(5, 30)))

    written = await backfiller.backfill("BTC-USDT", to_ms(at(0)), to_ms(at(30)))

    assert written == 3  # settled before 10:03, two minutes behind 10:05:30
