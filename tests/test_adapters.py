"""Adapter contract tests, driven by frames recorded from the live feeds.

``binance_trades.jsonl`` and ``kraken_trades.jsonl`` are unedited frames
captured from the public endpoints; ``engine_events.jsonl`` is hand-written in
the engine adapter's default format. Nothing here touches the network.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio as aioredis

from mdp.normalizer import Rejected, RejectReason, SourceEvent
from mdp.schema import Side, Trade
from mdp.sources import binance, engine, kraken
from mdp.sources.binance import BinanceSource
from mdp.sources.engine import EngineFormat, EngineSource
from mdp.sources.kraken import KrakenSource
from mdp.sources.ws import Backoff, Socket, WebSocketFeed

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 9, 26, 18, 10, tzinfo=UTC)


def frames(name: str) -> list[str]:
    return (FIXTURES / name).read_text(encoding="utf-8").splitlines()


def only_trades(events: list[SourceEvent]) -> list[Trade]:
    assert all(isinstance(e, Trade) for e in events), events
    return [e for e in events if isinstance(e, Trade)]


# --- Binance ---------------------------------------------------------------

BINANCE_SYMBOLS = {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT"}


def test_binance_recorded_frames_all_decode_to_trades() -> None:
    events = [
        e
        for raw in frames("binance_trades.jsonl")
        for e in binance.parse_message(raw, BINANCE_SYMBOLS, NOW)
    ]

    trades = only_trades(events)
    assert len(trades) == 12
    assert {t.symbol for t in trades} == {"BTC-USDT", "ETH-USDT"}


def test_binance_fields_map_onto_the_schema() -> None:
    [first] = binance.parse_message(frames("binance_trades.jsonl")[0], BINANCE_SYMBOLS, NOW)

    assert first == Trade(
        source="binance",
        symbol="BTC-USDT",
        trade_id="6715234671",
        price=Decimal("84106.09000000"),
        qty=Decimal("0.00007000"),
        side=Side.SELL,  # "m": true, the buyer was the maker, so the taker sold
        ts_exchange=datetime(2026, 9, 26, 18, 9, 12, 678_000, tzinfo=UTC),  # "T", not "E"
        ts_ingested=NOW,
    )


def test_binance_taker_buy_when_the_buyer_is_not_the_maker() -> None:
    [second] = binance.parse_message(frames("binance_trades.jsonl")[1], BINANCE_SYMBOLS, NOW)

    assert isinstance(second, Trade)
    assert second.side is Side.BUY


def test_binance_control_frames_are_ignored() -> None:
    assert binance.parse_message('{"result":null,"id":1}', BINANCE_SYMBOLS) == []


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("not json", RejectReason.MALFORMED),
        ('{"data":{"e":"trade","s":"BTCUSDT"}}', RejectReason.MALFORMED),
        (
            '{"data":{"e":"trade","s":"BTCUSDT","t":1,"p":"-1","q":"1","T":1,"m":true}}',
            RejectReason.MALFORMED,
        ),
        (
            '{"data":{"e":"trade","s":"XRPUSDT","t":1,"p":"1","q":"1","T":1,"m":true}}',
            RejectReason.UNKNOWN_SYMBOL,
        ),
    ],
)
def test_binance_bad_frames_become_rejections(raw: str, reason: RejectReason) -> None:
    [event] = binance.parse_message(raw, BINANCE_SYMBOLS)

    assert isinstance(event, Rejected)
    assert event.reason is reason
    assert event.payload == raw


def test_binance_stream_url_names_every_symbol() -> None:
    assert binance.stream_url(["BTC-USDT", "ETH-USDT"], "wss://x/stream") == (
        "wss://x/stream?streams=btcusdt@trade/ethusdt@trade"
    )


# --- Kraken ----------------------------------------------------------------

KRAKEN_SYMBOLS = {"BTC/USD": "BTC-USD", "ETH/USD": "ETH-USD"}


def test_kraken_recorded_session_decodes_snapshots_and_updates() -> None:
    events = [
        e
        for raw in frames("kraken_trades.jsonl")
        for e in kraken.parse_message(raw, KRAKEN_SYMBOLS, NOW)
    ]

    trades = only_trades(events)
    assert len(trades) == 50 + 50 + 1 + 1  # two snapshots, two updates; heartbeats skipped
    assert len({t.key for t in trades}) == len(trades)


def test_kraken_numbers_keep_every_digit() -> None:
    snapshot = frames("kraken_trades.jsonl")[3]
    first = only_trades(kraken.parse_message(snapshot, KRAKEN_SYMBOLS, NOW))[0]

    assert first.price == Decimal("84098.4")
    assert first.qty == Decimal("0.04661621")
    assert first.trade_id == "109183836"
    assert first.side is Side.BUY
    assert first.ts_exchange == datetime(2026, 9, 26, 18, 7, 54, 626_190, tzinfo=UTC)


def test_kraken_subscription_names_venue_symbols() -> None:
    assert '"symbol": ["BTC/USD", "ETH/USD"]' in kraken.subscribe_message(["BTC-USD", "ETH-USD"])


# --- Matching engine -------------------------------------------------------


def test_engine_events_are_filtered_scaled_and_mapped() -> None:
    fmt = EngineFormat(price_scale=2, qty_scale=5)
    lookup = engine.symbol_lookup(["BTC-USDT"])
    events = [
        e
        for raw in frames("engine_events.jsonl")
        for e in engine.parse_message(raw, fmt, lookup, now=NOW)
    ]

    first, second, unknown, garbled = events  # order events skipped
    assert isinstance(first, Trade)
    assert (first.price, first.qty, first.side) == (
        Decimal("84106.09"),
        Decimal("0.00007"),
        Side.SELL,
    )
    assert first.ts_exchange == datetime(2026, 9, 26, 18, 9, 12, 678_000, tzinfo=UTC)
    assert isinstance(second, Trade)
    assert (second.symbol, second.side) == ("BTC-USDT", Side.BUY)  # alias and case accepted
    assert isinstance(unknown, Rejected)
    assert unknown.reason is RejectReason.UNKNOWN_SYMBOL
    assert isinstance(garbled, Rejected)
    assert garbled.reason is RejectReason.MALFORMED


def test_engine_timestamps_in_other_units_are_exact() -> None:
    fmt = EngineFormat(ts="t", ts_unit="ns")
    raw = (
        '{"trade_id":1,"symbol":"BTC-USDT","price":"1.5","qty":"2","taker_side":"buy",'
        '"t":1790446152678123456}'
    )

    [trade] = engine.parse_message(raw, fmt, engine.symbol_lookup(["BTC-USDT"]))

    assert isinstance(trade, Trade)
    assert trade.ts_exchange == datetime(2026, 9, 26, 18, 9, 12, 678_123, tzinfo=UTC)


async def test_engine_over_a_redis_stream_resumes_and_acknowledges(redis: aioredis.Redis) -> None:
    for raw in frames("engine_events.jsonl")[:3]:
        await redis.xadd("engine:events", {"data": raw})
    source = EngineSource.over_redis(
        redis, "engine:events", ["BTC-USDT"], fmt=EngineFormat(price_scale=2, qty_scale=5)
    )

    got: list[SourceEvent] = []
    async for event in source.events():
        got.append(event)
        if len(got) == 2:
            break

    assert [e.trade_id for e in only_trades(got)] == ["1001", "1002"]
    pending = await redis.xpending("engine:events", "mdp-ingest")
    assert pending["pending"] <= 1  # at most the entry being handled when we stopped


# --- Source classes over a fake feed ---------------------------------------


class FakeFeed:
    def __init__(self, url: str, **kwargs: Any) -> None:
        self.url = url
        self.kwargs = kwargs
        self.lines: list[str] = []

    async def messages(self) -> AsyncIterator[str]:
        for line in self.lines:
            yield line


async def test_binance_source_parses_what_its_feed_yields() -> None:
    feeds: list[FakeFeed] = []

    def factory(url: str) -> Any:
        feeds.append(FakeFeed(url))
        feeds[-1].lines = frames("binance_trades.jsonl")
        return feeds[-1]

    source = BinanceSource(["BTC-USDT", "ETH-USDT"], feed_factory=factory)
    trades = only_trades([e async for e in source.events()])

    assert len(trades) == 12
    assert feeds[0].url.endswith("?streams=btcusdt@trade/ethusdt@trade")


async def test_kraken_source_subscribes_and_watches_for_silence() -> None:
    feeds: list[FakeFeed] = []

    def factory(url: str, **kwargs: Any) -> Any:
        feeds.append(FakeFeed(url, **kwargs))
        feeds[-1].lines = frames("kraken_trades.jsonl")
        return feeds[-1]

    source = KrakenSource(["BTC-USD", "ETH-USD"], feed_factory=factory)
    trades = only_trades([e async for e in source.events()])

    assert len(trades) == 102
    assert feeds[0].kwargs["stale_after_s"] == 15.0
    assert "subscribe" in feeds[0].kwargs["subscribe"][0]


# --- The reconnecting feed -------------------------------------------------


class ScriptedSocket:
    """Plays back frames, then fails with ``end`` (an exception, or None to hang)."""

    def __init__(self, frames: list[str], end: BaseException | None) -> None:
        self.frames = list(frames)
        self.end = end
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        if self.frames:
            return self.frames.pop(0)
        if self.end is None:
            await asyncio.sleep(3600)  # a silent but open connection
        raise self.end  # type: ignore[misc]


def scripted(*sockets: ScriptedSocket | BaseException) -> Any:
    queue = list(sockets)

    @asynccontextmanager
    async def connector(url: str) -> AsyncIterator[Socket]:
        item = queue.pop(0)
        if isinstance(item, BaseException):
            raise item
        yield item

    return connector


async def take(feed: WebSocketFeed, n: int) -> list[str]:
    out: list[str] = []
    async for message in feed.messages():
        out.append(message)
        if len(out) == n:
            break
    return out


async def test_the_feed_reconnects_resubscribes_and_backs_off() -> None:
    first = ScriptedSocket(["a", "b"], ConnectionResetError())
    second = ScriptedSocket(["c"], ConnectionResetError())
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    feed = WebSocketFeed(
        "wss://feed",
        subscribe=["SUB"],
        connector=scripted(first, OSError("refused"), OSError("refused"), second),
        sleep=sleep,
        backoff=Backoff(base_s=1, cap_s=8),
        rng=random.Random(1),
    )

    assert await take(feed, 3) == ["a", "b", "c"]
    assert first.sent == ["SUB"]
    assert second.sent == ["SUB"]
    assert len(slept) == 3  # one wait per reconnect
    # attempt 0 (after a healthy connection), then 1 and 2 while refused
    assert [s <= cap for s, cap in zip(slept, [1, 2, 4], strict=True)] == [True, True, True]


async def test_a_silent_connection_is_dropped_by_the_watchdog() -> None:
    silent = ScriptedSocket(["hello"], None)
    fresh = ScriptedSocket(["again"], ConnectionResetError())

    async def no_sleep(seconds: float) -> None:
        return None

    feed = WebSocketFeed(
        "wss://feed", stale_after_s=0.05, connector=scripted(silent, fresh), sleep=no_sleep
    )

    assert await take(feed, 2) == ["hello", "again"]


def test_full_jitter_stays_under_the_exponential_cap() -> None:
    backoff = Backoff(base_s=0.5, cap_s=30)
    rng = random.Random(3)

    for attempt in range(12):
        ceiling = min(30.0, 0.5 * 2**attempt)
        delays = [backoff.delay(attempt, rng) for _ in range(200)]
        assert all(0 <= d <= ceiling for d in delays)
        assert max(delays) > ceiling / 2  # jitter spreads over the whole range
