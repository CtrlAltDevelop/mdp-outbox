"""Ingest -> Redis Streams -> aggregation, against Redis and the memory store.

Uses the real Redis at ``REDIS_URL`` when there is one, fakeredis otherwise.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from itertools import takewhile
from typing import cast

import pytest
import redis.asyncio as aioredis
from prometheus_client import REGISTRY

from mdp.aggregation import AggregationService
from mdp.aggregator import Aggregator
from mdp.ingest import IngestService
from mdp.normalizer import Normalizer, Rejected, RejectReason, SourceEvent
from mdp.schema import Candle, Trade
from mdp.sources.synthetic import SyntheticSource
from mdp.storage.memory import MemoryStore
from mdp.streams import DLQ_STREAM, candle_channel, trades_stream
from mdp.timeframes import Timeframe, to_ms
from tests.helpers import at, latest, trade

SYMBOLS = ["BTC-USDT", "ETH-USDT"]


class ListSource:
    """A source that yields a fixed list of events, then ends."""

    def __init__(self, events: list[SourceEvent]) -> None:
        self._events = events

    @property
    def name(self) -> str:
        return "list"

    async def events(self) -> AsyncIterator[SourceEvent]:
        for event in self._events:
            yield event


def replay(minutes: int = 3, seed: int = 4) -> list[Trade]:
    source = SyntheticSource(SYMBOLS, seed=seed, rate=8, start=at(0))
    return list(takewhile(lambda t: t.ts_exchange < at(minutes), source.trades()))


async def ingest(redis: aioredis.Redis, events: list[SourceEvent]) -> None:
    normalizer = Normalizer(SYMBOLS, clock=lambda: at(60))
    await IngestService(ListSource(events), normalizer, redis, batch_size=50).run()


def service(redis: aioredis.Redis, store: MemoryStore, **kwargs: object) -> AggregationService:
    # Replayed trades are stamped in the past, so the wall-clock tick is off.
    return AggregationService(
        redis,
        store,
        SYMBOLS,
        idle_grace_ms=None,
        batch_size=64,
        **kwargs,  # type: ignore[arg-type]
    )


async def drain(svc: AggregationService) -> None:
    while await svc.poll():
        pass


async def dead_letters(redis: aioredis.Redis) -> list[dict[bytes, bytes]]:
    entries = cast("list[tuple[bytes, dict[bytes, bytes]]]", await redis.xrange(DLQ_STREAM))
    return [fields for _, fields in entries]


def expected(trades: list[Trade]) -> dict[tuple[str, Timeframe, int], Candle]:
    return latest(Aggregator().process(trades).candles)


async def test_ingest_writes_one_stream_entry_per_unique_trade(redis: aioredis.Redis) -> None:
    trades = replay()

    await ingest(redis, [*trades, *trades[:20]])  # a reconnect replays the first 20

    lengths = [await redis.xlen(trades_stream(s)) for s in SYMBOLS]
    assert sum(lengths) == len(trades)


async def test_ingest_dead_letters_what_the_source_or_normalizer_refuses(
    redis: aioredis.Redis,
) -> None:
    garbled = Rejected("binance", RejectReason.MALFORMED, '{"p": "x"}', "price: invalid")
    unknown = trade(at(0), symbol="DOGE-USDT")

    await ingest(redis, [garbled, unknown])

    entries = await dead_letters(redis)
    assert [fields[b"reason"] for fields in entries] == [b"malformed", b"unknown_symbol"]
    assert entries[0][b"payload"] == b'{"p": "x"}'
    assert entries[0][b"stage"] == b"ingest"


async def test_the_pipeline_builds_the_same_candles_as_the_aggregator_alone(
    redis: aioredis.Redis,
) -> None:
    trades = replay()
    store = MemoryStore()
    await ingest(redis, list(trades))

    svc = service(redis, store)
    await svc.start()
    await drain(svc)

    assert store.candles == expected(trades)
    for stream in (trades_stream(s) for s in SYMBOLS):
        assert (await redis.xpending(stream, "aggregator"))["pending"] == 0


async def test_candles_are_published_on_their_channel(redis: aioredis.Redis) -> None:
    store = MemoryStore()
    await ingest(redis, [trade(at(0, 1), "100", trade_id=1)])
    pubsub = redis.pubsub()
    await pubsub.subscribe(candle_channel("BTC-USDT", Timeframe.M1))
    await pubsub.get_message(timeout=1)  # the subscribe confirmation

    svc = service(redis, store)
    await svc.start()
    await drain(svc)

    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
    assert message is not None
    payload = json.loads(message["data"])
    assert (payload["symbol"], payload["tf"], payload["close"]) == ("BTC-USDT", "1m", "100")
    await pubsub.aclose()  # type: ignore[no-untyped-call]


async def test_a_late_trade_is_dead_lettered_and_acknowledged(redis: aioredis.Redis) -> None:
    store = MemoryStore()
    await ingest(redis, [trade(at(0, 1), trade_id=1), trade(at(2), trade_id=2)])
    svc = service(redis, store)
    await svc.start()
    await drain(svc)

    await ingest(redis, [trade(at(0, 30), trade_id=3)])  # its minute closed long ago
    await drain(svc)

    [fields] = await dead_letters(redis)
    assert fields[b"reason"] == b"late"
    assert fields[b"stage"] == b"aggregate"
    assert (await redis.xpending(trades_stream("BTC-USDT"), "aggregator"))["pending"] == 0


class CrashingService(AggregationService):
    """Dies once, at a chosen point, after a chosen number of batches."""

    def __init__(self, *args: object, crash_after: int, when: str, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._left = crash_after
        self._when = when

    async def process(self, entries: object) -> list[Candle]:
        self._left -= 1
        if self._left == 0 and self._when == "before-commit":
            raise ConnectionError("killed before the transaction committed")
        return await super().process(entries)  # type: ignore[arg-type]

    async def _finish(self, *args: object, **kwargs: object) -> None:
        if self._left == 0 and self._when == "before-ack":
            raise ConnectionError("killed after the commit, before the ack")
        await super()._finish(*args, **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("when", ["before-commit", "before-ack"])
async def test_a_crash_mid_stream_still_counts_every_trade_exactly_once(
    redis: aioredis.Redis, when: str
) -> None:
    trades = replay()
    store = MemoryStore()
    await ingest(redis, list(trades))

    doomed = CrashingService(
        redis, store, SYMBOLS, idle_grace_ms=None, batch_size=64, crash_after=4, when=when
    )
    await doomed.start()
    with pytest.raises(ConnectionError):
        await drain(doomed)

    # A fresh process, a new consumer name: it must claim the dead one's
    # pending messages, resume from the checkpoint and skip what was counted.
    survivor = service(redis, store, consumer="aggregator-2")
    await survivor.start()
    await drain(survivor)

    assert store.candles == expected(trades)


async def test_restart_mid_bucket_closes_the_candle_correctly(redis: aioredis.Redis) -> None:
    store = MemoryStore()
    first_half = [trade(at(0, s), str(100 + s), trade_id=s) for s in range(1, 30)]
    second_half = [trade(at(0, s), str(100 + s), trade_id=s) for s in range(30, 60)]
    closer = trade(at(1, 10), "1", trade_id=99)

    await ingest(redis, list(first_half))
    before = service(redis, store)
    await before.start()
    await drain(before)
    assert not store.candles[("BTC-USDT", Timeframe.M1, to_ms(at(0)))].closed
    del before  # killed mid-minute

    await ingest(redis, [*second_half, closer])
    after = service(redis, store)
    await after.start()
    await drain(after)

    minute = store.candles[("BTC-USDT", Timeframe.M1, to_ms(at(0)))]
    assert minute == expected([*first_half, *second_half, closer])[minute.key]
    assert minute.closed
    assert minute.trades == 59
    assert (str(minute.open), str(minute.close)) == ("101", "159")


async def test_the_lag_probe_exports_stream_and_watermark_gauges(redis: aioredis.Redis) -> None:
    await ingest(redis, [trade(at(0, 1), trade_id=1), trade(at(0, 2), trade_id=2)])
    svc = service(redis, MemoryStore(), clock_ms=lambda: to_ms(at(1)))
    await svc.start()
    await svc.poll()

    await svc.probe_lag()

    assert REGISTRY.get_sample_value("mdp_stream_pending_entries", {"symbol": "BTC-USDT"}) == 0
    delay = REGISTRY.get_sample_value("mdp_watermark_delay_seconds", {"symbol": "BTC-USDT"})
    assert delay == 60 - 2 + 5  # the clock at 10:01, the watermark at 10:00:02 minus 5s
