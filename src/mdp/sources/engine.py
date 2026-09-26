"""A generic adapter for a matching engine's trade events: JSON over WebSocket or Redis.

Engines differ in field names and in how they encode numbers, so both are
configuration rather than code. The defaults match an engine that publishes
integer ticks and lots with a millisecond timestamp::

    {"type": "trade", "trade_id": 812, "symbol": "BTC-USDT",
     "price": 6331001, "qty": 250, "taker_side": "buy", "ts": 1727172000124}

with ``price_scale=2`` and ``qty_scale=4`` that is 63310.01 for 0.0250. Events
whose ``type`` is anything but ``trade`` (order accepted, cancelled...) are
skipped, so the adapter can sit on an engine's full event stream.

Two transports:

* **WebSocket** — through ``WebSocketFeed``, so reconnects and backoff come free;
* **Redis stream** — read with a consumer group, so a restart resumes where it
  stopped instead of at the stream's tail. An entry is acknowledged when the
  next one is requested, i.e. after its trade was handed to the ingest queue.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, cast

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from mdp.normalizer import Rejected, RejectReason, SourceEvent, parse_trade
from mdp.sources.ws import WebSocketFeed

type TimeUnit = Literal["s", "ms", "us", "ns", "iso"]
_PER_SECOND: dict[str, int] = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class EngineFormat:
    trade_id: str = "trade_id"
    symbol: str = "symbol"
    price: str = "price"
    qty: str = "qty"
    side: str = "taker_side"
    ts: str = "ts"
    ts_unit: TimeUnit = "ms"
    price_scale: int = 0  # decimal places implied by an integer price
    qty_scale: int = 0


DEFAULT_FORMAT = EngineFormat()


def _scaled(value: Any, scale: int) -> Decimal:
    # Via str: Decimal(0.1) is 0.1000000000000000055511151231257827...
    number = value if isinstance(value, Decimal) else Decimal(str(value))
    return number.scaleb(-scale) if scale else number


def _timestamp(value: Any, unit: TimeUnit) -> datetime:
    if unit == "iso":
        return datetime.fromisoformat(str(value))
    # Exact integer microseconds: a float of epoch nanoseconds has lost the
    # last digits before the conversion even starts.
    micros = int(Decimal(value) * 1_000_000 / _PER_SECOND[unit])
    return _EPOCH + timedelta(microseconds=micros)


def symbol_lookup(symbols: Sequence[str]) -> dict[str, str]:
    """Accept ``BTC-USDT``, ``BTCUSDT`` or ``BTC/USDT`` for the canonical ``BTC-USDT``."""
    lookup: dict[str, str] = {}
    for symbol in symbols:
        for alias in (symbol, symbol.replace("-", ""), symbol.replace("-", "/")):
            lookup[alias] = symbol
    return lookup


def parse_message(
    raw: str,
    fmt: EngineFormat,
    symbols: Mapping[str, str],
    *,
    source: str = "engine",
    now: datetime | None = None,
) -> list[SourceEvent]:
    """Decode a frame holding one event or a JSON array of them."""
    try:
        decoded = json.loads(raw, parse_float=Decimal)
        items = decoded if isinstance(decoded, list) else [decoded]
        ingested = now or datetime.now(UTC)
        events: list[SourceEvent] = []
        for item in items:
            if item.get("type", "trade") != "trade":
                continue
            symbol = symbols.get(str(item[fmt.symbol]))
            if symbol is None:
                events.append(Rejected(source, RejectReason.UNKNOWN_SYMBOL, raw, item[fmt.symbol]))
                continue
            events.append(
                parse_trade(
                    source,
                    raw,
                    symbol=symbol,
                    trade_id=str(item[fmt.trade_id]),
                    price=_scaled(item[fmt.price], fmt.price_scale),
                    qty=_scaled(item[fmt.qty], fmt.qty_scale),
                    side=str(item[fmt.side]).lower(),
                    ts_exchange=_timestamp(item[fmt.ts], fmt.ts_unit),
                    ts_ingested=ingested,
                )
            )
        return events
    except (ArithmeticError, ValueError, KeyError, TypeError, AttributeError) as exc:
        return [Rejected(source, RejectReason.MALFORMED, raw, repr(exc))]


async def redis_stream_messages(
    redis: aioredis.Redis,
    stream: str,
    *,
    group: str = "mdp-ingest",
    consumer: str = "ingest-1",
    field: str = "data",
    block_ms: int = 5_000,
) -> AsyncIterator[str]:
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    cursor = "0"  # our own unacknowledged entries first, then new ones
    while True:
        reply = cast(
            "list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes] | None]]]] | None",
            await redis.xreadgroup(group, consumer, {stream: cursor}, count=100, block=block_ms),
        )
        entries = [entry for _, batch in reply or [] for entry in batch]
        if not entries and cursor == "0":
            cursor = ">"
            continue
        for message_id, fields in entries:
            payload = (fields or {}).get(field.encode())
            if payload is not None:
                yield payload.decode()
            await redis.xack(stream, group, message_id)


class EngineSource:
    def __init__(
        self,
        messages: Callable[[], AsyncIterator[str]],
        symbols: Sequence[str],
        *,
        fmt: EngineFormat = DEFAULT_FORMAT,
        name: str = "engine",
    ) -> None:
        self._messages = messages
        self._symbols = symbol_lookup(symbols)
        self._fmt = fmt
        self._name = name

    @classmethod
    def over_websocket(cls, url: str, symbols: Sequence[str], **kwargs: Any) -> EngineSource:
        return cls(WebSocketFeed(url).messages, symbols, **kwargs)

    @classmethod
    def over_redis(
        cls, redis: aioredis.Redis, stream: str, symbols: Sequence[str], **kwargs: Any
    ) -> EngineSource:
        return cls(lambda: redis_stream_messages(redis, stream), symbols, **kwargs)

    @property
    def name(self) -> str:
        return self._name

    async def events(self) -> AsyncIterator[SourceEvent]:
        async for raw in self._messages():
            for event in parse_message(raw, self._fmt, self._symbols, source=self._name):
                yield event
