"""The stateful candle aggregator: trades in, OHLCV snapshots out.

Buckets are assigned by **event time** (the exchange's timestamp), never by
when a trade happened to reach us, so a replay of the same trades produces the
same candles however they were delayed or reordered on the way.

A bucket stays open until the symbol's **watermark** passes its end. The
watermark trails the newest event time seen by ``allowed_lateness``: it is the
pipeline's claim that nothing older is still in flight. Until then trades may
arrive in any order and land in the right bucket; ``open`` and ``close`` are
decided by event time (see ``_order_key``), not arrival order.

Every timeframe is computed directly from the trades rather than rolled up from
1m candles, so a 1d candle is live-updated on the same trade that moves the 1m
one. A trade is judged against its 1m bucket, the first to close; once it is
accepted it is folded into every timeframe, so no two timeframes ever disagree
about which trades they contain.

A quiet market produces no trades to push the watermark, so ``advance`` also
moves it on the wall clock: once ``allowed_lateness + idle_grace`` has passed
with nothing newer, the last candle closes on time instead of waiting for the
next trade. The trade-off — a source whose clock runs behind ours by more than
the grace has its trades judged late — is discussed in ADR 0001.

The aggregator is synchronous and does no I/O. The service around it decides
when to commit, publish and acknowledge; this class only decides what the
candles are.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from mdp.schema import Candle, Trade
from mdp.timeframes import BASE_TIMEFRAME, TIMEFRAMES, Timeframe

type OrderKey = tuple[int, int, str, str]
type BucketKey = tuple[Timeframe, int]


def _order_key(trade: Trade) -> OrderKey:
    """Event-time order, with a deterministic tie-break for equal timestamps.

    Exchanges stamp in milliseconds, so ties are common. Ids break them: most
    sources issue increasing numeric ids, and comparing ``(len, id)`` orders
    "9" before "10" the way the numbers do without assuming ids are numeric.
    """
    return (trade.ts_ms, len(trade.trade_id), trade.trade_id, trade.source)


@dataclass(slots=True)
class CandleBuilder:
    symbol: str
    tf: Timeframe
    start: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trades: int
    first: OrderKey
    last: OrderKey

    @classmethod
    def from_trade(cls, trade: Trade, tf: Timeframe, key: OrderKey) -> CandleBuilder:
        return cls(
            symbol=trade.symbol,
            tf=tf,
            start=tf.floor(trade.ts_ms),
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.qty,
            quote_volume=trade.price * trade.qty,
            trades=1,
            first=key,
            last=key,
        )

    @property
    def end(self) -> int:
        return self.start + self.tf.ms

    def add(self, trade: Trade, key: OrderKey) -> None:
        price = trade.price
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        if key < self.first:
            self.first, self.open = key, price
        if key >= self.last:
            self.last, self.close = key, price
        self.volume += trade.qty
        self.quote_volume += price * trade.qty
        self.trades += 1

    def to_state(self) -> dict[str, Any]:
        return {
            "tf": self.tf.value,
            "start": self.start,
            "ohlc": [str(self.open), str(self.high), str(self.low), str(self.close)],
            "volume": str(self.volume),
            "quote_volume": str(self.quote_volume),
            "trades": self.trades,
            "first": list(self.first),
            "last": list(self.last),
        }

    @classmethod
    def from_state(cls, symbol: str, data: dict[str, Any]) -> CandleBuilder:
        o, h, lo, c = (Decimal(v) for v in data["ohlc"])
        first, last = data["first"], data["last"]
        return cls(
            symbol=symbol,
            tf=Timeframe(data["tf"]),
            start=data["start"],
            open=o,
            high=h,
            low=lo,
            close=c,
            volume=Decimal(data["volume"]),
            quote_volume=Decimal(data["quote_volume"]),
            trades=data["trades"],
            first=(first[0], first[1], first[2], first[3]),
            last=(last[0], last[1], last[2], last[3]),
        )

    def snapshot(self, *, closed: bool) -> Candle:
        return Candle(
            symbol=self.symbol,
            tf=self.tf,
            time=self.start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            quote_volume=self.quote_volume,
            trades=self.trades,
            closed=closed,
        )


@dataclass(slots=True)
class _SymbolState:
    watermark: int | None = None  # None until the first trade: nothing can be late yet
    max_event: int | None = None
    open: dict[BucketKey, CandleBuilder] = field(default_factory=dict)
    # Keys of the trades folded into each open 1m bucket. A duplicate always
    # lands in the same bucket as its original, so this is all the memory
    # dedup needs, and it is dropped the moment the bucket closes.
    seen: dict[int, set[tuple[str, str]]] = field(default_factory=dict)
    dirty: set[BucketKey] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class BatchResult:
    candles: list[Candle]
    late: list[Trade]


class Aggregator:
    def __init__(
        self,
        *,
        allowed_lateness_ms: int = 5_000,
        timeframes: Sequence[Timeframe] = TIMEFRAMES,
    ) -> None:
        if allowed_lateness_ms < 0:
            raise ValueError("allowed lateness cannot be negative")
        if BASE_TIMEFRAME not in timeframes:
            raise ValueError(f"{BASE_TIMEFRAME} is required: lateness is judged against it")
        self._lateness = allowed_lateness_ms
        self._timeframes = tuple(tf for tf in TIMEFRAMES if tf in timeframes)
        self._symbols: dict[str, _SymbolState] = {}

    def watermark(self, symbol: str) -> int | None:
        state = self._symbols.get(symbol)
        return state.watermark if state else None

    def advance(self, now_ms: int, idle_grace_ms: int) -> None:
        """Move every watermark up to ``now - lateness - grace`` on the wall clock.

        Symbols that have never traded are left alone: they have nothing open,
        and starting their watermark here would mark a slow first trade late.
        """
        floor = now_ms - self._lateness - idle_grace_ms
        for state in self._symbols.values():
            if state.watermark is not None and floor > state.watermark:
                state.watermark = floor

    def observe(self, trades: Iterable[Trade]) -> tuple[list[Trade], list[Trade]]:
        """Split trades into on-time and late, advancing watermarks as it goes.

        Order matters: a trade is judged against the watermark as it stood when
        the trade arrived, exactly as if trades were fed one at a time. Buckets
        are not closed here — ``flush`` does that after ``apply`` — so an early
        trade in a batch still lands in a bucket that a later trade in the same
        batch pushes past the watermark.
        """
        on_time: list[Trade] = []
        late: list[Trade] = []
        lateness = self._lateness
        for trade in trades:
            state = self._symbols.get(trade.symbol)
            if state is None:
                state = self._symbols[trade.symbol] = _SymbolState()
            ts = trade.ts_ms
            if state.watermark is not None and BASE_TIMEFRAME.floor(ts) + BASE_TIMEFRAME.ms <= (
                state.watermark
            ):
                late.append(trade)
                continue
            on_time.append(trade)
            if state.max_event is None or ts > state.max_event:
                state.max_event = ts
                candidate = ts - lateness
                if state.watermark is None or candidate > state.watermark:
                    state.watermark = candidate
        return on_time, late

    def apply(self, trades: Iterable[Trade]) -> None:
        """Fold trades that ``observe`` accepted into their open buckets."""
        for trade in trades:
            state = self._symbols[trade.symbol]
            base = BASE_TIMEFRAME.floor(trade.ts_ms)
            seen = state.seen.setdefault(base, set())
            if trade.key in seen:
                continue
            seen.add(trade.key)
            key = _order_key(trade)
            for tf in self._timeframes:
                bucket = (tf, tf.floor(trade.ts_ms))
                builder = state.open.get(bucket)
                if builder is None:
                    state.open[bucket] = CandleBuilder.from_trade(trade, tf, key)
                else:
                    builder.add(trade, key)
                state.dirty.add(bucket)

    def flush(self) -> list[Candle]:
        """Close every bucket the watermark has passed; snapshot what changed.

        Returns one snapshot per candle touched since the last flush — however
        many trades touched it — so downstream writes scale with candles, not
        trades. Closed candles are always included, marked ``closed=True``.
        """
        out: list[Candle] = []
        for state in self._symbols.values():
            watermark = state.watermark
            if watermark is not None:
                for bucket in [b for b, c in state.open.items() if c.end <= watermark]:
                    out.append(state.open.pop(bucket).snapshot(closed=True))
                    state.dirty.discard(bucket)
                for start in [s for s in state.seen if s + BASE_TIMEFRAME.ms <= watermark]:
                    del state.seen[start]
            out.extend(state.open[bucket].snapshot(closed=False) for bucket in state.dirty)
            state.dirty.clear()
        out.sort(key=lambda c: (c.symbol, c.time, c.tf.ms))
        return out

    def export_state(self, symbol: str) -> dict[str, Any] | None:
        """Everything needed to resume ``symbol`` after a restart, as plain JSON types.

        Call it straight after ``flush``: the open builders are then exactly the
        candles the watermark has not passed, which is what a restart must
        rebuild. The dedup keys are deliberately left out — after a restart the
        trade log's unique key catches replays, and it is the only dedup that
        survives a crash anyway.
        """
        state = self._symbols.get(symbol)
        if state is None or state.watermark is None:
            return None
        return {
            "watermark": state.watermark,
            "max_event": state.max_event,
            "open": [builder.to_state() for builder in state.open.values()],
        }

    def restore_state(self, symbol: str, data: dict[str, Any]) -> None:
        builders = [CandleBuilder.from_state(symbol, item) for item in data["open"]]
        self._symbols[symbol] = _SymbolState(
            watermark=data["watermark"],
            max_event=data["max_event"],
            open={(b.tf, b.start): b for b in builders if b.tf in self._timeframes},
        )

    def process(self, trades: Iterable[Trade]) -> BatchResult:
        """``observe``, ``apply`` and ``flush`` in one step, when no store sits between."""
        on_time, late = self.observe(trades)
        self.apply(on_time)
        return BatchResult(self.flush(), late)
