"""An in-process store with the same transactional contract as TimescaleDB.

Used by the tests, the benchmarks and a database-free demo. Writes inside a
transaction are buffered and applied only if the block exits cleanly, so a test
can crash the aggregator mid-batch and see exactly what a real rollback leaves.
"""

from __future__ import annotations

import copy
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any

from mdp.schema import Candle, Trade
from mdp.timeframes import BASE_TIMEFRAME, Timeframe

type CandleKey = tuple[str, Timeframe, int]


class _MemoryTransaction:
    def __init__(self, store: MemoryStore) -> None:
        self._store = store
        self.trade_keys: set[tuple[str, str]] = set()
        self.candles: dict[CandleKey, Candle] = {}
        self.states: dict[str, dict[str, Any]] = {}

    async def insert_trades(self, trades: Sequence[Trade]) -> list[Trade]:
        fresh: list[Trade] = []
        for trade in trades:
            if trade.key in self._store.trade_keys or trade.key in self.trade_keys:
                continue
            self.trade_keys.add(trade.key)
            fresh.append(trade)
        return fresh

    async def upsert_candles(self, candles: Sequence[Candle]) -> None:
        for candle in candles:
            current = self.candles.get(candle.key) or self._store.candles.get(candle.key)
            # Same guard as the SQL upsert: an open snapshot never reopens a closed candle.
            if current is not None and current.closed and not candle.closed:
                continue
            self.candles[candle.key] = candle

    async def save_state(self, symbol: str, state: dict[str, Any]) -> None:
        self.states[symbol] = copy.deepcopy(state)


class MemoryStore:
    def __init__(self) -> None:
        self.trade_keys: set[tuple[str, str]] = set()
        self.candles: dict[CandleKey, Candle] = {}
        self.states: dict[str, dict[str, Any]] = {}

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_MemoryTransaction]:
        tx = _MemoryTransaction(self)
        yield tx
        self.trade_keys |= tx.trade_keys
        self.candles.update(tx.candles)
        self.states.update(tx.states)

    async def load_states(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
        return {s: copy.deepcopy(self.states[s]) for s in symbols if s in self.states}

    async def get_candles(
        self, symbol: str, tf: Timeframe, start_ms: int, end_ms: int, limit: int
    ) -> list[Candle]:
        rows = sorted(
            (c for (s, t, time), c in self.candles.items() if s == symbol and t is tf),
            key=lambda c: c.time,
        )
        return [c for c in rows if start_ms <= c.time < end_ms][:limit]

    async def missing_minutes(self, symbol: str, start_ms: int, end_ms: int) -> list[int]:
        step = BASE_TIMEFRAME.ms
        present = {
            time
            for (s, tf, time) in self.candles
            if s == symbol and tf is BASE_TIMEFRAME and start_ms <= time < end_ms
        }
        if not present:
            return []
        return [t for t in range(min(present), end_ms, step) if t not in present]

    async def rollup(self, symbol: str, tf: Timeframe, start_ms: int, end_ms: int) -> int:
        start, end = tf.floor(start_ms), tf.floor(end_ms)
        groups: dict[int, list[Candle]] = {}
        for (s, t, time), candle in self.candles.items():
            if s == symbol and t is BASE_TIMEFRAME and start <= tf.floor(time) < end:
                groups.setdefault(tf.floor(time), []).append(candle)
        for bucket, minutes in groups.items():
            minutes.sort(key=lambda c: c.time)
            self.candles[(symbol, tf, bucket)] = Candle(
                symbol=symbol,
                tf=tf,
                time=bucket,
                open=minutes[0].open,
                high=max(c.high for c in minutes),
                low=min(c.low for c in minutes),
                close=minutes[-1].close,
                volume=sum((c.volume for c in minutes), Decimal(0)),
                quote_volume=sum((c.quote_volume for c in minutes), Decimal(0)),
                trades=sum(c.trades for c in minutes),
                closed=True,
            )
        return len(groups)

    async def ping(self) -> None:
        return None

    async def close(self) -> None:
        return None
