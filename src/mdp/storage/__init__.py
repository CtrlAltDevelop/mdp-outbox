"""Persistence: the trade log, the candles, and the aggregator's checkpoint.

The aggregator writes all three in **one transaction per batch**. That is what
turns at-least-once delivery from Redis into an exactly-once effect on the
candles: the trade log's ``(source, trade_id)`` key reports which trades in a
redelivered batch are genuinely new, and the candles and checkpoint that
include them commit or roll back together with them. See ADR 0002.
"""

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from mdp.schema import Candle, Trade
from mdp.timeframes import Timeframe


class StoreTransaction(Protocol):
    async def insert_trades(self, trades: Sequence[Trade]) -> list[Trade]:
        """Log the trades; return only those not already logged, first copy of each."""
        ...

    async def upsert_candles(self, candles: Sequence[Candle]) -> None:
        """Write full candle snapshots. Replaying the same snapshot changes nothing."""
        ...

    async def save_state(self, symbol: str, state: dict[str, Any]) -> None: ...


class CandleReader(Protocol):
    async def get_candles(
        self, symbol: str, tf: Timeframe, start_ms: int, end_ms: int, limit: int
    ) -> list[Candle]:
        """Candles with ``start_ms <= time < end_ms``, oldest first."""
        ...


class Store(CandleReader, Protocol):
    def transaction(self) -> AbstractAsyncContextManager[StoreTransaction]: ...

    async def load_states(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]: ...

    async def missing_minutes(self, symbol: str, start_ms: int, end_ms: int) -> list[int]:
        """1m buckets in ``[start_ms, end_ms)`` with no candle, after the first one that has."""
        ...

    async def rollup(self, symbol: str, tf: Timeframe, start_ms: int, end_ms: int) -> int:
        """Rebuild the ``tf`` candles covering ``[start_ms, end_ms)`` from 1m ones, as closed.

        Both bounds are rounded down to ``tf``, so only whole buckets ending by
        ``end_ms`` are written. Returns how many candles were written.
        """
        ...

    async def ping(self) -> None: ...

    async def close(self) -> None: ...
