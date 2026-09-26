"""The gate every trade passes before it is written to a stream.

Adapters turn a source's wire format into ``Trade`` objects; the normalizer
decides which of those the pipeline accepts. Anything it refuses becomes a
``Rejected`` record carrying the original payload, so the dead-letter queue
holds enough to replay or debug it later.

Deduplication here is a cheap first line, not the guarantee: a reconnecting
WebSocket replays the last few trades, and dropping them before Redis saves a
round trip. The durable guarantee is the ``(source, trade_id)`` key in the
trade log, which the aggregator checks inside its database transaction.
"""

from __future__ import annotations

from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from pydantic import ValidationError

from mdp.schema import Trade


class RejectReason(StrEnum):
    MALFORMED = "malformed"  # could not be decoded into a trade at all
    UNKNOWN_SYMBOL = "unknown_symbol"  # valid, but not a symbol this deployment handles
    FUTURE = "future"  # stamped further ahead of our clock than skew explains
    LATE = "late"  # arrived after its bucket closed (set by the aggregator)


@dataclass(frozen=True, slots=True)
class Rejected:
    source: str
    reason: RejectReason
    payload: str
    detail: str = ""


type SourceEvent = Trade | Rejected


def parse_trade(source: str, payload: str, **fields: Any) -> Trade | Rejected:
    """Build a trade from decoded fields, or a ``Rejected`` naming what was wrong."""
    try:
        return Trade(source=source, **fields)
    except ValidationError as exc:
        return Rejected(source, RejectReason.MALFORMED, payload, _summarise(exc))


def _summarise(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in exc.errors()
    )


class Normalizer:
    def __init__(
        self,
        symbols: Collection[str],
        *,
        dedup_window: int = 100_000,
        max_clock_skew: timedelta = timedelta(seconds=30),
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._symbols = frozenset(symbols)
        self._window = dedup_window
        self._max_skew = max_clock_skew
        self._clock = clock
        # A dict keeps insertion order, so the oldest key is always first:
        # a FIFO window with O(1) insert, lookup and eviction.
        self._seen: dict[tuple[str, str], None] = {}

    def admit(self, trade: Trade) -> Trade | Rejected | None:
        """Return the trade if accepted, a rejection, or ``None`` for a duplicate."""
        if trade.symbol not in self._symbols:
            return self._reject(trade, RejectReason.UNKNOWN_SYMBOL, trade.symbol)
        if trade.ts_exchange - self._clock() > self._max_skew:
            return self._reject(trade, RejectReason.FUTURE, trade.ts_exchange.isoformat())

        key = trade.key
        if key in self._seen:
            return None
        self._seen[key] = None
        if len(self._seen) > self._window:
            del self._seen[next(iter(self._seen))]
        return trade

    @staticmethod
    def _reject(trade: Trade, reason: RejectReason, detail: str) -> Rejected:
        return Rejected(trade.source, reason, trade.model_dump_json(), detail)
