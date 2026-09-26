"""Timeframes and the epoch-millisecond arithmetic that buckets trades into them.

Everything inside the pipeline works in integer milliseconds since the Unix
epoch: bucketing is then a floor division, exact and cheap, with no timezone or
DST to get wrong. Every timeframe here divides a UTC day evenly, so buckets line
up with midnight UTC — the same boundaries exchanges and Timescale's
``time_bucket`` use.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MS = timedelta(milliseconds=1)


class Timeframe(StrEnum):
    M1 = "1m"
    M5 = "5m"
    H1 = "1h"
    D1 = "1d"

    @property
    def ms(self) -> int:
        return _TIMEFRAME_MS[self]

    @property
    def interval(self) -> timedelta:
        return timedelta(milliseconds=self.ms)

    def floor(self, ts_ms: int) -> int:
        """Start of the bucket that contains ``ts_ms``."""
        return ts_ms - ts_ms % self.ms


_TIMEFRAME_MS: dict[Timeframe, int] = {
    Timeframe.M1: 60_000,
    Timeframe.M5: 300_000,
    Timeframe.H1: 3_600_000,
    Timeframe.D1: 86_400_000,
}

# Finest first. The aggregator relies on this order: a trade is judged late
# against its 1m bucket, which is always the first of its buckets to close.
TIMEFRAMES: tuple[Timeframe, ...] = (Timeframe.M1, Timeframe.M5, Timeframe.H1, Timeframe.D1)
BASE_TIMEFRAME = Timeframe.M1


def to_ms(ts: datetime) -> int:
    """Exact epoch milliseconds (floored) for an aware datetime."""
    return (ts - _EPOCH) // _ONE_MS


def from_ms(ts_ms: int) -> datetime:
    return _EPOCH + timedelta(milliseconds=ts_ms)
