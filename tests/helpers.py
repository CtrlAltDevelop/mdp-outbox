"""Builders shared by the test modules."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mdp.schema import Candle, Side, Trade
from mdp.timeframes import Timeframe

T0 = datetime(2026, 9, 24, 10, 0, tzinfo=UTC)


def at(minutes: float = 0, seconds: float = 0) -> datetime:
    return T0 + timedelta(minutes=minutes, seconds=seconds)


def trade(
    ts: datetime,
    price: str | Decimal = "100",
    qty: str | Decimal = "1",
    trade_id: str | int = "1",
    *,
    symbol: str = "BTC-USDT",
    source: str = "test",
) -> Trade:
    return Trade(
        source=source,
        symbol=symbol,
        trade_id=str(trade_id),
        price=Decimal(price),
        qty=Decimal(qty),
        side=Side.BUY,
        ts_exchange=ts,
        ts_ingested=ts,
    )


def latest(candles: Iterable[Candle]) -> dict[tuple[str, Timeframe, int], Candle]:
    """Fold a stream of snapshots into the last one seen for each candle."""
    out: dict[tuple[str, Timeframe, int], Candle] = {}
    for candle in candles:
        out[candle.key] = candle
    return out
