"""The two shapes that cross process boundaries: a normalized trade and a candle.

Prices and quantities are ``Decimal`` in memory and fixed-point strings on the
wire. A float cannot hold 0.1 exactly, and a volume summed from thousands of
floats drifts away from the exchange's own figure; strings keep every digit the
source sent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, PlainSerializer, field_validator

from mdp.timeframes import Timeframe, to_ms

# "12.5", never "1.25E+1": clients parse these with double.parse / parseFloat,
# and a fixed-point string is what every one of them handles.
DecimalStr = Annotated[Decimal, PlainSerializer(lambda d: format(d, "f"), return_type=str)]
PositiveDecimal = Annotated[DecimalStr, Field(gt=0, allow_inf_nan=False)]

SYMBOL_PATTERN = r"^[A-Z0-9]+-[A-Z0-9]+$"


class Side(StrEnum):
    """The taker's side: who crossed the spread."""

    BUY = "buy"
    SELL = "sell"


class Trade(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str = Field(min_length=1)
    symbol: str = Field(pattern=SYMBOL_PATTERN)
    trade_id: str = Field(min_length=1)
    price: PositiveDecimal
    qty: PositiveDecimal
    side: Side
    ts_exchange: AwareDatetime
    ts_ingested: AwareDatetime

    @field_validator("ts_exchange", "ts_ingested")
    @classmethod
    def _as_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @property
    def key(self) -> tuple[str, str]:
        """Identity for deduplication: a trade id is only unique within its source."""
        return (self.source, self.trade_id)

    @property
    def ts_ms(self) -> int:
        return to_ms(self.ts_exchange)


class Candle(BaseModel):
    """One OHLCV bucket.

    ``time`` is the bucket's open time in epoch milliseconds, the field name and
    unit ``ohlcv_chart``'s ``KLineEntity.fromJson`` reads. ``closed`` turns true
    once the watermark has passed the bucket's end; before that the candle is a
    live snapshot that later updates replace.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str
    tf: Timeframe
    time: int
    open: DecimalStr
    high: DecimalStr
    low: DecimalStr
    close: DecimalStr
    volume: DecimalStr
    quote_volume: DecimalStr
    trades: int
    closed: bool

    @property
    def key(self) -> tuple[str, Timeframe, int]:
        return (self.symbol, self.tf, self.time)
