import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from mdp.schema import Candle, Side, Trade
from mdp.timeframes import Timeframe, from_ms, to_ms


def make_trade(**overrides: object) -> Trade:
    fields: dict[str, object] = {
        "source": "binance",
        "symbol": "BTC-USDT",
        "trade_id": "42",
        "price": "65000.10",
        "qty": "0.00100000",
        "side": "buy",
        "ts_exchange": "2026-09-24T10:00:00.123Z",
        "ts_ingested": "2026-09-24T10:00:00.140Z",
    }
    fields.update(overrides)
    return Trade.model_validate(fields)


def test_trade_round_trips_through_json_with_decimal_strings() -> None:
    trade = make_trade()

    wire = json.loads(trade.model_dump_json())

    assert wire["price"] == "65000.10"
    assert wire["qty"] == "0.00100000"
    assert Trade.model_validate_json(trade.model_dump_json()) == trade


def test_small_decimals_are_written_fixed_point_not_scientific() -> None:
    trade = make_trade(qty=Decimal("1E-8"))

    assert json.loads(trade.model_dump_json())["qty"] == "0.00000001"


def test_timestamps_are_converted_to_utc() -> None:
    tehran = timezone(timedelta(hours=3, minutes=30))
    trade = make_trade(ts_exchange=datetime(2026, 9, 24, 13, 30, tzinfo=tehran))

    assert trade.ts_exchange == datetime(2026, 9, 24, 10, 0, tzinfo=UTC)
    assert trade.ts_exchange.tzinfo is UTC


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("price", "0"),
        ("qty", "-1"),
        ("price", "NaN"),
        ("side", "long"),
        ("symbol", "btcusdt"),
        ("ts_exchange", "2026-09-24T10:00:00"),  # naive: which clock?
        ("trade_id", ""),
    ],
)
def test_invalid_fields_are_refused(field: str, value: str) -> None:
    with pytest.raises(ValidationError):
        make_trade(**{field: value})


def test_trade_key_and_millisecond_timestamp() -> None:
    trade = make_trade()

    assert trade.key == ("binance", "42")
    assert trade.ts_ms == 1_790_244_000_123
    assert trade.side is Side.BUY


def test_timeframe_floor_aligns_to_utc_boundaries() -> None:
    ts = to_ms(datetime(2026, 9, 24, 10, 7, 59, 999_000, tzinfo=UTC))

    assert from_ms(Timeframe.M1.floor(ts)) == datetime(2026, 9, 24, 10, 7, tzinfo=UTC)
    assert from_ms(Timeframe.M5.floor(ts)) == datetime(2026, 9, 24, 10, 5, tzinfo=UTC)
    assert from_ms(Timeframe.H1.floor(ts)) == datetime(2026, 9, 24, 10, tzinfo=UTC)
    assert from_ms(Timeframe.D1.floor(ts)) == datetime(2026, 9, 24, tzinfo=UTC)


def test_candle_payload_uses_chart_friendly_field_names() -> None:
    candle = Candle(
        symbol="BTC-USDT",
        tf=Timeframe.M1,
        time=1_790_244_000_000,
        open=Decimal("1.5"),
        high=Decimal("2"),
        low=Decimal("1"),
        close=Decimal("1.75"),
        volume=Decimal("3"),
        quote_volume=Decimal("4.5"),
        trades=2,
        closed=True,
    )

    assert json.loads(candle.model_dump_json()) == {
        "symbol": "BTC-USDT",
        "tf": "1m",
        "time": 1_790_244_000_000,
        "open": "1.5",
        "high": "2",
        "low": "1",
        "close": "1.75",
        "volume": "3",
        "quote_volume": "4.5",
        "trades": 2,
        "closed": True,
    }
