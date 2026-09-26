from datetime import UTC, datetime, timedelta

from mdp.normalizer import Normalizer, Rejected, RejectReason, parse_trade
from mdp.schema import Trade

NOW = datetime(2026, 9, 24, 10, 0, tzinfo=UTC)


def trade(trade_id: str = "1", symbol: str = "BTC-USDT", ts: datetime = NOW) -> Trade:
    result = parse_trade(
        "binance",
        "{}",
        symbol=symbol,
        trade_id=trade_id,
        price="100",
        qty="1",
        side="sell",
        ts_exchange=ts,
        ts_ingested=ts,
    )
    assert isinstance(result, Trade)
    return result


def normalizer(window: int = 100) -> Normalizer:
    return Normalizer(["BTC-USDT"], dedup_window=window, clock=lambda: NOW)


def test_accepts_a_valid_trade() -> None:
    t = trade()

    assert normalizer().admit(t) is t


def test_drops_a_repeated_source_and_trade_id() -> None:
    n = normalizer()
    n.admit(trade("7"))

    assert n.admit(trade("7")) is None


def test_the_same_trade_id_from_another_source_is_not_a_duplicate() -> None:
    n = normalizer()
    n.admit(trade("7"))
    other = trade("7").model_copy(update={"source": "kraken"})

    assert n.admit(other) is other


def test_the_dedup_window_forgets_the_oldest_keys() -> None:
    n = normalizer(window=2)
    for trade_id in ("1", "2", "3"):
        n.admit(trade(trade_id))

    assert n.admit(trade("1")) is not None  # evicted, so admitted again
    assert n.admit(trade("3")) is None


def test_refuses_a_symbol_outside_the_deployment() -> None:
    result = normalizer().admit(trade(symbol="ETH-USDT"))

    assert isinstance(result, Rejected)
    assert result.reason is RejectReason.UNKNOWN_SYMBOL


def test_refuses_a_trade_stamped_far_in_the_future() -> None:
    result = normalizer().admit(trade(ts=NOW + timedelta(minutes=5)))

    assert isinstance(result, Rejected)
    assert result.reason is RejectReason.FUTURE


def test_parse_trade_reports_the_failing_field() -> None:
    result = parse_trade("binance", '{"p": "-1"}', symbol="BTC-USDT", trade_id="1")

    assert isinstance(result, Rejected)
    assert result.reason is RejectReason.MALFORMED
    assert result.payload == '{"p": "-1"}'
    assert "price" in result.detail
