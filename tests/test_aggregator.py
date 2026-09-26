import random
from decimal import Decimal
from itertools import takewhile

from mdp.aggregator import Aggregator
from mdp.schema import Candle, Trade
from mdp.sources.synthetic import SyntheticSource
from mdp.timeframes import Timeframe, to_ms
from tests.helpers import at, latest, trade

M1 = Timeframe.M1
M5 = Timeframe.M5


def one(candles: list[Candle], tf: Timeframe, minute: int) -> Candle:
    [match] = [c for c in candles if c.tf is tf and c.time == to_ms(at(minute))]
    return match


def run(trades: list[Trade], lateness_ms: int = 5_000, batch: int = 1) -> list[Candle]:
    agg = Aggregator(allowed_lateness_ms=lateness_ms)
    out: list[Candle] = []
    for i in range(0, len(trades), batch):
        out.extend(agg.process(trades[i : i + batch]).candles)
    return out


def test_ohlcv_of_a_single_minute() -> None:
    candles = run(
        [
            trade(at(0, 1), "100", "1", 1),
            trade(at(0, 2), "105", "2", 2),
            trade(at(0, 3), "95", "0.5", 3),
            trade(at(0, 4), "102", "1.5", 4),
        ]
    )

    c = latest(candles)[("BTC-USDT", M1, to_ms(at(0)))]
    assert (c.open, c.high, c.low, c.close) == (
        Decimal("100"),
        Decimal("105"),
        Decimal("95"),
        Decimal("102"),
    )
    assert c.volume == Decimal("5")
    assert c.quote_volume == Decimal("100") + Decimal("210") + Decimal("47.5") + Decimal("153")
    assert c.trades == 4
    assert not c.closed


def test_a_trade_on_the_boundary_opens_the_next_minute() -> None:
    candles = latest(
        run([trade(at(0, 59.999), "100", trade_id=1), trade(at(1, 0), "200", trade_id=2)])
    )

    assert candles[("BTC-USDT", M1, to_ms(at(0)))].close == Decimal("100")
    assert candles[("BTC-USDT", M1, to_ms(at(1)))].open == Decimal("200")
    five = candles[("BTC-USDT", M5, to_ms(at(0)))]
    assert (five.open, five.close, five.trades) == (Decimal("100"), Decimal("200"), 2)


def test_a_candle_closes_only_when_the_watermark_passes_its_end() -> None:
    agg = Aggregator(allowed_lateness_ms=5_000)
    agg.process([trade(at(0, 30), trade_id=1)])

    still_open = agg.process([trade(at(1, 4.999), trade_id=2)]).candles
    assert not any(c.closed for c in still_open)

    closing = agg.process([trade(at(1, 5), trade_id=3)]).candles
    assert one(closing, M1, 0).closed
    assert not one(closing, M1, 1).closed


def test_minutes_without_trades_get_no_candle() -> None:
    candles = latest(run([trade(at(0, 10), "100", trade_id=1), trade(at(3, 10), "90", trade_id=2)]))

    minutes = sorted(t for (_, tf, t) in candles if tf is M1)
    assert minutes == [to_ms(at(0)), to_ms(at(3))]
    # The next candle opens at its own first trade, not at the previous close.
    assert candles[("BTC-USDT", M1, to_ms(at(3)))].open == Decimal("90")
    assert candles[("BTC-USDT", M5, to_ms(at(0)))].trades == 2


def test_each_flush_emits_one_snapshot_per_changed_candle() -> None:
    trades = [trade(at(0, i / 10), "100", trade_id=i) for i in range(100)]

    candles = Aggregator().process(trades).candles

    assert sorted(c.tf for c in candles) == sorted(Timeframe)
    assert all(c.trades == 100 for c in candles)


def synthetic(minutes: int = 6, seed: int = 11) -> list[Trade]:
    source = SyntheticSource(["BTC-USDT", "ETH-USDT"], seed=seed, rate=15, start=at(0))
    return list(takewhile(lambda t: t.ts_exchange < at(minutes), source.trades()))


def delayed_arrival(trades: list[Trade], max_delay_ms: int, seed: int = 5) -> list[Trade]:
    """Reorder as a network would: each trade arrives up to ``max_delay_ms`` late."""
    rng = random.Random(seed)
    return sorted(trades, key=lambda t: t.ts_ms + rng.randrange(max_delay_ms))


def test_out_of_order_within_lateness_gives_the_same_candles_as_in_order() -> None:
    trades = synthetic()
    shuffled = delayed_arrival(trades, max_delay_ms=5_000)
    assert shuffled != trades

    assert latest(run(shuffled, batch=7)) == latest(run(trades, batch=7))


def test_every_trade_sent_twice_gives_the_same_candles() -> None:
    trades = synthetic()
    twice = delayed_arrival(trades + trades, max_delay_ms=3_000, seed=9)

    assert latest(run(twice, batch=13)) == latest(run(trades, batch=13))


def test_batch_size_does_not_change_the_candles() -> None:
    trades = delayed_arrival(synthetic(), max_delay_ms=2_000)

    assert latest(run(trades, batch=1)) == latest(run(trades, batch=500))


def test_equal_timestamps_are_ordered_by_trade_id_not_arrival() -> None:
    ts = at(0, 1)
    in_order = [trade(ts, "1", trade_id=9), trade(ts, "2", trade_id=10)]

    reversed_candles = latest(run(in_order[::-1]))
    candle = reversed_candles[("BTC-USDT", M1, to_ms(at(0)))]
    assert (candle.open, candle.close) == (Decimal("1"), Decimal("2"))
