"""Lateness, idle closing, and resuming from exported state."""

import json
from decimal import Decimal
from itertools import takewhile

from mdp.aggregator import Aggregator
from mdp.schema import Candle, Trade
from mdp.sources.synthetic import SyntheticSource
from mdp.timeframes import Timeframe, to_ms
from tests.helpers import at, latest, trade

M1_AT_0 = ("BTC-USDT", Timeframe.M1, to_ms(at(0)))


def test_a_trade_older_than_the_watermark_is_kept_while_its_bucket_is_open() -> None:
    agg = Aggregator(allowed_lateness_ms=5_000)
    agg.process([trade(at(0, 30), trade_id=1), trade(at(0, 50), trade_id=2)])

    result = agg.process([trade(at(0, 40), "90", trade_id=3)])  # behind the watermark

    assert result.late == []
    assert latest(result.candles)[M1_AT_0].low == Decimal("90")


def test_a_trade_for_a_closed_bucket_is_late_and_changes_nothing() -> None:
    agg = Aggregator(allowed_lateness_ms=5_000)
    closed = latest(
        agg.process([trade(at(0, 30), trade_id=1), trade(at(1, 10), trade_id=2)]).candles
    )[M1_AT_0]
    assert closed.closed

    straggler = trade(at(0, 59), "1", trade_id=3)
    result = agg.process([straggler])

    assert result.late == [straggler]
    assert result.candles == []


def test_a_late_trade_does_not_move_the_watermark() -> None:
    agg = Aggregator(allowed_lateness_ms=0)
    agg.process([trade(at(2), trade_id=1)])

    agg.process([trade(at(0), trade_id=2)])

    assert agg.watermark("BTC-USDT") == to_ms(at(2))


def test_the_wall_clock_closes_a_quiet_minute() -> None:
    agg = Aggregator(allowed_lateness_ms=5_000)
    agg.process([trade(at(0, 30), trade_id=1)])

    agg.advance(to_ms(at(1, 6)), idle_grace_ms=2_000)  # watermark 10:00:59: still open
    assert not any(c.closed for c in agg.flush())

    agg.advance(to_ms(at(1, 7)), idle_grace_ms=2_000)  # watermark 10:01:00: closed
    assert latest(agg.flush())[M1_AT_0].closed


def test_the_wall_clock_never_moves_a_watermark_backwards() -> None:
    agg = Aggregator(allowed_lateness_ms=0)
    agg.process([trade(at(5), trade_id=1)])

    agg.advance(to_ms(at(1)), idle_grace_ms=0)

    assert agg.watermark("BTC-USDT") == to_ms(at(5))


def replay_trades(minutes: int) -> list[Trade]:
    source = SyntheticSource(["BTC-USDT", "ETH-USDT"], seed=21, rate=10, start=at(0))
    return list(takewhile(lambda t: t.ts_exchange < at(minutes), source.trades()))


def run_through(agg: Aggregator, trades: list[Trade], batch: int = 25) -> list[Candle]:
    out: list[Candle] = []
    for i in range(0, len(trades), batch):
        out.extend(agg.process(trades[i : i + batch]).candles)
    return out


def test_restart_mid_bucket_resumes_from_exported_state() -> None:
    trades = replay_trades(minutes=4)
    # Cut in the middle of a minute, with that minute's candle still open.
    cut = next(i for i, t in enumerate(trades) if t.ts_exchange >= at(2, 30))

    uninterrupted = latest(run_through(Aggregator(), trades))

    first = Aggregator()
    before = run_through(first, trades[:cut])
    saved = {s: first.export_state(s) for s in ("BTC-USDT", "ETH-USDT")}
    del first  # the process dies; only what was exported survives

    second = Aggregator()
    for symbol, state in json.loads(json.dumps(saved)).items():
        second.restore_state(symbol, state)
    after = run_through(second, trades[cut:])

    assert latest(before + after) == uninterrupted


def test_export_is_empty_for_a_symbol_that_never_traded() -> None:
    assert Aggregator().export_state("BTC-USDT") is None
