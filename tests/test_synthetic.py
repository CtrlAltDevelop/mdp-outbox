from datetime import UTC, datetime

from mdp.normalizer import SourceEvent
from mdp.schema import Trade
from mdp.sources.synthetic import SyntheticSource

START = datetime(2026, 9, 24, 10, 0, tzinfo=UTC)


def replay(seed: int = 7, limit: int = 500) -> list[Trade]:
    return list(
        SyntheticSource(["BTC-USDT", "ETH-USDT"], seed=seed, start=START, limit=limit).trades()
    )


def test_the_same_seed_gives_the_same_trades() -> None:
    assert replay(seed=3) == replay(seed=3)


def test_a_different_seed_gives_different_trades() -> None:
    assert replay(seed=3) != replay(seed=4)


def test_trades_are_valid_ordered_and_uniquely_identified() -> None:
    trades = replay()

    assert [t.ts_exchange for t in trades] == sorted(t.ts_exchange for t in trades)
    assert len({t.key for t in trades}) == len(trades)
    assert all(t.ts_exchange >= START for t in trades)
    assert {t.symbol for t in trades} == {"BTC-USDT", "ETH-USDT"}


async def test_live_mode_sleeps_out_the_gaps_and_stamps_the_wall_clock() -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    source = SyntheticSource(
        ["BTC-USDT"], seed=1, rate=10, limit=3, clock=lambda: START, sleep=fake_sleep
    )
    events: list[SourceEvent] = [event async for event in source.events()]

    assert len(events) == 3
    assert len(slept) == 3
    assert all(isinstance(e, Trade) and e.ts_exchange == START for e in events)
