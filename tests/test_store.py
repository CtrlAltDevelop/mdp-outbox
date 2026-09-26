"""The store contract, run against the memory store and TimescaleDB alike."""

from decimal import Decimal

import pytest

from mdp.schema import Candle
from mdp.storage import Store
from mdp.timeframes import Timeframe, to_ms
from tests.helpers import at, trade

M1 = Timeframe.M1


def candle(
    minute: int,
    *,
    tf: Timeframe = M1,
    o: str = "100",
    h: str = "110",
    lo: str = "90",
    c: str = "105",
    volume: str = "2",
    trades: int = 3,
    closed: bool = True,
) -> Candle:
    return Candle(
        symbol="BTC-USDT",
        tf=tf,
        time=to_ms(at(minute)),
        open=Decimal(o),
        high=Decimal(h),
        low=Decimal(lo),
        close=Decimal(c),
        volume=Decimal(volume),
        quote_volume=Decimal(volume) * Decimal(c),
        trades=trades,
        closed=closed,
    )


async def all_candles(store: Store, tf: Timeframe = M1) -> list[Candle]:
    return await store.get_candles("BTC-USDT", tf, to_ms(at(-60)), to_ms(at(60)), 1_000)


async def test_insert_trades_reports_only_the_new_ones(store: Store) -> None:
    a, b = trade(at(0, 1), trade_id=1), trade(at(0, 2), trade_id=2)
    async with store.transaction() as tx:
        assert await tx.insert_trades([a, a, b]) == [a, b]

    async with store.transaction() as tx:
        assert await tx.insert_trades([b, trade(at(0, 3), trade_id=3)]) == [
            trade(at(0, 3), trade_id=3)
        ]


async def test_upserting_the_same_snapshot_twice_changes_nothing(store: Store) -> None:
    for _ in range(2):
        async with store.transaction() as tx:
            await tx.upsert_candles([candle(0)])

    assert await all_candles(store) == [candle(0)]


async def test_a_later_snapshot_replaces_an_open_candle(store: Store) -> None:
    async with store.transaction() as tx:
        await tx.upsert_candles([candle(0, c="101", closed=False)])
    async with store.transaction() as tx:
        await tx.upsert_candles([candle(0, c="102", closed=True)])

    assert await all_candles(store) == [candle(0, c="102", closed=True)]


async def test_an_open_snapshot_never_reopens_a_closed_candle(store: Store) -> None:
    async with store.transaction() as tx:
        await tx.upsert_candles([candle(0, c="102", closed=True)])
    async with store.transaction() as tx:
        await tx.upsert_candles([candle(0, c="999", closed=False)])

    assert await all_candles(store) == [candle(0, c="102", closed=True)]


async def write_everything_then_crash(store: Store) -> None:
    async with store.transaction() as tx:
        await tx.insert_trades([trade(at(0), trade_id=1)])
        await tx.upsert_candles([candle(0)])
        await tx.save_state("BTC-USDT", {"watermark": 1})
        raise RuntimeError("crash before commit")


async def test_a_failed_transaction_leaves_nothing_behind(store: Store) -> None:
    with pytest.raises(RuntimeError):
        await write_everything_then_crash(store)

    assert await all_candles(store) == []
    assert await store.load_states(["BTC-USDT"]) == {}
    async with store.transaction() as tx:
        assert len(await tx.insert_trades([trade(at(0), trade_id=1)])) == 1


async def test_state_round_trips(store: Store) -> None:
    state = {"watermark": 5, "max_event": 10, "open": [{"tf": "1m", "ohlc": ["1", "2"]}]}
    async with store.transaction() as tx:
        await tx.save_state("BTC-USDT", state)

    assert await store.load_states(["BTC-USDT", "ETH-USDT"]) == {"BTC-USDT": state}


async def test_get_candles_is_half_open_ordered_and_limited(store: Store) -> None:
    async with store.transaction() as tx:
        await tx.upsert_candles([candle(m) for m in (3, 1, 2, 0)])

    got = await store.get_candles("BTC-USDT", M1, to_ms(at(1)), to_ms(at(3)), 10)
    assert [c.time for c in got] == [to_ms(at(1)), to_ms(at(2))]
    limited = await store.get_candles("BTC-USDT", M1, to_ms(at(0)), to_ms(at(4)), 2)
    assert [c.time for c in limited] == [to_ms(at(0)), to_ms(at(1))]


async def test_missing_minutes_start_after_the_first_candle(store: Store) -> None:
    async with store.transaction() as tx:
        await tx.upsert_candles([candle(m) for m in (2, 3, 6)])

    missing = await store.missing_minutes("BTC-USDT", to_ms(at(0)), to_ms(at(8)))

    assert missing == [to_ms(at(m)) for m in (4, 5, 7)]


async def test_missing_minutes_is_empty_without_any_history(store: Store) -> None:
    assert await store.missing_minutes("BTC-USDT", to_ms(at(0)), to_ms(at(8))) == []


async def test_rollup_rebuilds_higher_timeframes_from_minutes(store: Store) -> None:
    minutes = [
        candle(m, o=str(100 + m), h=str(200 + m), lo=str(50 - m), c=str(101 + m), trades=1)
        for m in range(10)
    ]
    async with store.transaction() as tx:
        await tx.upsert_candles(minutes)

    written = await store.rollup("BTC-USDT", Timeframe.M5, to_ms(at(0)), to_ms(at(10)))

    assert written == 2
    first, second = await all_candles(store, Timeframe.M5)
    assert (first.open, first.high, first.low, first.close) == (
        Decimal(100),
        Decimal(204),
        Decimal(46),
        Decimal(105),
    )
    assert first.volume == Decimal(10)
    assert first.trades == 5
    assert first.closed
    assert (second.open, second.close) == (Decimal(105), Decimal(110))
