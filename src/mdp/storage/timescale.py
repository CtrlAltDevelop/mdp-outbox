"""The TimescaleDB store, on psycopg 3 and a connection pool.

Batches are written with ``unnest`` over typed arrays: one statement and one
round trip per batch whatever its size, rather than one per row.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from importlib.resources import files
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from mdp.schema import Candle, Trade
from mdp.timeframes import BASE_TIMEFRAME, Timeframe, from_ms, to_ms

_INSERT_TRADES = """
INSERT INTO trades (ts, source, trade_id, symbol, price, qty, side, ts_ingested)
SELECT * FROM unnest(
    %s::timestamptz[], %s::text[], %s::text[], %s::text[],
    %s::numeric[], %s::numeric[], %s::text[], %s::timestamptz[]
)
ON CONFLICT DO NOTHING
RETURNING source, trade_id
"""

# The WHERE clause keeps a closed candle closed: only another closed snapshot
# (a repair, a backfill) may overwrite it, never a stale open one.
_UPSERT_CANDLES = """
INSERT INTO candles
    (symbol, tf, bucket, open, high, low, close, volume, quote_volume, trades, closed)
SELECT * FROM unnest(
    %s::text[], %s::text[], %s::timestamptz[], %s::numeric[], %s::numeric[],
    %s::numeric[], %s::numeric[], %s::numeric[], %s::numeric[], %s::bigint[], %s::boolean[]
)
ON CONFLICT (symbol, tf, bucket) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    volume = EXCLUDED.volume, quote_volume = EXCLUDED.quote_volume,
    trades = EXCLUDED.trades, closed = EXCLUDED.closed, updated_at = now()
WHERE NOT candles.closed OR EXCLUDED.closed
"""

_SAVE_STATE = """
INSERT INTO aggregator_state (symbol, state) VALUES (%s, %s)
ON CONFLICT (symbol) DO UPDATE SET state = EXCLUDED.state, updated_at = now()
"""

_SELECT_CANDLES = """
SELECT symbol, tf, bucket, open, high, low, close, volume, quote_volume, trades, closed
FROM candles
WHERE symbol = %s AND tf = %s AND bucket >= %s AND bucket < %s
ORDER BY bucket
LIMIT %s
"""

# Only minutes after the first candle in the window count: before it the
# symbol may simply not have been listed or ingested yet.
_MISSING_MINUTES = """
WITH first AS (
    SELECT min(bucket) AS b FROM candles
    WHERE symbol = %(symbol)s AND tf = '1m' AND bucket >= %(start)s AND bucket < %(end)s
)
SELECT g FROM first, generate_series(first.b, %(end)s - interval '1 minute', interval '1 minute') g
WHERE NOT EXISTS (
    SELECT 1 FROM candles c WHERE c.symbol = %(symbol)s AND c.tf = '1m' AND c.bucket = g
)
ORDER BY g
"""

# `first` and `last` are Timescale aggregates: the value at the earliest and
# latest bucket, which is exactly a candle's open and close.
_ROLLUP = """
INSERT INTO candles
    (symbol, tf, bucket, open, high, low, close, volume, quote_volume, trades, closed)
SELECT symbol, %(tf)s, time_bucket(%(width)s, bucket) AS b,
       first(open, bucket), max(high), min(low), last(close, bucket),
       sum(volume), sum(quote_volume), sum(trades), true
FROM candles
WHERE symbol = %(symbol)s AND tf = '1m' AND bucket >= %(start)s AND bucket < %(end)s
GROUP BY symbol, b
ON CONFLICT (symbol, tf, bucket) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    volume = EXCLUDED.volume, quote_volume = EXCLUDED.quote_volume,
    trades = EXCLUDED.trades, closed = true, updated_at = now()
"""


def schema_sql() -> str:
    return files("mdp.storage").joinpath("schema.sql").read_text(encoding="utf-8")


async def migrate(database_url: str) -> None:
    async with await AsyncConnection.connect(database_url, autocommit=True) as conn:
        # Bytes, because a script read from a file is not a LiteralString, and
        # psycopg only accepts plain `str` queries that are.
        await conn.execute(schema_sql().encode())


class _TimescaleTransaction:
    def __init__(self, conn: AsyncConnection[TupleRow]) -> None:
        self._conn = conn

    async def insert_trades(self, trades: Sequence[Trade]) -> list[Trade]:
        if not trades:
            return []
        cur = await self._conn.execute(
            _INSERT_TRADES,
            (
                [t.ts_exchange for t in trades],
                [t.source for t in trades],
                [t.trade_id for t in trades],
                [t.symbol for t in trades],
                [t.price for t in trades],
                [t.qty for t in trades],
                [t.side.value for t in trades],
                [t.ts_ingested for t in trades],
            ),
        )
        inserted = {(row[0], row[1]) for row in await cur.fetchall()}
        fresh: list[Trade] = []
        for trade in trades:
            if trade.key in inserted:
                inserted.discard(trade.key)  # a second copy in the same batch is a duplicate
                fresh.append(trade)
        return fresh

    async def upsert_candles(self, candles: Sequence[Candle]) -> None:
        if not candles:
            return
        await self._conn.execute(
            _UPSERT_CANDLES,
            (
                [c.symbol for c in candles],
                [c.tf.value for c in candles],
                [from_ms(c.time) for c in candles],
                [c.open for c in candles],
                [c.high for c in candles],
                [c.low for c in candles],
                [c.close for c in candles],
                [c.volume for c in candles],
                [c.quote_volume for c in candles],
                [c.trades for c in candles],
                [c.closed for c in candles],
            ),
        )

    async def save_state(self, symbol: str, state: dict[str, Any]) -> None:
        await self._conn.execute(_SAVE_STATE, (symbol, Jsonb(state)))


def _candle(row: tuple[Any, ...]) -> Candle:
    symbol, tf, bucket, o, h, lo, c, volume, quote_volume, trades, closed = row
    return Candle(
        symbol=symbol,
        tf=Timeframe(tf),
        time=to_ms(bucket),
        open=o,
        high=h,
        low=lo,
        close=c,
        volume=volume,
        quote_volume=quote_volume,
        trades=trades,
        closed=closed,
    )


class TimescaleStore:
    def __init__(self, pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, database_url: str, *, max_size: int = 10) -> TimescaleStore:
        pool = AsyncConnectionPool(database_url, min_size=1, max_size=max_size, open=False)
        await pool.open(wait=True)
        return cls(pool)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_TimescaleTransaction]:
        async with self._pool.connection() as conn, conn.transaction():
            yield _TimescaleTransaction(conn)

    async def load_states(self, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT symbol, state FROM aggregator_state WHERE symbol = ANY(%s)",
                (list(symbols),),
            )
            return {row[0]: row[1] for row in await cur.fetchall()}

    async def get_candles(
        self, symbol: str, tf: Timeframe, start_ms: int, end_ms: int, limit: int
    ) -> list[Candle]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                _SELECT_CANDLES, (symbol, tf.value, from_ms(start_ms), from_ms(end_ms), limit)
            )
            return [_candle(row) for row in await cur.fetchall()]

    async def missing_minutes(self, symbol: str, start_ms: int, end_ms: int) -> list[int]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                _MISSING_MINUTES,
                {"symbol": symbol, "start": from_ms(start_ms), "end": from_ms(end_ms)},
            )
            return [to_ms(row[0]) for row in await cur.fetchall()]

    async def rollup(self, symbol: str, tf: Timeframe, start_ms: int, end_ms: int) -> int:
        if tf is BASE_TIMEFRAME:
            raise ValueError("1m candles are the input of a rollup, not its output")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                _ROLLUP,
                {
                    "tf": tf.value,
                    "width": tf.interval,
                    "symbol": symbol,
                    "start": from_ms(tf.floor(start_ms)),
                    "end": from_ms(tf.floor(end_ms)),
                },
            )
            return cur.rowcount

    async def ping(self) -> None:
        async with self._pool.connection() as conn:
            await conn.execute("SELECT 1")

    async def close(self) -> None:
        await self._pool.close()
