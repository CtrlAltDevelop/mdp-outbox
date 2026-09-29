"""The aggregation service: Redis stream in, candles out.

Per batch read from the consumer group:

1. decode, and let the aggregator split late trades from on-time ones;
2. in **one database transaction**: log the on-time trades (the log reports
   which are new), fold only the new ones into the candles, write the changed
   candles and the aggregator's checkpoint;
3. dead-letter the refused messages, publish the candles, acknowledge the batch.

A crash before step 2 commits rolls everything back and leaves the batch
pending, so it is read again. A crash after the commit but before the
acknowledgement redelivers a batch whose trades are already logged: the insert
reports none of them as new, and nothing is counted twice. Restart state comes
from the checkpoint committed with the candles, so it always matches them.

One consumer owns a symbol at a time — the consumer group is used for durable
offsets and redelivery, not to spread one symbol's trades across workers,
which would split its candles. See ADR 0003.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from typing import Any, cast

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from mdp.aggregator import Aggregator
from mdp.metrics import (
    BATCH_SECONDS,
    CANDLES_WRITTEN,
    DEAD_LETTERS,
    DUPLICATES,
    STREAM_LAG,
    STREAM_PENDING,
    TRADE_LAG_SECONDS,
    TRADES_AGGREGATED,
    WATERMARK_DELAY,
)
from mdp.normalizer import Rejected, RejectReason
from mdp.schema import Candle, Trade
from mdp.storage import Store, StoreTransaction
from mdp.streams import DLQ_STREAM, candle_channel, dead_letter, decode_trade, trades_stream
from mdp.timeframes import from_ms

log = logging.getLogger(__name__)

type Entry = tuple[bytes, bytes, dict[bytes, bytes]]  # (stream, message id, fields)


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


class AggregationService:
    def __init__(
        self,
        redis: aioredis.Redis,
        store: Store,
        symbols: Sequence[str],
        *,
        group: str = "aggregator",
        consumer: str = "aggregator-1",
        allowed_lateness_ms: int = 5_000,
        idle_grace_ms: int | None = 2_000,
        batch_size: int = 1_000,
        dlq_maxlen: int = 100_000,
        clock_ms: Callable[[], int] = _wall_clock_ms,
    ) -> None:
        self._redis = redis
        self._store = store
        self._symbols = list(symbols)
        self._streams = [trades_stream(s) for s in self._symbols]
        self._group = group
        self._consumer = consumer
        self._idle_grace_ms = idle_grace_ms
        self._batch_size = batch_size
        self._dlq_maxlen = dlq_maxlen
        self._clock_ms = clock_ms
        self._aggregator = Aggregator(allowed_lateness_ms=allowed_lateness_ms)
        self._pending_drained = False

    async def start(self) -> None:
        """Create the groups, restore the checkpoint, and take over orphaned messages."""
        for stream in self._streams:
            try:
                # From "0", not "$": a new group first works through whatever
                # the ingester wrote before it existed. The trade log makes
                # re-reading already-counted trades harmless.
                await self._redis.xgroup_create(stream, self._group, id="0", mkstream=True)
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise
            await self._claim_orphans(stream)
        for symbol, state in (await self._store.load_states(self._symbols)).items():
            self._aggregator.restore_state(symbol, state)
            log.info("resumed %s at watermark %s", symbol, from_ms(state["watermark"]))

    async def _claim_orphans(self, stream: str) -> None:
        # Messages delivered to a consumer that died (say, one renamed on
        # redeploy) sit in its pending list forever unless someone claims them.
        # This service owns the symbol outright, so it claims them all.
        # (Not JUSTID: redis-py drops the cursor from that form of the reply.)
        cursor: Any = "0-0"
        while True:
            reply = await self._redis.xautoclaim(
                stream, self._group, self._consumer, min_idle_time=0, start_id=cursor, count=1000
            )
            cursor = reply[0]
            if cursor in (b"0-0", "0-0"):
                return

    async def run(self, block_ms: int = 1_000, probe_every_s: float = 5.0) -> None:
        await self.start()
        next_probe = 0.0
        while True:
            await self.poll(block_ms)
            if time.monotonic() >= next_probe:
                await self.probe_lag()
                next_probe = time.monotonic() + probe_every_s

    async def probe_lag(self) -> None:
        """Export how far the group trails each stream, and the watermarks."""
        now = self._clock_ms()
        for symbol, stream in zip(self._symbols, self._streams, strict=True):
            for info in await self._redis.xinfo_groups(stream):
                if _text(info["name"]) == self._group:
                    # "lag" is Redis 7+, and null while it cannot be computed cheaply.
                    STREAM_LAG.labels(symbol).set(info.get("lag") or 0)
                    STREAM_PENDING.labels(symbol).set(info["pending"])
            watermark = self._aggregator.watermark(symbol)
            if watermark is not None:
                WATERMARK_DELAY.labels(symbol).set((now - watermark) / 1000)

    async def poll(self, block_ms: int | None = None) -> int:
        """Read and process one batch; return how many messages it held.

        Pending messages (delivered before a crash, never acknowledged) are
        worked through first, then new ones. An empty read still runs a batch:
        it is the wall-clock tick that closes candles in a quiet market.
        """
        if not self._pending_drained:
            entries = await self._read("0", block_ms=None)
            if not entries:
                self._pending_drained = True
        if self._pending_drained:
            entries = await self._read(">", block_ms)
        await self.process(entries)
        return len(entries)

    async def _read(self, cursor: str, block_ms: int | None) -> list[Entry]:
        reply = cast(
            "list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes] | None]]]] | None",
            await self._redis.xreadgroup(
                self._group,
                self._consumer,
                dict.fromkeys(self._streams, cursor),
                count=self._batch_size,
                block=block_ms,
            ),
        )
        return [
            # A pending entry whose message has since been trimmed comes back
            # with no fields. It is kept so that it still gets acknowledged.
            (stream, message_id, fields or {})
            for stream, messages in reply or []
            for message_id, fields in messages
        ]

    async def process(self, entries: Sequence[Entry]) -> list[Candle]:
        with BATCH_SECONDS.time():
            return await self._process(entries)

    async def _process(self, entries: Sequence[Entry]) -> list[Candle]:
        trades: list[Trade] = []
        rejected: list[Rejected] = []
        for _, _, fields in entries:
            if not fields:
                continue
            decoded = decode_trade(fields)
            if isinstance(decoded, Trade):
                trades.append(decoded)
            else:
                rejected.append(decoded)

        on_time, late = self._aggregator.observe(trades)
        touched = {t.symbol for t in trades}
        fresh: list[Trade] = []
        if on_time:
            async with self._store.transaction() as tx:
                fresh = await tx.insert_trades(on_time)
                self._aggregator.apply(fresh)
                candles = self._close()
                await self._checkpoint(tx, candles, touched)
        else:
            # Nothing to log: only open a transaction if the clock closed something.
            candles = self._close()
            if candles or touched:
                async with self._store.transaction() as tx:
                    await self._checkpoint(tx, candles, touched)

        rejected.extend(self._late(trade) for trade in late)
        await self._finish(entries, candles, rejected)
        self._record(on_time, fresh, candles, rejected)
        return candles

    def _record(
        self,
        on_time: Sequence[Trade],
        fresh: Sequence[Trade],
        candles: Sequence[Candle],
        rejected: Sequence[Rejected],
    ) -> None:
        DUPLICATES.labels("aggregate").inc(len(on_time) - len(fresh))
        for item in rejected:
            DEAD_LETTERS.labels("aggregate", item.reason.value).inc()
        for candle in candles:
            CANDLES_WRITTEN.labels(candle.tf.value, "closed" if candle.closed else "open").inc()
        published = self._clock_ms()
        for trade in fresh:
            TRADES_AGGREGATED.labels(trade.symbol).inc()
            TRADE_LAG_SECONDS.observe((published - trade.ts_ms) / 1000)

    def _close(self) -> list[Candle]:
        if self._idle_grace_ms is not None:
            self._aggregator.advance(self._clock_ms(), self._idle_grace_ms)
        return self._aggregator.flush()

    async def _checkpoint(
        self, tx: StoreTransaction, candles: Sequence[Candle], touched: set[str]
    ) -> None:
        await tx.upsert_candles(candles)
        for symbol in touched | {c.symbol for c in candles}:
            state = self._aggregator.export_state(symbol)
            if state is not None:
                await tx.save_state(symbol, state)

    def _late(self, trade: Trade) -> Rejected:
        watermark = self._aggregator.watermark(trade.symbol)
        detail = f"watermark {from_ms(watermark).isoformat()}" if watermark is not None else ""
        return Rejected(trade.source, RejectReason.LATE, trade.model_dump_json(), detail)

    async def _finish(
        self, entries: Sequence[Entry], candles: Sequence[Candle], rejected: Sequence[Rejected]
    ) -> None:
        """Dead-letter, publish and acknowledge in one round trip, after the commit."""
        if not entries and not candles:
            return
        pipe = self._redis.pipeline(transaction=False)
        for item in rejected:
            pipe.xadd(
                DLQ_STREAM,
                dead_letter(item, "aggregate"),  # type: ignore[arg-type]
                maxlen=self._dlq_maxlen,
                approximate=True,
            )
        for candle in candles:
            pipe.publish(candle_channel(candle.symbol, candle.tf), candle.model_dump_json())
        by_stream: dict[bytes, list[bytes]] = {}
        for stream, message_id, _ in entries:
            by_stream.setdefault(stream, []).append(message_id)
        for stream, ids in by_stream.items():
            pipe.xack(stream, self._group, *ids)
        await pipe.execute()
