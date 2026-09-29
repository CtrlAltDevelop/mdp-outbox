"""The ingest service: one source, through the normalizer, into Redis.

Reading and writing run as two tasks joined by a bounded queue. The reader
never waits on Redis, and the writer drains whatever has queued up since its
last round trip into one pipelined batch — small batches when the feed is quiet
(low latency), large ones under load (high throughput), with no timer to tune.
If Redis stalls long enough to fill the queue, the reader blocks and the
source's own buffering takes over: backpressure instead of unbounded memory.
"""

from __future__ import annotations

import asyncio
import logging

import redis.asyncio as aioredis

from mdp.metrics import DEAD_LETTERS, DUPLICATES, TRADES_INGESTED
from mdp.normalizer import Normalizer, Rejected
from mdp.schema import Trade
from mdp.sources import TradeSource
from mdp.streams import DLQ_STREAM, dead_letter, encode_trade, trades_stream

log = logging.getLogger(__name__)


class IngestService:
    def __init__(
        self,
        source: TradeSource,
        normalizer: Normalizer,
        redis: aioredis.Redis,
        *,
        stream_maxlen: int = 1_000_000,
        dlq_maxlen: int = 100_000,
        batch_size: int = 500,
        queue_size: int = 10_000,
    ) -> None:
        self._source = source
        self._normalizer = normalizer
        self._redis = redis
        self._stream_maxlen = stream_maxlen
        self._dlq_maxlen = dlq_maxlen
        self._batch_size = batch_size
        self._queue: asyncio.Queue[Trade | Rejected | None] = asyncio.Queue(queue_size)

    async def run(self) -> None:
        """Run until the source ends (replay) or the task is cancelled (live)."""
        async with asyncio.TaskGroup() as group:
            group.create_task(self._read())
            group.create_task(self._write())

    async def _read(self) -> None:
        async for event in self._source.events():
            admitted = event if isinstance(event, Rejected) else self._normalizer.admit(event)
            if admitted is None:
                DUPLICATES.labels("ingest").inc()
            else:
                await self._queue.put(admitted)
        await self._queue.put(None)  # the source ended: flush and stop

    async def _write(self) -> None:
        done = False
        while not done:
            batch = [await self._queue.get()]
            while len(batch) < self._batch_size and not self._queue.empty():
                batch.append(self._queue.get_nowait())
            if batch[-1] is None:
                batch.pop()
                done = True
            if batch:
                await self._send(batch)

    async def _send(self, batch: list[Trade | Rejected | None]) -> None:
        pipe = self._redis.pipeline(transaction=False)
        for item in batch:
            if isinstance(item, Trade):
                TRADES_INGESTED.labels(item.source, item.symbol).inc()
                # MAXLEN ~ trims in whole macro-nodes: cheap, and the stream
                # only has to cover how far the aggregator can fall behind.
                pipe.xadd(
                    trades_stream(item.symbol),
                    encode_trade(item),  # type: ignore[arg-type]
                    maxlen=self._stream_maxlen,
                    approximate=True,
                )
            elif isinstance(item, Rejected):
                log.warning("rejected %s trade: %s %s", item.source, item.reason, item.detail)
                DEAD_LETTERS.labels("ingest", item.reason.value).inc()
                pipe.xadd(
                    DLQ_STREAM,
                    dead_letter(item, "ingest"),  # type: ignore[arg-type]
                    maxlen=self._dlq_maxlen,
                    approximate=True,
                )
        await pipe.execute()
