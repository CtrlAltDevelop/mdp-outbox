"""WebSocket fan-out: one Redis subscription per channel, however many clients.

A thousand charts on ``candles:BTC-USDT:1m`` cost one Redis subscription and
one decode per update; each frame is built once and handed to every
subscriber's queue. The first client on a channel subscribes it in Redis and
the last one to leave unsubscribes it.

Every client has a bounded queue. A client that cannot keep up — its queue is
full — is disconnected rather than buffered without limit or allowed to slow
the others down. That costs it nothing it cannot recover: frames are whole
candle snapshots, so on reconnecting it reloads history over REST and the next
update brings it current.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from mdp.metrics import WS_CHANNELS, WS_CLIENTS, WS_FRAMES, WS_SLOW_DISCONNECTS
from mdp.schema import SYMBOL_PATTERN
from mdp.streams import CANDLE_CHANNEL_PREFIX
from mdp.timeframes import Timeframe

log = logging.getLogger(__name__)

_TIMEFRAMES = "|".join(tf.value for tf in Timeframe)
CHANNEL_PATTERN = re.compile(
    rf"^{CANDLE_CHANNEL_PREFIX}(?P<symbol>{SYMBOL_PATTERN[1:-1]}):(?P<tf>{_TIMEFRAMES})$"
)


class ClientTooSlowError(Exception):
    """The client's queue overflowed; it has been cut off."""


@dataclass(eq=False)
class Client:
    queue: asyncio.Queue[str | None]
    channels: set[str] = field(default_factory=set)
    dropped: bool = False

    def offer(self, frame: str) -> None:
        if self.dropped:
            return
        try:
            self.queue.put_nowait(frame)
        except asyncio.QueueFull:
            self.dropped = True
            WS_SLOW_DISCONNECTS.inc()
            # Make room for the sentinel so the writer wakes up and closes.
            self.queue.get_nowait()
            self.queue.put_nowait(None)

    async def next_frame(self) -> str:
        frame = await self.queue.get()
        if frame is None:
            raise ClientTooSlowError
        return frame


class CandleHub:
    def __init__(self, redis: aioredis.Redis, *, client_queue_size: int = 256) -> None:
        self._redis = redis
        self._pubsub = redis.pubsub()
        self._queue_size = client_queue_size
        self._subscribers: dict[str, set[Client]] = {}
        self._lock = asyncio.Lock()
        self._active = asyncio.Event()  # set while at least one channel is subscribed
        self._reader: asyncio.Task[None] | None = None

    @property
    def clients(self) -> int:
        return len({c for clients in self._subscribers.values() for c in clients})

    def channels(self) -> list[str]:
        return sorted(self._subscribers)

    async def start(self) -> None:
        self._reader = asyncio.create_task(self._read(), name="candle-hub-reader")

    async def stop(self) -> None:
        if self._reader is not None:
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader
        await self._pubsub.aclose()  # type: ignore[no-untyped-call]

    def connect(self) -> Client:
        WS_CLIENTS.inc()
        return Client(asyncio.Queue(self._queue_size))

    async def subscribe(self, client: Client, channel: str) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(channel)
            if subscribers is None:
                await self._pubsub.subscribe(channel)
                subscribers = self._subscribers[channel] = set()
                self._active.set()
                WS_CHANNELS.set(len(self._subscribers))
            subscribers.add(client)
            client.channels.add(channel)

    async def unsubscribe(self, client: Client, channel: str) -> None:
        async with self._lock:
            client.channels.discard(channel)
            subscribers = self._subscribers.get(channel)
            if subscribers is None:
                return
            subscribers.discard(client)
            if not subscribers:
                del self._subscribers[channel]
                await self._pubsub.unsubscribe(channel)
                WS_CHANNELS.set(len(self._subscribers))
                if not self._subscribers:
                    self._active.clear()

    async def disconnect(self, client: Client) -> None:
        WS_CLIENTS.dec()
        for channel in list(client.channels):
            await self.unsubscribe(client, channel)

    async def _read(self) -> None:
        while True:
            # redis-py's reader returns at once while nothing is subscribed,
            # so wait for the first subscription instead of spinning.
            await self._active.wait()
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
            except RedisConnectionError:
                log.warning("lost the Redis subscription; retrying")
                await asyncio.sleep(1.0)
                continue
            if message is not None:
                self._dispatch(message)

    def _dispatch(self, message: dict[str, Any]) -> None:
        channel = _text(message["channel"])
        subscribers = self._subscribers.get(channel)
        if not subscribers:
            return
        # The payload is already JSON; splice it in rather than decode and
        # re-encode it once per update.
        frame = f'{{"channel":"{channel}","data":{_text(message["data"])}}}'
        for client in list(subscribers):
            client.offer(frame)
        WS_FRAMES.inc(len(subscribers))


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value
