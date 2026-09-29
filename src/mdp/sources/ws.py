"""A WebSocket feed that stays connected: reconnect, backoff, heartbeat.

Three ways a feed dies, and what catches each:

* the server closes or the TCP connection resets — ``recv`` raises;
* the connection is silently dead (a NAT dropped it) — the protocol-level
  ping gets no pong within ``ping_timeout`` and the library closes it;
* the connection is alive but the feed has stopped sending — nothing above
  notices, so a watchdog reconnects after ``stale_after`` seconds of silence.
  Feeds with an application heartbeat (Kraken sends one a second) make this
  exact; for the others it should exceed the quietest expected gap.

Reconnects wait an exponentially growing delay with **full jitter** — a random
point between zero and the cap — so a fleet of clients cut off together does
not return together and knock the server over again.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Protocol

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidHandshake, WebSocketException

from mdp.metrics import SOURCE_RECONNECTS

log = logging.getLogger(__name__)


class Socket(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...


type Connector = Callable[[str], AbstractAsyncContextManager[Socket]]


def default_connector(url: str) -> AbstractAsyncContextManager[Socket]:
    return connect(url, ping_interval=20, ping_timeout=20, max_size=2**22, open_timeout=10)


@dataclass(frozen=True, slots=True)
class Backoff:
    base_s: float = 0.5
    cap_s: float = 30.0
    factor: float = 2.0

    def delay(self, attempt: int, rng: random.Random) -> float:
        """Full jitter: uniform in ``[0, min(cap, base * factor**attempt)]``."""
        return rng.uniform(0.0, min(self.cap_s, self.base_s * self.factor**attempt))


_DEFAULT_BACKOFF = Backoff()


class WebSocketFeed:
    def __init__(
        self,
        url: str,
        *,
        subscribe: Sequence[str] = (),
        stale_after_s: float = 30.0,
        backoff: Backoff = _DEFAULT_BACKOFF,
        connector: Connector = default_connector,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self.url = url
        self._subscribe = list(subscribe)
        self._stale_after = stale_after_s
        self._backoff = backoff
        self._connector = connector
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def messages(self) -> AsyncIterator[str]:
        """Yield text frames forever, reconnecting (and resubscribing) as needed."""
        attempt = 0
        while True:
            try:
                async with self._connector(self.url) as ws:
                    for message in self._subscribe:
                        await ws.send(message)
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self._stale_after)
                        attempt = 0  # a frame arrived: the connection is healthy again
                        yield raw.decode() if isinstance(raw, bytes) else raw
            except TimeoutError:
                log.warning("%s silent for %.0fs, reconnecting", self.url, self._stale_after)
            except (OSError, InvalidHandshake, WebSocketException) as exc:
                log.warning("%s disconnected: %r", self.url, exc)
            delay = self._backoff.delay(attempt, self._rng)
            attempt += 1
            SOURCE_RECONNECTS.labels(self.url).inc()
            await self._sleep(delay)
