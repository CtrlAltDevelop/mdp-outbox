"""REST and WebSocket, through FastAPI's test client, over the memory store.

The app runs its lifespan in the test client's own event loop, so the async
Redis client is created there by the ``resources`` factory; the test publishes
through a synchronous client on the same server (real Redis or one fakeredis
server shared by both).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal

import fakeredis
import pytest
import redis
import redis.asyncio as aioredis
from fastapi.testclient import TestClient

from mdp.api import Backend, create_app
from mdp.schema import Candle
from mdp.storage.memory import MemoryStore
from mdp.streams import candle_channel
from mdp.timeframes import Timeframe, to_ms
from tests.helpers import at

REDIS_URL = os.environ.get("REDIS_URL", "")
CHANNEL = candle_channel("BTC-USDT", Timeframe.M1)


def candle(minute: int, close: str = "100", *, closed: bool = True) -> Candle:
    price = Decimal(close)
    return Candle(
        symbol="BTC-USDT",
        tf=Timeframe.M1,
        time=to_ms(at(minute)),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1.5"),
        quote_volume=price * Decimal("1.5"),
        trades=3,
        closed=closed,
    )


class DownStore(MemoryStore):
    async def ping(self) -> None:
        raise ConnectionRefusedError


@dataclass
class Harness:
    client: TestClient
    store: MemoryStore
    publisher: redis.Redis

    def publish(self, candle: Candle) -> None:
        self.publisher.publish(candle_channel(candle.symbol, candle.tf), candle.model_dump_json())


def harness(store: MemoryStore) -> Iterator[Harness]:
    if REDIS_URL:
        publisher = redis.Redis.from_url(REDIS_URL)

        def make_redis() -> aioredis.Redis:
            return aioredis.Redis.from_url(REDIS_URL)
    else:
        server = fakeredis.FakeServer()
        publisher = fakeredis.FakeRedis(server=server)

        def make_redis() -> aioredis.Redis:
            return fakeredis.FakeAsyncRedis(server=server)

    @asynccontextmanager
    async def resources() -> AsyncIterator[tuple[Backend, aioredis.Redis]]:
        client = make_redis()
        try:
            yield store, client
        finally:
            await client.aclose()

    app = create_app(resources, ["BTC-USDT", "ETH-USDT"])
    with TestClient(app) as client:
        yield Harness(client, store, publisher)
    publisher.close()


@pytest.fixture
def api() -> Iterator[Harness]:
    store = MemoryStore()
    store.candles.update({c.key: c for c in [candle(0, "100"), candle(1, "101"), candle(2, "102")]})
    yield from harness(store)


def test_candles_in_a_range_oldest_first(api: Harness) -> None:
    response = api.client.get(
        "/v1/candles",
        params={
            "symbol": "BTC-USDT",
            "tf": "1m",
            "from": "2026-09-24T10:00:00Z",
            "to": to_ms(at(2)),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert (body["symbol"], body["tf"]) == ("BTC-USDT", "1m")
    assert [c["close"] for c in body["candles"]] == ["100", "101"]
    assert body["candles"][0] == {
        "symbol": "BTC-USDT",
        "tf": "1m",
        "time": to_ms(at(0)),
        "open": "100",
        "high": "100",
        "low": "100",
        "close": "100",
        "volume": "1.5",
        "quote_volume": "150.0",
        "trades": 3,
        "closed": True,
    }


def test_limit_caps_the_number_of_candles(api: Harness) -> None:
    params: dict[str, str | int] = {
        "symbol": "BTC-USDT",
        "tf": "1m",
        "from": to_ms(at(0)),
        "to": to_ms(at(3)),
        "limit": 2,
    }

    assert len(api.client.get("/v1/candles", params=params).json()["candles"]) == 2


@pytest.mark.parametrize(
    ("params", "status"),
    [
        ({"symbol": "DOGE-USDT", "tf": "1m"}, 404),
        ({"symbol": "BTC-USDT", "tf": "3m"}, 422),
        ({"symbol": "BTC-USDT", "tf": "1m", "from": "yesterday"}, 422),
        ({"symbol": "BTC-USDT", "tf": "1m", "from": "1000", "to": "1000"}, 422),
        ({"symbol": "BTC-USDT", "tf": "1m", "limit": "0"}, 422),
    ],
)
def test_bad_queries_are_refused(api: Harness, params: dict[str, str], status: int) -> None:
    assert api.client.get("/v1/candles", params=params).status_code == status


def test_health_and_readiness(api: Harness) -> None:
    assert api.client.get("/healthz").json() == {"status": "ok"}
    assert api.client.get("/readyz").json() == {"ready": True, "redis": "ok", "database": "ok"}


def test_readiness_fails_when_the_database_is_down() -> None:
    for h in harness(DownStore()):
        response = h.client.get("/readyz")
        assert response.status_code == 503
        assert response.json()["database"] == "error: ConnectionRefusedError"


def test_websocket_relays_published_candles_to_subscribers(api: Harness) -> None:
    with api.client.websocket_connect("/v1/ws") as ws:
        ws.send_json({"op": "subscribe", "channel": CHANNEL})
        assert ws.receive_json() == {"op": "subscribed", "channel": CHANNEL}

        api.publish(candle(3, "103", closed=False))

        frame = ws.receive_json()
        assert frame["channel"] == CHANNEL
        assert (frame["data"]["time"], frame["data"]["close"]) == (to_ms(at(3)), "103")
        assert frame["data"]["closed"] is False


def test_websocket_refuses_unknown_channels_and_ops(api: Harness) -> None:
    with api.client.websocket_connect("/v1/ws") as ws:
        ws.send_json({"op": "subscribe", "channel": "candles:DOGE-USDT:1m"})
        assert ws.receive_json()["error"] == "unknown channel"
        ws.send_json({"op": "subscribe", "channel": "candles:BTC-USDT:2m"})
        assert ws.receive_json()["error"] == "unknown channel"
        ws.send_json({"op": "dance", "channel": CHANNEL})
        assert ws.receive_json()["error"] == "unknown op 'dance'"
        ws.send_text("{not json")
        assert ws.receive_json()["op"] == "error"


def test_after_unsubscribing_nothing_more_arrives(api: Harness) -> None:
    with api.client.websocket_connect("/v1/ws") as ws:
        ws.send_json({"op": "subscribe", "channel": CHANNEL})
        ws.receive_json()
        ws.send_json({"op": "unsubscribe", "channel": CHANNEL})
        assert ws.receive_json() == {"op": "unsubscribed", "channel": CHANNEL}

        api.publish(candle(4))
        ws.send_json({"op": "subscribe", "channel": CHANNEL})

        # The next frame is the ack, not the candle published while unsubscribed.
        assert ws.receive_json() == {"op": "subscribed", "channel": CHANNEL}
