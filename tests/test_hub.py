"""The fan-out hub, driven directly: subscription sharing and slow clients."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import cast

import pytest
import redis.asyncio as aioredis

from mdp.hub import CHANNEL_PATTERN, CandleHub, Client, ClientTooSlowError

CHANNEL = "candles:BTC-USDT:1m"


@pytest.fixture
async def hub(redis: aioredis.Redis) -> AsyncIterator[CandleHub]:
    hub = CandleHub(redis, client_queue_size=4)
    await hub.start()
    yield hub
    await hub.stop()


async def subscriptions(redis: aioredis.Redis, channel: str) -> int:
    counts = cast("list[tuple[bytes, int]]", await redis.pubsub_numsub(channel))
    return counts[0][1]


async def read_until_cut_off(client: Client) -> None:
    while True:
        await asyncio.wait_for(client.next_frame(), 3)


async def test_many_clients_share_one_redis_subscription(
    hub: CandleHub, redis: aioredis.Redis
) -> None:
    a, b = hub.connect(), hub.connect()
    await hub.subscribe(a, CHANNEL)
    await hub.subscribe(b, CHANNEL)

    assert await subscriptions(redis, CHANNEL) == 1
    await redis.publish(CHANNEL, '{"close":"1"}')

    for client in (a, b):
        frame = json.loads(await asyncio.wait_for(client.next_frame(), 3))
        assert frame == {"channel": CHANNEL, "data": {"close": "1"}}


async def test_the_last_client_out_unsubscribes_the_channel(
    hub: CandleHub, redis: aioredis.Redis
) -> None:
    a, b = hub.connect(), hub.connect()
    await hub.subscribe(a, CHANNEL)
    await hub.subscribe(b, CHANNEL)

    await hub.disconnect(a)
    assert await subscriptions(redis, CHANNEL) == 1
    await hub.disconnect(b)
    assert await subscriptions(redis, CHANNEL) == 0
    assert hub.channels() == []


async def test_a_client_that_falls_behind_is_cut_off_not_buffered(
    hub: CandleHub, redis: aioredis.Redis
) -> None:
    slow, fast = hub.connect(), hub.connect()
    await hub.subscribe(slow, CHANNEL)
    await hub.subscribe(fast, CHANNEL)

    for i in range(6):  # the queues hold 4
        await redis.publish(CHANNEL, json.dumps({"n": i}))
        await asyncio.wait_for(fast.next_frame(), 3)  # the fast client keeps up

    await asyncio.sleep(0.2)
    assert slow.dropped
    with pytest.raises(ClientTooSlowError):
        await read_until_cut_off(slow)


@pytest.mark.parametrize(
    ("channel", "valid"),
    [
        ("candles:BTC-USDT:1m", True),
        ("candles:ETH-USDT:1d", True),
        ("candles:BTC-USDT:2m", False),
        ("candles:btc-usdt:1m", False),
        ("trades:BTC-USDT:1m", False),
        ("candles:BTC-USDT:1m:extra", False),
    ],
)
def test_channel_names(channel: str, valid: bool) -> None:
    assert (CHANNEL_PATTERN.match(channel) is not None) is valid
