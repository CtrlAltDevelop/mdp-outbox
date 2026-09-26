"""Fixtures for the backing services.

Redis: the real server at ``REDIS_URL`` when one answers, fakeredis otherwise,
so the whole suite runs on a laptop with nothing installed. TimescaleDB has no
fake: tests that need it are marked ``timescale`` and skipped, loudly, when
``DATABASE_URL`` does not answer.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable, Mapping

import fakeredis
import psycopg
import pytest
import redis.asyncio as aioredis

from mdp.storage import Store
from mdp.storage.memory import MemoryStore
from mdp.storage.timescale import TimescaleStore, migrate

DATABASE_URL = os.environ.get("DATABASE_URL", "")
REDIS_URL = os.environ.get("REDIS_URL", "")


def pytest_asyncio_loop_factories(
    config: pytest.Config, item: pytest.Item
) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    # psycopg's async driver cannot run on Windows' default Proactor loop; a
    # selector loop works everywhere and is what the CLI runs on too.
    return {"selector": asyncio.SelectorEventLoop}


def _database_available() -> bool:
    if not DATABASE_URL:
        return False
    try:
        psycopg.connect(DATABASE_URL, connect_timeout=3).close()
    except psycopg.OperationalError:
        return False
    return True


TIMESCALE_UP = _database_available()


_migrated = False


async def _fresh_timescale() -> TimescaleStore:
    """A connected store over empty tables, migrating the database on first use."""
    global _migrated  # noqa: PLW0603 - once per test session, not per test
    if not TIMESCALE_UP:
        pytest.skip("TimescaleDB not reachable at DATABASE_URL")
    if not _migrated:
        await migrate(DATABASE_URL)
        _migrated = True
    async with await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True) as conn:
        await conn.execute("TRUNCATE trades, candles, aggregator_state")
    return await TimescaleStore.connect(DATABASE_URL, max_size=4)


@pytest.fixture
async def timescale() -> AsyncIterator[TimescaleStore]:
    store = await _fresh_timescale()
    try:
        yield store
    finally:
        await store.close()


@pytest.fixture(params=["memory", pytest.param("timescale", marks=pytest.mark.timescale)])
async def store(request: pytest.FixtureRequest) -> AsyncIterator[Store]:
    """Every store test runs against both implementations: one contract."""
    if request.param == "memory":
        yield MemoryStore()
        return
    timescale = await _fresh_timescale()
    try:
        yield timescale
    finally:
        await timescale.close()


@pytest.fixture
async def redis() -> AsyncIterator[aioredis.Redis]:
    client: aioredis.Redis
    if REDIS_URL:
        client = aioredis.Redis.from_url(REDIS_URL)
        await client.flushdb()
    else:
        client = fakeredis.FakeAsyncRedis()
    try:
        yield client
    finally:
        await client.aclose()
