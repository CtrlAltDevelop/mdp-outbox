"""The HTTP face of the pipeline: candle history over REST, live candles over WebSocket.

REST ``GET /v1/candles`` reads TimescaleDB; the WebSocket at ``/v1/ws`` relays
the aggregator's snapshots through the ``CandleHub``. A client loads history
first, then subscribes: whatever changed in between arrives as the next
snapshot of the same candle, so there is no gap to stitch.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any, Protocol

import redis.asyncio as aioredis
from fastapi import (
    APIRouter,
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
from pydantic import BaseModel

from mdp import __version__
from mdp.hub import CHANNEL_PATTERN, CandleHub, Client, ClientTooSlowError
from mdp.schema import Candle
from mdp.storage import CandleReader
from mdp.timeframes import Timeframe, to_ms

log = logging.getLogger(__name__)

MAX_LIMIT = 5_000
MAX_CHANNELS_PER_CLIENT = 50


class Backend(CandleReader, Protocol):
    async def ping(self) -> None: ...


type Resources = Callable[[], AbstractAsyncContextManager[tuple[Backend, aioredis.Redis]]]


class CandlesResponse(BaseModel):
    symbol: str
    tf: Timeframe
    candles: list[Candle]


def parse_time(value: str) -> int:
    """Epoch milliseconds, or an ISO 8601 timestamp (UTC unless it says otherwise)."""
    if value.lstrip("-").isdigit():
        return int(value)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(422, f"not a timestamp: {value!r}") from None
    return to_ms(parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC))


router = APIRouter()


def create_app(resources: Resources, symbols: Sequence[str]) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with resources() as (store, redis):
            hub = CandleHub(redis)
            await hub.start()
            app.state.store, app.state.redis, app.state.hub = store, redis, hub
            try:
                yield
            finally:
                await hub.stop()

    app = FastAPI(title="Market Data Pipeline", version=__version__, lifespan=lifespan)
    app.state.symbols = frozenset(symbols)
    app.include_router(router)
    app.mount("/metrics", make_asgi_app())
    return app


@router.get("/v1/candles")
async def candles(
    request: Request,
    *,
    symbol: str,
    tf: Timeframe,
    start: Annotated[str | None, Query(alias="from")] = None,
    end: Annotated[str | None, Query(alias="to")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 1_000,
) -> CandlesResponse:
    """Candles with ``from <= time < to``, oldest first.

    ``from`` and ``to`` take epoch milliseconds or ISO 8601. ``to`` defaults to
    now and ``from`` to ``limit`` buckets before ``to``.
    """
    state = request.app.state
    if symbol not in state.symbols:
        raise HTTPException(404, f"unknown symbol {symbol!r}; serving {sorted(state.symbols)}")
    end_ms = parse_time(end) if end else to_ms(datetime.now(UTC))
    start_ms = parse_time(start) if start else end_ms - limit * tf.ms
    if start_ms >= end_ms:
        raise HTTPException(422, "`from` must be before `to`")
    rows = await state.store.get_candles(symbol, tf, start_ms, end_ms, limit)
    return CandlesResponse(symbol=symbol, tf=tf, candles=rows)


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the process is up and serving."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    """Readiness: Redis and the database both answer."""
    state = request.app.state
    checks: dict[str, str] = {}
    for name, probe in (("redis", state.redis.ping), ("database", state.store.ping)):
        try:
            await asyncio.wait_for(probe(), timeout=2.0)
            checks[name] = "ok"
        except Exception as exc:  # any failure at all means "not ready"
            checks[name] = f"error: {exc.__class__.__name__}"
    ready = all(v == "ok" for v in checks.values())
    return JSONResponse({"ready": ready, **checks}, status_code=200 if ready else 503)


@router.websocket("/v1/ws")
async def ws(socket: WebSocket) -> None:
    """Subscribe with ``{"op": "subscribe", "channel": "candles:BTC-USDT:1m"}``."""
    await socket.accept()
    hub: CandleHub = socket.app.state.hub
    symbols: frozenset[str] = socket.app.state.symbols
    client = hub.connect()
    writer = asyncio.create_task(_write(socket, client))
    try:
        while True:
            reply = await _handle(hub, client, symbols, await socket.receive_text())
            await socket.send_text(json.dumps(reply))
    except WebSocketDisconnect:
        pass
    finally:
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer
        await hub.disconnect(client)


async def _write(socket: WebSocket, client: Client) -> None:
    try:
        while True:
            await socket.send_text(await client.next_frame())
    except ClientTooSlowError:
        log.info("disconnecting a client that fell behind")
        await socket.close(code=1013, reason="too slow; reconnect and reload")


async def _handle(hub: CandleHub, client: Client, known: frozenset[str], text: str) -> Any:
    try:
        request = json.loads(text)
        op, channel = request["op"], request["channel"]
    except (ValueError, KeyError, TypeError):
        return {"op": "error", "error": 'expected {"op": ..., "channel": ...}'}
    match = CHANNEL_PATTERN.match(channel) if isinstance(channel, str) else None
    if match is None or match["symbol"] not in known:
        return {"op": "error", "channel": channel, "error": "unknown channel"}
    if op == "subscribe":
        if len(client.channels) >= MAX_CHANNELS_PER_CLIENT:
            return {"op": "error", "channel": channel, "error": "too many subscriptions"}
        await hub.subscribe(client, channel)
        return {"op": "subscribed", "channel": channel}
    if op == "unsubscribe":
        await hub.unsubscribe(client, channel)
        return {"op": "unsubscribed", "channel": channel}
    return {"op": "error", "channel": channel, "error": f"unknown op {op!r}"}
