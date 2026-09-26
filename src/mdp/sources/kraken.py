"""Kraken spot: the v2 WebSocket ``trade`` channel.

Kraken sends prices and sizes as JSON *numbers*. Decoding those into floats
would round them before we ever saw them, so the frame is parsed with
``parse_float=Decimal`` and every digit survives.

On (re)subscribing Kraken first sends a snapshot of the latest trades. After a
reconnect that snapshot overlaps what we already have — the dedup key drops the
overlap — and whatever it holds from the outage itself fills part of the gap.

Wire format::

    {"channel": "trade", "type": "update",
     "data": [{"symbol": "BTC/USD", "side": "sell", "price": 63287.4, "qty": 0.00253,
               "ord_type": "market", "trade_id": 74913551,
               "timestamp": "2024-09-24T10:00:00.412871Z"}]}
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal

from mdp.normalizer import Rejected, RejectReason, SourceEvent, parse_trade
from mdp.sources.ws import WebSocketFeed

WS_URL = "wss://ws.kraken.com/v2"
SOURCE = "kraken"


def venue_symbol(symbol: str) -> str:
    return symbol.replace("-", "/")


def subscribe_message(symbols: Sequence[str]) -> str:
    return json.dumps(
        {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [venue_symbol(s) for s in symbols],
                "snapshot": True,
            },
        }
    )


def parse_message(
    raw: str, symbols: Mapping[str, str], now: datetime | None = None
) -> list[SourceEvent]:
    """Decode one frame. ``symbols`` maps venue names (``BTC/USD``) to canonical ones."""
    try:
        message = json.loads(raw, parse_float=Decimal)
        if message.get("channel") != "trade":
            return []  # heartbeat, status, subscription acks
        ingested = now or datetime.now(UTC)
        events: list[SourceEvent] = []
        for item in message["data"]:
            symbol = symbols.get(item["symbol"])
            if symbol is None:
                events.append(Rejected(SOURCE, RejectReason.UNKNOWN_SYMBOL, raw, item["symbol"]))
                continue
            events.append(
                parse_trade(
                    SOURCE,
                    raw,
                    symbol=symbol,
                    trade_id=str(item["trade_id"]),
                    price=item["price"],
                    qty=item["qty"],
                    side=item["side"],
                    ts_exchange=item["timestamp"],
                    ts_ingested=ingested,
                )
            )
        return events
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        return [Rejected(SOURCE, RejectReason.MALFORMED, raw, repr(exc))]


class KrakenSource:
    def __init__(
        self,
        symbols: Sequence[str],
        *,
        url: str = WS_URL,
        feed_factory: Callable[..., WebSocketFeed] = WebSocketFeed,
    ) -> None:
        self._symbols = {venue_symbol(s): s for s in symbols}
        # Kraken heartbeats every second once subscribed; 15s of silence is a dead feed.
        self._feed = feed_factory(url, subscribe=[subscribe_message(symbols)], stale_after_s=15.0)

    @property
    def name(self) -> str:
        return SOURCE

    async def events(self) -> AsyncIterator[SourceEvent]:
        async for raw in self._feed.messages():
            for event in parse_message(raw, self._symbols):
                yield event
