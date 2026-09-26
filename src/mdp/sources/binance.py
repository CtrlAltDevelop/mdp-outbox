"""Binance spot: the public ``@trade`` stream.

One combined-stream connection carries every symbol. Binance names symbols
without a separator (``BTCUSDT``), so the canonical ``BTC-USDT`` is mapped by
removing the dash, and back through a lookup table.

Wire format (combined stream)::

    {"stream": "btcusdt@trade",
     "data": {"e": "trade", "E": 1727172000125, "s": "BTCUSDT", "t": 3891232123,
              "p": "63310.01000000", "q": "0.00079000", "T": 1727172000124,
              "m": true, "M": true}}

``m`` is "the buyer is the maker": the resting order was the bid, so the
aggressor — the side a candle's buy/sell split cares about — sold.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import UTC, datetime

from mdp.normalizer import Rejected, RejectReason, SourceEvent, parse_trade
from mdp.schema import Side
from mdp.sources.ws import WebSocketFeed
from mdp.timeframes import from_ms

WS_URL = "wss://stream.binance.com:9443/stream"
SOURCE = "binance"


def venue_symbol(symbol: str) -> str:
    return symbol.replace("-", "")


def stream_url(symbols: Sequence[str], base: str = WS_URL) -> str:
    return f"{base}?streams=" + "/".join(f"{venue_symbol(s).lower()}@trade" for s in symbols)


def parse_message(
    raw: str, symbols: Mapping[str, str], now: datetime | None = None
) -> list[SourceEvent]:
    """Decode one frame. ``symbols`` maps venue names (``BTCUSDT``) to canonical ones."""
    try:
        message = json.loads(raw)
        data = message.get("data", message)  # combined streams wrap, raw streams do not
        if data.get("e") != "trade":
            return []  # subscription acks and other control frames
        symbol = symbols.get(data["s"])
        if symbol is None:
            return [Rejected(SOURCE, RejectReason.UNKNOWN_SYMBOL, raw, data["s"])]
        return [
            parse_trade(
                SOURCE,
                raw,
                symbol=symbol,
                trade_id=str(data["t"]),
                price=data["p"],
                qty=data["q"],
                side=Side.SELL if data["m"] else Side.BUY,
                ts_exchange=from_ms(data["T"]),
                ts_ingested=now or datetime.now(UTC),
            )
        ]
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        return [Rejected(SOURCE, RejectReason.MALFORMED, raw, repr(exc))]


class BinanceSource:
    def __init__(
        self,
        symbols: Sequence[str],
        *,
        base_url: str = WS_URL,
        feed_factory: Callable[[str], WebSocketFeed] = WebSocketFeed,
    ) -> None:
        self._symbols = {venue_symbol(s): s for s in symbols}
        self._feed = feed_factory(stream_url(symbols, base_url))

    @property
    def name(self) -> str:
        return SOURCE

    async def events(self) -> AsyncIterator[SourceEvent]:
        async for raw in self._feed.messages():
            for event in parse_message(raw, self._symbols):
                yield event
