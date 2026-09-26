"""Redis key names and the encoding of what travels through them.

* ``mdp:trades:{symbol}`` — one stream per symbol. A symbol's trades must be
  folded by one aggregator in order, so the symbol is the unit of partitioning.
* ``mdp:dlq`` — one dead-letter stream for everything refused, with the
  reason, the stage that refused it and the original payload.
* ``candles:{symbol}:{tf}`` — a pub/sub channel of candle snapshots. Pub/sub
  rather than a stream because every message is a whole snapshot: a client that
  misses one is corrected by the next, and history lives in the database.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import ValidationError

from mdp.normalizer import Rejected, RejectReason
from mdp.schema import Trade
from mdp.timeframes import Timeframe

DLQ_STREAM = "mdp:dlq"
CANDLE_CHANNEL_PREFIX = "candles:"

type Fields = dict[str, str]


def trades_stream(symbol: str) -> str:
    return f"mdp:trades:{symbol}"


def candle_channel(symbol: str, tf: Timeframe | str) -> str:
    return f"{CANDLE_CHANNEL_PREFIX}{symbol}:{tf}"


def encode_trade(trade: Trade) -> Fields:
    return {"data": trade.model_dump_json()}


def decode_trade(fields: dict[bytes, bytes]) -> Trade | Rejected:
    raw = fields.get(b"data", b"")
    try:
        return Trade.model_validate_json(raw)
    except ValidationError as exc:
        payload = raw.decode("utf-8", errors="replace")
        return Rejected("unknown", RejectReason.MALFORMED, payload, str(exc.errors()[:3]))


def dead_letter(rejected: Rejected, stage: str) -> Fields:
    return {
        "reason": rejected.reason.value,
        "stage": stage,
        "source": rejected.source,
        "detail": rejected.detail,
        "payload": rejected.payload,
        "at": datetime.now(UTC).isoformat(),
    }
