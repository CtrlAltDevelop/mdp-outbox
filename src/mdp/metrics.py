"""Prometheus metrics, shared by every process.

Each service exposes the default registry: the API on its own ``/metrics``
route, the ingest, aggregate and repair workers on a small HTTP server of
their own. Names follow Prometheus conventions (``_total`` for counters, base
units in the name). Labels are bounded — source, symbol, timeframe, reason —
never a trade id or a client address.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

TRADES_INGESTED = Counter(
    "mdp_trades_ingested_total", "Trades written to a stream", ["source", "symbol"]
)
DUPLICATES = Counter("mdp_duplicates_total", "Trades dropped as already seen", ["stage"])
DEAD_LETTERS = Counter(
    "mdp_dead_letters_total", "Messages sent to the dead-letter stream", ["stage", "reason"]
)
SOURCE_RECONNECTS = Counter(
    "mdp_source_reconnects_total", "WebSocket reconnects to a trade source", ["url"]
)

TRADES_AGGREGATED = Counter(
    "mdp_trades_aggregated_total", "New trades folded into candles", ["symbol"]
)
CANDLES_WRITTEN = Counter(
    "mdp_candles_written_total", "Candle snapshots upserted and published", ["tf", "state"]
)
BATCH_SECONDS = Histogram(
    "mdp_aggregate_batch_seconds",
    "Time to process one batch, from read to acknowledgement",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
# Event time to publish: how far behind the exchange a candle update is when
# it leaves the aggregator. The WebSocket hop adds a few milliseconds on top.
TRADE_LAG_SECONDS = Histogram(
    "mdp_trade_to_publish_seconds",
    "Exchange timestamp of a trade to the publish of the candle that includes it",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)
WATERMARK_DELAY = Gauge(
    "mdp_watermark_delay_seconds", "Wall clock minus the symbol's watermark", ["symbol"]
)
STREAM_LAG = Gauge(
    "mdp_stream_lag_entries", "Stream entries not yet delivered to the aggregator", ["symbol"]
)
STREAM_PENDING = Gauge(
    "mdp_stream_pending_entries", "Entries delivered but not yet acknowledged", ["symbol"]
)

WS_CLIENTS = Gauge("mdp_ws_clients", "Connected WebSocket clients")
WS_CHANNELS = Gauge("mdp_ws_channels", "Channels with at least one subscriber")
WS_FRAMES = Counter("mdp_ws_frames_total", "Candle frames queued to WebSocket clients")
WS_SLOW_DISCONNECTS = Counter("mdp_ws_slow_disconnects_total", "Clients cut off for falling behind")

GAP_MINUTES = Gauge(
    "mdp_gap_minutes", "Minutes with no candle found by the last repair run", ["symbol"]
)
GAP_REPAIRED = Counter(
    "mdp_gap_repaired_minutes_total", "Minutes recovered from exchange klines", ["symbol"]
)
