# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
For this project, "breaking" means a change to the candle payload, the
WebSocket protocol, the REST parameters or an `MDP_*` setting.

## [Unreleased]

## [0.1.0] - 2026-09-29

### Added

- A normalized trade schema with decimal-string prices, and deduplication on
  `(source, trade_id)` at ingest and in the trade log.
- Source adapters for the Binance and Kraken public WebSockets and for a
  matching engine's JSON events over WebSocket or a Redis stream, all behind
  one `TradeSource` protocol. Reconnects use exponential backoff with full
  jitter, and a watchdog catches silent connections.
- A seeded synthetic source for demos, tests and benchmarks.
- An event-time candle aggregator for 1m, 5m, 1h and 1d with a watermark,
  allowed lateness, a wall-clock idle advance, and dead-lettering of late
  trades.
- Redis Streams with consumer groups between ingest and aggregation, and a
  dead-letter stream with the reason and original payload of every refusal.
- TimescaleDB storage: a candles hypertable with idempotent snapshot upserts,
  a deduplicating trade log, and a per-symbol checkpoint written in the same
  transaction. Compression and retention policies.
- Restart recovery from consumer-group offsets, orphaned messages and the
  checkpoint.
- `GET /v1/candles`, the `/v1/ws` WebSocket with `candles:{symbol}:{tf}`
  channels fanned out from one Redis subscription per channel, `/healthz`,
  `/readyz` and `/metrics/`.
- Backfill from Binance and Kraken REST klines, and a repair job that finds
  minutes with no candle and fetches them, then rebuilds the closed higher
  timeframes.
- Prometheus metrics for throughput, lag, errors and fan-out, and a
  provisioned Grafana dashboard.
- The `mdp` command, a Dockerfile, and a docker compose stack that runs the
  whole pipeline on the synthetic source.
- Benchmarks for aggregator throughput, end-to-end latency and storage.

[Unreleased]: https://github.com/CtrlAltDevelop/mdp-outbox/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/CtrlAltDevelop/mdp-outbox/releases/tag/v0.1.0
