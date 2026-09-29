# 2. Exactly-once effect from snapshot upserts and a deduplicating trade log

- Status: accepted
- Date: 2026-09-24

## Context

Redis Streams consumer groups deliver **at least once**: a message stays
pending until it is acknowledged, and one delivered to a consumer that crashed
is delivered again. Exchanges add their own duplicates: Kraken replays a
snapshot of recent trades on every subscribe, and Binance can resend around a
reconnect. Counting any trade twice inflates volume and can move the high or
low.

The aggregator keeps its state in memory for speed, and that memory is lost on
a crash. Whatever is durable has to be enough to resume without double
counting and without losing a trade.

## Decision

Three rules together:

1. **Candles are written as whole snapshots**, not increments, with
   `INSERT ... ON CONFLICT (symbol, tf, bucket) DO UPDATE`. Writing the same
   snapshot twice leaves the row as it was. A guard,
   `WHERE NOT candles.closed OR EXCLUDED.closed`, stops an open snapshot from
   ever reopening a closed candle.
2. **Trades go into a log keyed on `(source, trade_id)`**, the dedup key the
   normalized schema defines. `INSERT ... ON CONFLICT DO NOTHING RETURNING`
   reports which trades in a batch are genuinely new, and only those are
   folded into candles.
3. **One transaction per batch**: the trade-log insert, the candle upserts and
   the aggregator checkpoint (watermark and open buckets) commit together or
   not at all. The batch is acknowledged in Redis only after the commit.

A crash before the commit leaves the batch pending and the database untouched,
so the batch is processed again from scratch. A crash after the commit but
before the acknowledgement redelivers a batch whose trades are already in the
log: none are reported as new, and nothing changes. Restart state is always the
checkpoint that committed with the candles, so the two never disagree.

The ingester and the aggregator also drop duplicates in memory: a bounded
window of recent keys at ingest, and a set per open 1m bucket in the
aggregator. They save work but are not relied on, since neither survives a
restart.

## Alternatives considered

- **Increment candles (`volume = volume + x`).** Cheaper per write, but
  replaying a batch counts it twice. Idempotency would then depend on perfectly
  tracking what was applied, which is the problem this design avoids.
- **Checkpoint the last stream id instead of logging trades.** Enough for
  redelivery, but blind to a source sending the same trade twice under new
  stream ids, which is exactly what reconnects produce.
- **Kafka transactions.** They give exactly-once within Kafka, but the output
  here is a Postgres row, so the database transaction is needed anyway
  (ADR 0003).

## Consequences

- Tests crash the service before the commit and between the commit and the
  acknowledgement, restart it under a new consumer name, and get candles
  identical to an uninterrupted run.
- Every trade costs a row in the trade log. The log is a hypertable, compressed
  after a day and dropped after seven, since it only has to outlive the
  lateness window and a restart. That seven days is also the dedup horizon: a
  duplicate older than that is not caught by the log, but it is already late,
  so it is dead-lettered and not applied anyway.
- The trade-log insert is the most expensive step of a batch (see the
  benchmarks in the README). It is the price of the guarantee.
