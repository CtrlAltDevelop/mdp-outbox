# 3. Redis Streams, not Kafka, between ingest and aggregation

- Status: accepted
- Date: 2026-09-24

## Context

Ingest and aggregation are separate processes so a slow database never stalls
a WebSocket reader, and so either side can restart without losing trades. They
need a durable log with consumer offsets and redelivery between them. Kafka
(or Redpanda) is the standard answer; Redis Streams is the lighter one.

The pipeline also needs pub/sub for the WebSocket fan-out and would benefit
from a shared cache, which is Redis either way.

## Decision

Use **Redis Streams** with consumer groups. There is one stream per symbol,
`mdp:trades:{symbol}`, since a symbol is the unit that must be processed in
order by one aggregator. The ingester trims each stream to about a million
entries (`MAXLEN ~`), which bounds memory and sets how far the aggregator can
fall behind before data is lost.

A symbol is owned by exactly one aggregator. The consumer group provides
durable offsets, the pending list and redelivery. It is not used to spread
one symbol's trades across workers: that would split a candle's trades across
two processes. To scale out, symbols are partitioned statically: each
aggregator gets its own `MDP_SYMBOLS`. On start an aggregator claims every
pending message in its streams (`XAUTOCLAIM`), so a renamed or replaced
consumer does not strand messages.

## Why not Kafka

- **Operational weight.** Kafka is a cluster (or Redpanda, a lighter one) to
  run, tune and monitor for a load that fits in one Redis. Redis is already
  needed for pub/sub.
- **Partitioning buys little here.** Kafka's key is ordered per-key
  partitions with rebalancing. With one stream per symbol and static
  ownership, Redis gives the same ordering and lets the database decide
  exactly-once (ADR 0002).
- **Exactly-once does not transfer.** Kafka transactions cover
  Kafka-to-Kafka. The output here is a Postgres row, so the database
  transaction is required regardless.

## When to revisit

- Retention measured in days, or replaying history from the log itself: Kafka
  on disk is far cheaper per byte than Redis in memory.
- Many consumers of the raw trades beyond this aggregator (analytics, a
  surveillance job): Kafka's independent consumer groups on durable storage
  suit that better.
- Automatic rebalancing of symbols across a fleet of aggregators.

## Consequences

- Retention is bounded by memory. An aggregator down long enough for the
  stream to trim past its offset loses those trades. That shows as a rising
  `mdp_stream_lag_entries`, and afterwards as missing minutes that gap repair
  fills from the exchange.
- Redis should run with AOF persistence (the compose file enables it), or a
  Redis restart drops un-aggregated trades.
- Moving to Kafka later touches two modules: `ingest.py` writes and
  `aggregation.py` reads. The aggregator, the store and the API do not care
  where the trades came from.
