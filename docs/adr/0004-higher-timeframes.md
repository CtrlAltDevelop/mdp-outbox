# 4. Higher timeframes are computed in the stream; repair rolls them up in SQL

- Status: accepted
- Date: 2026-09-25

## Context

The pipeline serves 1m, 5m, 1h and 1d candles. They can be built three ways:

1. from the raw trades, each timeframe on its own, in the stream;
2. from the 1m candles, by folding them into larger buckets;
3. from the 1m rows in the database, with TimescaleDB continuous aggregates.

The live path has to update every timeframe on every trade: a chart showing
1h candles expects the current hour to move with each trade, not once a
minute.

## Decision

**Live**: the aggregator folds each accepted trade into all four timeframes
directly. It is four updates to in-memory builders per trade, the same code
for every timeframe, and every snapshot is exact the moment it is published.

**History and repair**: when candles are written from somewhere other than the
live path (exchange klines from a backfill or a gap repair), 5m, 1h and 1d are
rebuilt from the 1m rows with a SQL rollup (`time_bucket`, `first`, `last`,
`max`, `min`, `sum`) and upserted as closed. Only buckets the aggregator's
watermark has passed are rolled up. An open hour or day belongs to the live
path until it closes.

OHLCV is associative (the first open, the last close, the max of the highs,
the min of the lows, the sum of the volumes), so a rollup of complete 1m
candles equals the candle computed from their trades. The tests check this for
both the SQL and the in-memory store.

## Alternatives considered

- **Continuous aggregates.** They are materialized on a refresh policy, which
  lags by at least one refresh interval, and real-time aggregation only helps
  queries, not the WebSocket feed. The stream would compute the live values
  anyway, leaving two implementations of the same candle that have to agree,
  one of them in a refresh job whose timing is separate from the watermark.
  They also cannot express "closed": a continuous aggregate row does not know
  whether its bucket is final.
- **Fold 1m candles into higher ones in the stream.** It works for closed
  minutes, but the open minute is always partial, so the current hour would
  have to combine closed minutes with the open one on every update, which is
  more code than updating four builders.

## Consequences

- Per trade the aggregator does four builder updates instead of one. The
  throughput benchmark includes them.
- The rollup is the single piece of SQL that has to match the Python builder.
  It is covered by the store contract tests, which run against both stores.
- A candle repaired from exchange klines carries the exchange's own values,
  including its trade count and quote volume. For Kraken, which publishes no
  quote volume, it is reconstructed as `vwap * volume`.
