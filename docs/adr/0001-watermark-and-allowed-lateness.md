# 1. Close candles on an event-time watermark; dead-letter what arrives later

- Status: accepted
- Date: 2026-09-24

## Context

A candle is defined by *event time*, the exchange's timestamp for each trade,
and trades do not arrive in event-time order. A reconnecting WebSocket, two
sources merged into one stream, or plain network jitter all deliver some trades
after later ones. If buckets were keyed on arrival (processing) time, the same
trades would produce different candles depending on how the network behaved
that day, and no replay or backfill could reproduce them.

Event time raises the question processing time avoids: when is a bucket
*finished*? At some point the pipeline has to declare 10:00–10:01 final, write
it as closed, and tell clients it will not change.

## Decision

Each symbol has a **watermark**: the newest event time seen, minus a fixed
`allowed_lateness` (5 s by default). A bucket closes when the watermark passes
its end. Until then, trades for it are accepted in any order, and `open` and
`close` come from the earliest and latest trade by event time, with equal
timestamps broken by trade id, so arrival order never shows in the result.

A trade whose 1m bucket has already closed is **late**. It is not applied; it
goes to the dead-letter stream with reason `late` and the watermark it missed.
The 1m bucket is the reference for every timeframe: a trade that is too late
for its minute is refused for its hour and day too, even though those are still
open, so all timeframes always agree on which trades they contain.

A quiet market sends no trades to move the watermark, so the last candle would
stay open until the next trade. The aggregator therefore also advances the
watermark on the wall clock, to `now - allowed_lateness - idle_grace`
(2 s by default), and never moves it backwards.

## Alternatives considered

- **Reopen and amend closed candles.** It is the most faithful to the data, but
  "closed" would stop meaning anything, and every consumer (the chart, the
  REST cache, anything computing indicators) would have to handle a candle
  that changes after being final. The exchange's own REST klines are a better
  way to correct history, and gap repair already uses them (ADR 0004).
- **Processing-time buckets.** Simple and never late, but not reproducible, and
  wrong by exactly the network delay at every minute boundary.
- **Per-source watermarks.** More precise when sources have very different
  delays, but a symbol here has one source (see the README's known
  limitations), so the extra state buys nothing yet.

## Consequences

- Candles are deterministic: the tests feed shuffled and duplicated trades and
  get the same candles as in-order ones, as long as the disorder stays within
  the allowed lateness.
- Live updates are not held back: every snapshot is published as soon as its
  batch commits. Only *closing* waits, until `allowed_lateness` after the
  bucket's end (plus `idle_grace` in a quiet market). The lateness setting
  trades how soon a candle is final against how many stragglers it can absorb.
- The wall-clock advance assumes the source's clock is within
  `allowed_lateness + idle_grace` of ours. A source running further behind has
  its trades judged late. That shows up plainly on the dashboard as dead
  letters with reason `late`; the remedy is to raise the lateness.
- After a crash between committing a batch and acknowledging it, redelivered
  trades whose minute has since closed are dead-lettered as late. They were
  counted the first time; the entry is noise, not data loss (ADR 0002).
