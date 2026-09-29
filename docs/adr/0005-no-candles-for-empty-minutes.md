# 5. No candle for a minute without trades, so a missing row always means a gap

- Status: accepted
- Date: 2026-09-25

## Context

Some venues publish a flat, zero-volume candle for a minute with no trades
(open = high = low = close = the previous close). Charts like a continuous
series, and filling the minute is easy: the aggregator knows the previous
close.

But the aggregator cannot tell a quiet minute from a minute it never saw. If
the feed drops for ten minutes, the next trade after reconnecting looks
exactly like the first trade after ten quiet minutes. Filling both with flat
candles would hide every outage as a quiet market, and the pipeline would lose
its only way to detect a gap.

## Decision

The aggregator never produces a candle it has no trades for. A minute without
a row means only "we did not see any trades", and gap repair asks the exchange
what happened in each such minute that lies inside the symbol's history. Where
the exchange has a kline, the repair writes it, including the zero-volume
klines venues publish for genuinely quiet minutes, so those minutes are then
filled with the venue's own authoritative data.

## Consequences

- Gaps are detectable with one query (`generate_series` against the 1m rows),
  and `mdp_gap_minutes` reports them per symbol.
- A consumer that wants a continuous series fills it itself; the chart
  mapping in the README shows where. The API does not invent data.
- For sources without a klines API (the synthetic source, a matching engine),
  quiet minutes stay as gaps. That is accurate: nothing traded.
- A minute the exchange has no kline for either is reported again on every
  repair run until it leaves the lookback window. The window bounds the cost.
