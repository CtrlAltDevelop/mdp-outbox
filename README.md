# Market Data Pipeline

[![CI](https://github.com/CtrlAltDevelop/mdp-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/CtrlAltDevelop/mdp-outbox/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Ingest trades from exchanges and a matching engine, aggregate them into OHLCV
candles (1m, 5m, 1h, 1d) live and historically, and serve them over REST and
WebSocket. Built on asyncio, Redis Streams, TimescaleDB and FastAPI.

- **Event-time candles** with a watermark and allowed lateness. Trades that
  arrive shuffled or duplicated give the same candles as in-order ones.
- **An exactly-once effect** from at-least-once delivery: a deduplicating trade
  log, snapshot upserts and a checkpoint, all committed in one transaction.
- **Restart without loss**: consumer-group offsets, orphan claiming, and open
  candles rebuilt from the checkpoint in the database.
- **Adapters** for Binance and Kraken public WebSockets (reconnect with backoff
  and full jitter, heartbeat watchdog) and for any matching engine that
  publishes JSON over WebSocket or a Redis stream.
- **Gap detection and repair** from the exchanges' REST klines.
- **One Redis subscription per channel** fanned out in-process to any number of
  WebSocket clients, with slow clients cut off rather than buffered.
- **Prometheus metrics** and a provisioned Grafana dashboard.

## How the data flows

```mermaid
flowchart LR
    subgraph sources[Sources]
        BN[Binance WS]
        KR[Kraken WS]
        EN[Matching engine<br/>WS or Redis stream]
        SY[Synthetic]
    end
    subgraph ingest[mdp ingest]
        AD[Adapter] --> NO[Normalizer<br/>validate, dedup]
    end
    sources --> AD
    NO -->|XADD| TS[("Redis stream<br/>mdp:trades:{symbol}")]
    NO -->|refused| DLQ[("Redis stream<br/>mdp:dlq")]
    TS -->|XREADGROUP| AG
    subgraph aggregate[mdp aggregate]
        AG[Aggregator<br/>watermark, 4 timeframes]
    end
    AG -->|late| DLQ
    AG -->|"one transaction:<br/>trade log + candles + checkpoint"| DB[(TimescaleDB)]
    AG -->|PUBLISH| PS{{"Redis pub/sub<br/>candles:{symbol}:{tf}"}}
    subgraph api[mdp api]
        REST["GET /v1/candles"]
        HUB[WebSocket hub<br/>/v1/ws]
    end
    DB --> REST
    PS --> HUB
    REST --> CH[ohlcv_chart]
    HUB --> CH
    EX[Exchange REST klines] --> RP[mdp repair]
    RP -->|missing minutes| DB
```

A candle update travels from the aggregator to a chart as a whole snapshot, so
a client that misses one is corrected by the next. History comes from REST.

## Quick start

```bash
docker compose up --build
```

This starts Redis, TimescaleDB, Prometheus, Grafana, a one-shot schema
migration and the three pipeline processes, fed by the deterministic synthetic
source for BTC-USDT and ETH-USDT.

| What | Where |
| --- | --- |
| REST | <http://localhost:8000/v1/candles?symbol=BTC-USDT&tf=1m> |
| WebSocket | `ws://localhost:8000/v1/ws` |
| API docs (OpenAPI) | <http://localhost:8000/docs> |
| Grafana, the "Market Data Pipeline" dashboard | <http://localhost:3000> |
| Prometheus | <http://localhost:9090> |

Host ports can be moved with `MDP_API_PORT`, `MDP_GRAFANA_PORT` and
`MDP_PROMETHEUS_PORT`. To follow a real exchange, copy `.env.example` to `.env`
and set:

```bash
MDP_SOURCE=binance            # or kraken
MDP_SYMBOLS=BTC-USDT,ETH-USDT # Kraken: BTC-USD,ETH-USD
```

Then start gap repair alongside: `docker compose --profile repair up --build`.

Without Docker, with Redis and TimescaleDB running somewhere:

```bash
uv sync
export MDP_REDIS_URL=redis://localhost:6379/0
export MDP_DATABASE_URL=postgresql://mdp:mdp@localhost:5432/mdp
uv run mdp migrate
MDP_METRICS_PORT=9101 uv run mdp ingest &
MDP_METRICS_PORT=9102 uv run mdp aggregate &
uv run mdp api
```

Every pipeline setting is an `MDP_*` environment variable. `.env.example`
lists them with their defaults, and `src/mdp/config.py` is the reference.

## Schema

### Trade

Every adapter produces this, and it is what travels through the stream.

```json
{
  "source": "binance",
  "symbol": "BTC-USDT",
  "trade_id": "6715234671",
  "price": "84106.09000000",
  "qty": "0.00007000",
  "side": "sell",
  "ts_exchange": "2026-09-26T18:09:12.678000Z",
  "ts_ingested": "2026-09-26T18:09:12.702114Z"
}
```

- Symbols are canonical `BASE-QUOTE`, uppercase. Adapters map venue names
  (`BTCUSDT`, `BTC/USD`) to them.
- `side` is the taker's side, the aggressor's.
- `(source, trade_id)` is the identity used for deduplication.
- Prices and quantities are decimal strings, never floats. Kraken sends JSON
  numbers, and its adapter parses them straight to `Decimal`.

### Candle

```json
{
  "symbol": "BTC-USDT",
  "tf": "1m",
  "time": 1790446140000,
  "open": "84106.09",
  "high": "84110.00",
  "low": "84101.20",
  "close": "84106.10",
  "volume": "3.46100000",
  "quote_volume": "291182.10406440",
  "trades": 443,
  "closed": false
}
```

- `time` is the bucket's open time in epoch milliseconds, UTC. Buckets align to
  UTC boundaries, so a 1d candle runs from midnight to midnight UTC.
- `closed` becomes `true` once the watermark passes the bucket's end, and the
  candle does not change after that. Until then, each update replaces the
  previous one with the same `time`.
- A minute with no trades has no candle (see ADR 0005).

In TimescaleDB, `candles` is a hypertable keyed on `(symbol, tf, bucket)`,
`trades` is the deduplicating trade log keyed on `(source, trade_id, ts)` and
kept for seven days, and `aggregator_state` is the per-symbol checkpoint. See
[`src/mdp/storage/schema.sql`](src/mdp/storage/schema.sql).

## API

### `GET /v1/candles`

| Parameter | | |
| --- | --- | --- |
| `symbol` | required | a configured symbol, e.g. `BTC-USDT` |
| `tf` | required | `1m`, `5m`, `1h` or `1d` |
| `from` | optional | epoch ms or ISO 8601; defaults to `limit` buckets before `to` |
| `to` | optional | epoch ms or ISO 8601, exclusive; defaults to now |
| `limit` | optional | 1 to 5000, default 1000 |

```bash
curl 'http://localhost:8000/v1/candles?symbol=BTC-USDT&tf=5m&from=2026-09-26T00:00:00Z&limit=3'
```

```json
{"symbol": "BTC-USDT", "tf": "5m", "candles": [{"symbol": "BTC-USDT", "tf": "5m", "time": 1790380800000, "...": "..."}]}
```

It returns 404 for a symbol this deployment does not serve, and 422 for a bad
timeframe or range.

### WebSocket `/v1/ws`

Channels are named `candles:{symbol}:{tf}`. A client may hold up to 50.

```jsonc
// client to server
{"op": "subscribe", "channel": "candles:BTC-USDT:1m"}
{"op": "unsubscribe", "channel": "candles:BTC-USDT:1m"}

// server to client
{"op": "subscribed", "channel": "candles:BTC-USDT:1m"}
{"channel": "candles:BTC-USDT:1m", "data": { /* a candle, as above */ }}
{"op": "error", "channel": "candles:DOGE-USDT:1m", "error": "unknown channel"}
```

The recommended client flow is to load history over REST, subscribe, then
replace or append by `time`. A client that falls more than 256 frames behind
is closed with code 1013, and should reconnect and reload.

### Health and metrics

- `GET /healthz`: liveness, always `{"status": "ok"}` while the process runs.
- `GET /readyz`: readiness, pinging Redis and the database; 503 if either fails.
- `GET /metrics/`: Prometheus metrics for the API process. The ingest,
  aggregate and repair workers serve theirs on `MDP_METRICS_PORT` (9100).

## Using it with `ohlcv_chart`

[`ohlcv_chart`](https://pub.dev/packages/ohlcv_chart) draws `KLineEntity`
objects. The candle payload keeps the field names and units it uses where it
can (`time` in ms, `open`, `high`, `low`, `close`), and the rest maps one to
one:

| Pipeline | `KLineEntity` |
| --- | --- |
| `time` (epoch ms, UTC) | `dateTime` |
| `open`, `high`, `low`, `close` | `open`, `high`, `low`, `close` |
| `volume` | `vol` |
| `quote_volume` | `amount` |

The numbers arrive as decimal strings, so they go through `double.parse`
rather than `KLineEntity.fromJson`, which expects JSON numbers:

```dart
import 'dart:convert';

import 'package:ohlcv_chart/ohlcv_chart.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

KLineEntity toKLine(Map<String, dynamic> c) => KLineEntity.fromCustom(
      open: double.parse(c['open'] as String),
      high: double.parse(c['high'] as String),
      low: double.parse(c['low'] as String),
      close: double.parse(c['close'] as String),
      vol: double.parse(c['volume'] as String),
      amount: double.parse(c['quote_volume'] as String),
      dateTime: DateTime.fromMillisecondsSinceEpoch(c['time'] as int, isUtc: true),
    );

/// Apply one live update: the same `time` replaces the last candle, a new one appends.
void onFrame(List<KLineEntity> candles, String frame) {
  final data = (jsonDecode(frame) as Map<String, dynamic>)['data'];
  if (data == null) return; // a subscribe ack
  final next = toKLine(data as Map<String, dynamic>);
  if (candles.isNotEmpty && candles.last.dateTime == next.dateTime) {
    candles[candles.length - 1] = next;
  } else {
    candles.add(next);
  }
  DataUtil.calculate(candles); // refresh the indicators before repainting
}

final channel = WebSocketChannel.connect(Uri.parse('ws://localhost:8000/v1/ws'))
  ..sink.add(jsonEncode({'op': 'subscribe', 'channel': 'candles:BTC-USDT:1m'}));
```

## The matching engine adapter

`MDP_SOURCE=engine` reads trade events from an engine over WebSocket
(`MDP_ENGINE_URL=ws://engine:8080/events`) or from a Redis stream through a
consumer group (`MDP_ENGINE_URL=redis://engine-redis:6379/0#engine:events`).
The default format expects integer ticks and lots:

```json
{"type": "trade", "trade_id": 812, "symbol": "BTC-USDT", "price": 8410609, "qty": 7, "taker_side": "sell", "ts": 1790446152678}
```

`MDP_ENGINE_PRICE_SCALE` and `MDP_ENGINE_QTY_SCALE` give the implied decimal
places. Field names and the timestamp unit are configurable in
`EngineFormat`. Events of any other `type` (order accepted, order cancelled)
are skipped, so the adapter can read the engine's whole event stream.

## Operating it

- **Dead letters** are in the `mdp:dlq` stream, each with a `reason`
  (`malformed`, `unknown_symbol`, `future`, `late`), `stage`, `source`,
  `detail` and the original `payload`:
  `redis-cli XREVRANGE mdp:dlq + - COUNT 10`.
- **Backfill** a range from the exchange, overwriting what is there:
  `mdp backfill BTC-USDT 2026-09-01T00:00:00Z 2026-09-02T00:00:00Z`.
- **Repair once**: `mdp repair --once` prints, for each symbol, what was
  missing and what was recovered.
- **Scaling out**: run one aggregator per group of symbols, each with its own
  `MDP_SYMBOLS` and `MDP_CONSUMER`. A symbol must belong to exactly one
  (ADR 0003).

The Grafana dashboard covers ingest and aggregation throughput,
trade-to-publish latency (p50 and p99), watermark delay, stream backlog,
reconnects, dead letters by reason, duplicates, missing and repaired minutes,
and WebSocket clients and frames.

## Performance

Measured on the development machine: Windows 11, Python 3.14.7, with Redis 7
and TimescaleDB (latest-pg16) in Docker Desktop. It is a slow machine (a
million `dict.get` calls take 0.13 to 0.18 s, several times longer than on a
typical laptop), so read these as a floor. `make bench` reruns all three.

**Throughput** (`bench/aggregator.py`): 200,000 synthetic trades over 4
symbols in batches of 1,000; the median and range of four runs.

| Stage | What it includes | trades/s |
| --- | --- | --- |
| core | the aggregator alone, all four timeframes | 62,000 (55,000 to 78,000) |
| decode | plus JSON decoding and validation of each trade | 36,000 (23,000 to 43,000) |
| service | the whole service: `XREADGROUP`, the trade log, candles and checkpoint in one transaction, publish, `XACK` | 6,300 (4,400 to 7,300) |

The service is bound by the trade-log insert, which takes 55 to 65 ms of
server time per 1,000 rows here. A plain Postgres table on the same setup
takes 43 to 50 ms, so the cost is mostly this machine and Docker Desktop, not
the hypertable. The log is what makes redelivery safe (ADR 0002).

**End to end** (`bench/latency.py`): from a trade's exchange timestamp to the
candle update that includes it arriving on a WebSocket client, through ingest,
Redis, the aggregator's transaction, pub/sub and the API hub. 50 trades/s for
60 s, about 2,000 updates per run, two runs:

| | run 1 | run 2 |
| --- | --- | --- |
| p50 | 22.3 ms | 20.3 ms |
| p90 | 36.1 ms | 32.2 ms |
| p99 | 56.1 ms | 47.7 ms |

**Storage** (`bench/storage.py`): 30 days of 1m candles for one symbol
(43,200 rows), plus their 5m, 1h and 1d rollups (9,390 rows), take 10.88 MiB
in the `candles` hypertable, and 2.62 MiB once compressed: 4.2 times smaller.

## Development

```bash
uv sync
make services   # Redis on 56380 and TimescaleDB on 55433, in Docker
make check      # ruff, ruff format --check, mypy --strict, pytest
```

The tests use the real Redis at `REDIS_URL` when it answers, and fakeredis
otherwise. Tests that need TimescaleDB are marked `timescale` and skipped,
with a reason, when `DATABASE_URL` does not answer. The store contract tests
run against both the in-memory store and TimescaleDB.

What the suite pins down:

- OHLCV at minute boundaries; minutes without trades get no candle.
- Shuffled trades within the allowed lateness give the same candles as
  in-order ones. So does sending every trade twice, and so does any batch size.
- A crash before the commit, or between the commit and the acknowledgement,
  followed by a restart under a new consumer name, gives the same candles as
  an uninterrupted run. A restart in the middle of a minute closes that minute
  correctly.
- Adapter contract tests replay frames recorded from the live Binance and
  Kraken feeds and REST APIs. Nothing in the suite touches the network.
- The reconnect loop's backoff, resubscription and silence watchdog; the
  hub's shared subscriptions and slow-client cut-off; the REST and WebSocket
  APIs.

## Design decisions

| ADR | |
| --- | --- |
| [0001](docs/adr/0001-watermark-and-allowed-lateness.md) | Close candles on an event-time watermark; dead-letter what arrives later |
| [0002](docs/adr/0002-idempotent-writes.md) | Exactly-once effect from snapshot upserts and a deduplicating trade log |
| [0003](docs/adr/0003-redis-streams-over-kafka.md) | Redis Streams, not Kafka, between ingest and aggregation |
| [0004](docs/adr/0004-higher-timeframes.md) | Higher timeframes computed in the stream; repair rolls them up in SQL |
| [0005](docs/adr/0005-no-candles-for-empty-minutes.md) | No candle for a minute without trades, so a missing row always means a gap |

## Known limitations

- **Static partitioning.** A symbol belongs to one aggregator, assigned by
  configuration. There is no automatic failover or rebalancing; a replacement
  process picks up where the old one stopped.
- **One source per symbol.** Two sources feeding the same canonical symbol
  would be merged into one candle. Use distinct symbols per venue.
- **Late trades are not applied.** A trade later than the allowed lateness is
  dead-lettered, and closed candles are never amended from the stream. The
  exchange's klines, through `mdp backfill`, are the way to correct history.
- **The wall-clock watermark assumes the clocks agree** within
  `allowed_lateness + idle_grace`. A source running further behind has its
  trades judged late.
- **Redis bounds retention.** An aggregator down long enough for its stream to
  be trimmed loses those trades. Gap repair can refill the minutes only for
  exchanges with a klines API.
- **Repair coverage.** Kraken's OHLC endpoint serves only its latest 720
  minutes. The synthetic and engine sources have no history to repair from.
- **Pub/sub is fire-and-forget.** A WebSocket client can miss updates during a
  Redis blip, including a candle's closing snapshot. The next update corrects
  an open candle, and REST has the closed one.
- **No authentication or TLS** on the API, and the compose file's credentials
  are demo values. Put it behind a gateway before exposing it.

## Project layout

```text
src/mdp/
  schema.py         Trade and Candle, the two wire shapes
  timeframes.py     bucket arithmetic in epoch milliseconds
  normalizer.py     validation, dedup window, rejection reasons
  sources/          Binance, Kraken, matching engine, synthetic; the reconnecting feed
  ingest.py         source to Redis stream
  aggregator.py     the stateful, I/O-free candle aggregator
  aggregation.py    the service around it: read, transact, publish, acknowledge
  storage/          the Store protocol, TimescaleDB and in-memory stores, schema.sql
  hub.py, api.py    WebSocket fan-out, REST, health
  backfill.py       klines clients, gap detection and repair
  metrics.py        Prometheus metrics
  cli.py            the mdp command
bench/              throughput, latency and storage benchmarks
ops/                Prometheus config, Grafana provisioning and dashboard
docs/adr/           architecture decision records
```

## License

[MIT](LICENSE) © 2026 Mohammad Zarif
