-- Market Data Pipeline schema. Idempotent: `mdp migrate` runs it on every start.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- The trade log. Its primary key is the durable dedup: a redelivered trade
-- conflicts and is skipped. Timescale requires the partitioning column in
-- every unique index; a trade's timestamp never changes, so adding `ts` to the
-- key does not weaken it.
CREATE TABLE IF NOT EXISTS trades (
    ts          timestamptz NOT NULL,
    source      text        NOT NULL,
    trade_id    text        NOT NULL,
    symbol      text        NOT NULL,
    price       numeric     NOT NULL,
    qty         numeric     NOT NULL,
    side        text        NOT NULL,
    ts_ingested timestamptz NOT NULL,
    PRIMARY KEY (source, trade_id, ts)
);
SELECT create_hypertable('trades', by_range('ts', INTERVAL '1 day'), if_not_exists => TRUE);

-- One row per (symbol, timeframe, bucket). Rows are whole snapshots, so an
-- upsert that replays an old write leaves the row exactly as it was.
CREATE TABLE IF NOT EXISTS candles (
    symbol       text        NOT NULL,
    tf           text        NOT NULL,
    bucket       timestamptz NOT NULL,
    open         numeric     NOT NULL,
    high         numeric     NOT NULL,
    low          numeric     NOT NULL,
    close        numeric     NOT NULL,
    volume       numeric     NOT NULL,
    quote_volume numeric     NOT NULL,
    trades       bigint      NOT NULL,
    closed       boolean     NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, tf, bucket)
);
SELECT create_hypertable('candles', by_range('bucket', INTERVAL '7 days'), if_not_exists => TRUE);

-- The aggregator's checkpoint: watermark and open buckets per symbol, written
-- in the same transaction as the candles it produced.
CREATE TABLE IF NOT EXISTS aggregator_state (
    symbol     text        PRIMARY KEY,
    state      jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Compression. Changing these settings once chunks are compressed is an
-- error, so they are applied only the first time.
DO $$
BEGIN
    IF NOT (SELECT compression_enabled FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'trades') THEN
        ALTER TABLE trades SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol',
            timescaledb.compress_orderby = 'ts DESC'
        );
    END IF;
    IF NOT (SELECT compression_enabled FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'candles') THEN
        ALTER TABLE candles SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol, tf',
            timescaledb.compress_orderby = 'bucket DESC'
        );
    END IF;
END
$$;

-- The trade log only has to outlive the lateness window and a restart; a week
-- is generous. Candles are the product and are kept, compressed after a week.
SELECT add_compression_policy('trades', INTERVAL '1 day', if_not_exists => TRUE);
SELECT add_retention_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('candles', INTERVAL '7 days', if_not_exists => TRUE);
