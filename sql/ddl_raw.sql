-- Raw layer: append-only landing zone for CoinGecko API snapshots.
-- No primary key by design; every extract run appends a new batch of rows
-- identified by ingested_at. Deduplication/validation happens downstream.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.coin_snapshots (
    coin_id            VARCHAR(100)     NOT NULL,
    symbol             VARCHAR(20),
    name               VARCHAR(200),
    current_price      NUMERIC,
    market_cap         NUMERIC,
    total_volume       NUMERIC,
    price_change_24h   NUMERIC,
    ingested_at        TIMESTAMP        NOT NULL
);

-- Non-unique index to speed up per-batch lookups/validation and
-- the LAG()/window queries used by the transform step.
CREATE INDEX IF NOT EXISTS ix_coin_snapshots_coin_ingested
    ON raw.coin_snapshots (coin_id, ingested_at);

CREATE INDEX IF NOT EXISTS ix_coin_snapshots_ingested_at
    ON raw.coin_snapshots (ingested_at);
