-- Mart layer: dimensional model consumed by BI / analytics.

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.dim_coin (
    coin_id      VARCHAR(100) PRIMARY KEY,
    symbol       VARCHAR(20),
    name         VARCHAR(200),
    updated_at   TIMESTAMP NOT NULL
);

-- No FK to dim_coin: the pipeline runs transform (writes facts) before
-- load_dim (writes dimensions), so enforcing the FK here would break the
-- very first batch for any newly-seen coin_id.
CREATE TABLE IF NOT EXISTS mart.fact_price_snapshot (
    coin_id           VARCHAR(100) NOT NULL,
    snapshot_time     TIMESTAMP    NOT NULL,
    price             NUMERIC,
    market_cap        NUMERIC,
    volume            NUMERIC,
    price_change_pct  NUMERIC,
    moving_avg_24h    NUMERIC,
    market_cap_rank   INTEGER,
    PRIMARY KEY (coin_id, snapshot_time)
);

CREATE INDEX IF NOT EXISTS ix_fact_price_snapshot_time
    ON mart.fact_price_snapshot (snapshot_time);
