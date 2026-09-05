-- Transform step: turn the raw batch identified by %(ingested_at)s into
-- mart.fact_price_snapshot rows.
--
-- price_change_pct  = percentage change of current_price vs. the previous
--                      snapshot for the same coin (LAG() OVER coin_id/ingested_at).
-- moving_avg_24h    = average price over the trailing 24h window for the
--                      same coin, evaluated as of this snapshot.
--
-- Executed with psycopg2 using a single named parameter: ingested_at
-- (the batch timestamp produced by the extract task).

WITH ranked AS (
    SELECT
        coin_id,
        current_price,
        market_cap,
        total_volume,
        ingested_at,
        LAG(current_price) OVER (
            PARTITION BY coin_id ORDER BY ingested_at
        ) AS prev_price,
        AVG(current_price) OVER (
            PARTITION BY coin_id ORDER BY ingested_at
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) AS moving_avg_24h,
        DENSE_RANK() OVER (
            PARTITION BY ingested_at ORDER BY market_cap DESC
        ) AS market_cap_rank
    FROM raw.coin_snapshots
    WHERE coin_id IN (
        SELECT DISTINCT coin_id
        FROM raw.coin_snapshots
        WHERE ingested_at = %(ingested_at)s::timestamp
    )
)
INSERT INTO mart.fact_price_snapshot (
    coin_id, snapshot_time, price, market_cap, volume,
    price_change_pct, moving_avg_24h, market_cap_rank
)
SELECT
    coin_id,
    ingested_at        AS snapshot_time,
    current_price      AS price,
    market_cap,
    total_volume        AS volume,
    CASE
        WHEN prev_price IS NOT NULL AND prev_price <> 0
            THEN ROUND(((current_price - prev_price) / prev_price) * 100, 4)
        ELSE NULL
    END AS price_change_pct,
    moving_avg_24h,
    market_cap_rank
FROM ranked
WHERE ingested_at = %(ingested_at)s::timestamp
ON CONFLICT (coin_id, snapshot_time) DO UPDATE SET
    price             = EXCLUDED.price,
    market_cap        = EXCLUDED.market_cap,
    volume            = EXCLUDED.volume,
    price_change_pct  = EXCLUDED.price_change_pct,
    moving_avg_24h    = EXCLUDED.moving_avg_24h,
    market_cap_rank   = EXCLUDED.market_cap_rank;
