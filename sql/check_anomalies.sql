--check for anomalies in the current_price column of the raw.coin_snapshots table

SELECT
    coin_id,
    snapshot_time,
    price_change_pct
FROM mart.fact_price_snapshot
WHERE abs(price_change_pct) > 10
    AND price_change_pct IS NOT NULL
    AND snapshot_time = %(ingested_at)s::timestamp
;