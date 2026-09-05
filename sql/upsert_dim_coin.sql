-- load_dim step: upsert mart.dim_coin from the raw batch identified by
-- %(ingested_at)s. Inserts coins seen for the first time and refreshes
-- name/symbol (and updated_at) only when something actually changed.

INSERT INTO mart.dim_coin (coin_id, symbol, name, updated_at)
SELECT DISTINCT
    coin_id,
    symbol,
    name,
    %(ingested_at)s::timestamp AS updated_at
FROM raw.coin_snapshots
WHERE ingested_at = %(ingested_at)s::timestamp
ON CONFLICT (coin_id) DO UPDATE SET
    symbol      = EXCLUDED.symbol,
    name        = EXCLUDED.name,
    updated_at  = EXCLUDED.updated_at
WHERE
    mart.dim_coin.name   IS DISTINCT FROM EXCLUDED.name
    OR mart.dim_coin.symbol IS DISTINCT FROM EXCLUDED.symbol;
