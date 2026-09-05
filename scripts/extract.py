"""Extract task: pull the top-100 coins by market cap from CoinGecko and
append them to raw.coin_snapshots.
"""

import logging
from datetime import datetime, timezone

import requests
from psycopg2.extras import execute_values

from db import get_connection

API_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=100"
)

INSERT_SQL = """
    INSERT INTO raw.coin_snapshots (
        coin_id, symbol, name, current_price, market_cap,
        total_volume, price_change_24h, ingested_at
    ) VALUES %s
"""

log = logging.getLogger(__name__)


def run(**context):
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if not payload:
        raise RuntimeError("CoinGecko API returned an empty payload")

    rows = [
        (
            coin.get("id"),
            coin.get("symbol"),
            coin.get("name"),
            coin.get("current_price"),
            coin.get("market_cap"),
            coin.get("total_volume"),
            coin.get("price_change_24h"),
            ingested_at,
        )
        for coin in payload
    ]

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, rows)
        conn.commit()
    finally:
        conn.close()

    log.info("Inserted %d rows for batch ingested_at=%s", len(rows), ingested_at)

    ti = context["ti"]
    ti.xcom_push(key="ingested_at", value=ingested_at.isoformat())
    return ingested_at.isoformat()
