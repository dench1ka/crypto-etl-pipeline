"""Load-dim task: upsert mart.dim_coin from the current batch.

Inserts coins seen for the first time; updates name/symbol (and
updated_at) only for coins whose name or symbol actually changed.
"""

import logging
from pathlib import Path

from db import get_connection

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "upsert_dim_coin.sql"

log = logging.getLogger(__name__)


def run(**context):
    ti = context["ti"]
    ingested_at = ti.xcom_pull(task_ids="transform", key="ingested_at")
    if not ingested_at:
        raise RuntimeError(
            "No ingested_at was found in XCom from the transform task."
        )

    sql = SQL_PATH.read_text()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"ingested_at": ingested_at})
            updated_rows = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    log.info(
        "load_dim: upserted %d row(s) into mart.dim_coin for batch ingested_at=%s",
        updated_rows,
        ingested_at,
    )
    return ingested_at
