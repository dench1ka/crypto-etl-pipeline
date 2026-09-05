"""Transform task: run sql/transform_mart.sql for the current batch,
computing price_change_pct and moving_avg_24h and writing into
mart.fact_price_snapshot.
"""

import logging
from pathlib import Path

from db import get_connection

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "transform_mart.sql"

log = logging.getLogger(__name__)


def run(**context):
    ti = context["ti"]
    ingested_at = ti.xcom_pull(task_ids="validate", key="ingested_at")
    if not ingested_at:
        raise RuntimeError(
            "No ingested_at was found in XCom from the validate task."
        )

    sql = SQL_PATH.read_text()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, {"ingested_at": ingested_at})
        conn.commit()
    finally:
        conn.close()

    log.info("Transformed batch ingested_at=%s into mart.fact_price_snapshot", ingested_at)
    ti.xcom_push(key="ingested_at", value=ingested_at)
    return ingested_at
