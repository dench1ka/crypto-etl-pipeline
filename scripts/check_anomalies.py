"""
Check for anomalies in the price data.
"""

import logging
from pathlib import Path

from db import get_connection


SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "check_anomalies.sql"

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
            anomalies = cur.fetchall()
    finally:
        conn.close()

    if anomalies:
        log.warning(
            "Found %d anomalies in price data for batch ingested_at=%s",
            len(anomalies),
            ingested_at,
        )

        ti.xcom_push(key="anomalies", value=anomalies)
        
        for anomaly in anomalies:
            log.warning("Anomaly: %s", anomaly)

        return "send_anomaly_alert"
    else:
        log.info(
            "No anomalies found in price data for batch ingested_at=%s",
            ingested_at,
        )
        return "skip_alert"

    
    