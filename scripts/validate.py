"""Validate task: sanity-check the batch that extract just wrote before it
is allowed to flow into the mart layer.

Checks:
  1. the batch is not empty;
  2. there are no duplicate (coin_id, ingested_at) pairs in the batch.
"""

import logging

from db import get_connection

log = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised with a human-readable message when a batch fails validation."""


def run(**context):
    ti = context["ti"]
    ingested_at = ti.xcom_pull(task_ids="extract", key="ingested_at")
    if not ingested_at:
        raise ValidationError(
            "No ingested_at was found in XCom from the extract task; "
            "the extract task may not have run successfully."
        )

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw.coin_snapshots WHERE ingested_at = %s",
                (ingested_at,),
            )
            total_rows = cur.fetchone()[0]
            if total_rows == 0:
                raise ValidationError(
                    f"Batch ingested_at={ingested_at} is empty: "
                    "no rows found in raw.coin_snapshots."
                )

            cur.execute(
                """
                SELECT coin_id, COUNT(*)
                FROM raw.coin_snapshots
                WHERE ingested_at = %s
                GROUP BY coin_id
                HAVING COUNT(*) > 1
                """,
                (ingested_at,),
            )
            duplicates = cur.fetchall()
            if duplicates:
                dupe_desc = ", ".join(f"{coin_id} (x{count})" for coin_id, count in duplicates)
                raise ValidationError(
                    f"Batch ingested_at={ingested_at} contains duplicate "
                    f"(coin_id, ingested_at) pairs: {dupe_desc}"
                )
    finally:
        conn.close()

    log.info("Batch ingested_at=%s passed validation (%d rows)", ingested_at, total_rows)
    ti.xcom_push(key="ingested_at", value=ingested_at)
    return ingested_at
