"""One-off bootstrap: apply sql/ddl_raw.sql and sql/ddl_mart.sql to the
warehouse database. Run once by the airflow-init service on startup.
"""

import logging
from pathlib import Path

from db import get_connection

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
DDL_FILES = ["ddl_raw.sql", "ddl_mart.sql"]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for filename in DDL_FILES:
                sql = (SQL_DIR / filename).read_text()
                log.info("Applying %s", filename)
                cur.execute(sql)
        conn.commit()
    finally:
        conn.close()
    log.info("Warehouse schema is up to date.")


if __name__ == "__main__":
    main()
