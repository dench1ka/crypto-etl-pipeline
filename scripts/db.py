"""Connection helper for the warehouse (postgres-warehouse) database.

Connection settings come from environment variables so nothing is
hardcoded; docker-compose.yml sets sane defaults for the local stack.
"""

import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_DB_HOST", "postgres-warehouse"),
        port=os.environ.get("WAREHOUSE_DB_PORT", "5432"),
        dbname=os.environ.get("WAREHOUSE_DB_NAME", "warehouse"),
        user=os.environ.get("WAREHOUSE_DB_USER", "warehouse"),
        password=os.environ.get("WAREHOUSE_DB_PASSWORD", "warehouse"),
    )
