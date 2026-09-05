"""crypto_price_pipeline

Hourly ETL: pull top-100 coins from CoinGecko into raw.coin_snapshots,
validate the batch, transform it into mart.fact_price_snapshot (with
price_change_pct / moving_avg_24h), then upsert mart.dim_coin.

extract >> validate >> transform >> load_dim
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# ./scripts is mounted at /opt/airflow/scripts (see docker-compose.yml).
sys.path.append("/opt/airflow/scripts")

import extract  # noqa: E402
import load_dim  # noqa: E402
import transform  # noqa: E402
import validate  # noqa: E402
import check_anomalies  # noqa: E402
from telegram_alert import send_failure_alert, send_anomaly_alert  # noqa: E402

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_alert,
}

with DAG(
    dag_id="crypto_price_pipeline",
    description="Hourly CoinGecko ETL into the crypto warehouse mart",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract.run,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate.run,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform.run,
    )

    load_dim_task = PythonOperator(
        task_id="load_dim",
        python_callable=load_dim.run,
    )

    check_anomalies_task = BranchPythonOperator(
        task_id="check_anomalies",
        python_callable=check_anomalies.run
    )

    send_anomaly_alert_task = PythonOperator(
        task_id="send_anomaly_alert",
        python_callable=send_anomaly_alert
    )

    skip_alert_task = EmptyOperator(
        task_id="skip_alert"
    )

    extract_task >> validate_task >> transform_task >> load_dim_task >> check_anomalies_task >> [send_anomaly_alert_task, skip_alert_task] 
