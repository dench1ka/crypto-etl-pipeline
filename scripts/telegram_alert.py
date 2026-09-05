"""on_failure_callback: notify a Telegram chat when a DAG task fails.

Bot token and chat id are read from Airflow Variables (telegram_bot_token,
telegram_chat_id) rather than hardcoded, so they can be set/rotated via the
Airflow UI (Admin -> Variables) without touching code.
"""

import logging

import requests
from airflow.models import Variable

log = logging.getLogger(__name__)

TELEGRAM_TOKEN_VAR = "telegram_bot_token"
TELEGRAM_CHAT_ID_VAR = "telegram_chat_id"


def send_failure_alert(context):
    token, chat_id = get_telegram_variables()
    if not token or not chat_id:
        log.warning("Telegram variables not set; skipping failure alert.")
        return

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else (task_instance.dag_id if task_instance else "unknown")
    task_id = task_instance.task_id if task_instance else "unknown"
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception")
    log_url = getattr(task_instance, "log_url", "")

    text = (
        "\U0001F534 Airflow task failed\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run: {run_id}\n"
        f"Error: {exception}\n"
        f"Log: {log_url}"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        response = requests.post(
            url, data={"chat_id": chat_id, "text": text}, timeout=10
        )
        response.raise_for_status()
    except requests.RequestException:
        log.exception("Failed to send Telegram failure alert")



def send_anomaly_alert(**context):
    token, chat_id = get_telegram_variables()
    if not token or not chat_id:
        log.warning("Telegram variables not set; skipping anomaly alert.")
        return

    ti = context["ti"]
    anomalies = ti.xcom_pull(task_ids="check_anomalies", key="anomalies")
    if anomalies:
        log.info("Anomalies found; sending Telegram alert.")

    text = (
        "\U0001F7E1 Airflow anomaly detected\n"
        f"DAG: {context.get('dag').dag_id}\n"
        f"Task: {context.get('task_instance').task_id}\n"
        f"Run: {context.get('run_id')}\n"
        f"Anomalies: {anomalies}"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        response = requests.post(
            url, data={"chat_id": chat_id, "text": text}, timeout=10
        )
        response.raise_for_status()
    except requests.RequestException:
        log.exception("Failed to send Telegram anomaly alert")



def get_telegram_variables():
    """Check if the required Airflow Variables for Telegram alerts are set."""
    try:
        token = Variable.get(TELEGRAM_TOKEN_VAR)
        chat_id = Variable.get(TELEGRAM_CHAT_ID_VAR)
        return token, chat_id
    except KeyError:
        log.warning(
            "Airflow Variables '%s' / '%s' are not set; Telegram alerts will be disabled.",
            TELEGRAM_TOKEN_VAR,
            TELEGRAM_CHAT_ID_VAR,
        )
        return None, None