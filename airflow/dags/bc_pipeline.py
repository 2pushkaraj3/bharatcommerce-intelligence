"""
Bharatcommerce Intelligence Platform
Airflow DAG — bc_main_pipeline

Schedule: every 6 hours
Flow:
    check_new_data → load_orders_to_bq → load_returns_to_bq
                   → dbt_run → dbt_test → notify_success

Design decisions:
  - LocalExecutor (no Celery) — sufficient for solo/small team
  - Sensors before load tasks — skip run if no new Parquet files
  - dbt run via BashOperator — simple, debuggable, no extra plugins
  - on_failure_callback on every task — know immediately when it breaks
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────

PROJECT   = os.getenv("GCP_PROJECT",   "sigma-composite-492018-p3")
DATASET   = os.getenv("BQ_DATASET",    "bharatcommerce")
KEY_FILE  = os.getenv("GCP_KEY_FILE",  "/opt/airflow/gcp_key.json")
DATA_LAKE = Path(os.getenv("DATA_LAKE_PATH", "/opt/airflow/data_lake"))
DBT_DIR   = os.getenv("DBT_DIR",       "/opt/airflow/dbt")

# ── Default args ───────────────────────────────────────────────

def on_failure(context):
    """Called on any task failure — add Slack/email here later."""
    task_id = context["task_instance"].task_id
    dag_id  = context["task_instance"].dag_id
    log.error("TASK FAILED: %s in DAG %s", task_id, dag_id)
    log.error("Execution date: %s", context["execution_date"])


default_args = {
    "owner":            "bharatcommerce-de",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "on_failure_callback": on_failure,
    "email_on_failure": False,
}

# ── DAG definition ─────────────────────────────────────────────

dag = DAG(
    dag_id          = "bc_main_pipeline",
    description     = "Bharatcommerce: load Parquet → BigQuery → dbt run → dbt test",
    schedule_interval = "0 */6 * * *",   # every 6 hours
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    max_active_runs = 1,                 # never run two loads in parallel
    default_args    = default_args,
    tags            = ["bharatcommerce", "production"],
)

# ── Task functions ─────────────────────────────────────────────

def check_new_parquet_files(**context) -> bool:
    """
    ShortCircuitOperator: returns False to skip downstream tasks
    if no Parquet files exist yet. Prevents empty runs.
    """
    order_files  = list((DATA_LAKE / "orders").rglob("*.parquet"))  \
                   if (DATA_LAKE / "orders").exists() else []
    return_files = list((DATA_LAKE / "returns").rglob("*.parquet")) \
                   if (DATA_LAKE / "returns").exists() else []

    total = len(order_files) + len(return_files)
    log.info("Found %d order files + %d return files in data lake",
             len(order_files), len(return_files))

    if total == 0:
        log.warning("No Parquet files found — skipping this run")
        return False

    # Push counts to XCom for downstream tasks
    context["ti"].xcom_push("order_file_count",  len(order_files))
    context["ti"].xcom_push("return_file_count", len(return_files))
    return True


def load_topic_to_bq(topic: str, table_name: str, **context):
    """
    Load all Parquet files for a topic into BigQuery.
    Uses WRITE_TRUNCATE on first file, WRITE_APPEND on rest.
    Idempotent — safe to re-run.
    """
    import warnings
    warnings.filterwarnings("ignore")
    from google.cloud import bigquery

    folder = DATA_LAKE / topic
    files  = list(folder.rglob("*.parquet")) if folder.exists() else []

    if not files:
        log.info("No files for topic '%s' — skipping", topic)
        return {"rows_loaded": 0, "files_loaded": 0}

    client    = bigquery.Client.from_service_account_json(KEY_FILE, project=PROJECT)
    table_ref = f"{PROJECT}.{DATASET}.{table_name}"
    loaded    = 0

    log.info("Loading %d files into %s ...", len(files), table_ref)

    for i, f in enumerate(files, 1):
        disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if i == 1 else
            bigquery.WriteDisposition.WRITE_APPEND
        )
        cfg = bigquery.LoadJobConfig(
            source_format     = bigquery.SourceFormat.PARQUET,
            write_disposition = disposition,
            autodetect        = True,
        )
        with open(f, "rb") as fh:
            job = client.load_table_from_file(fh, table_ref, job_config=cfg)
            job.result()
        loaded += 1

        if loaded % 50 == 0:
            log.info("  %d/%d files loaded", loaded, len(files))

    tbl = client.get_table(table_ref)
    log.info("Done — %d rows in %s", tbl.num_rows, table_ref)

    # Push row count to XCom for monitoring
    context["ti"].xcom_push("rows_loaded",  tbl.num_rows)
    context["ti"].xcom_push("files_loaded", loaded)
    return {"rows_loaded": tbl.num_rows, "files_loaded": loaded}


def log_pipeline_summary(**context):
    """Summarise the run — useful for monitoring dashboards later."""
    ti = context["ti"]

    order_rows  = ti.xcom_pull(task_ids="load_orders_to_bq",  key="rows_loaded") or 0
    return_rows = ti.xcom_pull(task_ids="load_returns_to_bq", key="rows_loaded") or 0
    order_files = ti.xcom_pull(task_ids="check_new_data",     key="order_file_count") or 0
    return_files= ti.xcom_pull(task_ids="check_new_data",     key="return_file_count") or 0

    log.info("=" * 50)
    log.info("PIPELINE RUN COMPLETE")
    log.info("  Order files processed:  %d", order_files)
    log.info("  Return files processed: %d", return_files)
    log.info("  Orders in BigQuery:     %d", order_rows)
    log.info("  Returns in BigQuery:    %d", return_rows)
    log.info("  Execution date:         %s", context["execution_date"])
    log.info("=" * 50)


# ── Tasks ──────────────────────────────────────────────────────

with dag:

    start = EmptyOperator(task_id="start")

    check_new_data = ShortCircuitOperator(
        task_id         = "check_new_data",
        python_callable = check_new_parquet_files,
        provide_context = True,
        doc_md          = "Skip entire DAG if no Parquet files in data_lake/",
    )

    load_orders = PythonOperator(
        task_id         = "load_orders_to_bq",
        python_callable = load_topic_to_bq,
        op_kwargs       = {"topic": "orders", "table_name": "orders_raw"},
        provide_context = True,
        doc_md          = "Load data_lake/orders/ → BigQuery orders_raw (TRUNCATE + APPEND)",
    )

    load_returns = PythonOperator(
        task_id         = "load_returns_to_bq",
        python_callable = load_topic_to_bq,
        op_kwargs       = {"topic": "returns", "table_name": "returns_raw"},
        provide_context = True,
        doc_md          = "Load data_lake/returns/ → BigQuery returns_raw",
    )

    # dbt run — builds all 8 models in dependency order
    dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = f"cd {DBT_DIR} && dbt run --profiles-dir . --target dev",
        doc_md       = "Build Bronze → Silver → Gold models in BigQuery",
    )

    # dbt test — validates data quality on every column
    dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = f"cd {DBT_DIR} && dbt test --profiles-dir . --target dev",
        doc_md       = "Run 13 data quality tests — nulls, uniqueness, ranges, accepted values",
    )

    pipeline_summary = PythonOperator(
        task_id         = "pipeline_summary",
        python_callable = log_pipeline_summary,
        provide_context = True,
        trigger_rule    = TriggerRule.ALL_SUCCESS,
        doc_md          = "Log run stats — rows loaded, files processed",
    )

    end = EmptyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.ALL_DONE,
    )

    # ── DAG flow ───────────────────────────────────────────────
    #
    #  start → check_new_data → load_orders  ──┐
    #                         → load_returns ──┤→ dbt_run → dbt_test → summary → end
    #
    start >> check_new_data >> [load_orders, load_returns] >> dbt_run >> dbt_test >> pipeline_summary >> end
