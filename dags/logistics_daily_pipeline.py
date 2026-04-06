"""
logistics_daily_pipeline.py
===========================
Airflow DAG that orchestrates the daily logistics delivery events pipeline.

Flow:
    check_source_availability
        -> ingest_delivery_events        (RAW layer)
        -> transform_delivery_facts      (CURATED layer, MERGE/upsert)
        -> aggregate_daily_metrics       (AGGREGATED layer)
        -> notify_completion
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from utils.spark_config import get_spark_conf

log = logging.getLogger(__name__)

# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["data-team@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
}

# ── Helper: Airflow Variables ─────────────────────────────────────────────────

def _get_azure_vars() -> dict:
    return {
        "storage_account": Variable.get("azure_storage_account"),
        "client_id": Variable.get("azure_client_id"),
        "client_secret": Variable.get("azure_client_secret"),
        "tenant_id": Variable.get("azure_tenant_id"),
    }


# ── Task: check_source_availability ──────────────────────────────────────────

def check_source_availability(**context) -> None:
    """Verify that the JDBC source is reachable before launching Spark jobs."""
    import jaydebeapi  # type: ignore  # noqa: F401  (provided by airflow-providers-jdbc)

    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("delivery_jdbc")
    log.info(
        "Checking JDBC connectivity: host=%s schema=%s",
        conn.host,
        conn.schema,
    )

    try:
        import psycopg2  # common driver; swap for your DB

        with psycopg2.connect(
            host=conn.host,
            port=conn.port or 5432,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            connect_timeout=10,
        ) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT 1")
        log.info("JDBC source is reachable.")
    except Exception as exc:
        raise RuntimeError(f"Cannot connect to JDBC source: {exc}") from exc


# ── Task: notify_completion ───────────────────────────────────────────────────

def notify_completion(**context) -> None:
    """Log a summary after all Spark jobs have completed successfully."""
    logical_date = context["ds"]
    dag_run = context["dag_run"]
    log.info(
        "Pipeline logistics_daily_pipeline completed successfully. "
        "logical_date=%s  run_id=%s",
        logical_date,
        dag_run.run_id,
    )


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="logistics_daily_pipeline",
    description="Daily delivery events pipeline: ingest → transform → aggregate (Delta Lake on ADLS)",
    schedule="0 6 * * 1-5",   # Mon-Fri at 06:00 UTC
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["logistics", "spark", "delta"],
    doc_md=__doc__,
) as dag:

    # ── 1. Availability check ─────────────────────────────────────────────────
    check_source = PythonOperator(
        task_id="check_source_availability",
        python_callable=check_source_availability,
    )

    # ── Spark conf (built at parse time; vars must exist in Airflow) ──────────
    # We defer variable resolution to runtime via a lambda so that the DAG
    # parses even when variables are not yet set in a fresh environment.
    def _spark_conf():
        az = _get_azure_vars()
        return get_spark_conf(
            storage_account=az["storage_account"],
            client_id=az["client_id"],
            client_secret=az["client_secret"],
            tenant_id=az["tenant_id"],
        )

    _SPARK_CONF = {
        # These are static defaults; ADLS auth keys are injected at runtime
        # by reading Airflow Variables inside get_spark_conf().
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    }

    _SPARK_PACKAGES = (
        "io.delta:delta-core_2.12:3.0.0,"
        "org.apache.hadoop:hadoop-azure:3.3.4,"
        "com.microsoft.azure:azure-storage:8.6.6"
    )

    # ── 2. Ingest (RAW layer) ─────────────────────────────────────────────────
    ingest = SparkSubmitOperator(
        task_id="ingest_delivery_events",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/ingest_deliveries.py",
        application_args=[
            "--date", "{{ ds }}",
            "--config", "/opt/airflow/config/pipeline_config.yaml",
        ],
        conf=_SPARK_CONF,
        packages=_SPARK_PACKAGES,
        executor_memory="4g",
        driver_memory="2g",
        executor_cores=2,
        num_executors=4,
        name="logistics-ingest-{{ ds_nodash }}",
        verbose=False,
    )

    # ── 3. Transform (CURATED layer, MERGE) ───────────────────────────────────
    transform = SparkSubmitOperator(
        task_id="transform_delivery_facts",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/transform_deliveries.py",
        application_args=[
            "--date", "{{ ds }}",
            "--config", "/opt/airflow/config/pipeline_config.yaml",
        ],
        conf=_SPARK_CONF,
        packages=_SPARK_PACKAGES,
        executor_memory="4g",
        driver_memory="2g",
        executor_cores=2,
        num_executors=4,
        name="logistics-transform-{{ ds_nodash }}",
        verbose=False,
    )

    # ── 4. Aggregate (AGGREGATED layer) ───────────────────────────────────────
    aggregate = SparkSubmitOperator(
        task_id="aggregate_daily_metrics",
        conn_id="spark_default",
        application="/opt/airflow/spark_jobs/aggregate_deliveries.py",
        application_args=[
            "--date", "{{ ds }}",
            "--config", "/opt/airflow/config/pipeline_config.yaml",
        ],
        conf=_SPARK_CONF,
        packages=_SPARK_PACKAGES,
        executor_memory="2g",
        driver_memory="1g",
        executor_cores=2,
        num_executors=2,
        name="logistics-aggregate-{{ ds_nodash }}",
        verbose=False,
    )

    # ── 5. Notify ─────────────────────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    check_source >> ingest >> transform >> aggregate >> notify
