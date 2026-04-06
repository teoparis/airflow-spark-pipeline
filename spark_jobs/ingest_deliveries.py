"""
ingest_deliveries.py
====================
PySpark job — Layer 1: Ingest delivery events from JDBC into the RAW Delta layer.

Run via SparkSubmitOperator or directly:
    spark-submit ingest_deliveries.py --date 2024-01-15 --config config/pipeline_config.yaml
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Allow import of shared utils when running via spark-submit
sys.path.insert(0, os.path.dirname(__file__))
from utils.schema import DELIVERY_EVENT_RAW_SCHEMA  # noqa: E402

log = logging.getLogger(__name__)

# ── Source schema (JDBC side — no ingestion metadata columns) ─────────────────
_SOURCE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("courier_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("location_lat", DoubleType(), nullable=True),
        StructField("location_lon", DoubleType(), nullable=True),
        StructField("status_code", StringType(), nullable=True),
        StructField("notes", StringType(), nullable=True),
    ]
)


def build_spark_session(config: dict) -> SparkSession:
    """Create or reuse a SparkSession with Delta Lake + ADLS extensions."""
    storage_account = os.environ.get(
        "AZURE_STORAGE_ACCOUNT", config["azure"]["storage_account"]
    )
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    tenant_id = os.environ["AZURE_TENANT_ID"]
    oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    builder = (
        SparkSession.builder.appName("logistics-ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
            "OAuth",
        )
        .config(
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
            client_id,
        )
        .config(
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
            client_secret,
        )
        .config(
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
            oauth_endpoint,
        )
        .config(
            "spark.executor.memory",
            config["spark"].get("executor_memory", "4g"),
        )
        .config(
            "spark.executor.cores",
            str(config["spark"].get("executor_cores", 2)),
        )
    )
    return builder.getOrCreate()


def get_watermark(spark: SparkSession, watermark_path: str, default_lookback_days: int) -> str:
    """Read the last successful watermark; fall back to (today - lookback_days)."""
    try:
        wm_df = spark.read.format("delta").load(watermark_path)
        row = wm_df.filter(F.col("job") == "ingest_delivery_events").orderBy(
            F.col("watermark_ts").desc()
        ).first()
        if row:
            ts = row["watermark_ts"]
            log.info("Watermark found: %s", ts)
            return str(ts)
    except Exception:
        pass

    fallback = datetime.utcnow() - timedelta(days=default_lookback_days)
    fallback_str = fallback.strftime("%Y-%m-%d %H:%M:%S")
    log.info("No watermark found; using fallback: %s", fallback_str)
    return fallback_str


def update_watermark(spark: SparkSession, watermark_path: str, run_date: str) -> None:
    """Persist the current run timestamp as the new watermark."""
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("job", StringType(), nullable=False),
            StructField("watermark_ts", TimestampType(), nullable=False),
            StructField("run_date", StringType(), nullable=False),
        ]
    )
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    row = [("ingest_delivery_events", now, run_date)]
    wm_df = spark.createDataFrame(row, schema=["job", "watermark_ts", "run_date"])

    wm_df.write.format("delta").mode("append").save(watermark_path)
    log.info("Watermark updated to %s", now)


def ingest(date: str, config: dict, spark: SparkSession) -> int:
    """
    Read delivery events from JDBC (filtered by watermark) and append to RAW Delta.

    Returns
    -------
    int
        Number of rows written.
    """
    jdbc_conf = config["jdbc"]
    pipeline_conf = config["pipeline"]
    paths = config["paths"]
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", config["azure"]["storage_account"])

    raw_path = paths["raw_layer"].format(storage_account=storage_account)
    watermark_path = paths["watermark_path"].format(storage_account=storage_account)

    watermark_ts = get_watermark(spark, watermark_path, pipeline_conf["lookback_days"])

    jdbc_url = os.environ.get("JDBC_CONN_STRING", "jdbc:postgresql://localhost:5432/deliveries")
    jdbc_user = os.environ.get("JDBC_USER", "readonly_user")
    jdbc_password = os.environ.get("JDBC_PASSWORD", "")

    query = (
        f"(SELECT * FROM {jdbc_conf['source_table']} "
        f"WHERE event_timestamp > '{watermark_ts}') AS t"
    )

    log.info("Reading from JDBC: query=%s", query)

    raw_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("fetchsize", jdbc_conf.get("fetch_size", 10000))
        .option("numPartitions", jdbc_conf.get("num_partitions", 8))
        .option("partitionColumn", jdbc_conf.get("partition_column", "event_id"))
        .schema(_SOURCE_SCHEMA)
        .load()
    )

    now = F.current_timestamp()

    enriched_df = (
        raw_df
        .withColumn("_ingested_at", now)
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("year", F.year("event_timestamp"))
        .withColumn("month", F.month("event_timestamp"))
        .withColumn("day", F.dayofmonth("event_timestamp"))
    )

    row_count = enriched_df.count()
    log.info("Rows fetched from JDBC: %d", row_count)

    if row_count == 0:
        log.warning("No new rows for date=%s (watermark=%s). Skipping write.", date, watermark_ts)
        return 0

    (
        enriched_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month", "day", "courier_id")
        .save(raw_path)
    )

    log.info("Written %d rows to RAW layer: %s", row_count, raw_path)

    update_watermark(spark, watermark_path, date)

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest delivery events into Delta Lake RAW layer")
    parser.add_argument("--date", required=True, help="Logical date (YYYY-MM-DD)")
    parser.add_argument("--config", required=True, help="Path to pipeline_config.yaml")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    with open(args.config) as fh:
        config = yaml.safe_load(fh)

    spark = build_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    try:
        rows_written = ingest(args.date, config, spark)
        log.info("Ingest complete. date=%s rows_written=%d", args.date, rows_written)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
