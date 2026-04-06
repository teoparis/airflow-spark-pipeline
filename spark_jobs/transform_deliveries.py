"""
transform_deliveries.py
=======================
PySpark job — Layer 2: Transform raw delivery events into curated facts.

Steps:
  1. Read RAW Delta for the given date.
  2. Deduplicate on event_id using ROW_NUMBER (keep latest event_timestamp).
  3. For each order_id compute: pickup_time, delivery_time, is_failed.
  4. Compute delivery_duration_hours and is_on_time (SLA <= 48h).
  5. Derive late_reason from status_code.
  6. MERGE result into curated Delta layer on event_id (idempotent upsert).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))
from utils.delta_utils import upsert_to_delta  # noqa: E402

log = logging.getLogger(__name__)

_SLA_DEFAULT_HOURS = 48.0


def build_spark_session(config: dict) -> SparkSession:
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", config["azure"]["storage_account"])
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    tenant_id = os.environ["AZURE_TENANT_ID"]
    oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    return (
        SparkSession.builder.appName("logistics-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        .config(
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
        .config(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
        .config(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", oauth_endpoint)
        .getOrCreate()
    )


def _derive_late_reason(config: dict) -> F.Column:
    """Map status_code to a human-readable late_reason using config rules."""
    late_conf = config.get("pipeline", {}).get("late_reasons", {})
    no_pickup_codes = late_conf.get("no_pickup", [])
    courier_delay_codes = late_conf.get("courier_delay", [])
    address_issue_codes = late_conf.get("address_issue", [])

    return (
        F.when(F.col("status_code").isin(no_pickup_codes), F.lit("no_pickup"))
        .when(F.col("status_code").isin(courier_delay_codes), F.lit("courier_delay"))
        .when(F.col("status_code").isin(address_issue_codes), F.lit("address_issue"))
        .otherwise(F.lit(None).cast("string"))
    )


def transform(date: str, config: dict, spark: SparkSession) -> int:
    """
    Read raw events for ``date``, apply business logic, and MERGE into curated layer.

    Returns
    -------
    int
        Number of rows upserted.
    """
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", config["azure"]["storage_account"])
    raw_path = config["paths"]["raw_layer"].format(storage_account=storage_account)
    curated_path = config["paths"]["curated_layer"].format(storage_account=storage_account)
    sla_hours = float(config.get("pipeline", {}).get("sla_hours", _SLA_DEFAULT_HOURS))

    # ── Read RAW for the given date ───────────────────────────────────────────
    raw_df = (
        spark.read.format("delta").load(raw_path)
        .filter(F.col("event_date") == F.lit(date))
    )

    if raw_df.rdd.isEmpty():
        log.warning("No raw data found for date=%s. Skipping transform.", date)
        return 0

    # ── Step 1: Deduplicate on event_id, keep row with latest event_timestamp ─
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
    deduped_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Step 2: Per-order aggregations ───────────────────────────────────────
    order_agg = deduped_df.groupBy("order_id").agg(
        F.min(F.when(F.col("event_type") == "pickup", F.col("event_timestamp"))).alias("pickup_time"),
        F.min(F.when(F.col("event_type") == "delivered", F.col("event_timestamp"))).alias("delivery_time"),
        F.max(F.when(F.col("event_type") == "failed", F.lit(True)).otherwise(F.lit(False))).alias("is_failed"),
    )

    enriched_df = deduped_df.join(order_agg, on="order_id", how="left")

    # ── Step 3: Duration and SLA ──────────────────────────────────────────────
    duration_expr = (
        F.when(
            F.col("pickup_time").isNotNull() & F.col("delivery_time").isNotNull(),
            (F.unix_timestamp("delivery_time") - F.unix_timestamp("pickup_time")) / 3600.0,
        )
        .otherwise(F.lit(None).cast("double"))
    )

    on_time_expr = (
        F.when(
            F.col("delivery_duration_hours").isNotNull(),
            F.col("delivery_duration_hours") <= sla_hours,
        )
        .otherwise(F.lit(None).cast("boolean"))
    )

    # ── Step 4: Late reason ───────────────────────────────────────────────────
    late_reason_expr = (
        F.when(
            (F.col("is_on_time") == False) | (F.col("is_failed") == True),  # noqa: E712
            _derive_late_reason(config),
        )
        .otherwise(F.lit(None).cast("string"))
    )

    now = F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    facts_df = (
        enriched_df
        .withColumn("delivery_duration_hours", duration_expr)
        .withColumn("is_on_time", on_time_expr)
        .withColumn("late_reason", late_reason_expr)
        .withColumn("_transformed_at", now)
        .select(
            "event_id", "order_id", "courier_id", "event_type", "event_timestamp",
            "event_date", "location_lat", "location_lon", "status_code", "notes",
            "pickup_time", "delivery_time", "delivery_duration_hours",
            "is_failed", "is_on_time", "late_reason",
            "_ingested_at", "_transformed_at",
        )
    )

    row_count = facts_df.count()
    log.info("Upserting %d rows into curated layer: %s", row_count, curated_path)

    upsert_to_delta(spark, facts_df, curated_path, merge_keys=["event_id"])

    log.info("Transform complete. date=%s rows=%d", date, row_count)
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform delivery events into curated Delta facts")
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
        transform(args.date, config, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
