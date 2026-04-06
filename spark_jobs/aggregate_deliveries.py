"""
aggregate_deliveries.py
=======================
PySpark job — Layer 3: Aggregate curated delivery facts into daily courier KPIs.

Aggregations per (courier_id, event_date):
  - total_deliveries
  - delivered_count
  - failed_count
  - on_time_count
  - on_time_rate_pct
  - avg_duration_hours
  - p95_duration_hours (percentile_approx at 0.95)

Writes to the AGGREGATED Delta layer, overwriting the partition for event_date
(safe re-run: same date always produces the same result).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)


def build_spark_session(config: dict) -> SparkSession:
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", config["azure"]["storage_account"])
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    tenant_id = os.environ["AZURE_TENANT_ID"]
    oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    return (
        SparkSession.builder.appName("logistics-aggregate")
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
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def aggregate(date: str, config: dict, spark: SparkSession) -> int:
    """
    Compute courier-level KPIs for ``date`` and write to AGGREGATED layer.

    Returns
    -------
    int
        Number of (courier_id, event_date) rows written.
    """
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", config["azure"]["storage_account"])
    curated_path = config["paths"]["curated_layer"].format(storage_account=storage_account)
    aggregated_path = config["paths"]["aggregated_layer"].format(storage_account=storage_account)

    facts_df = (
        spark.read.format("delta").load(curated_path)
        .filter(F.col("event_date") == F.lit(date))
    )

    if facts_df.rdd.isEmpty():
        log.warning("No curated data for date=%s. Skipping aggregation.", date)
        return 0

    now = F.lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    agg_df = facts_df.groupBy("courier_id", "event_date").agg(
        F.count("*").alias("total_deliveries"),
        F.sum(F.when(F.col("event_type") == "delivered", 1).otherwise(0)).alias("delivered_count"),
        F.sum(F.when(F.col("is_failed") == True, 1).otherwise(0)).alias("failed_count"),  # noqa: E712
        F.sum(F.when(F.col("is_on_time") == True, 1).otherwise(0)).alias("on_time_count"),  # noqa: E712
        F.round(
            F.mean(F.when(F.col("is_on_time").isNotNull(), F.col("is_on_time").cast("int"))) * 100.0,
            2,
        ).alias("on_time_rate_pct"),
        F.round(F.avg("delivery_duration_hours"), 2).alias("avg_duration_hours"),
        F.round(
            F.percentile_approx("delivery_duration_hours", 0.95),
            2,
        ).alias("p95_duration_hours"),
    ).withColumn("_aggregated_at", now)

    row_count = agg_df.count()
    log.info("Aggregated %d courier-day rows for date=%s", row_count, date)

    (
        agg_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .save(aggregated_path)
    )

    log.info("Written aggregated metrics to: %s", aggregated_path)
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate delivery facts into daily courier KPIs")
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
        aggregate(args.date, config, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
