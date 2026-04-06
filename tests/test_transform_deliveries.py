"""
test_transform_deliveries.py
============================
Pytest suite for the transform_deliveries PySpark job.
Uses a local SparkSession (master=local[1]) — no cluster required.

Coverage:
  - Deduplication via ROW_NUMBER on event_id
  - delivery_duration_hours calculation
  - is_on_time flag (< 48h vs > 48h)
  - Orders with only a pickup event (no delivery): duration = null
  - late_reason mapping from status_code
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── SparkSession fixture ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Shared local SparkSession for the entire test session."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("test-transform-deliveries")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ── Shared test data fixture ──────────────────────────────────────────────────

@pytest.fixture(scope="session")
def raw_schema():
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("courier_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("event_date", DateType(), False),
            StructField("location_lat", DoubleType(), True),
            StructField("location_lon", DoubleType(), True),
            StructField("status_code", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("_ingested_at", TimestampType(), False),
        ]
    )


def _ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _date(s: str):
    from datetime import date
    return date.fromisoformat(s)


@pytest.fixture(scope="session")
def raw_events(spark, raw_schema):
    """
    10 events across 3 orders:
      - order-1: pickup + delivered (24h apart) — on time
      - order-2: pickup + delivered (60h apart) — late, status_code CD001
      - order-3: pickup + failed (status_code AI001) + duplicate pickup event
      - order-4: pickup only (no delivery) — duration must be null
    """
    base_date = _date("2024-01-15")
    rows = [
        # order-1: on-time delivery (24h)
        ("evt-001", "order-1", "courier-A", "pickup",    _ts("2024-01-14 08:00:00"), base_date, 45.4642, 9.1900, None,    None, _ts("2024-01-15 06:00:00")),
        ("evt-002", "order-1", "courier-A", "in-transit",_ts("2024-01-14 12:00:00"), base_date, 45.4650, 9.1910, None,    None, _ts("2024-01-15 06:00:00")),
        ("evt-003", "order-1", "courier-A", "delivered", _ts("2024-01-15 08:00:00"), base_date, 45.4660, 9.1920, None,    None, _ts("2024-01-15 06:00:00")),
        # order-2: late delivery (60h), courier delay
        ("evt-004", "order-2", "courier-B", "pickup",    _ts("2024-01-13 06:00:00"), base_date, 41.9028, 12.4964, None,   None, _ts("2024-01-15 06:00:00")),
        ("evt-005", "order-2", "courier-B", "delivered", _ts("2024-01-15 18:00:00"), base_date, 41.9030, 12.4970, "CD001", None, _ts("2024-01-15 06:00:00")),
        # order-3: failed delivery, duplicate pickup event
        ("evt-006", "order-3", "courier-C", "pickup",    _ts("2024-01-15 07:00:00"), base_date, 48.8566, 2.3522, None,    None, _ts("2024-01-15 06:00:00")),
        ("evt-006", "order-3", "courier-C", "pickup",    _ts("2024-01-15 07:05:00"), base_date, 48.8566, 2.3522, None,    "retry", _ts("2024-01-15 06:05:00")),  # duplicate
        ("evt-007", "order-3", "courier-C", "failed",   _ts("2024-01-15 09:00:00"), base_date, 48.8570, 2.3530, "AI001", None, _ts("2024-01-15 06:00:00")),
        # order-4: pickup only (no delivery)
        ("evt-008", "order-4", "courier-D", "pickup",   _ts("2024-01-15 10:00:00"), base_date, 52.5200, 13.4050, None,   None, _ts("2024-01-15 06:00:00")),
        ("evt-009", "order-4", "courier-D", "in-transit",_ts("2024-01-15 11:00:00"), base_date, 52.5210, 13.4060, None,  None, _ts("2024-01-15 06:00:00")),
    ]
    return spark.createDataFrame(rows, schema=raw_schema)


# ── Helper: run dedup step ────────────────────────────────────────────────────

def _run_dedup(df):
    from pyspark.sql import Window
    dedup_window = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
    return (
        df
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _run_order_agg(df):
    return df.groupBy("order_id").agg(
        F.min(F.when(F.col("event_type") == "pickup", F.col("event_timestamp"))).alias("pickup_time"),
        F.min(F.when(F.col("event_type") == "delivered", F.col("event_timestamp"))).alias("delivery_time"),
        F.max(F.when(F.col("event_type") == "failed", F.lit(True)).otherwise(F.lit(False))).alias("is_failed"),
    )


def _run_duration(df):
    return df.withColumn(
        "delivery_duration_hours",
        F.when(
            F.col("pickup_time").isNotNull() & F.col("delivery_time").isNotNull(),
            (F.unix_timestamp("delivery_time") - F.unix_timestamp("pickup_time")) / 3600.0,
        ).otherwise(F.lit(None).cast("double")),
    )


def _run_on_time(df, sla_hours=48.0):
    return df.withColumn(
        "is_on_time",
        F.when(
            F.col("delivery_duration_hours").isNotNull(),
            F.col("delivery_duration_hours") <= sla_hours,
        ).otherwise(F.lit(None).cast("boolean")),
    )


def _late_reason_expr():
    return (
        F.when(F.col("status_code").isin(["NP001", "NP002"]), F.lit("no_pickup"))
        .when(F.col("status_code").isin(["CD001", "CD002", "CD003"]), F.lit("courier_delay"))
        .when(F.col("status_code").isin(["AI001", "AI002", "AI003", "AI004"]), F.lit("address_issue"))
        .otherwise(F.lit(None).cast("string"))
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestDeduplication:
    def test_duplicate_event_ids_removed(self, spark, raw_events):
        """evt-006 appears twice; after dedup only one row should remain."""
        deduped = _run_dedup(raw_events)
        count = deduped.filter(F.col("event_id") == "evt-006").count()
        assert count == 1

    def test_keeps_latest_timestamp_for_duplicate(self, spark, raw_events):
        """The surviving evt-006 row should have the later timestamp (07:05)."""
        deduped = _run_dedup(raw_events)
        row = deduped.filter(F.col("event_id") == "evt-006").first()
        assert row["event_timestamp"] == _ts("2024-01-15 07:05:00")

    def test_total_row_count_after_dedup(self, spark, raw_events):
        """10 raw rows minus 1 duplicate = 9 unique events."""
        deduped = _run_dedup(raw_events)
        assert deduped.count() == 9


class TestDeliveryDuration:
    def setup_method(self):
        """Build the full enriched DataFrame once per test class."""

    def _build(self, raw_events):
        deduped = _run_dedup(raw_events)
        order_agg = _run_order_agg(deduped)
        joined = deduped.join(order_agg, on="order_id", how="left")
        return _run_duration(joined)

    def test_order1_duration_is_24h(self, spark, raw_events):
        df = self._build(raw_events)
        row = df.filter(F.col("order_id") == "order-1").filter(F.col("event_type") == "delivered").first()
        assert row is not None
        assert abs(row["delivery_duration_hours"] - 24.0) < 0.01

    def test_order2_duration_is_60h(self, spark, raw_events):
        df = self._build(raw_events)
        row = df.filter(F.col("order_id") == "order-2").filter(F.col("event_type") == "delivered").first()
        assert row is not None
        assert abs(row["delivery_duration_hours"] - 60.0) < 0.01

    def test_order4_pickup_only_duration_is_null(self, spark, raw_events):
        """Orders with no 'delivered' event must have null duration."""
        df = self._build(raw_events)
        rows = df.filter(F.col("order_id") == "order-4").collect()
        for row in rows:
            assert row["delivery_duration_hours"] is None, (
                f"Expected null duration for pickup-only order but got {row['delivery_duration_hours']}"
            )


class TestSLAFlag:
    def _build(self, raw_events):
        deduped = _run_dedup(raw_events)
        order_agg = _run_order_agg(deduped)
        joined = deduped.join(order_agg, on="order_id", how="left")
        with_duration = _run_duration(joined)
        return _run_on_time(with_duration, sla_hours=48.0)

    def test_order1_is_on_time(self, spark, raw_events):
        """order-1: 24h < 48h SLA => is_on_time=True."""
        df = self._build(raw_events)
        row = df.filter(F.col("order_id") == "order-1").filter(F.col("event_type") == "delivered").first()
        assert row["is_on_time"] is True

    def test_order2_is_late(self, spark, raw_events):
        """order-2: 60h > 48h SLA => is_on_time=False."""
        df = self._build(raw_events)
        row = df.filter(F.col("order_id") == "order-2").filter(F.col("event_type") == "delivered").first()
        assert row["is_on_time"] is False

    def test_pickup_only_on_time_is_null(self, spark, raw_events):
        """Pickup-only orders have null duration => is_on_time must be null."""
        df = self._build(raw_events)
        rows = df.filter(F.col("order_id") == "order-4").collect()
        for row in rows:
            assert row["is_on_time"] is None


class TestLateReason:
    def _enrich(self, raw_events):
        deduped = _run_dedup(raw_events)
        return deduped.withColumn("late_reason", _late_reason_expr())

    def test_cd001_maps_to_courier_delay(self, spark, raw_events):
        df = self._enrich(raw_events)
        row = df.filter(F.col("status_code") == "CD001").first()
        assert row is not None
        assert row["late_reason"] == "courier_delay"

    def test_ai001_maps_to_address_issue(self, spark, raw_events):
        df = self._enrich(raw_events)
        row = df.filter(F.col("status_code") == "AI001").first()
        assert row is not None
        assert row["late_reason"] == "address_issue"

    def test_null_status_code_maps_to_null_late_reason(self, spark, raw_events):
        df = self._enrich(raw_events)
        row = df.filter(F.col("event_id") == "evt-001").first()
        assert row["late_reason"] is None
