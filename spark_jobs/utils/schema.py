"""
schema.py
=========
Canonical PySpark StructType schemas for the logistics delivery pipeline.
Shared across ingest, transform, and aggregate jobs to guarantee consistency.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
    DateType,
    FloatType,
)

# ── Raw layer schema ──────────────────────────────────────────────────────────
# Direct representation of the JDBC source table plus ingestion metadata.

DELIVERY_EVENT_RAW_SCHEMA = StructType(
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
        # Ingestion metadata (added during ingest job)
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
        StructField("day", IntegerType(), nullable=False),
    ]
)

# ── Curated / facts schema ────────────────────────────────────────────────────
# One row per event_id; enriched with business logic.

DELIVERY_FACT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("courier_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("location_lat", DoubleType(), nullable=True),
        StructField("location_lon", DoubleType(), nullable=True),
        StructField("status_code", StringType(), nullable=True),
        StructField("notes", StringType(), nullable=True),
        # Business columns
        StructField("pickup_time", TimestampType(), nullable=True),
        StructField("delivery_time", TimestampType(), nullable=True),
        StructField("delivery_duration_hours", DoubleType(), nullable=True),
        StructField("is_failed", BooleanType(), nullable=False),
        StructField("is_on_time", BooleanType(), nullable=True),
        StructField("late_reason", StringType(), nullable=True),
        # Metadata
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_transformed_at", TimestampType(), nullable=False),
    ]
)

# ── Aggregated layer schema ───────────────────────────────────────────────────
# One row per (courier_id, event_date); pre-computed KPIs.

DELIVERY_AGGREGATE_SCHEMA = StructType(
    [
        StructField("courier_id", StringType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("total_deliveries", LongType(), nullable=False),
        StructField("delivered_count", LongType(), nullable=False),
        StructField("failed_count", LongType(), nullable=False),
        StructField("on_time_count", LongType(), nullable=False),
        StructField("on_time_rate_pct", DoubleType(), nullable=True),
        StructField("avg_duration_hours", DoubleType(), nullable=True),
        StructField("p95_duration_hours", DoubleType(), nullable=True),
        StructField("_aggregated_at", TimestampType(), nullable=False),
    ]
)
