"""
delta_utils.py
==============
Reusable Delta Lake helpers: upsert (MERGE), table creation, and maintenance.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from delta.tables import DeltaTable  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)


def upsert_to_delta(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    merge_keys: list[str],
) -> None:
    """
    Upsert ``df`` into a Delta table at ``target_path`` using MERGE.

    For each row in ``df``:
    - If a matching row exists in the target (matched on all ``merge_keys``),
      all columns are updated with the incoming values.
    - If no match exists, the row is inserted.

    This is idempotent: running the same ``df`` twice produces no duplicates.

    Parameters
    ----------
    spark:
        Active SparkSession.
    df:
        Source DataFrame to merge into the target.
    target_path:
        ABFS path (or local path for tests) of the Delta table.
    merge_keys:
        List of column names that uniquely identify a row (e.g. ['event_id']).
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        log.info("Target %s does not exist yet; creating via initial write.", target_path)
        df.write.format("delta").mode("overwrite").save(target_path)
        return

    delta_table = DeltaTable.forPath(spark, target_path)

    # Build the join condition from merge_keys
    merge_condition = " AND ".join(
        f"target.{key} = source.{key}" for key in merge_keys
    )
    log.info(
        "MERGE INTO %s ON (%s)  source rows=%d",
        target_path,
        merge_condition,
        df.count(),
    )

    (
        delta_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("MERGE complete.")


def get_or_create_delta_table(
    spark: SparkSession,
    schema: StructType,
    path: str,
    partition_cols: list[str] | None = None,
) -> DeltaTable:
    """
    Return the Delta table at ``path``, creating it (empty) if it does not exist.

    Parameters
    ----------
    spark:
        Active SparkSession.
    schema:
        StructType schema for the table (used only when creating).
    path:
        ABFS or local path of the Delta table.
    partition_cols:
        Optional list of column names to partition by.

    Returns
    -------
    DeltaTable
        Handle to the Delta table.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        log.info("Creating Delta table at %s (partition_cols=%s).", path, partition_cols)
        writer = spark.createDataFrame([], schema).write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)

    return DeltaTable.forPath(spark, path)


def optimize_and_vacuum(
    spark: SparkSession,
    path: str,
    retain_hours: int = 168,
) -> None:
    """
    Run OPTIMIZE (file compaction) followed by VACUUM on a Delta table.

    Parameters
    ----------
    spark:
        Active SparkSession with Delta extensions enabled.
    path:
        Path to the Delta table.
    retain_hours:
        Minimum file age in hours to retain during VACUUM (default 7 days).
        Set ``spark.databricks.delta.retentionDurationCheck.enabled=false``
        to allow values below 168 in test environments.
    """
    log.info("OPTIMIZE %s", path)
    spark.sql(f"OPTIMIZE delta.`{path}`")

    log.info("VACUUM %s RETAIN %d HOURS", path, retain_hours)
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    log.info("OPTIMIZE + VACUUM complete for %s.", path)
