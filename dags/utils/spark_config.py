"""
spark_config.py
===============
Build the Spark configuration dictionary for Delta Lake on Azure ADLS Gen2.
Imported by the Airflow DAG to pass conf= to SparkSubmitOperator.
"""

from __future__ import annotations


def get_spark_conf(
    storage_account: str,
    client_id: str,
    client_secret: str,
    tenant_id: str,
    executor_memory: str = "4g",
    executor_cores: int = 2,
) -> dict[str, str]:
    """
    Return a Spark configuration dict that:
    - Authenticates to Azure ADLS Gen2 via Service Principal (OAuth2).
    - Enables Delta Lake extensions.
    - Tunes executor resources.

    Parameters
    ----------
    storage_account:
        Azure Storage account name (no .dfs.core.windows.net suffix).
    client_id:
        Azure AD application (client) ID for the service principal.
    client_secret:
        Client secret for the service principal.
    tenant_id:
        Azure AD tenant ID.
    executor_memory:
        Spark executor memory string, e.g. '4g'.
    executor_cores:
        Number of cores per executor.

    Returns
    -------
    dict[str, str]
        Configuration dict suitable for SparkSubmitOperator(conf=...).
    """
    oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    return {
        # ── Azure ADLS Gen2 auth (OAuth2 service principal) ───────────────────
        f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": (
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        ),
        f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": client_id,
        f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": client_secret,
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": oauth_endpoint,

        # ── Delta Lake extensions ─────────────────────────────────────────────
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),

        # ── Delta Lake retention (allow aggressive VACUUM in dev/test) ────────
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",

        # ── Maven packages (Delta + Azure Hadoop connector) ───────────────────
        "spark.jars.packages": (
            "io.delta:delta-core_2.12:3.0.0,"
            "org.apache.hadoop:hadoop-azure:3.3.4,"
            "com.microsoft.azure:azure-storage:8.6.6"
        ),

        # ── Executor tuning ───────────────────────────────────────────────────
        "spark.executor.memory": executor_memory,
        "spark.executor.cores": str(executor_cores),
        "spark.sql.shuffle.partitions": "200",

        # ── Parquet / Delta write optimisations ───────────────────────────────
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.files.maxPartitionBytes": "134217728",  # 128 MB
    }
