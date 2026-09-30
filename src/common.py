"""
common.py
Helpers shared by the pipeline steps:
- Dimension configurations used by both SCD strategies
- Latest-version-per-key deduplication
- Null-safe change detection between incoming and current dimension rows
- Filesystem helpers for safely replacing a Parquet table that is also being read
"""

import logging
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# Dimension configurations with business keys and columns to track
DIMENSION_CONFIGS = {
    "dim_customer": {
        "business_keys": ["customer_id"],
        "tracked_columns": ["customer_name", "customer_dob", "employment_status", "annual_income"]
    },
    "dim_loan": {
        "business_keys": ["loan_id"],
        "tracked_columns": ["status", "interest_rate", "term_months", "loan_category"]
    }
}


def latest_per_key(df: DataFrame, business_keys, order_column: str = "last_updated") -> DataFrame:
    """
    Keep one row per business key: the most recent by `order_column`.
    Ties (and rows with a null `order_column`) fall back to the most recently ingested row
    when `ingestion_timestamp` is present, so the choice is deterministic.
    """
    if order_column not in df.columns:
        logger.warning(f"Column {order_column} not found; falling back to an arbitrary row per key.")
        return df.dropDuplicates(business_keys)

    ordering = [F.col(order_column).desc_nulls_last()]
    if "ingestion_timestamp" in df.columns:
        ordering.append(F.col("ingestion_timestamp").desc_nulls_last())

    window_spec = Window.partitionBy(*business_keys).orderBy(*ordering)
    return df.withColumn("_row_num", F.row_number().over(window_spec)) \
             .filter(F.col("_row_num") == 1) \
             .drop("_row_num")


def tracked_columns_changed(tracked_columns, left: str = "new", right: str = "current"):
    """Null-safe condition that is true when any tracked column differs between two aliases."""
    return reduce(
        lambda a, b: a | b,
        [~F.col(f"{left}.{col}").eqNullSafe(F.col(f"{right}.{col}")) for col in tracked_columns]
    )


def _hadoop_path(spark: SparkSession, path: str):
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jvm_path, jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())


def path_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a path exists (works for local, ABFS and OneLake paths)."""
    jvm_path, fs = _hadoop_path(spark, path)
    return fs.exists(jvm_path)


def overwrite_parquet(spark: SparkSession, df: DataFrame, path: str) -> int:
    """
    Replace the Parquet table at `path` with `df`, even when `df` is derived from `path`.

    Spark reads lazily, so `df.write.mode("overwrite").parquet(path)` on a plan that reads
    `path` deletes the source files before they are read. Instead, the result is fully
    written to a staging directory first and then swapped in with a rename. If the job dies
    between the delete and the rename, the complete new table is still in `<path>__staging`.

    Returns the number of rows in the new table.
    """
    staging_path = path.rstrip("/") + "__staging"
    df.write.mode("overwrite").parquet(staging_path)

    target, fs = _hadoop_path(spark, path)
    staging, _ = _hadoop_path(spark, staging_path)
    if fs.exists(target) and not fs.delete(target, True):
        raise IOError(f"Failed to delete {path} before replacing it")
    if not fs.rename(staging, target):
        raise IOError(f"Failed to move {staging_path} to {path}")

    return spark.read.parquet(path).count()
