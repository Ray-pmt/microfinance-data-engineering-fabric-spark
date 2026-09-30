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


def delete_path(spark: SparkSession, path: str):
    """Recursively delete `path` if it exists."""
    jvm_path, fs = _hadoop_path(spark, path)
    if fs.exists(jvm_path) and not fs.delete(jvm_path, True):
        raise IOError(f"Failed to delete {path}")


def partition_path(path: str, process_date: str) -> str:
    """Directory holding one process_date batch of a partitioned table."""
    return f"{path.rstrip('/')}/process_date={process_date}"


def write_batch_partition(spark: SparkSession, df: DataFrame, row_count: int, path: str, process_date: str):
    """
    Replace the `process_date` partition of the table at `path` with the rows of `df`.

    Dynamic partition overwrite only replaces partitions that receive rows, so when a re-run
    produces no rows the old partition is deleted explicitly. The table directory is always
    left in place, so downstream steps can tell an empty batch from a misconfigured path.
    """
    if row_count > 0:
        df.write.mode("overwrite") \
          .option("partitionOverwriteMode", "dynamic") \
          .partitionBy("process_date") \
          .parquet(path)
        return

    delete_path(spark, partition_path(path, process_date))
    base, fs = _hadoop_path(spark, path)
    fs.mkdirs(base)


def read_batch(spark: SparkSession, path: str, process_date: str = None):
    """
    Read a process_date-partitioned table: one batch when `process_date` is given, else all of it.
    Returns None when the requested batch does not exist (an empty batch). A missing table
    directory is an error, since that points at a wrong path rather than an empty batch.
    """
    if not path_exists(spark, path):
        raise FileNotFoundError(f"Input path does not exist: {path}")
    if process_date is None:
        return spark.read.parquet(path)
    batch_path = partition_path(path, process_date)
    if not path_exists(spark, batch_path):
        return None
    return spark.read.option("basePath", path).parquet(batch_path)


def _swap_paths(path: str):
    base = path.rstrip("/")
    return base + "__staging", base + "__previous"


def _rename(fs, src, dst):
    if not fs.rename(src, dst):
        raise IOError(f"Failed to move {src} to {dst}")


def recover_table(spark: SparkSession, path: str):
    """
    Repair the table at `path` after an interrupted `overwrite_parquet` swap.

    - Table missing, previous version present: the swap stopped midway -> restore the previous version.
    - Table and previous version both present: the swap finished -> drop the previous version.
    - Leftover staging data is always discarded; the next write recomputes it.

    Call this before deciding whether a table exists, so an interrupted swap is never
    mistaken for "no table yet" (which would trigger an initial load and discard history).
    """
    staging_path, previous_path = _swap_paths(path)
    target, fs = _hadoop_path(spark, path)
    previous, _ = _hadoop_path(spark, previous_path)
    staging, _ = _hadoop_path(spark, staging_path)

    if fs.exists(previous):
        if fs.exists(target):
            fs.delete(previous, True)
        else:
            logger.warning(f"Restoring {path} from {previous_path} after an interrupted swap")
            _rename(fs, previous, target)
    if fs.exists(staging):
        fs.delete(staging, True)


def overwrite_parquet(spark: SparkSession, df: DataFrame, path: str) -> int:
    """
    Replace the Parquet table at `path` with `df`, even when `df` is derived from `path`.

    Spark reads lazily, so `df.write.mode("overwrite").parquet(path)` on a plan that reads
    `path` deletes the source files before they are read. Instead:
      1. the result is fully written to `<path>__staging`
      2. the current table is moved aside to `<path>__previous`
      3. staging is moved into place
      4. the previous version is deleted
    The previous version is kept until the replacement is in place, and `recover_table`
    restores it if the process stops between steps 2 and 3.

    Returns the number of rows in the new table.
    """
    recover_table(spark, path)
    staging_path, previous_path = _swap_paths(path)
    df.write.mode("overwrite").parquet(staging_path)

    target, fs = _hadoop_path(spark, path)
    staging, _ = _hadoop_path(spark, staging_path)
    previous, _ = _hadoop_path(spark, previous_path)

    if fs.exists(target):
        _rename(fs, target, previous)
    try:
        _rename(fs, staging, target)
    except Exception:
        if fs.exists(previous) and not fs.exists(target):
            _rename(fs, previous, target)
        raise
    if fs.exists(previous):
        fs.delete(previous, True)

    return spark.read.parquet(path).count()


def _watermark_path(table_path: str) -> str:
    return table_path.rstrip("/") + "__watermark"


def read_watermark(spark: SparkSession, table_path: str):
    """Latest batch date applied to the table at `table_path`, or None if none is recorded."""
    path = _watermark_path(table_path)
    if not path_exists(spark, path):
        return None
    rows = spark.read.schema("last_processed_date string").json(path).collect()
    return max((r["last_processed_date"] for r in rows if r["last_processed_date"]), default=None)


def write_watermark(spark: SparkSession, table_path: str, process_date: str):
    """Record `process_date` as the latest batch applied to the table, whether or not it changed anything."""
    spark.createDataFrame([(process_date,)], "last_processed_date string") \
         .coalesce(1).write.mode("overwrite").json(_watermark_path(table_path))


def check_batch_order(spark: SparkSession, table_name: str, table_path: str,
                      effective_date: str, latest_change_date: str = None):
    """
    Refuse to apply a batch older than one the table already reflects.

    Batches must be applied in date order: an older batch would be recorded as a change on top
    of newer ones, with an earlier effective date. The cutoff is the latest batch applied
    (the watermark, which also covers batches that changed nothing) or, should the watermark
    lag behind (a crash between writing the table and the watermark), the latest change date.
    Re-applying the latest batch date is allowed.
    """
    cutoff = max(filter(None, [read_watermark(spark, table_path), latest_change_date]), default=None)
    if cutoff and effective_date < cutoff:
        raise ValueError(
            f"{table_name}: batches up to {cutoff} have already been applied; applying the "
            f"{effective_date} batch now would corrupt its history. Rebuild the table by "
            f"replaying batches in date order instead."
        )
