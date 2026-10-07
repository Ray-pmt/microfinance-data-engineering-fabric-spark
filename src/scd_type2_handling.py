#!/usr/bin/env python3
"""
SCD Type 2 Handling Script for Microfinance Dimensions on Microsoft Fabric

Features:
- Processes one process_date batch at a time, using that date as the effective date;
  records the latest batch applied (even one with no changes) and refuses older batches;
  the latest batch may be re-run only with identical data (corrections need a replay)
- Reads new data once and reuses it for each dimension (improves efficiency)
- Keeps the latest version of each business key (by last_updated) from the incoming data
- Only keys present in the incoming data can be expired; keys absent from a batch stay current
- Null-safe change detection on the tracked columns
- The dimension is rewritten via a staging directory, so reading and replacing the same
  path is safe (a plain overwrite would delete the files the plan is still reading);
  an interrupted swap is recovered on the next run instead of looking like a missing table
- Errors propagate (non-zero exit) instead of being logged and skipped
"""

import sys
import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (DIMENSION_CONFIGS, latest_per_key, tracked_columns_changed, path_exists,
                    overwrite_parquet, read_batch, recover_table, batch_fingerprint, check_batch,
                    write_watermark)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

HIGH_DATE = "9999-12-31"  # Indicates a record is current
SCD2_COLUMNS = ["surrogate_key", "effective_start_date", "effective_end_date", "is_current"]


def _add_current_versions(df, business_keys, first_key, effective_date):
    """Assign surrogate keys (continuing from `first_key`) and mark rows as current versions."""
    window_spec = Window.orderBy(*business_keys)
    return df.withColumn("surrogate_key", F.row_number().over(window_spec) + F.lit(first_key)) \
             .withColumn("effective_start_date", F.lit(effective_date)) \
             .withColumn("effective_end_date", F.lit(HIGH_DATE)) \
             .withColumn("is_current", F.lit(True))


def apply_scd_type2(spark, new_data_path, dimension_path, output_path, process_date=None):
    """
    Apply SCD Type 2 processing to dimension tables.

    Args:
        spark: SparkSession
        new_data_path: Path to the transformed data (partitioned by process_date)
        dimension_path: Base path for existing dimension tables
        output_path: Path to write updated dimension tables (may be the same as dimension_path)
        process_date: Batch (YYYY-MM-DD) to apply; also stamped as the effective date on
            new/expired versions. When omitted, all batches are read and today is used.
    """
    effective_date = process_date or date.today().isoformat()
    logger.info(
        f"Starting SCD Type 2 processing: new_data_path={new_data_path}, "
        f"dimension_path={dimension_path}, process_date={process_date}, effective_date={effective_date}"
    )

    # Finish or roll back any interrupted swap first, so it isn't mistaken for a missing table.
    # This runs even when the batch turns out to be empty.
    for dim_name in DIMENSION_CONFIGS:
        recover_table(spark, f"{dimension_path}/{dim_name}")
        if output_path != dimension_path:
            recover_table(spark, f"{output_path}/{dim_name}")

    # Read and cache this batch once for reuse. An empty batch still goes through the checks
    # below: a re-run that empties an already-applied batch must be refused, not ignored.
    new_data_full = read_batch(spark, new_data_path, process_date)
    if new_data_full is None:
        logger.info(f"No transformed data for process_date={process_date}.")
    else:
        new_data_full = new_data_full.cache()
        logger.info(f"Loaded new data with {new_data_full.count()} records")

    for dim_name, config in DIMENSION_CONFIGS.items():
        logger.info(f"Processing dimension: {dim_name}")
        business_keys = config["business_keys"]
        tracked_columns = config["tracked_columns"]
        all_columns = business_keys + tracked_columns

        # Latest version of each business key in the incoming data
        new_dim_data = None
        if new_data_full is not None:
            new_dim_data = latest_per_key(new_data_full, business_keys).select(*all_columns).cache()
        fingerprint = batch_fingerprint(new_dim_data, all_columns)
        logger.info(f"{dim_name}: batch fingerprint {fingerprint} (rows:sum of row hashes)")

        dim_file_path = f"{dimension_path}/{dim_name}"
        target_path = f"{output_path}/{dim_name}"

        # Initial load: every key becomes the first current version
        if not path_exists(spark, dim_file_path):
            # An earlier empty batch can have a watermark even before a table exists.
            check_batch(spark, dim_name, target_path, effective_date, fingerprint, None,
                        batch_effects_written=lambda: False)
            write_watermark(spark, target_path, effective_date, fingerprint)
            if new_dim_data is None:
                logger.info(f"{dim_name}: No data and no dimension yet; nothing to do.")
                continue
            logger.info(f"{dim_name}: Dimension does not exist yet. Creating it.")
            new_dim = _add_current_versions(new_dim_data, business_keys, 0, effective_date)
            count = overwrite_parquet(spark, new_dim, target_path)
            logger.info(f"{dim_name}: New dimension created with {count} records")
            continue

        # Refuse to silently rebuild (and lose the history of) a table with an unexpected layout
        dim_df = spark.read.parquet(dim_file_path)
        missing_cols = [col for col in all_columns + SCD2_COLUMNS if col not in dim_df.columns]
        if missing_cols:
            raise ValueError(f"{dim_name}: existing dimension at {dim_file_path} is missing columns {missing_cols}")
        dim_df = dim_df.select(*all_columns, *SCD2_COLUMNS)

        # Batches must be applied in date order, and an applied batch can only be re-run unchanged
        latest_change_date = dim_df.agg(F.max("effective_start_date")).collect()[0][0]
        check_batch(
            spark, dim_name, dim_file_path, effective_date, fingerprint, latest_change_date,
            batch_effects_written=lambda: dim_df.filter(
                (F.col("effective_start_date") == effective_date) | (F.col("effective_end_date") == effective_date)
            ).limit(1).count() > 0,
        )

        # Record every accepted batch, including empty ones, before changing the table.
        write_watermark(spark, target_path, effective_date, fingerprint)
        if new_dim_data is None:
            logger.info(f"{dim_name}: Empty batch; dimension left as is.")
            continue

        current_records = dim_df.filter(F.col("is_current"))
        join_condition = [F.col(f"new.{key}") == F.col(f"current.{key}") for key in business_keys]

        # Changed: key present in both the batch and the current dimension, with a tracked column different.
        # Keys that are absent from this batch are not touched.
        changed_records = new_dim_data.alias("new") \
            .join(current_records.alias("current"), on=join_condition, how="inner") \
            .filter(tracked_columns_changed(tracked_columns))

        # New: key not present in the current dimension at all
        new_records = new_dim_data.join(current_records.select(*business_keys), on=business_keys, how="left_anti")

        changed_count = changed_records.count()
        new_count = new_records.count()
        logger.info(f"{dim_name}: Found {changed_count} changed records and {new_count} new records.")

        if changed_count == 0 and new_count == 0:
            logger.info(f"{dim_name}: No changes; dimension left as is.")
            continue

        # Expire the current versions of changed keys
        keys_to_expire = changed_records.select(F.col("current.surrogate_key").alias("surrogate_key"))
        expired_records = dim_df.join(keys_to_expire, on="surrogate_key", how="inner") \
                                .withColumn("effective_end_date", F.lit(effective_date)) \
                                .withColumn("is_current", F.lit(False))
        retained_records = dim_df.join(keys_to_expire, on="surrogate_key", how="left_anti")

        # New current versions: updated rows for changed keys plus rows for brand-new keys
        incoming_versions = changed_records.select(*[F.col(f"new.{col}").alias(col) for col in all_columns]) \
                                           .unionByName(new_records.select(*all_columns))
        max_key_val = dim_df.agg(F.max("surrogate_key")).collect()[0][0] or 0
        new_versions = _add_current_versions(incoming_versions, business_keys, max_key_val, effective_date)

        final_dim = retained_records.select(*all_columns, *SCD2_COLUMNS) \
                                    .unionByName(expired_records.select(*all_columns, *SCD2_COLUMNS)) \
                                    .unionByName(new_versions.select(*all_columns, *SCD2_COLUMNS))

        final_record_count = overwrite_parquet(spark, final_dim, target_path)
        logger.info(f"{dim_name}: Updated dimension written with {final_record_count} records")

    if new_data_full is not None:
        new_data_full.unpersist()
    logger.info("SCD Type 2 processing completed successfully")

def main():
    if len(sys.argv) < 4:
        logger.error("Usage: scd_type2_handling.py <new_data_path> <dimension_path> <output_path> [process_date]")
        sys.exit(1)

    new_data_path = sys.argv[1]
    dimension_path = sys.argv[2]
    output_path = sys.argv[3]
    process_date = sys.argv[4] if len(sys.argv) > 4 else None

    spark = SparkSession.builder.appName("MicrofinanceSCDType2").getOrCreate()

    try:
        apply_scd_type2(spark, new_data_path, dimension_path, output_path, process_date)
    except Exception as e:
        logger.error(f"SCD Type 2 processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
