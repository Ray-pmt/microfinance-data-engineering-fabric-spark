#!/usr/bin/env python3
"""
SCD Type 4 Handling Script for Microfinance Dimensions on Microsoft Fabric

Implements the SCD Type 4 pattern (current table + separate history table):
- The *current* table holds exactly one row per business key with the latest attributes,
  so dashboard/serving queries stay small and fast.
- The *history* table is append-only: every superseded version is archived there with
  the date range it was valid for, preserving a full audit trail.

Complements scd_type2_handling.py (which versions rows inside a single table using
effective dates and is_current flags) so the pipeline demonstrates both patterns.

Features:
- Processes one process_date batch at a time, using that date as the effective date;
  records the latest batch applied (even one with no changes) and refuses older batches;
  a re-run of the latest batch is allowed only if it changes nothing (corrections need a replay)
- Reads new data once and reuses it for each dimension (improves efficiency)
- Config-driven dimensions sharing the same business keys / tracked columns as SCD Type 2
- Keeps the latest version of each business key (by last_updated) from the incoming data
- Null-safe change detection; keys absent from a batch stay in the current table
- The current table is rewritten via a staging directory, so reading and replacing the
  same path is safe; an interrupted swap is recovered on the next run
- History appends skip versions already archived, so retrying after a failed current-table
  write does not duplicate history
- Errors propagate (non-zero exit) instead of being logged and skipped
"""

import sys
import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import (DIMENSION_CONFIGS, latest_per_key, tracked_columns_changed, path_exists,
                    overwrite_parquet, read_batch, recover_table, check_batch_order, check_rerun_unchanged,
                    write_watermark)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def apply_scd_type4(spark, new_data_path, current_path, history_path, process_date=None):
    """
    Apply SCD Type 4 processing to dimension tables.

    Args:
        spark: SparkSession
        new_data_path: Path to the transformed data (partitioned by process_date)
        current_path: Base path for the *current* dimension tables (one row per key)
        history_path: Base path for the append-only *history* tables
        process_date: Batch (YYYY-MM-DD) to apply; also stamped as the effective date on
            new/archived versions. When omitted, all batches are read and today is used.
    """
    effective_date = process_date or date.today().isoformat()
    logger.info(
        f"Starting SCD Type 4 processing: new_data_path={new_data_path}, current_path={current_path}, "
        f"history_path={history_path}, process_date={process_date}, effective_date={effective_date}"
    )

    # Finish or roll back any interrupted swap first, so it isn't mistaken for a missing table.
    # This runs even when the batch turns out to be empty.
    for dim_name in DIMENSION_CONFIGS:
        recover_table(spark, f"{current_path}/{dim_name}_current")

    # Read and cache this batch once for reuse
    new_data_full = read_batch(spark, new_data_path, process_date)
    if new_data_full is None:
        logger.info(f"No transformed data for process_date={process_date}; tables left as is.")
        return
    new_data_full = new_data_full.cache()
    logger.info(f"Loaded new data with {new_data_full.count()} records")

    for dim_name, config in DIMENSION_CONFIGS.items():
        logger.info(f"Processing dimension: {dim_name}")
        business_keys = config["business_keys"]
        tracked_columns = config["tracked_columns"]
        all_columns = business_keys + tracked_columns

        # Latest version of each business key in the incoming data
        new_dim_data = latest_per_key(new_data_full, business_keys).select(*all_columns)
        logger.info(f"{dim_name}: {new_dim_data.count()} records loaded from new data")

        current_table_path = f"{current_path}/{dim_name}_current"
        history_table_path = f"{history_path}/{dim_name}_history"

        # Initial load: write the current table; the history table starts empty and
        # only receives rows once versions are superseded
        if not path_exists(spark, current_table_path):
            logger.info(f"{dim_name}: Current table does not exist yet. Creating it.")
            initial_current = new_dim_data.withColumn("record_effective_date", F.lit(effective_date))
            count = overwrite_parquet(spark, initial_current, current_table_path)
            write_watermark(spark, current_table_path, effective_date)
            logger.info(f"{dim_name}: Current table created with {count} records")
            continue

        # Refuse to silently rebuild (and lose) a table with an unexpected layout
        current_df = spark.read.parquet(current_table_path)
        missing_cols = [col for col in all_columns + ["record_effective_date"] if col not in current_df.columns]
        if missing_cols:
            raise ValueError(f"{dim_name}: existing current table at {current_table_path} is missing columns {missing_cols}")
        current_df = current_df.select(*all_columns, "record_effective_date")

        # Batches must be applied in date order
        latest_change_date = current_df.agg(F.max("record_effective_date")).collect()[0][0]
        cutoff = check_batch_order(spark, dim_name, current_table_path, effective_date, latest_change_date)

        join_condition = [F.col(f"new.{key}") == F.col(f"current.{key}") for key in business_keys]

        # Changed: key present in both the batch and the current table, with a tracked column different
        changed_records = new_dim_data.alias("new") \
            .join(current_df.alias("current"), on=join_condition, how="inner") \
            .filter(tracked_columns_changed(tracked_columns))

        # Entirely new records: no matching row in the current table
        new_records = new_dim_data.join(current_df.select(*business_keys), on=business_keys, how="left_anti")

        changed_count = changed_records.count()
        new_count = new_records.count()
        logger.info(f"{dim_name}: Found {changed_count} changed records and {new_count} new records.")
        check_rerun_unchanged(dim_name, effective_date, cutoff, changed_count + new_count)

        if changed_count == 0 and new_count == 0:
            logger.info(f"{dim_name}: No changes; tables left as is.")
            write_watermark(spark, current_table_path, effective_date)
            continue

        # Archive superseded versions into the append-only history table. History is appended
        # before the current table is replaced; if that replacement fails, the retry sees the same
        # changes again, so versions already in history (same key, values and date range) are skipped.
        if changed_count > 0:
            version_columns = all_columns + ["record_effective_date", "record_end_date"]
            archived_versions = changed_records.select(
                *[F.col(f"current.{col}").alias(col) for col in all_columns],
                F.col("current.record_effective_date").alias("record_effective_date")
            ).withColumn("record_end_date", F.lit(effective_date))
            if path_exists(spark, history_table_path):
                already_archived = spark.read.parquet(history_table_path).select(*version_columns)
                archived_versions = archived_versions.subtract(already_archived)
            archived_versions = archived_versions.withColumn("archived_at", F.current_timestamp()).cache()
            archived_count = archived_versions.count()
            if archived_count > 0:
                archived_versions.write.mode("append").parquet(history_table_path)
            archived_versions.unpersist()
            logger.info(f"{dim_name}: Archived {archived_count} superseded versions to history table "
                        f"({changed_count - archived_count} already present)")

        # Rebuild the current table: retained rows + refreshed versions + brand-new keys
        changed_keys = changed_records.select(*[F.col(f"new.{key}").alias(key) for key in business_keys])
        retained_records = current_df.join(changed_keys, on=business_keys, how="left_anti")

        refreshed_records = changed_records.select(
            *[F.col(f"new.{col}").alias(col) for col in all_columns]
        ).withColumn("record_effective_date", F.lit(effective_date))

        brand_new_records = new_records.select(*all_columns) \
                                       .withColumn("record_effective_date", F.lit(effective_date))

        updated_current = retained_records.select(*all_columns, "record_effective_date") \
                                          .unionByName(refreshed_records) \
                                          .unionByName(brand_new_records)

        # Write the rebuilt current table (exactly one row per business key)
        final_record_count = overwrite_parquet(spark, updated_current, current_table_path)
        write_watermark(spark, current_table_path, effective_date)
        logger.info(f"{dim_name}: Current table written with {final_record_count} records")

    new_data_full.unpersist()
    logger.info("SCD Type 4 processing completed successfully")


def main():
    if len(sys.argv) < 4:
        logger.error("Usage: scd_type4_handling.py <new_data_path> <current_path> <history_path> [process_date]")
        sys.exit(1)

    new_data_path = sys.argv[1]
    current_path = sys.argv[2]
    history_path = sys.argv[3]
    process_date = sys.argv[4] if len(sys.argv) > 4 else None

    spark = SparkSession.builder.appName("MicrofinanceSCDType4").getOrCreate()

    try:
        apply_scd_type4(spark, new_data_path, current_path, history_path, process_date)
    except Exception as e:
        logger.error(f"SCD Type 4 processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
