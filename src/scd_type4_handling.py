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
- Reads new data once and reuses it for each dimension (improves efficiency)
- Config-driven dimensions sharing the same business keys / tracked columns as SCD Type 2
- Detailed logging and error handling throughout
- Parquet storage for consistency with the rest of the pipeline (Delta Lake works as a
  drop-in replacement for ACID appends to the history table)
"""

import sys
import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
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


def apply_scd_type4(spark, new_data_path, current_path, history_path):
    """
    Apply SCD Type 4 processing to dimension tables.

    Args:
        spark: SparkSession
        new_data_path: Path to the new transformed data (read once for efficiency)
        current_path: Base path for the *current* dimension tables (one row per key)
        history_path: Base path for the append-only *history* tables
    """
    logger.info(
        f"Starting SCD Type 4 processing: new_data_path={new_data_path}, "
        f"current_path={current_path}, history_path={history_path}"
    )
    current_date = date.today().isoformat()

    # Read and cache new data once for reuse
    try:
        new_data_full = spark.read.parquet(new_data_path).cache()
        total_new_records = new_data_full.count()
        logger.info(f"Loaded new data with {total_new_records} records")
    except Exception as e:
        logger.error(f"Error loading new data from {new_data_path}: {str(e)}")
        sys.exit(1)

    for dim_name, config in DIMENSION_CONFIGS.items():
        logger.info(f"Processing dimension: {dim_name}")
        business_keys = config["business_keys"]
        tracked_columns = config["tracked_columns"]
        all_columns = business_keys + tracked_columns

        # Filter the new data for the current dimension
        try:
            new_dim_data = new_data_full.select(*all_columns).dropDuplicates(business_keys)
            new_dim_count = new_dim_data.count()
            logger.info(f"{dim_name}: {new_dim_count} records loaded from new data")
        except Exception as e:
            logger.error(f"Error processing new data for {dim_name}: {str(e)}")
            continue

        current_table_path = f"{current_path}/{dim_name}_current"
        history_table_path = f"{history_path}/{dim_name}_history"

        # Attempt to read the existing current table
        current_exists = True
        try:
            current_df = spark.read.parquet(current_table_path)
            logger.info(f"{dim_name}: Existing current table loaded with {current_df.count()} records")
            if "record_effective_date" not in current_df.columns:
                logger.warning(
                    f"{dim_name}: Missing record_effective_date column. "
                    "Recreating current table with SCD Type 4 structure."
                )
                current_exists = False
        except Exception as e:
            logger.info(f"{dim_name}: Current table does not exist or error encountered: {str(e)}")
            current_exists = False

        # Initial load: write the current table; the history table starts empty and
        # only receives rows once versions are superseded
        if not current_exists:
            try:
                logger.info(f"{dim_name}: Creating new current table.")
                initial_current = new_dim_data.withColumn("record_effective_date", F.lit(current_date))
                initial_current.write.mode("overwrite").parquet(current_table_path)
                logger.info(f"{dim_name}: Current table created with {initial_current.count()} records")
            except Exception as e:
                logger.error(f"{dim_name}: Failed to create current table: {str(e)}")
            continue

        # Full outer join between new data and the current table on the business keys
        join_condition = [F.col(f"new.{key}") == F.col(f"current.{key}") for key in business_keys]
        joined_data = new_dim_data.alias("new").join(current_df.alias("current"), on=join_condition, how="fullouter")

        # Build change detection conditions on the tracked columns
        change_conditions = None
        for col in tracked_columns:
            cond = (
                (F.col(f"new.{col}").isNotNull() & F.col(f"current.{col}").isNotNull() & (F.col(f"new.{col}") != F.col(f"current.{col}"))) |
                (F.col(f"new.{col}").isNotNull() & F.col(f"current.{col}").isNull()) |
                (F.col(f"new.{col}").isNull() & F.col(f"current.{col}").isNotNull())
            )
            change_conditions = cond if change_conditions is None else change_conditions | cond

        # Changed records: key exists in both sides and at least one tracked column differs
        changed_records = joined_data.filter(
            (change_conditions)
            & F.col(f"new.{business_keys[0]}").isNotNull()
            & F.col(f"current.{business_keys[0]}").isNotNull()
        )

        # Entirely new records: no matching row in the current table
        new_records = joined_data.filter(F.col(f"current.{business_keys[0]}").isNull())

        changed_count = changed_records.count()
        new_count = new_records.count()
        logger.info(f"{dim_name}: Found {changed_count} changed records and {new_count} new records.")

        # Archive superseded versions into the append-only history table
        if changed_count > 0:
            try:
                archived_versions = changed_records.select(
                    *[F.col(f"current.{col}").alias(col) for col in all_columns],
                    F.col("current.record_effective_date").alias("record_effective_date")
                ).withColumn("record_end_date", F.lit(current_date)) \
                 .withColumn("archived_at", F.current_timestamp())
                archived_versions.write.mode("append").parquet(history_table_path)
                logger.info(f"{dim_name}: Archived {changed_count} superseded versions to history table")
            except Exception as e:
                logger.error(f"{dim_name}: Failed to append to history table: {str(e)}")
                continue

        # Rebuild the current table: retained rows + refreshed versions + brand-new keys
        changed_keys = changed_records.select(
            *[F.col(f"new.{key}").alias(key) for key in business_keys]
        )
        retained_records = current_df.join(changed_keys, on=business_keys, how="left_anti")

        refreshed_records = changed_records.select(
            *[F.col(f"new.{col}").alias(col) for col in all_columns]
        ).withColumn("record_effective_date", F.lit(current_date))

        brand_new_records = new_records.select(
            *[F.col(f"new.{col}").alias(col) for col in all_columns]
        ).withColumn("record_effective_date", F.lit(current_date))

        # Cache so the count below materializes the result before the overwrite —
        # the write targets the same path the plan reads from
        updated_current = retained_records.select(*all_columns, "record_effective_date") \
                                          .union(refreshed_records) \
                                          .union(brand_new_records) \
                                          .cache()

        # Write the rebuilt current table (exactly one row per business key)
        try:
            final_record_count = updated_current.count()
            updated_current.write.mode("overwrite").parquet(current_table_path)
            logger.info(f"{dim_name}: Current table written with {final_record_count} records")
        except Exception as e:
            logger.error(f"{dim_name}: Failed to write current table: {str(e)}")

    logger.info("SCD Type 4 processing completed successfully")


def main():
    if len(sys.argv) < 4:
        logger.error("Usage: scd_type4_handling.py <new_data_path> <current_path> <history_path>")
        sys.exit(1)

    new_data_path = sys.argv[1]
    current_path = sys.argv[2]
    history_path = sys.argv[3]

    spark = SparkSession.builder.appName("MicrofinanceSCDType4").getOrCreate()

    try:
        apply_scd_type4(spark, new_data_path, current_path, history_path)
    except Exception as e:
        logger.error(f"SCD Type 4 processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
