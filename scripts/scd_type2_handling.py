#!/usr/bin/env python3
"""
Refined SCD Type 2 Handling Script for Microfinance Dimensions on Microsoft Fabric Databricks

Features:
- Reads new data once and reuses it for each dimension (improves efficiency)
- Uses union (instead of deprecated unionAll) to combine DataFrames
- Improved surrogate key assignment with proper handling when no existing data is present
- Detailed logging and error handling throughout
- Designed with flexibility in mind for further improvements (e.g., Delta Lake transactional writes)
"""

import sys
import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

def apply_scd_type2(spark, new_data_path, dimension_path, output_path):
    """
    Apply SCD Type 2 processing to dimension tables.

    Args:
        spark: SparkSession
        new_data_path: Path to the new transformed data (read once for efficiency)
        dimension_path: Base path for existing dimension tables
        output_path: Path to write updated dimension tables (consider Delta Lake for ACID transactions)
    """
    logger.info(f"Starting SCD Type 2 processing: new_data_path={new_data_path}, dimension_path={dimension_path}")
    current_date = date.today().isoformat()
    high_date = "9999-12-31"  # Indicates a record is current

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

        # Filter the new data for the current dimension
        try:
            if dim_name == "dim_customer":
                new_dim_data = new_data_full.select(
                    "customer_id", "customer_name", "customer_dob", "employment_status", "annual_income"
                ).dropDuplicates(business_keys)
            elif dim_name == "dim_loan":
                new_dim_data = new_data_full.select(
                    "loan_id", "status", "interest_rate", "term_months", "loan_category"
                ).dropDuplicates(business_keys)
            else:
                logger.warning(f"Unknown dimension {dim_name} encountered. Skipping.")
                continue

            new_dim_count = new_dim_data.count()
            logger.info(f"{dim_name}: {new_dim_count} records loaded from new data")
        except Exception as e:
            logger.error(f"Error processing new data for {dim_name}: {str(e)}")
            continue

        # Build full path for the dimension table and attempt to read existing data
        dim_file_path = f"{dimension_path}/{dim_name}"
        dim_exists = True
        try:
            dim_df = spark.read.parquet(dim_file_path)
            logger.info(f"{dim_name}: Existing dimension loaded with {dim_df.count()} records")
            # Verify that required SCD Type 2 columns exist
            required_columns = ["surrogate_key", "effective_start_date", "effective_end_date", "is_current"]
            missing_cols = [col for col in required_columns if col not in dim_df.columns]
            if missing_cols:
                logger.warning(f"{dim_name}: Missing SCD columns {missing_cols}. Recreating dimension with SCD Type 2 structure.")
                dim_exists = False
        except Exception as e:
            logger.info(f"{dim_name}: Dimension does not exist or error encountered: {str(e)}")
            dim_exists = False

        # If the dimension doesn't exist (or is missing SCD columns), create a new one from new data
        if not dim_exists:
            try:
                logger.info(f"{dim_name}: Creating new dimension.")
                window_spec = Window.orderBy(*business_keys)
                new_dim = new_dim_data.withColumn("surrogate_key", F.row_number().over(window_spec)) \
                                      .withColumn("effective_start_date", F.lit(current_date)) \
                                      .withColumn("effective_end_date", F.lit(high_date)) \
                                      .withColumn("is_current", F.lit(True))
                new_dim.write.mode("overwrite").parquet(f"{output_path}/{dim_name}")
                logger.info(f"{dim_name}: New dimension created with {new_dim.count()} records")
            except Exception as e:
                logger.error(f"{dim_name}: Failed to create new dimension: {str(e)}")
            continue

        # For existing dimensions, select only the active (current) records
        current_records = dim_df.filter(F.col("is_current") == True)
        logger.info(f"{dim_name}: {current_records.count()} current records in dimension.")

        # Perform a full outer join on the business keys between new data and current dimension records
        join_condition = [F.col(f"new.{key}") == F.col(f"current.{key}") for key in business_keys]
        joined_data = new_dim_data.alias("new").join(current_records.alias("current"), on=join_condition, how="fullouter")

        # Build change detection conditions on the tracked columns
        change_conditions = None
        for col in tracked_columns:
            cond = (
                (F.col(f"new.{col}").isNotNull() & F.col(f"current.{col}").isNotNull() & (F.col(f"new.{col}") != F.col(f"current.{col}"))) |
                (F.col(f"new.{col}").isNotNull() & F.col(f"current.{col}").isNull()) |
                (F.col(f"new.{col}").isNull() & F.col(f"current.{col}").isNotNull())
            )
            change_conditions = cond if change_conditions is None else change_conditions | cond

        # Identify records that have changes (only if there is a corresponding current record)
        changed_records = joined_data.filter((change_conditions) & (F.col(f"current.{business_keys[0]}").isNotNull()))

        # Identify entirely new records with no matching current record
        new_records = joined_data.filter(F.col(f"current.{business_keys[0]}").isNull())

        changed_count = changed_records.count()
        new_count = new_records.count()
        logger.info(f"{dim_name}: Found {changed_count} changed records and {new_count} new records.")

        # Process changed records:
        if changed_count > 0:
            # Select surrogate keys from current records that need to be expired
            keys_to_expire = changed_records.select(F.col("current.surrogate_key").alias("surrogate_key"))
            # Expire (update) records that are detected as changed
            expired_records = dim_df.join(
                keys_to_expire, on="surrogate_key", how="inner"
            ).withColumn("effective_end_date", F.lit(current_date)) \
             .withColumn("is_current", F.lit(False))
            # Retain records that remain unchanged
            retained_records = dim_df.join(
                keys_to_expire, on="surrogate_key", how="left_anti"
            )
            # Create new version records for the changed rows based on the new data
            changed_new = changed_records.select(
                *[F.col(f"new.{col}").alias(col) for col in (business_keys + tracked_columns)]
            )
            # Ensure we handle an empty dimension (max_key may be null)
            max_key_val = dim_df.agg(F.max("surrogate_key")).collect()[0][0] or 0
            window_spec = Window.orderBy(*business_keys)
            changed_new = changed_new.withColumn("row_num", F.row_number().over(window_spec)) \
                                     .withColumn("surrogate_key", F.col("row_num") + F.lit(max_key_val)) \
                                     .drop("row_num")
            changed_new = changed_new.withColumn("effective_start_date", F.lit(current_date)) \
                                     .withColumn("effective_end_date", F.lit(high_date)) \
                                     .withColumn("is_current", F.lit(True))
            updated_dim = retained_records.union(expired_records).union(changed_new)
        else:
            updated_dim = dim_df

        # Process new records (which did not match any current record)
        if new_count > 0:
            new_recs = new_records.select(
                *[F.col(f"new.{col}").alias(col) for col in (business_keys + tracked_columns)]
            )
            base_df = updated_dim if changed_count > 0 else dim_df
            max_key_val = base_df.agg(F.max("surrogate_key")).collect()[0][0] or 0
            window_spec = Window.orderBy(*business_keys)
            new_recs = new_recs.withColumn("row_num", F.row_number().over(window_spec)) \
                               .withColumn("surrogate_key", F.col("row_num") + F.lit(max_key_val)) \
                               .drop("row_num")
            new_recs = new_recs.withColumn("effective_start_date", F.lit(current_date)) \
                               .withColumn("effective_end_date", F.lit(high_date)) \
                               .withColumn("is_current", F.lit(True))
            final_dim = updated_dim.union(new_recs)
        else:
            final_dim = updated_dim

        # Write the updated dimension table back to output (consider using Delta format if available)
        try:
            final_record_count = final_dim.count()
            final_dim.write.mode("overwrite").parquet(f"{output_path}/{dim_name}")
            logger.info(f"{dim_name}: Updated dimension written with {final_record_count} records")
        except Exception as e:
            logger.error(f"{dim_name}: Failed to write updated dimension: {str(e)}")

    logger.info("SCD Type 2 processing completed successfully")

def main():
    if len(sys.argv) < 4:
        logger.error("Usage: scd_type2_handling.py <new_data_path> <dimension_path> <output_path>")
        sys.exit(1)

    new_data_path = sys.argv[1]
    dimension_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = SparkSession.builder.appName("MicrofinanceSCDType2").getOrCreate()

    try:
        apply_scd_type2(spark, new_data_path, dimension_path, output_path)
    except Exception as e:
        logger.error(f"SCD Type 2 processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
