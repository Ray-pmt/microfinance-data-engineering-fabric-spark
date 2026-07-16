#!/usr/bin/env python3
"""
data_ingestion.py
Simplified data ingestion script for a microfinance company on Microsoft Fabric.
Features:
- Schema enforcement
- Data validation
- Basic error handling and logging
- Partitioning for optimized querying
"""

import sys
import os
import logging
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# Configure logging (Fabric provides integrated monitoring, so this is minimal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_schema() -> StructType:
    """Define and return the schema for microfinance data."""
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("loan_id", StringType(), False),
        StructField("application_date", DateType(), True),
        StructField("loan_amount", DoubleType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("term_months", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_dob", DateType(), True),
        StructField("employment_status", StringType(), True),
        StructField("annual_income", DoubleType(), True),
        StructField("last_updated", TimestampType(), True)
    ])

def validate_data(df: DataFrame) -> (DataFrame, DataFrame):
    """
    Validate ingested data and split into valid and error records.
    Returns:
        valid_records: DataFrame with records passing validation.
        error_records: DataFrame with records failing validation.
    """
    validations = [
        ~F.col("customer_id").isNull() & ~F.col("loan_id").isNull(),
        (F.col("loan_amount").isNull()) | (F.col("loan_amount") > 0),
        (F.col("interest_rate").isNull()) | ((F.col("interest_rate") >= 0) & (F.col("interest_rate") <= 100)),
        (F.col("term_months").isNull()) | (F.col("term_months") > 0),
        (F.col("status").isNull()) | (F.col("status").isin(["Applied", "Approved", "Disbursed", "Repaid", "Defaulted"]))
    ]
    combined_validation = validations[0]
    for rule in validations[1:]:
        combined_validation = combined_validation & rule

    valid_records = df.filter(combined_validation)
    error_records = df.filter(~combined_validation)

    valid_count = valid_records.count()
    error_count = error_records.count()
    logger.info(f"Data validation completed: {valid_count} valid records, {error_count} errors.")
    
    return valid_records, error_records

def main(
    input_path: str = "data/sample_data.csv",
    output_path: str = "fabric_ingested_data/parquet_data",
    error_path: str = "fabric_ingested_data/error_data",
    partition_by: str = "application_date"
):
    logger.info(f"Starting data ingestion from {input_path} to {output_path}")
    
    # Build Spark session. Fabric auto-configures the environment, so no extra settings here.
    spark = SparkSession.builder.appName("MicrofinanceDataIngestion").getOrCreate()
    
    try:
        # Enforce schema on read
        schema = get_schema()
        df = spark.read.option("header", "true").schema(schema).csv(input_path)
        total_count = df.count()
        logger.info(f"Loaded {total_count} records from {input_path}")

        if total_count == 0:
            logger.info("No data found. Exiting ingestion.")
            return

        # Validate data
        valid_records, error_records = validate_data(df)

        # Write valid records with partitioning for query performance
        if valid_records.count() > 0:
            valid_records = valid_records.withColumn("ingestion_timestamp", F.current_timestamp())
            logger.info(f"Writing {valid_records.count()} valid records to {output_path}")
            valid_records.write.partitionBy(partition_by).mode("append").parquet(output_path)

        # Write error records for later review
        if error_records.count() > 0:
            error_records = error_records.withColumn("error_timestamp", F.current_timestamp())
            logger.warning(f"Writing {error_records.count()} error records to {error_path}")
            error_records.write.partitionBy("error_timestamp").mode("append").parquet(error_path)

        logger.info("Data ingestion completed successfully.")
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    # Command-line overrides for paths if needed.
    args = sys.argv[1:]
    kwargs = {}
    if len(args) > 0:
        kwargs["input_path"] = args[0]
    if len(args) > 1:
        kwargs["output_path"] = args[1]
    if len(args) > 2:
        kwargs["error_path"] = args[2]
    if len(args) > 3:
        kwargs["partition_by"] = args[3]
    
    main(**kwargs)
