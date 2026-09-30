#!/usr/bin/env python3
"""
data_ingestion.py
Simplified data ingestion script for a microfinance company on Microsoft Fabric.
Features:
- Schema enforcement (malformed rows are captured, not silently nulled)
- Data validation with invalid rows written to an error path
- Idempotent writes: each run replaces only its own process_date partition,
  including clearing it when a re-run has no rows for it
- Failures propagate (non-zero exit) so the orchestrator can see them
"""

import sys
import logging
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

from common import write_batch_partition

# Configure logging (Fabric provides integrated monitoring, so this is minimal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

CORRUPT_RECORD_COLUMN = "_corrupt_record"
VALID_STATUSES = ["Applied", "Approved", "Disbursed", "Repaid", "Defaulted"]


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
        (F.col("status").isNull()) | (F.col("status").isin(VALID_STATUSES))
    ]
    # Rows that failed to parse against the schema are errors, not rows full of nulls
    if CORRUPT_RECORD_COLUMN in df.columns:
        validations.append(F.col(CORRUPT_RECORD_COLUMN).isNull())

    combined_validation = validations[0]
    for rule in validations[1:]:
        combined_validation = combined_validation & rule
    # A rule that evaluates to NULL must not let a row escape both outputs
    combined_validation = F.coalesce(combined_validation, F.lit(False))

    valid_records = df.filter(combined_validation)
    error_records = df.filter(~combined_validation)

    return valid_records, error_records


def ingest(spark: SparkSession, input_path: str, output_path: str, error_path: str, process_date: str) -> dict:
    """
    Ingest one batch of raw CSV data.

    Valid and error records are both partitioned by `process_date`. Re-running the same
    process_date replaces that batch in both tables; a table that gets no rows on the re-run
    has the batch's old partition removed.

    Returns a dict with the total, valid and error record counts.
    """
    logger.info(f"Starting data ingestion from {input_path} to {output_path} for process_date={process_date}")

    # Enforce schema on read; keep the raw text of rows that do not fit it
    schema = get_schema().add(StructField(CORRUPT_RECORD_COLUMN, StringType(), True))
    df = spark.read.option("header", "true") \
                   .option("mode", "PERMISSIVE") \
                   .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN) \
                   .schema(schema) \
                   .csv(input_path) \
                   .withColumn("process_date", F.lit(process_date).cast("date")) \
                   .cache()
    total_count = df.count()
    logger.info(f"Loaded {total_count} records from {input_path}")

    valid_records, error_records = validate_data(df)
    valid_count = valid_records.count()
    error_count = error_records.count()
    logger.info(f"Data validation completed: {valid_count} valid records, {error_count} errors.")

    # Replace this batch's partitions (clearing them when there are no rows for them)
    logger.info(f"Writing {valid_count} valid records to {output_path}")
    write_batch_partition(
        spark,
        valid_records.drop(CORRUPT_RECORD_COLUMN).withColumn("ingestion_timestamp", F.current_timestamp()),
        valid_count, output_path, process_date,
    )

    if error_count > 0:
        logger.warning(f"Writing {error_count} error records to {error_path}")
    write_batch_partition(
        spark,
        error_records.withColumn("error_timestamp", F.current_timestamp()),
        error_count, error_path, process_date,
    )

    df.unpersist()
    logger.info("Data ingestion completed successfully.")
    return {"total": total_count, "valid": valid_count, "errors": error_count}


def main(
    input_path: str = "data/sample_data.csv",
    output_path: str = "fabric_ingested_data/parquet_data",
    error_path: str = "fabric_ingested_data/error_data",
    process_date: str = None
):
    process_date = process_date or date.today().isoformat()

    # Build Spark session. Fabric auto-configures the environment, so no extra settings here.
    spark = SparkSession.builder.appName("MicrofinanceDataIngestion").getOrCreate()

    try:
        ingest(spark, input_path, output_path, error_path, process_date)
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    # Usage: python data_ingestion.py [input_path] [output_path] [error_path] [process_date]
    args = sys.argv[1:]
    kwargs = {}
    if len(args) > 0:
        kwargs["input_path"] = args[0]
    if len(args) > 1:
        kwargs["output_path"] = args[1]
    if len(args) > 2:
        kwargs["error_path"] = args[2]
    if len(args) > 3:
        kwargs["process_date"] = args[3]

    main(**kwargs)
