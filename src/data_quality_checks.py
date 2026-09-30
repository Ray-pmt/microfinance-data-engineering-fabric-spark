#!/usr/bin/env python3
"""
data_quality_checks.py
A streamlined script for performing data quality checks on ingested microfinance data.
Features:
- Validates key fields (e.g., non-null customer_id/loan_id)
- Checks numeric ranges for financial metrics
- Verifies that status values are within allowed categories
- Aggregates quality metrics and writes a JSON report (via Spark, so it works on OneLake/ABFS)
- Writes out error records for further analysis
- Failures propagate (non-zero exit) so the orchestrator can see them
"""

import sys
import logging
import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure minimal logging (Fabric typically provides integrated logging)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_quality_checks(df):
    """
    Run data quality validations on the DataFrame.
    Returns a tuple of (error_records DataFrame, metrics dict).
    """
    # Define checks with conditions (1 indicates an error, 0 passes)
    quality_checks = {
        "missing_customer_id": F.when(F.col("customer_id").isNull(), 1).otherwise(0),
        "missing_loan_id": F.when(F.col("loan_id").isNull(), 1).otherwise(0),
        "invalid_loan_amount": F.when((F.col("loan_amount").isNotNull()) & (F.col("loan_amount") <= 0), 1).otherwise(0),
        "invalid_interest_rate": F.when((F.col("interest_rate").isNotNull()) & ((F.col("interest_rate") < 0) | (F.col("interest_rate") > 100)), 1).otherwise(0),
        "invalid_term": F.when((F.col("term_months").isNotNull()) & (F.col("term_months") <= 0), 1).otherwise(0),
        "invalid_status": F.when(~F.col("status").isin(["Applied", "Approved", "Disbursed", "Repaid", "Defaulted"]), 1).otherwise(0)
    }

    # Add each quality check as a column
    for check, expr in quality_checks.items():
        df = df.withColumn(check, expr)

    # Sum up errors per record; any record with a non-zero sum has quality issues.
    check_columns = list(quality_checks.keys())
    df = df.withColumn("quality_issue", sum([F.col(c) for c in check_columns]))

    error_records = df.filter(F.col("quality_issue") > 0)

    # Compute aggregate metrics for all checks in a single pass.
    totals = df.agg(F.count(F.lit(1)).alias("_total"), *[F.sum(c).alias(c) for c in check_columns]).first()
    total_records = totals["_total"] or 1  # Avoid division by zero
    metrics = {}
    for check in check_columns:
        error_count = totals[check] or 0
        metrics[check] = {
            "error_count": error_count,
            "error_percentage": (error_count / total_records) * 100
        }

    return error_records, metrics


def errors_output_path(output_report: str) -> str:
    """Error records are written next to the report, e.g. report.json -> report_errors.parquet."""
    base = output_report[:-len(".json")] if output_report.endswith(".json") else output_report
    return base + "_errors.parquet"


def check_quality(spark, input_path: str, output_report: str, process_date: str = None) -> dict:
    """
    Run the quality checks and write the report and error records.
    When `process_date` is given only that batch is checked.

    The report is written with Spark so the same code works for local paths and
    Lakehouse paths (Files/...); `output_report` becomes a directory holding one JSON file.

    Returns the quality report dict.
    """
    logger.info(f"Starting data quality checks on data from {input_path} (process_date={process_date})")

    # Read the ingested Parquet data.
    df = spark.read.parquet(input_path)
    if process_date:
        df = df.filter(F.col("process_date") == F.lit(process_date).cast("date"))
    record_count = df.count()
    logger.info(f"Loaded {record_count} records from {input_path}")

    if record_count == 0:
        logger.info("No data available for quality checks. Exiting.")
        return {}

    # Run quality validations.
    error_records, metrics = run_quality_checks(df)
    error_count = error_records.count()
    logger.info(f"Quality checks completed. Found {error_count} records with issues.")

    # Always overwrite so a clean run doesn't leave a previous run's errors behind.
    error_output_path = errors_output_path(output_report)
    error_records.write.mode("overwrite").parquet(error_output_path)
    logger.info(f"Error records written to {error_output_path}")

    # Prepare and write the quality report.
    quality_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "process_date": process_date or "all",
        "total_records": record_count,
        "error_records": error_count,
        "metrics": metrics
    }
    spark.createDataFrame([(json.dumps(quality_report, indent=4),)], ["value"]) \
         .coalesce(1).write.mode("overwrite").text(output_report)
    logger.info(f"Quality report saved to {output_report}")

    return quality_report


def main(input_path: str = "fabric_ingested_data/parquet_data",
         output_report: str = "fabric_data_quality_report.json",
         process_date: str = None):
    spark = SparkSession.builder.appName("MicrofinanceDataQualityChecks").getOrCreate()

    try:
        check_quality(spark, input_path, output_report, process_date)
    except Exception as e:
        logger.error(f"Data quality checks failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    # Usage: python data_quality_checks.py [input_path] [output_report] [process_date]
    args = sys.argv[1:]
    input_path = args[0] if len(args) > 0 else "fabric_ingested_data/parquet_data"
    output_report = args[1] if len(args) > 1 else "fabric_data_quality_report.json"
    process_date = args[2] if len(args) > 2 else None
    main(input_path, output_report, process_date)
