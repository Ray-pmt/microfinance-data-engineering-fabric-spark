#!/usr/bin/env python3
"""
data_transformation.py
A streamlined data transformation script for a microfinance company on Microsoft Fabric.
Features:
- Calculation of monthly loan payments using standard amortization formula
- Categorization of loans based on amount
- Idempotent writes: each run replaces only the process_date partitions it transforms
- Audit log kept outside the data directory so it never breaks reads of the table
- Failures propagate (non-zero exit) so the orchestrator can see them
"""

import sys
import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Configure minimal logging (Fabric provides integrated monitoring)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# UDF to compute monthly loan payment using an amortization formula.
def compute_monthly_payment(loan_amount, interest_rate, term_months):
    """
    Calculate the monthly payment for a loan.
    - loan_amount: principal amount of the loan.
    - interest_rate: annual interest rate in percentage.
    - term_months: duration of the loan in months.
    Returns monthly payment or None if term_months is invalid.
    """
    try:
        if term_months is None or term_months <= 0:
            return None
        # If interest_rate is 0 or null, assume simple division.
        if interest_rate is None or interest_rate == 0:
            return loan_amount / term_months
        monthly_rate = (interest_rate / 100) / 12
        payment = loan_amount * monthly_rate / (1 - (1 + monthly_rate) ** (-term_months))
        return float(payment)
    except Exception as e:
        return None

# Register the UDF
compute_monthly_payment_udf = F.udf(compute_monthly_payment, DoubleType())


def audit_log_path(output_path: str) -> str:
    """Audit log lives next to (not inside) the output table."""
    return output_path.rstrip("/") + "_audit"


def transform_data(spark, input_path: str, output_path: str, process_date: str = None) -> int:
    """
    Transform ingested data. When `process_date` is given only that batch is transformed;
    otherwise every ingested batch is (re)transformed.

    Returns the number of records transformed.
    """
    logger.info(f"Starting data transformation from {input_path} to {output_path} (process_date={process_date})")

    # Read the ingested data (Parquet, partitioned by process_date)
    df = spark.read.parquet(input_path)
    if process_date:
        df = df.filter(F.col("process_date") == F.lit(process_date).cast("date"))
    total_records = df.count()
    logger.info(f"Loaded {total_records} records from {input_path}")

    if total_records == 0:
        logger.info("No data to transform. Exiting.")
        return 0

    # Calculate monthly payment and add as a new column.
    # Assumes columns: loan_amount, interest_rate, term_months exist.
    df = df.withColumn(
        "monthly_payment",
        compute_monthly_payment_udf(F.col("loan_amount"), F.col("interest_rate"), F.col("term_months"))
    )

    # Categorize loans based on amount.
    # Example thresholds: Small (<5000), Medium (5000-20000), Large (>20000)
    df = df.withColumn(
        "loan_category",
        F.when(F.col("loan_amount") < 5000, "Small")
         .when((F.col("loan_amount") >= 5000) & (F.col("loan_amount") <= 20000), "Medium")
         .otherwise("Large")
    )

    # Add a transformation timestamp column
    df = df.withColumn("transformation_timestamp", F.current_timestamp())

    # Replace only the process_date partitions present in this run, so re-runs don't duplicate rows
    logger.info("Writing transformed data.")
    df.write.mode("overwrite") \
      .option("partitionOverwriteMode", "dynamic") \
      .partitionBy("process_date") \
      .parquet(output_path)

    # Append a record of this run to the audit log.
    audit_data = {
        "transformation_timestamp": datetime.datetime.now().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "process_date": process_date or "all",
        "record_count": total_records
    }
    audit_path = audit_log_path(output_path)
    spark.createDataFrame([audit_data]).coalesce(1).write.mode("append").json(audit_path)
    logger.info(f"Audit log saved to {audit_path}")

    logger.info("Data transformation completed successfully.")
    return total_records


def main(input_path: str, output_path: str, process_date: str = None):
    # Build Spark session (Fabric auto-configures the environment)
    spark = SparkSession.builder.appName("MicrofinanceDataTransformation").getOrCreate()

    try:
        transform_data(spark, input_path, output_path, process_date)
    except Exception as e:
        logger.error(f"Transformation failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    # Usage: python data_transformation.py [input_path] [output_path] [process_date]
    args = sys.argv[1:]
    input_path = args[0] if len(args) > 0 else "fabric_ingested_data/parquet_data"
    output_path = args[1] if len(args) > 1 else "fabric_transformed_data/parquet_data"
    process_date = args[2] if len(args) > 2 else None

    main(input_path, output_path, process_date)
