#!/usr/bin/env python3
"""
data_transformation.py
A streamlined data transformation script for a microfinance company on Microsoft Fabric.
Features:
- Calculation of monthly loan payments using standard amortization formula
- Categorization of loans based on amount
- Basic error handling and logging
- Partitioned output for optimized queries
"""

import sys
import logging
import math
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

def transform_data(input_path: str, output_path: str, partition_by: str = "application_date"):
    logger.info(f"Starting data transformation from {input_path} to {output_path}")

    # Build Spark session (Fabric auto-configures the environment)
    spark = SparkSession.builder.appName("MicrofinanceDataTransformation").getOrCreate()
    
    try:
        # Read the ingested data (assumed to be in Parquet format)
        df = spark.read.parquet(input_path)
        total_records = df.count()
        logger.info(f"Loaded {total_records} records from {input_path}")

        if total_records == 0:
            logger.info("No data to transform. Exiting.")
            return

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

        # Write transformed data partitioned by the given column for query efficiency.
        logger.info("Writing transformed data.")
        df.write.partitionBy(partition_by).mode("append").parquet(output_path)

        # Optionally, write a simple audit log as a JSON file.
        audit_data = {
            "transformation_timestamp": datetime.datetime.now().isoformat(),
            "input_path": input_path,
            "output_path": output_path,
            "record_count": total_records
        }
        audit_path = output_path + "/audit_log.json"
        spark.createDataFrame([audit_data]).coalesce(1).write.mode("append").json(audit_path)
        logger.info(f"Audit log saved to {audit_path}")

        logger.info("Data transformation completed successfully.")

    except Exception as e:
        logger.error(f"Transformation failed: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    # Command-line overrides for input/output paths and partition column
    # Usage: python data_transformation.py [input_path] [output_path] [partition_by]
    args = sys.argv[1:]
    input_path = args[0] if len(args) > 0 else "fabric_ingested_data/parquet_data"
    output_path = args[1] if len(args) > 1 else "fabric_transformed_data/parquet_data"
    partition_by = args[2] if len(args) > 2 else "application_date"

    transform_data(input_path, output_path, partition_by)
