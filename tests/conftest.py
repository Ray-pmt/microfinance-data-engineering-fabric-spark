import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Python UDFs run in separate worker processes; they need src/ on their path too
SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [SRC_DIR, os.environ.get("PYTHONPATH")]))


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder \
        .master("local[1]") \
        .appName("microfinance-tests") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Columns produced by data_transformation.py that the SCD steps consume
TRANSFORMED_SCHEMA = (
    "customer_id string, loan_id string, customer_name string, customer_dob string, "
    "employment_status string, annual_income double, status string, interest_rate double, "
    "term_months int, loan_category string, last_updated string"
)


@pytest.fixture
def make_row():
    """Build one transformed row; override only the fields a test cares about."""
    def _row(customer_id, loan_id, name="Name", employment="Employed", income=1000.0,
             status="Approved", rate=5.0, term=12, category="Small", last_updated="2025-01-01 00:00:00"):
        return (customer_id, loan_id, name, "1990-01-01", employment, income,
                status, rate, term, category, last_updated)
    return _row


@pytest.fixture
def transformed_path(tmp_path):
    return str(tmp_path / "transformed")


@pytest.fixture
def write_batch(spark, transformed_path):
    """
    Write one process_date batch of transformed rows into the shared transformed table
    (replacing that date's partition, as data_transformation.py does) and return the table path.
    """
    def _write(rows, process_date):
        spark.createDataFrame(rows, TRANSFORMED_SCHEMA) \
             .withColumn("last_updated", F.to_timestamp("last_updated")) \
             .withColumn("process_date", F.lit(process_date).cast("date")) \
             .write.mode("overwrite") \
             .option("partitionOverwriteMode", "dynamic") \
             .partitionBy("process_date") \
             .parquet(transformed_path)
        return transformed_path

    return _write
