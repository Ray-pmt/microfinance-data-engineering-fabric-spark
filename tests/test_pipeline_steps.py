import json

import pytest
from pyspark.sql import functions as F

from data_ingestion import ingest
from data_quality_checks import check_quality
from data_transformation import compute_monthly_payment, transform_data

HEADER = ("customer_id,loan_id,application_date,loan_amount,interest_rate,term_months,status,"
          "customer_name,customer_dob,employment_status,annual_income,last_updated\n")


def _write_csv(tmp_path, lines, name="input.csv"):
    path = tmp_path / name
    path.write_text(HEADER + "\n".join(lines) + "\n")
    return str(path)


VALID = "C1,L1,2025-01-15,5000.00,5.5,24,Approved,John Doe,1985-04-12,Employed,45000.00,2025-01-15 10:30:00"
INVALID_STATUS = "C2,L2,2025-01-16,5000.00,5.5,24,Unknown,Jane Doe,1985-04-12,Employed,45000.00,2025-01-16 10:30:00"
MALFORMED_AMOUNT = "C3,L3,2025-01-17,not-a-number,5.5,24,Approved,Bob Roe,1985-04-12,Employed,45000.00,2025-01-17 10:30:00"


def test_ingestion_routes_invalid_and_malformed_rows_to_errors(spark, tmp_path):
    input_path = _write_csv(tmp_path, [VALID, INVALID_STATUS, MALFORMED_AMOUNT])
    out, err = str(tmp_path / "ingested"), str(tmp_path / "errors")

    counts = ingest(spark, input_path, out, err, "2025-01-20")

    assert counts == {"total": 3, "valid": 1, "errors": 2}
    assert [r["customer_id"] for r in spark.read.parquet(out).collect()] == ["C1"]
    errors = spark.read.parquet(err)
    assert sorted(r["customer_id"] for r in errors.collect()) == ["C2", "C3"]
    # The malformed row keeps its raw text for investigation
    assert errors.filter(F.col("_corrupt_record").isNotNull()).count() == 1


def test_ingestion_rerun_replaces_only_its_own_batch(spark, tmp_path):
    input_path = _write_csv(tmp_path, [VALID])
    out, err = str(tmp_path / "ingested"), str(tmp_path / "errors")

    ingest(spark, input_path, out, err, "2025-01-20")
    ingest(spark, input_path, out, err, "2025-01-20")
    assert spark.read.parquet(out).count() == 1

    ingest(spark, input_path, out, err, "2025-01-21")
    assert spark.read.parquet(out).count() == 2


def test_ingestion_failure_propagates(spark, tmp_path):
    with pytest.raises(Exception):
        ingest(spark, str(tmp_path / "missing.csv"), str(tmp_path / "o"), str(tmp_path / "e"), "2025-01-20")


def test_transformation_is_rerunnable_and_output_stays_readable(spark, tmp_path):
    input_path = _write_csv(tmp_path, [VALID])
    ingested, transformed = str(tmp_path / "ingested"), str(tmp_path / "transformed")
    ingest(spark, input_path, ingested, str(tmp_path / "errors"), "2025-01-20")

    transform_data(spark, ingested, transformed, "2025-01-20")
    transform_data(spark, ingested, transformed, "2025-01-20")

    # Output is readable as a table (audit log lives outside it) and has no duplicates
    rows = spark.read.parquet(transformed).collect()
    assert len(rows) == 1
    assert rows[0]["loan_category"] == "Medium"
    assert rows[0]["monthly_payment"] == pytest.approx(compute_monthly_payment(5000.0, 5.5, 24))
    assert spark.read.json(transformed + "_audit").count() == 2


def test_monthly_payment():
    assert compute_monthly_payment(1200.0, 0.0, 12) == pytest.approx(100.0)
    assert compute_monthly_payment(10000.0, 12.0, 12) == pytest.approx(888.49, abs=0.01)
    assert compute_monthly_payment(1000.0, 5.0, 0) is None


def test_quality_report_is_written_to_a_new_directory(spark, tmp_path):
    input_path = _write_csv(tmp_path, [VALID])
    ingested = str(tmp_path / "ingested")
    ingest(spark, input_path, ingested, str(tmp_path / "errors"), "2025-01-20")
    report_path = str(tmp_path / "reports" / "nested" / "quality_report.json")

    report = check_quality(spark, ingested, report_path, "2025-01-20")

    assert report["total_records"] == 1
    assert report["error_records"] == 0
    written = json.loads("\n".join(r["value"] for r in spark.read.text(report_path).collect()))
    assert written["metrics"]["invalid_status"]["error_count"] == 0


def _count_for(spark, path, process_date):
    return spark.read.parquet(path).filter(F.col("process_date") == process_date).count()


def test_ingestion_rerun_clears_partitions_that_become_empty(spark, tmp_path):
    out, err = str(tmp_path / "ingested"), str(tmp_path / "errors")
    # Another date that must never be touched
    ingest(spark, _write_csv(tmp_path, [VALID, INVALID_STATUS], "other.csv"), out, err, "2025-01-19")

    ingest(spark, _write_csv(tmp_path, [VALID, INVALID_STATUS], "v1.csv"), out, err, "2025-01-20")
    assert _count_for(spark, out, "2025-01-20") == 1
    assert _count_for(spark, err, "2025-01-20") == 1

    # Corrected file: every row is now valid -> the old error rows for the date must go
    ingest(spark, _write_csv(tmp_path, [VALID], "v2.csv"), out, err, "2025-01-20")
    assert _count_for(spark, out, "2025-01-20") == 1
    assert _count_for(spark, err, "2025-01-20") == 0

    # Every row invalid -> the old valid rows for the date must go
    ingest(spark, _write_csv(tmp_path, [INVALID_STATUS], "v3.csv"), out, err, "2025-01-20")
    assert _count_for(spark, out, "2025-01-20") == 0
    assert _count_for(spark, err, "2025-01-20") == 1

    # Empty file -> both are cleared
    counts = ingest(spark, _write_csv(tmp_path, [], "v4.csv"), out, err, "2025-01-20")
    assert counts == {"total": 0, "valid": 0, "errors": 0}
    assert _count_for(spark, out, "2025-01-20") == 0
    assert _count_for(spark, err, "2025-01-20") == 0

    assert _count_for(spark, out, "2025-01-19") == 1
    assert _count_for(spark, err, "2025-01-19") == 1


def test_first_batch_with_no_valid_rows_does_not_break_downstream(spark, tmp_path):
    out, err = str(tmp_path / "ingested"), str(tmp_path / "errors")
    ingest(spark, _write_csv(tmp_path, [INVALID_STATUS]), out, err, "2025-01-20")

    assert transform_data(spark, out, str(tmp_path / "transformed"), "2025-01-20") == 0
    assert check_quality(spark, out, str(tmp_path / "report.json"), "2025-01-20")["total_records"] == 0


def test_transformation_rerun_clears_a_batch_that_became_empty(spark, tmp_path):
    ingested, err, transformed = str(tmp_path / "ingested"), str(tmp_path / "errors"), str(tmp_path / "transformed")
    ingest(spark, _write_csv(tmp_path, [VALID], "v1.csv"), ingested, err, "2025-01-19")
    ingest(spark, _write_csv(tmp_path, [VALID], "v2.csv"), ingested, err, "2025-01-20")
    transform_data(spark, ingested, transformed, "2025-01-19")
    transform_data(spark, ingested, transformed, "2025-01-20")

    ingest(spark, _write_csv(tmp_path, [INVALID_STATUS], "v3.csv"), ingested, err, "2025-01-20")
    assert transform_data(spark, ingested, transformed, "2025-01-20") == 0

    assert _count_for(spark, transformed, "2025-01-20") == 0
    assert _count_for(spark, transformed, "2025-01-19") == 1


def test_quality_rerun_on_empty_batch_replaces_report_and_errors(spark, tmp_path):
    ingested, err = str(tmp_path / "ingested"), str(tmp_path / "errors")
    report_path = str(tmp_path / "reports" / "quality_report.json")
    ingest(spark, _write_csv(tmp_path, [VALID], "v1.csv"), ingested, err, "2025-01-20")
    check_quality(spark, ingested, report_path, "2025-01-20")
    assert (tmp_path / "reports" / "quality_report_errors.parquet").exists()

    ingest(spark, _write_csv(tmp_path, [], "v2.csv"), ingested, err, "2025-01-20")
    check_quality(spark, ingested, report_path, "2025-01-20")

    written = json.loads("\n".join(r["value"] for r in spark.read.text(report_path).collect()))
    assert written["total_records"] == 0
    assert not (tmp_path / "reports" / "quality_report_errors.parquet").exists()


def test_steps_fail_on_a_missing_input_table(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        transform_data(spark, str(tmp_path / "nope"), str(tmp_path / "out"), "2025-01-20")
