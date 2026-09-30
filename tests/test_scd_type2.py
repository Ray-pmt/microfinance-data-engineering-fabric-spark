import shutil

import pytest
from pyspark.sql import functions as F

from scd_type2_handling import apply_scd_type2


def _customers(spark, dim_path):
    return {
        (r["customer_id"], r["is_current"]): r
        for r in spark.read.parquet(f"{dim_path}/dim_customer").collect()
    }


def test_initial_load_creates_one_current_version_per_key(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    batch = write_batch([make_row("C1", "L1"), make_row("C2", "L2")], "2025-01-01")

    apply_scd_type2(spark, batch, dim_path, dim_path, process_date="2025-01-01")

    dim = spark.read.parquet(f"{dim_path}/dim_customer")
    assert dim.count() == 2
    assert dim.filter(F.col("is_current")).count() == 2
    assert sorted(r["surrogate_key"] for r in dim.collect()) == [1, 2]


def test_changed_new_and_absent_keys(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    apply_scd_type2(
        spark,
        write_batch([make_row("C1", "L1", income=1000.0), make_row("C2", "L2")], "2025-01-01"),
        dim_path, dim_path, process_date="2025-01-01",
    )

    # C1 changes, C2 is absent from this batch, C3 is new
    apply_scd_type2(
        spark,
        write_batch([make_row("C1", "L1", income=2000.0), make_row("C3", "L3")], "2025-02-01"),
        dim_path, dim_path, process_date="2025-02-01",
    )

    dim = spark.read.parquet(f"{dim_path}/dim_customer")
    # No row may lose its business key
    assert dim.filter(F.col("customer_id").isNull()).count() == 0
    assert dim.count() == 4

    customers = _customers(spark, dim_path)
    # C1: old version expired, new version current
    assert customers[("C1", False)]["annual_income"] == 1000.0
    assert customers[("C1", False)]["effective_end_date"] == "2025-02-01"
    assert customers[("C1", True)]["annual_income"] == 2000.0
    assert customers[("C1", True)]["effective_start_date"] == "2025-02-01"
    # C2 was simply not in the batch: it must stay current and untouched
    assert customers[("C2", True)]["effective_end_date"] == "9999-12-31"
    assert ("C2", False) not in customers
    # C3 is new
    assert customers[("C3", True)]["effective_start_date"] == "2025-02-01"
    # Surrogate keys stay unique
    keys = [r["surrogate_key"] for r in dim.collect()]
    assert len(keys) == len(set(keys))


def test_only_the_requested_batch_is_applied(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0, last_updated="2025-02-01 00:00:00")], "2025-02-01")

    # Both dates exist in the transformed table; processing 2025-01-01 must not pick up the later row
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-01-01")

    customers = _customers(spark, dim_path)
    assert customers[("C1", True)]["annual_income"] == 1000.0
    assert customers[("C1", True)]["effective_start_date"] == "2025-01-01"


def test_rerun_with_same_batch_is_idempotent(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    apply_scd_type2(spark, write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01"),
                    dim_path, dim_path, process_date="2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")

    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-02-01")
    first = sorted(tuple(r) for r in spark.read.parquet(f"{dim_path}/dim_customer").collect())
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-02-01")
    second = sorted(tuple(r) for r in spark.read.parquet(f"{dim_path}/dim_customer").collect())

    assert first == second
    assert len(second) == 2


def test_rerunning_an_earlier_date_is_refused(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-01-01")
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-02-01")
    before = sorted(tuple(r) for r in spark.read.parquet(f"{dim_path}/dim_customer").collect())

    with pytest.raises(ValueError, match="date order"):
        apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-01-01")

    after = sorted(tuple(r) for r in spark.read.parquet(f"{dim_path}/dim_customer").collect())
    assert before == after


def test_missing_batch_leaves_dimension_untouched(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    batches = write_batch([make_row("C1", "L1")], "2025-01-01")
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-01-01")

    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-02-01")

    assert spark.read.parquet(f"{dim_path}/dim_customer").count() == 1


def test_latest_version_per_key_wins(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    batch = write_batch([
        make_row("C1", "L1", status="Disbursed", last_updated="2025-01-02 00:00:00"),
        make_row("C1", "L1", status="Repaid", last_updated="2025-01-03 00:00:00"),
        make_row("C1", "L1", status="Approved", last_updated="2025-01-01 00:00:00"),
    ], "2025-01-03")

    apply_scd_type2(spark, batch, dim_path, dim_path, process_date="2025-01-03")

    loans = spark.read.parquet(f"{dim_path}/dim_loan").collect()
    assert len(loans) == 1
    assert loans[0]["status"] == "Repaid"


def test_null_to_value_is_a_change(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    apply_scd_type2(spark, write_batch([make_row("C1", "L1", employment=None)], "2025-01-01"),
                    dim_path, dim_path, process_date="2025-01-01")
    apply_scd_type2(spark, write_batch([make_row("C1", "L1", employment="Employed")], "2025-02-01"),
                    dim_path, dim_path, process_date="2025-02-01")

    customers = _customers(spark, dim_path)
    assert customers[("C1", False)]["employment_status"] is None
    assert customers[("C1", True)]["employment_status"] == "Employed"


def test_interrupted_swap_is_rolled_back_not_reloaded(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    apply_scd_type2(spark, write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01"),
                    dim_path, dim_path, process_date="2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-02-01")

    # Simulate a crash after the table was moved aside but before staging was moved into place
    table = f"{dim_path}/dim_customer"
    shutil.move(table, table + "__previous")
    (tmp_path / "dim" / "dim_customer__staging").mkdir()

    apply_scd_type2(spark, write_batch([make_row("C1", "L1", income=3000.0)], "2025-03-01"),
                    dim_path, dim_path, process_date="2025-03-01")

    # History from before the crash is intact (an initial load would have left a single row)
    incomes = sorted(r["annual_income"] for r in spark.read.parquet(table).collect())
    assert incomes == [1000.0, 2000.0, 3000.0]
    assert not (tmp_path / "dim" / "dim_customer__previous").exists()
    assert not (tmp_path / "dim" / "dim_customer__staging").exists()


def test_completed_swap_with_leftover_previous_version_keeps_new_table(spark, tmp_path, write_batch, make_row):
    dim_path = str(tmp_path / "dim")
    batches = write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01")
    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-01-01")
    apply_scd_type2(spark, write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01"),
                    dim_path, dim_path, process_date="2025-02-01")

    # Simulate a crash after the swap but before the previous version was deleted
    table = f"{dim_path}/dim_customer"
    shutil.copytree(table, table + "__previous")

    apply_scd_type2(spark, batches, dim_path, dim_path, process_date="2025-02-01")

    assert spark.read.parquet(table).count() == 2
    assert not (tmp_path / "dim" / "dim_customer__previous").exists()
