import pytest

import scd_type4_handling
from common import path_exists
from scd_type4_handling import apply_scd_type4


def test_changed_new_and_absent_keys(spark, tmp_path, write_batch, make_row):
    current_path = str(tmp_path / "current")
    history_path = str(tmp_path / "history")
    apply_scd_type4(
        spark,
        write_batch([make_row("C1", "L1", income=1000.0), make_row("C2", "L2")], "2025-01-01"),
        current_path, history_path, process_date="2025-01-01",
    )
    assert not path_exists(spark, f"{history_path}/dim_customer_history")

    # C1 changes, C2 is absent from this batch, C3 is new
    apply_scd_type4(
        spark,
        write_batch([make_row("C1", "L1", income=2000.0), make_row("C3", "L3")], "2025-02-01"),
        current_path, history_path, process_date="2025-02-01",
    )

    current = {r["customer_id"]: r for r in spark.read.parquet(f"{current_path}/dim_customer_current").collect()}
    assert sorted(current) == ["C1", "C2", "C3"]
    assert current["C1"]["annual_income"] == 2000.0
    assert current["C1"]["record_effective_date"] == "2025-02-01"
    assert current["C2"]["record_effective_date"] == "2025-01-01"

    history = spark.read.parquet(f"{history_path}/dim_customer_history").collect()
    assert len(history) == 1
    assert history[0]["customer_id"] == "C1"
    assert history[0]["annual_income"] == 1000.0
    assert history[0]["record_effective_date"] == "2025-01-01"
    assert history[0]["record_end_date"] == "2025-02-01"


def test_only_the_requested_batch_is_applied(spark, tmp_path, write_batch, make_row):
    current_path = str(tmp_path / "current")
    write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0, last_updated="2025-02-01 00:00:00")], "2025-02-01")

    apply_scd_type4(spark, batches, current_path, str(tmp_path / "history"), process_date="2025-01-01")

    current = spark.read.parquet(f"{current_path}/dim_customer_current").collect()
    assert current[0]["annual_income"] == 1000.0


def test_rerunning_an_earlier_date_is_refused(spark, tmp_path, write_batch, make_row):
    current_path, history_path = str(tmp_path / "current"), str(tmp_path / "history")
    write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")
    apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-01-01")
    apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-02-01")

    with pytest.raises(ValueError, match="date order"):
        apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-01-01")

    assert spark.read.parquet(f"{history_path}/dim_customer_history").count() == 1


def test_rerun_with_same_batch_is_idempotent(spark, tmp_path, write_batch, make_row):
    current_path = str(tmp_path / "current")
    history_path = str(tmp_path / "history")
    apply_scd_type4(spark, write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01"),
                    current_path, history_path, process_date="2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")

    apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-02-01")
    apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-02-01")

    assert spark.read.parquet(f"{current_path}/dim_customer_current").count() == 1
    assert spark.read.parquet(f"{history_path}/dim_customer_history").count() == 1


def test_retry_after_failed_current_write_does_not_duplicate_history(
        spark, tmp_path, write_batch, make_row, monkeypatch):
    current_path = str(tmp_path / "current")
    history_path = str(tmp_path / "history")
    apply_scd_type4(spark, write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01"),
                    current_path, history_path, process_date="2025-01-01")
    batches = write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")

    # History is appended, then replacing the current table fails
    def failing_overwrite(*args, **kwargs):
        raise IOError("simulated failure while replacing the current table")
    monkeypatch.setattr(scd_type4_handling, "overwrite_parquet", failing_overwrite)
    with pytest.raises(IOError):
        apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-02-01")
    assert spark.read.parquet(f"{history_path}/dim_customer_history").count() == 1
    monkeypatch.undo()

    # The retry sees the same change again but must not archive the same version twice
    apply_scd_type4(spark, batches, current_path, history_path, process_date="2025-02-01")

    history = spark.read.parquet(f"{history_path}/dim_customer_history").collect()
    assert [(r["customer_id"], r["annual_income"]) for r in history] == [("C1", 1000.0)]
    current = spark.read.parquet(f"{current_path}/dim_customer_current").collect()
    assert [(r["customer_id"], r["annual_income"]) for r in current] == [("C1", 2000.0)]
