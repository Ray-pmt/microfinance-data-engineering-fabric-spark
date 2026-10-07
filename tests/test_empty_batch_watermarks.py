from pathlib import Path

import pytest
from pyspark.sql import functions as F

from common import path_exists, read_watermark
from scd_type2_handling import apply_scd_type2
from scd_type4_handling import apply_scd_type4


def _apply(spark, source, tmp_path, scd_type, process_date):
    dimensions = str(tmp_path / "dimensions")
    if scd_type == 2:
        apply_scd_type2(spark, source, dimensions, dimensions, process_date)
    else:
        apply_scd_type4(spark, source, dimensions, str(tmp_path / "history"), process_date)


def _table(tmp_path, dimension, scd_type):
    suffix = "" if scd_type == 2 else "_current"
    return str(tmp_path / "dimensions" / (dimension + suffix))


@pytest.mark.parametrize("scd_type", [2, 4])
def test_empty_batch_advances_watermark_and_blocks_older_batches(
        spark, tmp_path, write_batch, make_row, scd_type):
    source = write_batch([make_row("C1", "L1", income=1000.0)], "2025-01-01")
    _apply(spark, source, tmp_path, scd_type, "2025-01-01")

    # March has no partition. It still counts as the latest processed batch.
    _apply(spark, source, tmp_path, scd_type, "2025-03-01")
    _apply(spark, source, tmp_path, scd_type, "2025-03-01")
    for dimension in ("dim_customer", "dim_loan"):
        assert read_watermark(spark, _table(tmp_path, dimension, scd_type)) == ("2025-03-01", "0:0")

    write_batch([make_row("C1", "L1", income=2000.0)], "2025-02-01")
    with pytest.raises(ValueError, match="date order"):
        _apply(spark, source, tmp_path, scd_type, "2025-02-01")
    customers = spark.read.parquet(_table(tmp_path, "dim_customer", scd_type))
    assert customers.count() == 1 and customers.first().annual_income == 1000.0

    # The latest empty batch has no effects to undo, so it can still be corrected.
    write_batch([make_row("C1", "L1", income=3000.0)], "2025-03-01")
    _apply(spark, source, tmp_path, scd_type, "2025-03-01")
    customers = spark.read.parquet(_table(tmp_path, "dim_customer", scd_type))
    if scd_type == 2:
        customers = customers.filter(F.col("is_current"))
    assert customers.first().annual_income == 3000.0


@pytest.mark.parametrize("scd_type", [2, 4])
def test_first_empty_batch_keeps_order_before_a_dimension_exists(
        spark, tmp_path, transformed_path, write_batch, make_row, scd_type):
    Path(transformed_path).mkdir()
    _apply(spark, transformed_path, tmp_path, scd_type, "2025-03-01")

    for dimension in ("dim_customer", "dim_loan"):
        table = _table(tmp_path, dimension, scd_type)
        assert not path_exists(spark, table)
        assert read_watermark(spark, table) == ("2025-03-01", "0:0")

    write_batch([make_row("C1", "L1")], "2025-02-01")
    with pytest.raises(ValueError, match="date order"):
        _apply(spark, transformed_path, tmp_path, scd_type, "2025-02-01")

    # Retry the empty batch, then supply its first rows; there is no history to undo.
    _apply(spark, transformed_path, tmp_path, scd_type, "2025-03-01")
    write_batch([make_row("C1", "L1")], "2025-03-01")
    _apply(spark, transformed_path, tmp_path, scd_type, "2025-03-01")
    for dimension in ("dim_customer", "dim_loan"):
        assert spark.read.parquet(_table(tmp_path, dimension, scd_type)).count() == 1
