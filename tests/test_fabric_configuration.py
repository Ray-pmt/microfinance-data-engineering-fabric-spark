import json
from datetime import date
from pathlib import Path

from data_ingestion import ingest
from data_quality_checks import check_quality
from data_transformation import transform_data
from scd_type2_handling import apply_scd_type2


def test_template_arguments_run_one_consistent_batch(spark, tmp_path):
    """Exercise the template's ordered arguments against the real pipeline steps."""
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "config/fabric_spark_pipeline.json").read_text())["properties"]
    parameters = {name: spec["defaultValue"] for name, spec in config["parameters"].items()}
    parameters.update({
        "inputPath": str(root / "data/sample_data.csv"),
        "ingestedDataPath": str(tmp_path / "ingested"),
        "errorDataPath": str(tmp_path / "errors"),
        "transformedDataPath": str(tmp_path / "transformed"),
        "dimensionDataPath": str(tmp_path / "dimensions"),
        "reportsPath": str(tmp_path / "reports/quality_report.json"),
    })
    process_date = date.fromisoformat(parameters["processDate"])
    steps = {
        "data_ingestion.py": ingest,
        "data_transformation.py": transform_data,
        "data_quality_checks.py": check_quality,
        "scd_type2_handling.py": apply_scd_type2,
    }
    executed = []
    for activity in config["activities"]:
        if activity["type"] != "SparkJob":
            continue
        properties = activity["typeProperties"]
        arguments = []
        for argument in properties["arguments"]:
            expression = argument["value"]["value"]
            prefix = "@pipeline().parameters."
            assert expression.startswith(prefix), f"Unsupported argument expression: {expression}"
            arguments.append(parameters[expression[len(prefix):]])
        steps[properties["entryFilePath"]](spark, *arguments)
        executed.append(properties["entryFilePath"])
    assert set(executed) == set(steps)

    # Check real rows and effective dates, so a boolean in the date slot cannot pass.
    transformed = spark.read.parquet(parameters["transformedDataPath"]).collect()
    assert len(transformed) == 5
    assert {row.process_date for row in transformed} == {process_date}
    report = json.loads("\n".join(row.value for row in spark.read.text(parameters["reportsPath"]).collect()))
    assert report["process_date"] == process_date.isoformat()
    assert report["total_records"] == 5 and report["error_records"] == 0
    for dimension in ("dim_customer", "dim_loan"):
        rows = spark.read.parquet(parameters["dimensionDataPath"] + "/" + dimension).collect()
        assert len(rows) == 5
        assert all(row.is_current and row.effective_start_date == process_date.isoformat() for row in rows)
