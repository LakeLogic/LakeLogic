"""lakelogic profile --contract: the contract's profiling block, and the profile sent to LakeLogic."""
import pytest

from lakelogic.core.profile_report import build_payload, contract_location, profile_contract, profiling_settings


def test_no_block_profiles_everything():
    assert profiling_settings({}) == {"mode": "full"}


def test_a_sample_block_becomes_sample_settings():
    s = profiling_settings({"profiling": {"mode": "sample", "sample_percent": 5, "sample_order": "random"}})
    assert s == {"mode": "sample", "sample_unit": "percent", "sample_rows": None, "sample_percent": 5,
                 "sample_order": "random"}


def test_profiling_turned_off_is_refused():
    with pytest.raises(ValueError):
        profiling_settings({"profiling": {"enabled": False}})


def test_only_a_concrete_source_path_is_used():
    assert contract_location({"source": {"path": "/data/drivers"}}) == "/data/drivers"
    assert contract_location({"source": {"path": "{landing_root}/drivers"}}) is None


def test_the_payload_is_a_profile_record_not_a_run():
    doc = {"name": "drivers", "status": "profiled", "metrics": {"row_count": 12}, "finding_counts": {"gap": 1}}
    p = build_payload(doc, dataset="silver_drivers", started_at="a", finished_at="b", environment="dev")
    assert (p["engine"], p["metadata"]["record_type"], p["rows_input"]) == ("profile", "profile", 12)
    assert p["metadata"]["profile"] is doc and p["metadata"]["finding_counts"] == {"gap": 1}


def test_a_contract_profiles_its_own_table(tmp_path):
    (tmp_path / "data").mkdir()
    rows = ["driver_id,email,rating"] + [f"{i},d{i}@x.io,{i % 5}" for i in range(1, 51)] + ["50,d50@x.io,1"]
    (tmp_path / "data" / "drivers.csv").write_text("\n".join(rows) + "\n")
    contract = {
        "version": "1.0.0", "info": {"title": "Drivers", "table_name": "silver_drivers"},
        "source": {"type": "landing", "path": (tmp_path / "data").as_posix(), "format": "csv"},
        "model": {"fields": [{"name": "driver_id", "type": "integer"}, {"name": "email", "type": "string", "pii": True},
                             {"name": "rating", "type": "integer"}]},
    }
    out = profile_contract(contract)
    assert out["sent"] is False  # no endpoint: nothing leaves the machine
    assert out["payload"]["rows_input"] == 51
    email = next(c for c in out["profile"]["profile"]["columns"] if c["name"] == "email")
    assert email["sensitive"] and email.get("min") is None  # pii: min/max withheld


def _spark_contract(tmp_path, path=None):
    return {
        "version": "1.0.0", "info": {"title": "Drivers", "table_name": "profile_drivers"},
        **({"source": {"type": "landing", "path": path, "format": "csv"}} if path else {}),
        "model": {"fields": [{"name": "driver_id", "type": "integer"}, {"name": "email", "type": "string", "pii": True},
                             {"name": "rating", "type": "integer"}]},
        "profiling": {"findings": True},
    }


def test_spark_profiles_a_table_and_writes_one_row_per_column(tmp_path):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.createDataFrame([(i, f"d{i}@x.io", i % 5) for i in range(1, 41)] + [(40, "d40@x.io", 1)],
                          "driver_id int, email string, rating int").createOrReplaceTempView("profile_drivers")
    spark.sql("DROP TABLE IF EXISTS lakelogic_profiles_t")
    out = profile_contract(_spark_contract(tmp_path), spark=spark, results_table="lakelogic_profiles_t")
    assert out["payload"]["rows_input"] == 41 and out["results_table_error"] is None
    rows = {r["column_name"]: r for r in spark.table("lakelogic_profiles_t").collect()}
    assert set(rows) == {"driver_id", "email", "rating"} and rows["driver_id"]["row_count"] == 41
    assert rows["email"]["sensitive"] == "True" and rows["email"]["min_value"] is None


def test_spark_profiles_files_in_a_folder(tmp_path):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    (tmp_path / "data" / "y_2026").mkdir(parents=True)  # nested, as landing folders are
    (tmp_path / "data" / "y_2026" / "drivers.csv").write_text(
        "driver_id,email,rating\n" + "\n".join(f"{i},d{i}@x.io,{i % 5}" for i in range(1, 31)) + "\n")
    out = profile_contract(_spark_contract(tmp_path, (tmp_path / "data").as_posix()), spark=spark)
    assert out["payload"]["rows_input"] == 30
    assert out["profile"]["name"].endswith("/data")
