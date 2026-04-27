from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

typer = pytest.importorskip("typer")
from typer.testing import CliRunner

cli_main = importlib.import_module("lakelogic.cli.main")


def test_run_command_handles_missing_source_and_success_paths(monkeypatch, tmp_path):
    logs = []
    trace_calls = []
    materialized = []

    monkeypatch.setattr(cli_main.logger, "remove", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli_main.logger, "add", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli_main.logger, "error", logs.append)
    monkeypatch.setattr(cli_main.logger, "info", logs.append)
    monkeypatch.setattr(cli_main.logger, "exception", logs.append)
    monkeypatch.setattr(cli_main, "_display_trace", lambda trace: trace_calls.append(trace.run_id))

    contract = tmp_path / "contract.yaml"
    contract.write_text("version: 1.0.0\n", encoding="utf-8")
    missing_source = tmp_path / "missing.csv"

    with pytest.raises(typer.Exit) as excinfo:
        cli_main.run(
            contract=contract,
            source=missing_source,
            engine="polars",
            stage=None,
            output_good=None,
            output_bad=None,
            output_format=None,
            materialize=False,
            materialize_target=None,
            verbose=False,
            trace=False,
        )
    assert excinfo.value.exit_code == 1
    assert any("Source file not found" in message for message in logs)

    class FakeFrame:
        def __init__(self):
            self.csv_paths = []
            self.parquet_paths = []

        def write_csv(self, path):
            self.csv_paths.append(Path(path))

        def write_parquet(self, path):
            self.parquet_paths.append(Path(path))

    class FakeProcessor:
        def __init__(self, engine, contract, stage=None):
            self.engine = engine
            self.contract = contract
            self.stage = stage

        def run_source(self, source):
            return types.SimpleNamespace(
                good=FakeFrame(),
                bad=FakeFrame(),
                trace=types.SimpleNamespace(run_id="trace-1", steps=[], total_duration_ms=12.5),
            )

        def materialize(self, good_df, bad_df, target_path=None):
            materialized.append(target_path)

    monkeypatch.setattr(cli_main, "DataProcessor", FakeProcessor)
    source = tmp_path / "source.csv"
    source.write_text("id\n1\n", encoding="utf-8")
    output_good = tmp_path / "good.csv"
    output_bad = tmp_path / "bad.parquet"
    cli_main.run(
        contract=contract,
        source=source,
        engine="polars",
        stage=None,
        output_good=output_good,
        output_bad=output_bad,
        output_format=None,
        materialize=True,
        materialize_target=tmp_path / "target",
        trace=True,
        verbose=True,
    )
    assert materialized == [tmp_path / "target"]
    assert trace_calls == ["trace-1"]
    assert any("Saved good records" in message for message in logs)
    assert any("Saved quarantined records" in message for message in logs)


def test_display_trace_and_setup_oss(monkeypatch):
    echoes = []
    logs = []
    monkeypatch.setattr(cli_main.typer, "echo", lambda message="", **kwargs: echoes.append(message))
    monkeypatch.setattr(cli_main.logger, "info", logs.append)
    monkeypatch.setattr(cli_main.logger, "warning", logs.append)
    monkeypatch.setattr(cli_main.logger, "debug", logs.append)
    monkeypatch.setattr(cli_main.logger, "error", logs.append)

    trace = types.SimpleNamespace(
        run_id="run-1",
        total_duration_ms=42.0,
        steps=[
            types.SimpleNamespace(step="read", status="ok", input_rows=10, output_rows=10, duration_ms=1.5, details={"path": "/tmp/file"}),
            types.SimpleNamespace(step="validate", status="error", input_rows=10, output_rows=8, duration_ms=2.0, details={"errors": [1, 2]}),
        ],
    )
    cli_main._display_trace(trace)
    assert any("EXECUTION TRACE" in str(message) for message in echoes)
    assert any("TOTAL DURATION" in str(message) for message in echoes)

    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object() if name == "deltalake" else None)
    fake_duckdb = types.SimpleNamespace(
        install_extension=lambda ext: logs.append(f"install::{ext}"),
        load_extension=lambda ext: logs.append(f"load::{ext}"),
    )
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)
    cli_main.setup_oss()
    assert "install::httpfs" in logs
    assert "load::azure" in logs
    assert any("OSS environment setup finished" in message for message in logs)

    logs.clear()
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(install_extension=lambda ext: (_ for _ in ()).throw(RuntimeError("boom"))))
    cli_main.setup_oss()
    assert any("deltalake is NOT installed" in message for message in logs)
    assert any("Could not load duckdb extension" in message for message in logs)


def test_help_generate_and_assert_commands(monkeypatch, tmp_path):
    runner = CliRunner()
    result = runner.invoke(cli_main.app, ["help", "bootstrap"])
    assert result.exit_code == 0
    assert "Bootstrap Help" in result.output

    outputs = []
    monkeypatch.setattr(cli_main.typer, "echo", lambda message="", **kwargs: outputs.append(message))
    monkeypatch.setattr(cli_main.logger, "error", outputs.append)
    monkeypatch.setattr(cli_main.logger, "exception", outputs.append)

    class FakeGeneratedFrame:
        def head(self, preview):
            return self

        def __str__(self):
            return "fake-frame"

    class FakeGenerator:
        def __init__(self, contract, seed=None):
            self.contract = contract
            self.seed = seed

        def generate(self, **kwargs):
            return FakeGeneratedFrame()

        def generation_report(self):
            return {
                "test_cases": [
                    {"id": "TC001", "type": "NOT_NULL_VIOLATION", "field": "status", "rows_generated": 2},
                ]
            }

        def save_with_report(self, df, output_path, format):
            return (
                Path(output_path) / f"sample.{format}",
                Path(output_path) / f"sample_invalid.{format}",
                Path(output_path) / "report.json",
            )

        def save(self, df, output, format):
            return Path(output)

    monkeypatch.setitem(sys.modules, "lakelogic.core.generator", types.SimpleNamespace(DataGenerator=FakeGenerator))
    contract = tmp_path / "orders.yaml"
    contract.write_text("version: 1.0.0\n", encoding="utf-8")
    output_dir = tmp_path / "generated"
    output_dir.mkdir()
    cli_main.generate(
        contract=contract,
        rows=10,
        output=output_dir,
        format="json",
        engine="polars",
        invalid_ratio=0.2,
        seed=None,
        preview=3,
        ai=False,
        ai_provider=None,
        ai_model=None,
    )
    assert any("Generated 10 rows" in str(message) for message in outputs)
    assert any("sample_invalid.json" in str(message) for message in outputs)

    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(
            {
                "contract": "orders",
                "seed": 7,
                "summary": {"total_rows": 10, "invalid_rows": 2, "invalid_ratio": 0.2},
                "test_cases": [{"id": "TC001", "type": "NOT_NULL_VIOLATION", "field": "status", "rows_generated": 2}],
            }
        ),
        encoding="utf-8",
    )
    outputs.clear()
    cli_main.assert_report(
        report=report_path,
        quarantine_log=None,
        expect_all_invalid_quarantined=True,
        min_coverage=0.9,
    )
    assert any("All assertions passed" in str(message) for message in outputs)


def test_generate_assert_import_dbt_and_doctor_error_paths(monkeypatch, tmp_path):
    outputs = []
    monkeypatch.setattr(cli_main.typer, "echo", lambda message="", **kwargs: outputs.append(message))
    monkeypatch.setattr(cli_main.logger, "error", outputs.append)
    monkeypatch.setattr(cli_main.logger, "exception", outputs.append)

    missing_contract = tmp_path / "missing.yaml"
    with pytest.raises(typer.Exit) as excinfo:
        cli_main.generate(
            contract=missing_contract,
            rows=5,
            output=None,
            format="parquet",
            engine="polars",
            invalid_ratio=0.0,
            seed=None,
            preview=0,
            ai=False,
            ai_provider=None,
            ai_model=None,
        )
    assert excinfo.value.exit_code == 1

    contract = tmp_path / "orders.yaml"
    contract.write_text("version: 1.0.0\n", encoding="utf-8")
    with pytest.raises(typer.Exit):
        cli_main.generate(
            contract=contract,
            rows=5,
            output=None,
            format="parquet",
            engine="invalid",
            invalid_ratio=0.0,
            seed=None,
            preview=0,
            ai=False,
            ai_provider=None,
            ai_model=None,
        )
    with pytest.raises(typer.Exit):
        cli_main.generate(
            contract=contract,
            rows=5,
            output=None,
            format="badfmt",
            engine="polars",
            invalid_ratio=0.0,
            seed=None,
            preview=0,
            ai=False,
            ai_provider=None,
            ai_model=None,
        )

    with pytest.raises(typer.Exit):
        cli_main.assert_report(
            report=tmp_path / "missing_report.json",
            quarantine_log=None,
            expect_all_invalid_quarantined=False,
            min_coverage=0.0,
        )

    class FakeContract:
        def __init__(self, dataset):
            self.dataset = dataset

        def model_dump(self, exclude_none=True, by_alias=True):
            return {"version": "1.0", "dataset": self.dataset}

    class FakeAdapter:
        def __init__(self, schema):
            if str(schema).endswith("missing.yml"):
                raise FileNotFoundError("schema missing")

        def source_to_contract(self, source_name, source_table):
            return FakeContract("source_contract")

        def model_to_contract(self, model):
            if model == "broken":
                raise RuntimeError("bad model")
            return FakeContract(model)

        def list_models(self):
            return ["orders", "customers"]

    monkeypatch.setitem(sys.modules, "lakelogic.adapters.dbt", types.SimpleNamespace(DbtAdapter=FakeAdapter))
    schema = tmp_path / "schema.yml"
    schema.write_text("version: 2\n", encoding="utf-8")

    out_dir = tmp_path / "contracts"
    cli_main.import_dbt(
        schema=schema,
        model=None,
        source_name=None,
        source_table=None,
        output=out_dir,
        overwrite=False,
        dry_run=False,
        verbose=True,
    )
    assert (out_dir / "orders.yaml").exists()
    assert (out_dir / "customers.yaml").exists()

    outputs.clear()
    cli_main.import_dbt(
        schema=schema,
        model="orders",
        source_name=None,
        source_table=None,
        output=None,
        overwrite=False,
        dry_run=True,
        verbose=False,
    )
    assert any("# --- orders ---" in str(message) for message in outputs)

    outputs.clear()
    cli_main.import_dbt(
        schema=schema,
        model=None,
        source_name="raw",
        source_table="orders",
        output=tmp_path / "single.yaml",
        overwrite=True,
        dry_run=False,
        verbose=False,
    )
    assert (tmp_path / "single.yaml").exists()

    with pytest.raises(typer.Exit):
        cli_main.import_dbt(
            schema=schema,
            model="broken",
            source_name=None,
            source_table=None,
            output=None,
            overwrite=False,
            dry_run=False,
            verbose=False,
        )
    with pytest.raises(typer.Exit):
        cli_main.import_dbt(
            schema=tmp_path / "missing.yml",
            model=None,
            source_name=None,
            source_table=None,
            output=None,
            overwrite=False,
            dry_run=False,
            verbose=False,
        )

    outputs.clear()
    fake_lakelogic = types.SimpleNamespace(__version__="9.9.9")
    monkeypatch.setitem(sys.modules, "lakelogic", fake_lakelogic)

    import builtins

    real_import = builtins.__import__

    fake_modules = {
        "polars": types.SimpleNamespace(__version__="1.0.0"),
        "duckdb": types.SimpleNamespace(__version__="1.1.0"),
        "pandas": types.SimpleNamespace(__version__="2.0.0"),
        "pydantic": types.SimpleNamespace(__version__="2.1.0"),
    }

    def fake_import(name, *args, **kwargs):
        if name in fake_modules:
            return fake_modules[name]
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    try:
        cli_main.doctor()
    finally:
        monkeypatch.setattr(builtins, "__import__", real_import)
    assert any("LakeLogic Doctor" in str(message) for message in outputs)


def test_write_or_print_contract_and_import_dbt_no_models(monkeypatch, tmp_path):
    outputs = []
    monkeypatch.setattr(cli_main.typer, "echo", lambda message="", **kwargs: outputs.append(message))

    contract = types.SimpleNamespace(dataset="orders", model_dump=lambda exclude_none=True, by_alias=True: {"version": "1.0", "dataset": "orders"})

    cli_main._write_or_print_contract(contract, None, None, overwrite=False, dry_run=True, verbose=False)
    assert any("# --- orders ---" in str(message) for message in outputs)

    outputs.clear()
    out_dir = tmp_path / "contracts"
    cli_main._write_or_print_contract(contract, None, out_dir, overwrite=False, dry_run=False, verbose=True)
    assert (out_dir / "orders.yaml").exists()
    assert any("Written:" in str(message) for message in outputs)

    outputs.clear()
    cli_main._write_or_print_contract(contract, None, out_dir, overwrite=False, dry_run=False, verbose=False)
    assert any("Skipped (already exists)" in str(message) for message in outputs)

    outputs.clear()
    cli_main._write_or_print_contract(contract, None, None, overwrite=False, dry_run=False, verbose=False)
    assert any("dataset: orders" in str(message) for message in outputs)

    class EmptyAdapter:
        def __init__(self, schema):
            self.schema = schema

        def list_models(self):
            return []

    monkeypatch.setitem(sys.modules, "lakelogic.adapters.dbt", types.SimpleNamespace(DbtAdapter=EmptyAdapter))
    schema = tmp_path / "schema.yml"
    schema.write_text("version: 2\n", encoding="utf-8")

    with pytest.raises(typer.Exit) as excinfo:
        cli_main.import_dbt(
            schema=schema,
            model=None,
            source_name=None,
            source_table=None,
            output=None,
            overwrite=False,
            dry_run=False,
            verbose=False,
        )
    assert excinfo.value.exit_code == 1
    assert any("No models found in schema file" in str(message) for message in outputs)
