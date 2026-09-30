"""The `_lakelogic_` naming standard for LakeLogic-owned tables and files (2026-09-26).

One module decides every name LakeLogic gives its own tables; these tests pin the rule,
the underscore-free fallback, legacy preference for existing estates, verbatim use of
configured names, and the erasure-evidence table written beside the run log.
"""

import sqlite3

import pytest

from lakelogic.core import metadata_names as mn
from lakelogic.core.models import DataContract, FieldDefinition, Info, Model

# ── The rule ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("kind", mn.METADATA_KINDS)
def test_every_kind_gets_the_lakelogic_prefix(kind):
    assert mn.metadata_table_name(kind) == f"_lakelogic_{kind}"


@pytest.mark.parametrize("kind", mn.METADATA_KINDS)
def test_path_based_names_drop_the_leading_underscore(kind):
    # Spark/Hadoop hide `_`-prefixed paths, so a directory must not start with one.
    assert mn.metadata_table_name(kind, path_based=True) == f"lakelogic_{kind}"


def test_fabric_is_underscore_unsafe_and_other_backends_are_not():
    assert "fabric" in mn.UNDERSCORE_UNSAFE_BACKENDS
    assert mn.metadata_table_name("run_log", backend="fabric") == "lakelogic_run_log"
    assert mn.metadata_table_name("run_log", backend="FABRIC") == "lakelogic_run_log"
    assert mn.metadata_table_name("run_log", backend="spark") == "_lakelogic_run_log"
    assert mn.metadata_table_name("run_log", backend="duckdb") == "_lakelogic_run_log"


def test_file_names_are_underscore_free_with_extension():
    assert mn.metadata_file_name("run_log", "duckdb") == "lakelogic_run_log.duckdb"
    assert mn.metadata_file_name("pipeline_runs", ".sqlite") == "lakelogic_pipeline_runs.sqlite"


def test_unknown_kind_is_rejected():
    with pytest.raises(ValueError):
        mn.metadata_table_name("run_logs")


def test_the_required_kinds_exist():
    assert set(mn.METADATA_KINDS) == {
        "run_log",
        "slo_checks",
        "logs",
        "pipeline_runs",
        "erasure_evidence",
        "retention_evidence",
        "erasure_requests",
    }


# ── Legacy names ────────────────────────────────────────────────────────────


def test_legacy_names_cover_every_kind_and_every_name_used_before():
    assert set(mn.LEGACY_NAMES) == set(mn.METADATA_KINDS)
    assert "_run_logs" in mn.LEGACY_NAMES["run_log"]
    assert "run_logs" in mn.LEGACY_NAMES["run_log"]
    assert "lakelogic_run_logs.duckdb" in mn.LEGACY_NAMES["run_log"]
    assert "slo_checks" in mn.LEGACY_NAMES["slo_checks"]
    assert "_logs" in mn.LEGACY_NAMES["logs"]
    assert "pipeline_runs" in mn.LEGACY_NAMES["pipeline_runs"]
    # A legacy name is never the new name.
    for kind, names in mn.LEGACY_NAMES.items():
        assert mn.metadata_table_name(kind) not in names
        assert mn.metadata_file_name(kind, "duckdb") not in names


def test_legacy_table_names_excludes_files():
    assert mn.legacy_table_names("run_log") == ("_run_logs", "run_logs")


def test_resolve_existing_prefers_an_existing_legacy_table():
    existing = {"_logs"}
    assert mn.resolve_existing("logs", ["_lakelogic_logs", "_logs"], existing.__contains__) == "_logs"


def test_resolve_existing_uses_the_new_name_for_a_new_estate():
    assert mn.resolve_existing("logs", ["_lakelogic_logs", "_logs"], lambda _: False) == "_lakelogic_logs"


def test_resolve_existing_treats_a_failing_check_as_absent():
    def boom(_):
        raise RuntimeError("no catalog")

    assert mn.resolve_existing("logs", ["_lakelogic_logs", "_logs"], boom) == "_lakelogic_logs"


def test_resolve_local_file_keeps_a_legacy_db_file(tmp_path):
    assert mn.resolve_local_file("run_log", tmp_path, "duckdb") == tmp_path / "lakelogic_run_log.duckdb"
    (tmp_path / "lakelogic_run_logs.duckdb").write_bytes(b"")
    assert mn.resolve_local_file("run_log", tmp_path, "duckdb") == tmp_path / "lakelogic_run_logs.duckdb"


def test_resolve_path_dir_keeps_a_legacy_local_directory(tmp_path):
    root = str(tmp_path).replace("\\", "/")
    assert mn.resolve_path_dir("logs", root) == f"{root}/lakelogic_logs"
    (tmp_path / "_logs").mkdir()
    assert mn.resolve_path_dir("logs", root) == f"{root}/_logs"


def test_resolve_path_dir_uses_the_new_name_for_cloud_uris():
    assert mn.resolve_path_dir("logs", "abfss://lake/root") == "abfss://lake/root/lakelogic_logs"


def test_quarantine_is_not_a_lakelogic_metadata_kind():
    # Quarantine holds the client's own rejected rows, so it keeps its original names.
    import types

    from lakelogic.core import paths

    assert "quarantine" not in mn.METADATA_KINDS
    assert "quarantine" not in mn.LEGACY_NAMES
    with pytest.raises(ValueError):
        mn.metadata_table_name("quarantine")
    storage = types.SimpleNamespace(quarantine_path=None, quarantine_root=None, external_location_root="s3://b")
    assert paths.resolve_quarantine_path(registry_storage=storage, entity="orders") == "s3://b/_quarantine/orders"


# ── Evidence tables ─────────────────────────────────────────────────────────


def _pii_contract(metadata):
    return DataContract(
        version="1.0",
        info=Info(title="Customer Data", version="2.1.0", table_name="customers"),
        dataset="customers",
        metadata=metadata,
        model=Model(
            fields=[
                FieldDefinition(name="customer_id", type="string", required=True),
                FieldDefinition(name="email", type="string", pii=True),
            ]
        ),
    )


def _frame():
    import polars as pl

    return pl.DataFrame({"customer_id": ["c1", "c2", "c3", "c1"], "email": ["a@x", "b@x", "c@x", "d@x"]})


def test_evidence_target_sits_beside_the_run_log():
    from lakelogic.core.evidence_tables import resolve_evidence_target

    assert resolve_evidence_target("erasure_evidence", {}) is None
    assert resolve_evidence_target(
        "erasure_evidence", {"run_log_table": "`cat`.sales.my_run_log", "run_log_backend": "spark"}
    ) == ("spark", "`cat`.sales._lakelogic_erasure_evidence", None)
    assert resolve_evidence_target(
        "retention_evidence", {"run_log_table": "abfss://lake/root/_logs", "run_log_backend": "delta"}
    ) == ("delta", "abfss://lake/root/lakelogic_retention_evidence", None)
    assert resolve_evidence_target(
        "erasure_evidence", {"run_log_table": "cat.sales.log", "run_log_backend": "spark", "platform": "fabric"}
    ) == ("spark", "cat.sales.lakelogic_erasure_evidence", None)


def test_erasure_writes_an_evidence_row_sqlite(tmp_path):
    from lakelogic.core.gdpr import forget_subjects

    db = tmp_path / "logs.sqlite"
    contract = _pii_contract({"run_log_table": "my_run_log", "run_log_backend": "sqlite", "run_log_database": str(db)})
    forget_subjects(_frame(), contract, "customer_id", ["c1"])

    con = sqlite3.connect(str(db))
    try:
        rows = con.execute(
            "SELECT profile, dataset, table_name, subject_count, rows_affected, reason, contract, "
            "contract_version, timestamp FROM _lakelogic_erasure_evidence"
        ).fetchall()
        # The configured run-log name is used verbatim.
        assert con.execute("SELECT COUNT(*) FROM my_run_log").fetchone()[0] >= 1
    finally:
        con.close()
    assert len(rows) == 1
    profile, dataset, table, subjects, affected, reason, title, version, ts = rows[0]
    assert (profile, dataset, table, subjects, affected) == ("gdpr", "customers", "customers", 1, 2)
    assert reason and title == "Customer Data" and version == "2.1.0"
    assert ts.endswith("+00:00")


def test_erasure_writes_an_evidence_row_duckdb(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    from lakelogic.core.gdpr import forget_subjects

    db = tmp_path / "logs.duckdb"
    contract = _pii_contract({"run_log_table": "run_log", "run_log_backend": "duckdb", "run_log_database": str(db)})
    forget_subjects(_frame(), contract, "customer_id", ["c1", "c2"])

    con = duckdb.connect(str(db))
    try:
        row = con.execute(
            "SELECT profile, subject_count, rows_affected, timestamp FROM _lakelogic_erasure_evidence"
        ).fetchone()
    finally:
        con.close()
    assert row[:3] == ("gdpr", 2, 3)
    assert row[3].tzinfo is not None


def test_erasure_writes_no_evidence_without_a_run_log(tmp_path, monkeypatch):
    from lakelogic.core import evidence_tables
    from lakelogic.core.gdpr import forget_subjects

    calls = []
    monkeypatch.setattr(evidence_tables, "write_evidence_rows", lambda *a, **k: calls.append(a))
    forget_subjects(_frame(), _pii_contract({}), "customer_id", ["c1"])
    assert calls == []


def test_directory_style_duckdb_run_log_uses_the_new_file_and_table(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    from lakelogic.core.run_log import _write_run_log_table

    log_dir = tmp_path / "lakelogic_logs"
    contract = _pii_contract({"run_log_table": str(log_dir), "run_log_backend": "duckdb"})
    _write_run_log_table({"run_id": "r1", "contract": "c", "counts": {}}, contract, engine_name="polars")
    con = duckdb.connect(str(log_dir / "lakelogic_run_log.duckdb"))
    try:
        assert con.execute("SELECT COUNT(*) FROM _lakelogic_run_log").fetchone()[0] == 1
    finally:
        con.close()


def test_directory_style_duckdb_run_log_keeps_a_legacy_file(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    from lakelogic.core.run_log import _write_run_log_table

    log_dir = tmp_path / "_logs"
    log_dir.mkdir()
    duckdb.connect(str(log_dir / "lakelogic_run_logs.duckdb")).close()
    contract = _pii_contract({"run_log_table": str(log_dir), "run_log_backend": "duckdb"})
    _write_run_log_table({"run_id": "r1", "contract": "c", "counts": {}}, contract, engine_name="polars")
    assert not (log_dir / "lakelogic_run_log.duckdb").exists()
    con = duckdb.connect(str(log_dir / "lakelogic_run_logs.duckdb"))
    try:
        assert con.execute("SELECT COUNT(*) FROM run_logs").fetchone()[0] == 1
    finally:
        con.close()


def test_delta_path_evidence_lands_beside_the_run_log(tmp_path):
    deltalake = pytest.importorskip("deltalake")
    from datetime import datetime, timezone

    from lakelogic.core.evidence_tables import write_evidence_rows

    log_dir = (tmp_path / "lakelogic_logs").as_posix()
    ts = datetime(2026, 9, 26, tzinfo=timezone.utc)
    row = {"run_id": "r1", "timestamp": ts, "table_name": "t", "policy": "P7D", "cutoff": ts, "rows_expired": 4}
    target = write_evidence_rows("retention_evidence", [row], {"run_log_table": log_dir, "run_log_backend": "delta"})
    assert target == f"{tmp_path.as_posix()}/lakelogic_retention_evidence"
    tbl = deltalake.DeltaTable(target).to_pyarrow_table()
    assert tbl.num_rows == 1 and tbl.column("rows_expired").to_pylist() == [4]
    assert str(tbl.schema.field("timestamp").type.tz) == "UTC"


# ── dlt run-log dataset (legacy `run_logs` kept, 2026-09-26) ─────────────────────────────
from lakelogic.core.metadata_names import resolve_dlt_run_log_dataset


def test_dlt_keeps_an_existing_run_logs_dataset():
    assert resolve_dlt_run_log_dataset(lambda name: name == "run_logs") == "run_logs"


def test_dlt_new_estate_gets_the_new_dataset_without_a_leading_underscore():
    assert resolve_dlt_run_log_dataset(lambda name: False) == "lakelogic_run_log"


def test_dlt_keeps_the_legacy_name_when_the_destination_cannot_answer():
    def unreachable(name):
        raise ConnectionError("destination down")

    assert resolve_dlt_run_log_dataset(unreachable) == "run_logs"


# ── retention evidence status/detail (2026-09-27) ─────────────────────────────────────────
def test_retention_evidence_has_status_and_detail_and_old_tables_gain_them(tmp_path):
    import duckdb
    from lakelogic.core.evidence_tables import EVIDENCE_COLUMNS, write_evidence_rows

    assert {"status", "detail"} <= set(EVIDENCE_COLUMNS["retention_evidence"])
    db = tmp_path / "rl.duckdb"
    con = duckdb.connect(str(db))
    # A table made before the columns existed.
    con.execute(
        "CREATE TABLE _lakelogic_retention_evidence (run_id VARCHAR, timestamp TIMESTAMPTZ, "
        "table_name VARCHAR, policy VARCHAR, cutoff TIMESTAMPTZ, rows_expired BIGINT)"
    )
    con.close()
    meta = {"run_log_table": "_lakelogic_run_log", "run_log_backend": "duckdb", "run_log_database": str(db)}
    out = write_evidence_rows(
        "retention_evidence",
        [
            {
                "run_id": "r",
                "timestamp": "2026-09-27T00:00:00+00:00",
                "table_name": "d.bronze.t",
                "policy": "P7D",
                "status": "passed",
                "detail": "ok (oldest 47 min)",
            }
        ],
        meta,
        engine_name="duckdb",
    )
    assert out
    con = duckdb.connect(str(db))
    assert con.execute("SELECT policy, status, detail FROM _lakelogic_retention_evidence").fetchall() == [
        ("P7D", "passed", "ok (oldest 47 min)")
    ]
