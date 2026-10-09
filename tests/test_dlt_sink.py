"""``lakelogic.core.dlt_sink`` — the one dlt writer (no database needed; dlt is faked).

The real-database proof is ``tests/test_dlt_rdbms_real.py`` (Postgres + Azure SQL).
"""

from __future__ import annotations

import sys
import types

import polars as pl
import pytest

from lakelogic.core import dlt_sink


@pytest.fixture()
def fake_dlt(monkeypatch):
    rec = types.SimpleNamespace(dest_kwargs=None, pipeline=None, resource=None, fail=None)

    class _Factory:
        def __init__(self, name):
            self.name = name

        def __call__(self, **kw):
            rec.dest_kwargs = kw
            return f"dest:{self.name}"

    def resource(**kw):
        rec.resource = kw
        return lambda fn: fn

    def pipeline(**kw):
        rec.pipeline = kw

        def run(data):
            if rec.fail:
                raise RuntimeError(rec.fail)
            list(data)

        return types.SimpleNamespace(run=run)

    fake = types.ModuleType("dlt")
    fake.resource = resource
    fake.pipeline = pipeline
    fake.destinations = types.SimpleNamespace(
        postgres=_Factory("postgres"), mssql=_Factory("mssql"), duckdb=_Factory("duckdb")
    )
    monkeypatch.setitem(sys.modules, "dlt", fake)
    return rec


DF = pl.DataFrame({"id": [1, 2]})


def test_env_reference_is_resolved_and_never_logged_as_a_literal(fake_dlt, monkeypatch):
    monkeypatch.setenv("PG_URL", "postgresql://u:secret@h/db")
    out = dlt_sink.write_dlt(
        DF,
        table_name="orders",
        config={
            "dlt_destination": "postgres",
            "dlt_credentials": "env:PG_URL",
            "dlt_dataset_name": "sales",
            "dlt_create_indexes": False,
        },
    )
    assert fake_dlt.dest_kwargs == {"credentials": "postgresql://u:secret@h/db", "create_indexes": False}
    assert fake_dlt.pipeline["dataset_name"] == "sales"
    assert out == {
        "target": "postgres:sales.orders",
        "format": "dlt",
        "dlt_destination": "postgres",
        "rows_written": 2,
        "write_disposition": "append",
    }


def test_secret_uri_goes_through_the_vault_resolver(fake_dlt, monkeypatch):
    seen = []
    monkeypatch.setattr("lakelogic.core.vault_resolver._resolve", lambda uri: seen.append(uri) or "mssql://u:p@h/db")
    dlt_sink.write_dlt(
        DF, table_name="t", config={"dlt_destination": "mssql", "dlt_credentials": "keyvault://kv/sql-url"}
    )
    assert seen == ["keyvault://kv/sql-url"] and fake_dlt.dest_kwargs["credentials"] == "mssql://u:p@h/db"


def test_mapping_credentials_resolve_each_reference(monkeypatch):
    monkeypatch.setenv("PG_PASS", "s3cret")
    assert dlt_sink.resolve_credentials({"host": "h", "password": "env:PG_PASS"}) == {"host": "h", "password": "s3cret"}


def test_an_unset_reference_fails_loudly(fake_dlt, monkeypatch):
    monkeypatch.delenv("NOPE", raising=False)
    with pytest.raises(ValueError, match="'env:NOPE' is not set"):
        dlt_sink.write_dlt(DF, table_name="t", config={"dlt_destination": "postgres", "dlt_credentials": "env:NOPE"})


def test_missing_credentials_raise_for_a_primary_and_warn_for_a_lenient_secondary(fake_dlt, monkeypatch):
    monkeypatch.delenv("DESTINATION__POSTGRES__CREDENTIALS", raising=False)
    with pytest.raises(ValueError, match="No credentials for dlt destination 'postgres'"):
        dlt_sink.write_dlt(DF, table_name="t", config={"dlt_destination": "postgres"})
    assert (
        dlt_sink.write_dlt(DF, table_name="t", config={"dlt_destination": "postgres"}, require_credentials=False)[
            "rows_written"
        ]
        == 2
    )


@pytest.mark.parametrize(
    "strategy,pk,expected", [("append", None, "append"), ("overwrite", None, "replace"), ("merge", ["id"], "merge")]
)
def test_strategies_map_to_dlt_dispositions(strategy, pk, expected):
    assert dlt_sink.write_disposition(strategy, pk) == expected


@pytest.mark.parametrize(
    "strategy,pk,message",
    [("merge", None, "needs the contract's primary_key"), ("scd2", ["id"], "cannot be written through dlt")],
)
def test_strategies_dlt_cannot_express_are_refused(strategy, pk, message):
    with pytest.raises(ValueError, match=message):
        dlt_sink.write_disposition(strategy, pk)


def test_a_failure_never_carries_the_password(fake_dlt, monkeypatch):
    monkeypatch.setenv("PG_URL", "postgresql://user:Sup3rS3cret@host/db")
    fake_dlt.fail = "could not connect to postgresql://user:Sup3rS3cret@host/db"
    with pytest.raises(ValueError) as err:
        dlt_sink.write_dlt(DF, table_name="t", config={"dlt_destination": "postgres", "dlt_credentials": "env:PG_URL"})
    assert "Sup3rS3cret" not in str(err.value) and "dlt write to postgres failed" in str(err.value)


def test_frames_of_every_engine_become_arrow():
    import pandas as pd
    import pyarrow as pa

    for frame in (DF, DF.to_pandas(), DF.to_arrow()):
        assert dlt_sink.to_arrow(frame).num_rows == 2
    assert isinstance(dlt_sink.to_arrow(pd.DataFrame({"a": [1]})), pa.Table)
    with pytest.raises(TypeError):
        dlt_sink.to_arrow(object())


def test_a_load_left_by_a_failed_run_is_dropped_before_writing(fake_dlt, monkeypatch):
    dropped = []
    real_pipeline = sys.modules["dlt"].pipeline

    def pipeline(**kw):
        p = real_pipeline(**kw)
        p.has_pending_data = True
        p.pipeline_name = kw["pipeline_name"]
        p.drop_pending_packages = lambda: dropped.append(kw["pipeline_name"])
        return p

    monkeypatch.setattr(sys.modules["dlt"], "pipeline", pipeline)
    dlt_sink.write_dlt(DF, table_name="t", config={"dlt_destination": "duckdb", "dlt_dataset_name": "d"})
    assert dropped == ["lakelogic_d_t_duckdb"]


def test_each_destination_gets_its_own_pipeline():
    names = {dlt_sink.pipeline_name({}, dest, "d", "t") for dest in ("duckdb", "postgres", "mssql")}
    assert len(names) == 3
    assert dlt_sink.pipeline_name({"dlt_pipeline_name": "my run"}, "postgres", "d", "t") == "my_run_postgres"


def test_list_columns_become_json_where_the_database_cannot_store_them():
    import pyarrow as pa

    t = pa.table({"id": [1, 2], "_lakelogic_errors": [["fare < 0", "no city"], None]})
    out = dlt_sink.nested_to_json(t)
    assert out.column("_lakelogic_errors").to_pylist() == ['["fare < 0", "no city"]', None]
    assert out.column("id").to_pylist() == [1, 2]
    assert "duckdb" in dlt_sink.NESTED_DESTINATIONS and "postgres" not in dlt_sink.NESTED_DESTINATIONS
