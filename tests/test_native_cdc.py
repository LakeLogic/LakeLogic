"""Native change capture: read the change log, don't poll a watermark.

What existed before was watermark extraction — ``WHERE updated_at > x`` — under the
name CDC. The distinction is not academic:

* a DELETE is invisible to a watermark query (the row is simply gone), so the target
  keeps deleted rows forever;
* several changes between polls collapse into whatever the final state happened to
  be, so the history is lost;
* it depends on an ``updated_at`` the application maintains — where it doesn't, rows
  are missed with no error.

``load_mode: cdc`` + ``options.cdc_provider`` reads the database's own change table
instead, and normalises the provider's operation codes so the existing CDC
consumption path (merge + soft-delete) does not care which database produced them.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core.processor import DataProcessor


def _proc() -> DataProcessor:
    return DataProcessor(
        {
            "version": "1.0.0",
            "dataset": "customers",
            "model": {"fields": [{"name": "id", "type": "int"}]},
        },
        engine="polars",
    )


# ── query generation ─────────────────────────────────────────────────────────


def test_sqlserver_reads_the_change_table_not_the_base_table():
    """The whole point: the FROM clause is the CDC function, not `customers`."""
    sql = _proc()._build_cdc_query("sqlserver", "customers", '"id"')

    assert "cdc.fn_cdc_get_all_changes_customers" in sql
    assert "sys.fn_cdc_get_max_lsn()" in sql
    # A watermark poll would look like this; it must NOT.
    assert "WHERE updated_at >" not in sql


def test_update_before_images_are_excluded():
    """__$operation 3 is the row as it was BEFORE an update. Merging it would
    resurrect the pre-update state, so it is filtered out server-side."""
    sql = _proc()._build_cdc_query("sqlserver", "customers", "*")
    assert "__$operation <> 3" in sql


def test_first_run_starts_at_the_earliest_available_change():
    sql = _proc()._build_cdc_query("sqlserver", "customers", "*")
    assert "sys.fn_cdc_get_min_lsn(" in sql


def test_subsequent_runs_resume_after_the_last_consumed_change():
    """`smallest greater than`, not `largest less than or equal` — resuming AT the
    last consumed LSN would replay a change that was already merged."""
    sql = _proc()._build_cdc_query("sqlserver", "customers", "*", watermark_iso="2026-08-29T00:00:00+00:00")
    assert "smallest greater than" in sql
    assert "2026-08-29T00:00:00+00:00" in sql


def test_capture_instance_defaults_to_the_sql_server_convention():
    """SQL Server names the capture instance `schema_table` by default."""
    sql = _proc()._build_cdc_query("sqlserver", "dbo.customers", "*")
    assert "cdc.fn_cdc_get_all_changes_dbo_customers" in sql


def test_explicit_capture_instance_is_honoured():
    sql = _proc()._build_cdc_query("sqlserver", "dbo.customers", "*", capture_instance="my_capture")
    assert "cdc.fn_cdc_get_all_changes_my_capture" in sql


def test_changes_are_ordered_by_lsn():
    """Out-of-order application would merge an older change over a newer one."""
    assert "ORDER BY __$start_lsn" in _proc()._build_cdc_query("sqlserver", "c", "*")


@pytest.mark.parametrize("alias", ["sqlserver", "mssql", "azuresql", "azure_sql"])
def test_azure_sql_aliases_all_resolve(alias):
    """Azure SQL IS SQL Server; the name used in the contract should not matter."""
    assert "fn_cdc_get_all_changes" in _proc()._build_cdc_query(alias, "c", "*")


# ── refusals: never silently degrade to a watermark poll ─────────────────────


def test_postgres_is_refused_with_the_reason_and_the_alternative():
    """Postgres changes live in a replication slot, not a queryable table. Falling
    back to a watermark query would LOOK like CDC and miss every delete — so it is
    refused, and the message names the route that does work."""
    with pytest.raises(NotImplementedError) as exc:
        _proc()._build_cdc_query("postgres", "customers", "*")
    msg = str(exc.value)
    assert "replication slot" in msg
    assert "load_mode: cdc" in msg  # the path that DOES handle it


def test_an_unknown_provider_is_refused_by_name():
    with pytest.raises(NotImplementedError) as exc:
        _proc()._build_cdc_query("oracle", "customers", "*")
    assert "oracle" in str(exc.value)


# ── operation normalisation (executed, not asserted on strings) ──────────────


def test_sqlserver_operation_codes_become_insert_update_delete():
    """Downstream merge/soft-delete must not know about `__$operation`."""
    df = pl.DataFrame({"id": [1, 2, 3], "_lakelogic_cdc_op_raw": [2, 4, 1]})
    out = _proc()._normalise_cdc_ops(df, "sqlserver")

    assert out["_lakelogic_cdc_op"].to_list() == ["insert", "update", "delete"]
    assert "_lakelogic_cdc_op_raw" not in out.columns  # raw code is consumed


def test_the_delete_survives_normalisation():
    """The single most important row in this file: a delete reaching the merge path
    is the entire reason native CDC exists."""
    df = pl.DataFrame({"id": [7], "_lakelogic_cdc_op_raw": [1]})
    out = _proc()._normalise_cdc_ops(df, "sqlserver")
    assert out["_lakelogic_cdc_op"].to_list() == ["delete"]


def test_postgres_action_codes_are_mapped():
    df = pl.DataFrame({"id": [1, 2, 3], "_lakelogic_cdc_op_raw": ["I", "U", "D"]})
    out = _proc()._normalise_cdc_ops(df, "postgres")
    assert out["_lakelogic_cdc_op"].to_list() == ["insert", "update", "delete"]


def test_a_frame_without_cdc_columns_is_untouched():
    """Normalisation runs on every database read, so a non-CDC load must pass
    through unchanged rather than gaining a spurious column."""
    df = pl.DataFrame({"id": [1, 2]})
    out = _proc()._normalise_cdc_ops(df, "sqlserver")
    assert out.columns == ["id"]


def test_no_provider_means_no_transformation():
    df = pl.DataFrame({"id": [1], "_lakelogic_cdc_op_raw": [2]})
    out = _proc()._normalise_cdc_ops(df, None)
    assert "_lakelogic_cdc_op_raw" in out.columns
