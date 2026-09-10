"""Why deletion vectors are written OFF, pinned so nobody has to re-derive it.

The framework forces ``delta.enableDeletionVectors = false`` in four places (``core/ddl``,
two writes in ``core/materialization``, the session default in ``pipeline/runner``). The
reason recorded there for years — "delta-rs (Polars/DuckDB) cannot read such tables" — is
no longer true, and that is the hazard these tests exist to defuse: anyone who re-checks
the ORIGINAL claim sees Polars read a deletion-vector table happily and concludes the
guard is dead weight.

It is not. The live blocker moved:

    pl.read_delta / duckdb delta_scan     read DV tables fine (deltalake 1.6.3)
    DeltaTable.to_pyarrow_table()         still cannot — "requires reader feature
                                          'deletionVectors' ... not supported using
                                          pyarrow Datasets"

and the framework reads through ``to_pyarrow_table`` / ``to_pyarrow_dataset`` in roughly
seventeen places, of which exactly one (``core/delta_compat``) falls back. So a DV table
is readable by the one path that has a fallback and unreadable by the other sixteen.

THE PROBE, AND WHY IT SKIPS RATHER THAN FAILS. Whether the Arrow route works is a property
of the installed deltalake, and this package supports a range. Broken -> pin the failure
mode, so the justification stays evidenced rather than asserted. Fixed -> skip with the
news, so the exit criterion reaches whoever reads the run instead of turning CI red for an
upstream improvement. Same idiom as ``_probe_bridge`` in ``test_delta_compat``.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("deltalake")
pa = pytest.importorskip("pyarrow")


def _deltalake_version() -> str:
    import deltalake

    return getattr(deltalake, "__version__", "unknown")


@pytest.fixture()
def dv_table(tmp_path):
    """A Delta table that actually carries a materialised deletion vector.

    Enabling the property is not enough — the reader feature only bites once a delete has
    written a vector, so this deletes rows and returns the path plus the expected count.
    """
    from deltalake import DeltaTable, write_deltalake

    path = str(tmp_path / "dv")
    write_deltalake(
        path,
        pa.table({"id": list(range(10))}),
        configuration={"delta.enableDeletionVectors": "true"},
    )
    DeltaTable(path).delete("id < 4")

    features = DeltaTable(path).protocol().reader_features or []
    if "deletionVectors" not in features:
        pytest.skip(
            f"deltalake {_deltalake_version()} did not advertise the deletionVectors "
            f"reader feature after a delete (got {features!r}), so there is nothing to "
            "probe on this version."
        )
    return path, 6


def test_the_framework_reader_survives_a_deletion_vector_table(dv_table):
    """`delta_compat.read_delta` must return the right rows, by whichever route.

    This is the ONE read path with a fallback, and it is why a DV table produced by an
    external writer (Databricks, say) does not take the whole run down.
    """
    from lakelogic.core.delta_compat import read_delta

    path, expected = dv_table
    assert read_delta(path).height == expected


def test_polars_reads_deletion_vectors_so_the_old_reason_is_stale(dv_table):
    """Pins the fact that makes the original comment misleading.

    If this ever fails, the ORIGINAL justification is live again and the comments in
    `core/materialization` should say so.
    """
    path, expected = dv_table
    assert pl.read_delta(path).height == expected


def test_the_arrow_route_is_what_still_blocks_enabling_them(dv_table):
    """The actual, current reason deletion vectors stay off.

    Fails-to-read -> pin it. Reads -> skip with the exit criterion, because at that point
    the remaining question is only whether the ~16 unprotected `to_pyarrow_*` callers and
    the `deltalake` floor agree.
    """
    from deltalake import DeltaTable

    path, expected = dv_table
    try:
        rows = DeltaTable(path).to_pyarrow_table().num_rows
    except Exception as exc:
        assert "deletionVectors" in str(exc) or "deletion" in str(exc).lower(), (
            f"to_pyarrow_table failed for an unexpected reason: {exc}"
        )
        return

    assert rows == expected
    pytest.skip(
        f"DeltaTable.to_pyarrow_table() now reads deletion vectors on deltalake "
        f"{_deltalake_version()}. The write-time disable in core/materialization, "
        "core/ddl and pipeline/runner may be removable — check the remaining "
        "to_pyarrow_table/to_pyarrow_dataset call sites and raise the deltalake floor "
        "in pyproject past the first version with this support before doing so."
    )
