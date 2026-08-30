"""Delta reads must not go through polars' broken Delta bridge.

``pl.read_delta()`` and ``pl.scan_delta()`` both raise against the CURRENT supported
combination — polars 1.40.1 with deltalake 1.6.2, where polars itself declares
``deltalake>=1.0.0``::

    TypeError: 'deltalake._internal.Schema' object is not iterable

deltalake is fine; ``DeltaTable.to_pyarrow_table()`` works. Only polars' bridge is
broken, so every read goes deltalake -> Arrow -> polars instead.

This was hit once before at deltalake 0.17.x and worked around inside
``core/slo.py`` alone, while four other modules kept calling ``pl.read_delta``
directly — so it recurred at 1.x. The point of these tests is that there is now ONE
reader and no call site bypasses it.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("deltalake")

from lakelogic.core.delta_compat import is_cloud_path, read_delta


@pytest.fixture
def delta_table(tmp_path):
    from deltalake import write_deltalake

    path = tmp_path / "t"
    write_deltalake(str(path), pl.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]}).to_pandas())
    return str(path)


def test_reads_a_delta_table(delta_table):
    df = read_delta(delta_table)
    assert df.height == 3
    assert set(df.columns) == {"id", "v"}


def test_the_polars_bridge_is_genuinely_broken(delta_table):
    """Pins WHY this module exists. If polars ever fixes read_delta this fails, and
    whoever sees it can delete the indirection — rather than it surviving forever as
    unexplained defensive code."""
    with pytest.raises(TypeError, match="not iterable"):
        pl.read_delta(delta_table)


def test_scan_delta_is_broken_too(delta_table):
    """Both entry points fail, so there is no lazy escape hatch either."""
    with pytest.raises(TypeError, match="not iterable"):
        pl.scan_delta(delta_table).collect()


def test_returns_a_polars_frame_not_arrow(delta_table):
    """Callers expect polars — the Arrow hop is an implementation detail."""
    assert isinstance(read_delta(delta_table), pl.DataFrame)


@pytest.mark.parametrize(
    "path,cloud",
    [
        ("abfss://c@a.dfs.core.windows.net/t", True),
        ("s3://bucket/t", True),
        ("gs://bucket/t", True),
        ("/local/path/t", False),
        ("C:\\data\\t", False),
    ],
)
def test_cloud_paths_are_recognised(path, cloud):
    """Cloud paths take the SAME Arrow route. The previous helper fell back to
    pl.read_delta for them, so it stayed broken exactly where data is biggest."""
    assert is_cloud_path(path) is cloud


# ── no call site bypasses the shared reader ──────────────────────────────────


def test_no_core_module_calls_pl_read_delta_directly():
    """The regression that produced this bug twice: one module is fixed, the others
    keep calling polars directly, and the next version bump breaks them again."""
    import ast
    import pathlib

    # Parse rather than grep: `materialization.py` DOCUMENTS the behaviour in a
    # docstring ("...so that pl.read_delta(root) works"), which a text search reads
    # as a call. A test that fails on its own documentation is the same trap as the
    # Spark hash-guard assertion earlier in this suite.
    root = pathlib.Path(__file__).resolve().parents[1] / "lakelogic"
    offenders = []
    for py in root.rglob("*.py"):
        if py.name == "delta_compat.py":  # the one place allowed to call it
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            if (
                isinstance(fn, ast.Attribute)
                and fn.attr in ("read_delta", "scan_delta")
                and isinstance(fn.value, ast.Name)
                and fn.value.id in ("pl", "polars")
            ):
                offenders.append(f"{py.relative_to(root)}:{node.lineno}")

    assert not offenders, (
        "these call polars' broken Delta bridge directly; use "
        "lakelogic.core.delta_compat.read_delta:\n  " + "\n  ".join(offenders)
    )
