"""Delta reads must not go through polars' broken Delta bridge.

``pl.read_delta()`` and ``pl.scan_delta()`` both raise on the versions this package
still supports — e.g. polars 1.40.1 with deltalake 1.6.2, where polars itself declares
``deltalake>=1.0.0``::

    TypeError: 'deltalake._internal.Schema' object is not iterable

deltalake is fine; ``DeltaTable.to_pyarrow_table()`` works. Only polars' bridge is
broken, so every read goes deltalake -> Arrow -> polars instead. Newer pairs (polars
1.44.1 / deltalake 1.6.3) have it fixed, so the two "is it still broken" tests probe
and skip rather than assert — see ``_probe_bridge``.

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


def _probe_bridge(call, entry_point: str):
    """Report whether polars' Delta bridge is still broken on the installed pair.

    Kept as a probe rather than a hard assertion: it is version-dependent, and this
    package supports a range. Broken -> the failure mode is pinned, so the reason
    delta_compat exists stays documented. Fixed -> skip with a note, so the news
    reaches whoever reads the run instead of turning CI red for good behaviour.
    """
    try:
        call()
    except TypeError as exc:
        assert "not iterable" in str(exc), f"pl.{entry_point} broke differently than pinned: {exc}"
    else:
        pytest.skip(
            f"pl.{entry_point} works on polars {pl.__version__} / deltalake "
            f"{_deltalake_version()} — delta_compat's Arrow hop is no longer needed "
            "for this pair, and the indirection can go once the supported floor "
            "moves past the broken versions."
        )


def _deltalake_version() -> str:
    import deltalake

    return getattr(deltalake, "__version__", "unknown")


def test_the_polars_bridge_is_genuinely_broken(delta_table):
    """Pins WHY this module exists, on the versions where the bridge is broken."""
    _probe_bridge(lambda: pl.read_delta(delta_table), "read_delta")


def test_scan_delta_is_broken_too(delta_table):
    """Both entry points fail together, so there is no lazy escape hatch either."""
    _probe_bridge(lambda: pl.scan_delta(delta_table).collect(), "scan_delta")


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
