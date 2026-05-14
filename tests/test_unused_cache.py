"""Tests for the AST-based unused-.cache() / .persist() detector."""

from __future__ import annotations

from pathlib import Path

from lakelogic.ai.tier1_runners import scan_unused_cache


def _write(p: Path, src: str) -> Path:
    p.write_text(src, encoding="utf-8")
    return p


def test_flags_cache_never_used(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "j.py",
        "df = spark.read.parquet('s3://x').cache()\n",
    )
    findings = scan_unused_cache([f])
    assert len(findings) == 1
    assert findings[0].rule == "unused_cache"
    assert findings[0].severity == "warning"


def test_flags_cache_used_once_as_info(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "j.py",
        "df = spark.read.parquet('s3://x').cache()\ndf.write.parquet('s3://out')\n",
    )
    findings = scan_unused_cache([f])
    assert len(findings) == 1
    assert findings[0].severity == "info"


def test_does_not_flag_cache_reused_multiple_times(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "j.py",
        "df = spark.read.parquet('s3://x').cache()\na = df.filter('x > 0').count()\nb = df.filter('x < 0').count()\n",
    )
    assert scan_unused_cache([f]) == []


def test_handles_persist_same_as_cache(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "j.py",
        "df = spark.read.parquet('s3://x').persist()\n",
    )
    findings = scan_unused_cache([f])
    assert findings and findings[0].rule == "unused_persist"


def test_ignores_chained_cache_without_assignment(tmp_path: Path) -> None:
    """`df.filter(...).cache().count()` doesn't create an unused binding — skip."""
    f = _write(
        tmp_path / "j.py",
        "spark.read.parquet('s3://x').cache().count()\n",
    )
    assert scan_unused_cache([f]) == []


def test_ignores_files_with_syntax_errors(tmp_path: Path) -> None:
    f = _write(tmp_path / "broken.py", "def oops(:\n")
    assert scan_unused_cache([f]) == []


def test_ignores_non_python_files(tmp_path: Path) -> None:
    f = _write(tmp_path / "x.sql", "SELECT 1;\n")
    assert scan_unused_cache([f]) == []


# ---------------------------------------------------------------------------
# Function-scoped use counting (regression for the cross-function name
# collision bug that previously hid unused-cache findings).
# ---------------------------------------------------------------------------


def test_use_count_is_scoped_to_enclosing_function(tmp_path: Path) -> None:
    """A cache used once in func A must still be flagged even if a SAME-NAMED
    variable in func B is read many times. Pre-fix, the AST walker counted
    references across the whole module so func B's reads silently inflated
    func A's count and the finding disappeared."""
    f = _write(
        tmp_path / "j.py",
        "def func_a(spark):\n"
        "    df = spark.read.parquet('a').cache()\n"  # used once → info
        "    return df.write.parquet('out')\n"
        "\n"
        "def func_b(spark):\n"
        "    df = spark.read.parquet('b').cache()\n"  # reused 3× → no finding
        "    a = df.filter('x > 0').count()\n"
        "    b = df.filter('x < 0').count()\n"
        "    c = df.filter('x = 0').count()\n"
        "    return a, b, c\n",
    )
    findings = scan_unused_cache([f])
    # Exactly one finding: func_a's cache (used once)
    assert len(findings) == 1
    assert findings[0].severity == "info"
    # Anchored at func_a's line, not func_b's
    assert findings[0].line == 2


def test_two_unused_caches_in_two_functions_both_flagged(tmp_path: Path) -> None:
    """Both functions cache and never read — both should be flagged."""
    f = _write(
        tmp_path / "j.py",
        "def func_a(spark):\n"
        "    df = spark.read.parquet('a').cache()\n"
        "    spark.sql('VACUUM').collect()\n"
        "\n"
        "def func_b(spark):\n"
        "    df = spark.read.parquet('b').cache()\n"
        "    spark.sql('VACUUM').collect()\n",
    )
    findings = scan_unused_cache([f])
    assert len(findings) == 2
    assert all(x.severity == "warning" for x in findings)


def test_module_level_cache_still_handled(tmp_path: Path) -> None:
    """Cache assignments at module scope (not inside a function) still get
    counted — the fallback uses the Module node as the scope."""
    f = _write(
        tmp_path / "j.py",
        "import pyspark\n"
        "spark = pyspark.sql.SparkSession.builder.getOrCreate()\n"
        "df = spark.read.parquet('x').cache()\n",  # never read → warning
    )
    findings = scan_unused_cache([f])
    assert len(findings) == 1
    assert findings[0].severity == "warning"
