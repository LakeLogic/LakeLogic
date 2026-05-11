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
