"""Tests for the additional Tier 1 perf checks: iterrows, coalesce(1),
withColumn-in-loop, show()/printSchema(), glob on cloud URLs."""

from __future__ import annotations

from pathlib import Path

from lakelogic.ai.tier1_runners import scan_perf_smells, scan_withcolumn_in_loop


def _w(p: Path, src: str) -> Path:
    p.write_text(src, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# pandas iterrows / itertuples
# ---------------------------------------------------------------------------


def test_flags_iterrows(tmp_path: Path) -> None:
    f = _w(tmp_path / "p.py", "for i, row in df.iterrows():\n    print(row)\n")
    assert any(x.rule == "pandas_iterrows" for x in scan_perf_smells([f]))


def test_flags_itertuples(tmp_path: Path) -> None:
    f = _w(tmp_path / "p.py", "for r in df.itertuples():\n    pass\n")
    assert any(x.rule == "pandas_iterrows" for x in scan_perf_smells([f]))


# ---------------------------------------------------------------------------
# coalesce(1) / repartition(1) — pyspark only
# ---------------------------------------------------------------------------


def test_flags_coalesce_one_in_pyspark_file(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "j.py",
        "import pyspark\ndf.coalesce(1).write.parquet('out')\n",
    )
    assert any(x.rule == "spark_coalesce_one" for x in scan_perf_smells([f]))


def test_flags_repartition_one(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "j.py",
        "from pyspark.sql import SparkSession\ndf.repartition(1).write.parquet('out')\n",
    )
    assert any(x.rule == "spark_coalesce_one" for x in scan_perf_smells([f]))


def test_does_not_flag_coalesce_one_outside_pyspark(tmp_path: Path) -> None:
    f = _w(tmp_path / "p.py", "x.coalesce(1)\n")
    assert not any(x.rule == "spark_coalesce_one" for x in scan_perf_smells([f]))


# ---------------------------------------------------------------------------
# show() / printSchema() — only outside test files
# ---------------------------------------------------------------------------


def test_flags_show_in_production_file(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "pipeline.py",
        "import pyspark\ndf.show()\n",
    )
    assert any(x.rule == "spark_show_in_prod" for x in scan_perf_smells([f]))


def test_does_not_flag_show_in_test_file(tmp_path: Path) -> None:
    test_dir = tmp_path / "tests"
    test_dir.mkdir()
    f = _w(
        test_dir / "test_pipeline.py",
        "import pyspark\ndf.show()\n",
    )
    assert not any(x.rule == "spark_show_in_prod" for x in scan_perf_smells([f]))


def test_does_not_flag_show_in_test_named_file(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "test_pipeline.py",
        "import pyspark\ndf.printSchema()\n",
    )
    assert not any(x.rule == "spark_show_in_prod" for x in scan_perf_smells([f]))


# ---------------------------------------------------------------------------
# glob.glob on cloud URLs
# ---------------------------------------------------------------------------


def test_flags_glob_on_s3(tmp_path: Path) -> None:
    f = _w(tmp_path / "load.py", "import glob\nfor p in glob.glob('s3://bucket/*.parquet'):\n    pass\n")
    assert any(x.rule == "glob_on_cloud_storage" for x in scan_perf_smells([f]))


def test_flags_iglob_on_abfss(tmp_path: Path) -> None:
    f = _w(tmp_path / "load.py", "import glob\nlist(glob.iglob('abfss://c@a.dfs.core.windows.net/x/*'))\n")
    assert any(x.rule == "glob_on_cloud_storage" for x in scan_perf_smells([f]))


def test_does_not_flag_local_glob(tmp_path: Path) -> None:
    f = _w(tmp_path / "load.py", "import glob\nglob.glob('/tmp/*.csv')\n")
    assert not any(x.rule == "glob_on_cloud_storage" for x in scan_perf_smells([f]))


# ---------------------------------------------------------------------------
# AST: withColumn in for/while loop
# ---------------------------------------------------------------------------


def test_flags_withcolumn_in_for_loop(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "j.py",
        "for col in cols:\n    df = df.withColumn(col, F.upper(F.col(col)))\n",
    )
    findings = scan_withcolumn_in_loop([f])
    assert findings and findings[0].rule == "withcolumn_in_loop"


def test_flags_withcolumn_in_while_loop(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "j.py",
        "while cond:\n    df = df.withColumn('x', F.lit(1))\n    cond = check(df)\n",
    )
    assert scan_withcolumn_in_loop([f])


def test_does_not_flag_withcolumn_outside_loop(tmp_path: Path) -> None:
    f = _w(tmp_path / "j.py", "df = df.withColumn('x', F.lit(1))\n")
    assert scan_withcolumn_in_loop([f]) == []


def test_emits_one_finding_per_loop_not_per_call(tmp_path: Path) -> None:
    f = _w(
        tmp_path / "j.py",
        "for col in cols:\n"
        "    df = df.withColumn(col + '_a', F.lit(1))\n"
        "    df = df.withColumn(col + '_b', F.lit(2))\n",
    )
    assert len(scan_withcolumn_in_loop([f])) == 1


def test_handles_syntax_errors_gracefully(tmp_path: Path) -> None:
    f = _w(tmp_path / "broken.py", "for x in:\n")
    assert scan_withcolumn_in_loop([f]) == []
