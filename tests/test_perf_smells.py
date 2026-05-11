"""Tests for the Tier 1 performance-smell runner."""

from __future__ import annotations

from pathlib import Path

from lakelogic.ai.tier1_runners import scan_perf_smells


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_flags_spark_collect_in_pyspark_file(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "job.py",
        "from pyspark.sql import SparkSession\n"
        "df = spark.read.parquet('s3://x')\n"
        "rows = df.collect()\n",
    )
    findings = scan_perf_smells([f])
    rules = [x.rule for x in findings]
    assert "spark_collect" in rules


def test_ignores_collect_in_non_pyspark_file(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "garbage.py",
        "from gc import collect\n"
        "collect()\n"
        "stats.collect()\n",  # not a Spark DF
    )
    assert not any(x.rule == "spark_collect" for x in scan_perf_smells([f]))


def test_flags_to_pandas(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "job.py",
        "import pyspark\n"
        "pdf = df.toPandas()\n",
    )
    assert any(x.rule == "spark_to_pandas" for x in scan_perf_smells([f]))


def test_flags_count_used_for_boolean(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "job.py",
        "from pyspark.sql import functions as F\n"
        "if df.count() > 0:\n"
        "    do_thing()\n",
    )
    assert any(x.rule == "spark_count_for_bool" for x in scan_perf_smells([f]))


def test_flags_read_csv_without_chunksize(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "load.py",
        "import pandas as pd\n"
        "df = pd.read_csv('huge.csv')\n",
    )
    assert any(x.rule == "pandas_read_csv_no_chunksize" for x in scan_perf_smells([f]))


def test_does_not_flag_read_csv_with_chunksize(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "load.py",
        "import pandas as pd\n"
        "for chunk in pd.read_csv('huge.csv', chunksize=10_000):\n"
        "    process(chunk)\n",
    )
    assert not any(x.rule == "pandas_read_csv_no_chunksize" for x in scan_perf_smells([f]))


def test_skips_commented_out_lines(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "job.py",
        "import pyspark\n"
        "# rows = df.collect()  # disabled for now\n",
    )
    assert not any(x.rule == "spark_collect" for x in scan_perf_smells([f]))


def test_ignores_non_python_files(tmp_path: Path) -> None:
    f = _write(tmp_path / "x.sql", "SELECT * FROM t;\n")
    assert scan_perf_smells([f]) == []
