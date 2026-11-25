"""Schema alignment on the delta-rs path reads Spark type NAMES without importing pyspark.

It called `spark_type_object` (which imports pyspark), so a pandas / Polars / DuckDB install
without pyspark saw a contract with no types: declared columns were not added, type changes did
not stop the run and `lakelogic migrate` changed nothing, all silently.
"""

import pytest

from lakelogic.core.types import spark_type_name


@pytest.mark.parametrize(
    "contract_type, expected",
    [
        ("string", "string"), ("int", "int"), ("integer", "int"), ("long", "bigint"),
        ("short", "smallint"), ("byte", "tinyint"), ("double", "double"), ("float", "float"),
        ("boolean", "boolean"), ("date", "date"), ("timestamp", "timestamp"),
        ("timestamp_ntz", "timestamp"), ("binary", "binary"),
    ],
)
def test_names_match_spark_without_pyspark(contract_type, expected):
    assert spark_type_name(contract_type) == expected


def test_matches_pyspark_simple_string_when_pyspark_is_installed():
    st = pytest.importorskip("pyspark.sql.types")  # noqa: F841 - only run where Spark exists
    from lakelogic.core.types import spark_type_object

    for t in ("string", "int", "bigint", "double", "boolean", "date", "timestamp", "binary"):
        assert spark_type_name(t) == spark_type_object(t).simpleString()
