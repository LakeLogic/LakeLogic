"""
Tests for:
  1. Tier normalization (TIER_CANONICAL_MAP, tier field on DataContract)
  2. TransformationFilter string shorthand
  3. TransformationDeduplicate 'by' alias
  4. sqlglot transpilation (_transpile, _regex_sql)
  5. ENGINE_DIALECT_MAP coverage
  6. Contract validation — tier warnings
"""

import pytest

from lakelogic.core.models import (
    TIER_CANONICAL_MAP,
    TIER_VALID_CANONICAL,
    DataContract,
    TransformationDeduplicate,
    TransformationFilter,
)
from lakelogic.core.schema_api import validate_contract


# ═════════════════════════════════════════════════════════════════════════════
# Tier Normalization
# ═════════════════════════════════════════════════════════════════════════════


class TestTierNormalization:
    """Tier values are normalized to canonical medallion names at parse time."""

    @pytest.mark.parametrize(
        "input_tier, expected",
        [
            # Canonical names pass through
            ("bronze", "bronze"),
            ("silver", "silver"),
            ("gold", "gold"),
            ("reference", "reference"),
            # raw/stage/curated synonyms
            ("raw", "bronze"),
            ("stage", "silver"),
            ("staging", "silver"),
            ("curated", "gold"),
            # landing/cleansed/refined synonyms
            ("landing", "bronze"),
            ("cleansed", "silver"),
            ("refined", "gold"),
            # ingestion/transform/presentation synonyms
            ("ingestion", "bronze"),
            ("ingest", "bronze"),
            ("transform", "silver"),
            ("presentation", "gold"),
            ("consumption", "gold"),
            # Reference synonyms
            ("ref", "reference"),
            ("seed", "reference"),
            ("lookup", "reference"),
            ("masterdata", "reference"),
            ("master_data", "reference"),
            # Case insensitive
            ("BRONZE", "bronze"),
            ("Silver", "silver"),
            ("GOLD", "gold"),
            ("RAW", "bronze"),
            ("Staging", "silver"),
        ],
    )
    def test_tier_normalization(self, input_tier, expected):
        contract = DataContract(version="1.0.0", tier=input_tier)
        assert contract.tier == expected

    def test_tier_none_when_not_provided(self):
        contract = DataContract(version="1.0.0")
        assert contract.tier is None

    def test_tier_custom_passthrough(self):
        """Custom tier names pass through as-is (lowercase)."""
        contract = DataContract(version="1.0.0", tier="platinum")
        assert contract.tier == "platinum"

    def test_tier_whitespace_trimmed(self):
        contract = DataContract(version="1.0.0", tier="  silver  ")
        assert contract.tier == "silver"

    def test_tier_from_layer_alias(self):
        """'layer' key should be accepted as an alias for 'tier'."""
        contract = DataContract.model_validate({"version": "1.0.0", "layer": "gold"})
        assert contract.tier == "gold"

    def test_tier_from_target_layer_alias(self):
        """'target_layer' key should be accepted as an alias for 'tier'."""
        contract = DataContract.model_validate(
            {"version": "1.0.0", "target_layer": "raw"}
        )
        assert contract.tier == "bronze"


class TestTierCanonicalMap:
    """Ensure the TIER_CANONICAL_MAP and TIER_VALID_CANONICAL are consistent."""

    def test_all_map_values_are_valid_canonical(self):
        for key, value in TIER_CANONICAL_MAP.items():
            assert (
                value in TIER_VALID_CANONICAL
            ), f"TIER_CANONICAL_MAP['{key}'] = '{value}' is not in TIER_VALID_CANONICAL"

    def test_canonical_names_map_to_themselves(self):
        for canonical in TIER_VALID_CANONICAL:
            assert (
                TIER_CANONICAL_MAP.get(canonical) == canonical
            ), f"Canonical '{canonical}' should map to itself"


# ═════════════════════════════════════════════════════════════════════════════
# TransformationFilter — String Shorthand
# ═════════════════════════════════════════════════════════════════════════════


class TestTransformationFilterShorthand:
    """TransformationFilter accepts both string and dict forms."""

    def test_string_shorthand(self):
        f = TransformationFilter.model_validate("order_id IS NOT NULL")
        assert f.sql == "order_id IS NOT NULL"

    def test_dict_form(self):
        f = TransformationFilter.model_validate({"sql": "x > 0"})
        assert f.sql == "x > 0"

    def test_complex_sql_string(self):
        sql = "customer_id IS NOT NULL AND customer_unique_id IS NOT NULL"
        f = TransformationFilter.model_validate(sql)
        assert f.sql == sql

    def test_in_full_contract(self):
        """String filter works inside a full contract transformation list."""
        contract = DataContract(
            version="1.0.0",
            transformations=[{"phase": "pre", "filter": "order_id IS NOT NULL"}],
        )
        assert contract.transformations[0].filter.sql == "order_id IS NOT NULL"

    def test_dict_filter_in_contract(self):
        """Dict filter also still works inside a full contract."""
        contract = DataContract(
            version="1.0.0",
            transformations=[
                {"phase": "pre", "filter": {"sql": "order_id IS NOT NULL"}}
            ],
        )
        assert contract.transformations[0].filter.sql == "order_id IS NOT NULL"


# ═════════════════════════════════════════════════════════════════════════════
# TransformationDeduplicate — 'by' Alias
# ═════════════════════════════════════════════════════════════════════════════


class TestTransformationDeduplicateAlias:
    """TransformationDeduplicate accepts both 'on' and 'by' keys."""

    def test_on_keyword(self):
        d = TransformationDeduplicate.model_validate({"on": ["id"]})
        assert d.on == ["id"]

    def test_by_alias(self):
        d = TransformationDeduplicate.model_validate({"by": ["customer_id"]})
        assert d.on == ["customer_id"]

    def test_by_with_sort(self):
        d = TransformationDeduplicate.model_validate(
            {"by": ["id"], "sort_by": ["updated_at"], "order": "desc"}
        )
        assert d.on == ["id"]
        assert d.sort_by == ["updated_at"]
        assert d.order == "desc"

    def test_in_full_contract(self):
        """'by' alias works inside a full contract transformation."""
        contract = DataContract(
            version="1.0.0",
            transformations=[
                {"deduplicate": {"by": ["customer_unique_id"]}}
            ],
        )
        assert contract.transformations[0].deduplicate.on == ["customer_unique_id"]


# ═════════════════════════════════════════════════════════════════════════════
# sqlglot Transpilation
# ═════════════════════════════════════════════════════════════════════════════


class TestTranspilation:
    """Test the _transpile method on EngineAdapter via a DummyAdapter."""

    @pytest.fixture
    def dummy_adapter(self):
        from lakelogic.engines.base import EngineAdapter

        class DummyAdapter(EngineAdapter):
            def execute(self, df):
                raise NotImplementedError

        contract = DataContract(version="1.0.0")
        adapter = DummyAdapter(contract)
        return adapter

    def test_transpile_identity_duckdb(self, dummy_adapter):
        """DuckDB to DuckDB should return the same SQL."""
        dummy_adapter.engine_dialect = "duckdb"
        result = dummy_adapter._transpile("SELECT COUNT(*) FROM t")
        assert "COUNT" in result
        assert "t" in result

    def test_transpile_regex_to_spark(self, dummy_adapter):
        """DuckDB REGEXP_MATCHES should become RLIKE in Spark."""
        dummy_adapter.engine_dialect = "spark"
        result = dummy_adapter._transpile("REGEXP_MATCHES(x, 'abc')")
        assert "RLIKE" in result.upper() or "REGEXP" in result.upper()

    def test_transpile_regex_to_bigquery(self, dummy_adapter):
        """DuckDB REGEXP_MATCHES should become REGEXP_CONTAINS in BigQuery."""
        dummy_adapter.engine_dialect = "bigquery"
        result = dummy_adapter._transpile("REGEXP_MATCHES(x, 'abc')")
        assert "REGEXP_CONTAINS" in result.upper()

    def test_transpile_datediff_to_postgres(self, dummy_adapter):
        """DuckDB DATEDIFF should be transpiled to Postgres EXTRACT."""
        dummy_adapter.engine_dialect = "postgres"
        result = dummy_adapter._transpile("DATEDIFF('day', a, b)")
        assert "EXTRACT" in result.upper() or "epoch" in result.lower()

    def test_transpile_returns_string(self, dummy_adapter):
        """Transpile should always return a string, not a list."""
        dummy_adapter.engine_dialect = "mysql"
        result = dummy_adapter._transpile("SELECT 1")
        assert isinstance(result, str)


# ═════════════════════════════════════════════════════════════════════════════
# ENGINE_DIALECT_MAP
# ═════════════════════════════════════════════════════════════════════════════


class TestEngineDialectMap:
    """Ensure ENGINE_DIALECT_MAP covers expected databases."""

    def test_has_major_databases(self):
        from lakelogic.engines.base import ENGINE_DIALECT_MAP

        expected = [
            "duckdb",
            "spark",
            "polars",
            "bigquery",
            "snowflake",
            "postgres",
            "mysql",
            "redshift",
            "clickhouse",
            "sqlserver",
            "oracle",
            "trino",
            "databricks",
        ]
        for db in expected:
            assert db in ENGINE_DIALECT_MAP, f"Missing dialect mapping for '{db}'"

    def test_aliases(self):
        from lakelogic.engines.base import ENGINE_DIALECT_MAP

        # postgresql should map same as postgres
        assert ENGINE_DIALECT_MAP["postgresql"] == ENGINE_DIALECT_MAP["postgres"]
        # mssql should map same as sqlserver
        assert ENGINE_DIALECT_MAP["mssql"] == ENGINE_DIALECT_MAP["sqlserver"]


# ═════════════════════════════════════════════════════════════════════════════
# Contract Validation — Tier Warnings
# ═════════════════════════════════════════════════════════════════════════════


class TestContractValidationTier:
    """validate_contract() should check for tier/layer."""

    def test_missing_tier_warns(self):
        result = validate_contract({"version": "1.0.0"})
        tier_issues = [e for e in result.errors if e.field == "tier"]
        assert len(tier_issues) == 1
        assert tier_issues[0].severity == "warning"
        assert "missing" in tier_issues[0].message.lower()

    def test_valid_tier_no_warning(self):
        result = validate_contract({"version": "1.0.0", "tier": "bronze"})
        tier_issues = [e for e in result.errors if e.field == "tier"]
        assert len(tier_issues) == 0

    def test_tier_synonym_accepted(self):
        result = validate_contract({"version": "1.0.0", "tier": "staging"})
        tier_issues = [e for e in result.errors if e.field == "tier"]
        assert len(tier_issues) == 0

    def test_unknown_tier_warns(self):
        result = validate_contract({"version": "1.0.0", "tier": "foobar"})
        tier_issues = [e for e in result.errors if e.field == "tier"]
        assert len(tier_issues) == 1
        assert tier_issues[0].severity == "warning"

    def test_tier_in_info_accepted(self):
        """Backward compat: tier in info.target_layer should be found."""
        result = validate_contract(
            {
                "version": "1.0.0",
                "info": {"title": "Test", "version": "1.0", "target_layer": "silver"},
            }
        )
        tier_issues = [e for e in result.errors if e.field == "tier"]
        assert len(tier_issues) == 0

    def test_contract_still_valid_without_tier(self):
        """Missing tier is a warning, not an error — contract remains valid."""
        result = validate_contract({"version": "1.0.0"})
        assert result.valid  # warnings don't make it invalid
