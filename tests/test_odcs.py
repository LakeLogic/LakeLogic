from lakelogic.core.models import DataContract, to_odcs


# ─────────────────────────────────────────────────────────────────────────────
# Full ODCS v3.x compliance
# ─────────────────────────────────────────────────────────────────────────────

REAL_ODCS_V3 = {
    "apiVersion": "v3.0.2",
    "kind": "DataContract",
    "id": "urn:lakelogic:silver:customers",
    "name": "customers",
    "version": "2.1.0",
    "status": "active",
    "tenant": "acme",
    "domain": "customer",
    "dataProduct": "customer-360",
    "tags": ["gold-source", "gdpr"],
    "description": {
        "purpose": "Curated customer master.",
        "usage": "Serves the customer-360 product.",
        "limitations": "EU customers only.",
    },
    "team": [
        {"username": "dana.owner@acme.io", "role": "owner"},
        {"username": "sam.steward@acme.io", "role": "steward"},
    ],
    "slaProperties": [
        {"property": "frequency", "value": 1, "unit": "d"},
        {"property": "retention", "value": 90, "unit": "d"},
    ],
    "servers": [
        {
            "server": "prod-adls",
            "type": "azure",
            "environment": "prod",
            "location": "abfss://silver@acme.dfs.core.windows.net/customers",
            "format": "delta",
        }
    ],
    "schema": [
        {
            "name": "customers",
            "physicalName": "silver_customers",
            "physicalType": "table",
            "description": "One row per customer.",
            "properties": [
                {
                    "name": "customer_id",
                    "logicalType": "integer",
                    "required": True,
                    "primaryKey": True,
                    "primaryKeyPosition": 1,
                    "description": "Surrogate key.",
                },
                {
                    "name": "email",
                    "logicalType": "string",
                    "required": True,
                    "classification": "PII",
                    "quality": [
                        {
                            "type": "library",
                            "rule": "nullCount",
                            "mustBe": 0,
                            "description": "email not null",
                        }
                    ],
                },
                {
                    "name": "status",
                    "logicalType": "string",
                    "quality": [
                        {
                            "type": "library",
                            "rule": "validValues",
                            "validValues": ["active", "churned", "prospect"],
                            "description": "status domain",
                        }
                    ],
                },
                {
                    "name": "lifetime_value",
                    "logicalType": "number",
                    "classification": "confidential",
                },
                {
                    "name": "signup_date",
                    "logicalType": "date",
                    "partitioned": True,
                    "partitionKeyPosition": 1,
                },
            ],
            "quality": [
                {
                    "type": "sql",
                    "query": "SELECT COUNT(*) FROM silver_customers",
                    "mustBeGreaterThan": 0,
                    "description": "table not empty",
                },
                {
                    "type": "sql",
                    "query": "lifetime_value >= 0",
                    "description": "ltv non-negative",
                },
            ],
        }
    ],
    "customProperties": {
        "lakelogic": {
            "tier": "silver",
            "source": {"type": "file", "path": "abfss://bronze/customers", "format": "parquet"},
            "materialization": {"strategy": "merge"},
        }
    },
}


def test_odcs_v3_full_import():
    """A real ODCS v3.x contract imports into an executable LakeLogic contract."""
    contract = DataContract(**REAL_ODCS_V3)

    # Fundamentals: version is the CONTRACT version (not apiVersion); spec preserved.
    assert contract.version == "2.1.0"
    assert contract.metadata["odcs_api_version"] == "v3.0.2"
    assert contract.info.title == "customers"
    assert contract.info.domain == "customer"
    assert "Curated customer master" in contract.info.description
    # team[] → owner (first member's username)
    assert contract.info.owner == "dana.owner@acme.io"

    # schema[].properties[] → model.fields with logicalType mapping
    names = [f.name for f in contract.model.fields]
    assert names == ["customer_id", "email", "status", "lifetime_value", "signup_date"]
    by_name = {f.name: f for f in contract.model.fields}
    assert by_name["customer_id"].type == "integer"
    assert by_name["customer_id"].required is True
    assert by_name["lifetime_value"].type == "double"  # number → double
    assert by_name["signup_date"].type == "timestamp"  # date → timestamp

    # primaryKey → ordered primary_key
    assert contract.primary_key == ["customer_id"]

    # classification → PII / sensitive + masking default
    assert by_name["email"].pii is True
    assert by_name["email"].masking is not None
    assert by_name["lifetime_value"].sensitive is True

    # partitioned → materialization.partition_by (merged with custom override)
    assert contract.materialization.partition_by == ["signup_date"]
    assert contract.materialization.strategy == "merge"  # from customProperties.lakelogic

    # quality: property library nullCount → row NOT NULL rule
    row_sql = [getattr(r, "sql", None) for r in contract.quality.row_rules]
    assert any("email IS NOT NULL" in (s or "") for s in row_sql)
    # validValues → IN (...) row rule
    assert any("status IN (" in (s or "") for s in row_sql)
    # schema sql predicate → row rule
    assert any("lifetime_value >= 0" in (s or "") for s in row_sql)
    # schema sql aggregate w/ operator → dataset rule with threshold
    ds = contract.quality.dataset_rules
    assert any(getattr(r, "must_be_greater_than", None) == 0 for r in ds)

    # slaProperties frequency → service_levels.freshness
    assert contract.service_levels.freshness == "1d"

    # customProperties.lakelogic execution context wins
    assert contract.tier == "silver"
    assert contract.source.path == "abfss://bronze/customers"
    assert contract.source.format == "parquet"


def test_odcs_multiple_schema_objects_selects_matching():
    """With multiple schema objects, LakeLogic picks the one matching name/id."""
    payload = {
        "apiVersion": "v3.0.2",
        "kind": "DataContract",
        "name": "orders",
        "version": "1.0.0",
        "schema": [
            {"name": "customers", "properties": [{"name": "customer_id", "logicalType": "integer"}]},
            {"name": "orders", "properties": [{"name": "order_id", "logicalType": "integer"}]},
        ],
    }
    contract = DataContract(**payload)
    assert [f.name for f in contract.model.fields] == ["order_id"]


def test_odcs_parser_intercepts_and_converts():
    """Test that a pure ODCS dictionary is successfully parsed into a LakeLogic DataContract."""
    odcs_payload = {
        "kind": "DataContract",
        "apiVersion": "v3.1.0",
        "dataset": "customers_silver",
        "schema": [
            {"name": "customer_id", "type": "integer", "required": True},
            {"name": "email", "type": "string", "pii": True},
            {"name": "created_at", "type": "timestamp"},
        ],
        "customProperties": {
            "lakelogic": {
                "tier": "silver",
                "source": {"type": "file", "path": "s3://bronze/customers", "format": "parquet"},
                "materialization": {"strategy": "merge"},
            }
        },
    }

    contract = DataContract(**odcs_payload)

    # Assert ODCS fields mapped beautifully
    assert contract.version == "v3.1.0"
    assert contract.info.title == "customers_silver"
    assert contract.tier == "silver"

    # Assert schema mapped to model.fields
    assert contract.model is not None
    assert len(contract.model.fields) == 3
    assert contract.model.fields[0].name == "customer_id"
    assert contract.model.fields[0].type == "integer"
    assert contract.model.fields[0].required is True
    assert contract.model.fields[1].name == "email"
    assert contract.model.fields[1].pii is True
    assert contract.model.fields[2].name == "created_at"

    # Assert LakeLogic pipeline specifics were lifted out of customProperties
    assert contract.source.path == "s3://bronze/customers"
    assert contract.source.format == "parquet"
    assert contract.materialization.strategy == "merge"


def test_lakelogic_native_skips_interceptor():
    """Test that standard LakeLogic contracts bypass the ODCS parser."""
    native_payload = {
        "version": "1.0",
        "info": {"title": "lakelogic-native"},
        "tier": "bronze",
        "dataset": "test-set",  # Even with 'dataset' present, lack of `kind: DataContract` halts parser
        "model": {"fields": [{"name": "id", "type": "integer"}]},
    }

    contract = DataContract(**native_payload)
    assert contract.version == "1.0"
    assert contract.info.title == "lakelogic-native"
    assert contract.tier == "bronze"


def test_odcs_partial_payload():
    """Test ODCS payload with missing schema or missing extension blocks."""
    odcs_payload = {"kind": "DataContract", "apiVersion": "v1.2", "dataset": "headless_contract"}

    # Because there are missing parts, Pydantic should still try its best,
    # but since LakeLogic allows no tier/source in libraries, it should parse.
    contract = DataContract(**odcs_payload)
    assert contract.version == "v1.2"
    assert contract.info.title == "headless_contract"
    assert contract.tier is None
    assert contract.model is None


def test_odcs_legacy_docs_example_still_parses():
    """The simplified example documented in docs/odcs.md must remain executable."""
    docs_example = {
        "kind": "DataContract",
        "apiVersion": "v3.1.0",
        "dataset": "customers",
        "schema": [
            {"name": "id", "type": "integer", "required": True, "description": "Primary customer ID"},
            {"name": "email", "type": "string", "pii": True, "required": True},
        ],
        "customProperties": {
            "lakelogic": {
                "tier": "silver",
                "source": {"type": "file", "path": "s3://landing/customers/", "format": "parquet"},
                "materialization": {
                    "strategy": "merge",
                    "primary_key": ["id"],
                    "target_path": "silver.customers",
                },
                "quality": {"row_rules": [{"name": "email_format", "sql": "email LIKE '%@%.%'"}]},
            }
        },
    }
    contract = DataContract(**docs_example)
    assert contract.info.title == "customers"
    assert contract.version == "v3.1.0"
    assert contract.tier == "silver"
    assert [f.name for f in contract.model.fields] == ["id", "email"]
    assert contract.model.fields[1].pii is True
    assert contract.source.path == "s3://landing/customers/"
    assert contract.materialization.strategy == "merge"
    # customProperties.lakelogic.quality preserved
    assert any("email LIKE" in getattr(r, "sql", "") for r in contract.quality.row_rules)


def test_odcs_roundtrip_export_and_reimport():
    """LakeLogic → to_odcs() → dict is valid ODCS and re-imports equivalently."""
    original = DataContract(**REAL_ODCS_V3)
    doc = original.to_odcs()

    # Valid ODCS v3.x shape
    assert doc["apiVersion"].startswith("v3")
    assert doc["kind"] == "DataContract"
    assert doc["id"]
    assert doc["name"] == "customers"
    assert doc["version"] == "2.1.0"
    assert isinstance(doc["schema"], list) and doc["schema"]
    props = doc["schema"][0]["properties"]
    prop_names = [p["name"] for p in props]
    assert prop_names == ["customer_id", "email", "status", "lifetime_value", "signup_date"]
    # reverse type mapping
    cid = next(p for p in props if p["name"] == "customer_id")
    assert cid["logicalType"] == "integer"
    assert cid["primaryKey"] is True
    # PII → classification + criticalDataElement
    email = next(p for p in props if p["name"] == "email")
    assert email["classification"]
    assert email["criticalDataElement"] is True
    # execution context carried for round-trip
    assert doc["customProperties"]["lakelogic"]["tier"] == "silver"
    assert doc["customProperties"]["lakelogic"]["source"]["path"] == "abfss://bronze/customers"

    # Re-import yields an equivalent executable contract
    reimported = DataContract(**doc)
    assert reimported.version == original.version
    assert reimported.info.title == original.info.title
    assert reimported.tier == original.tier
    assert [f.name for f in reimported.model.fields] == [f.name for f in original.model.fields]
    assert reimported.primary_key == original.primary_key
    assert reimported.source.path == original.source.path
    # PII flag survives the round trip
    reimported_by_name = {f.name: f for f in reimported.model.fields}
    assert reimported_by_name["email"].pii is True
    # module-level to_odcs() matches the method
    assert to_odcs(original)["name"] == doc["name"]
