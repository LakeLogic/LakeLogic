import pytest
from lakelogic.core.models import DataContract

def test_odcs_parser_intercepts_and_converts():
    """Test that a pure ODCS dictionary is successfully parsed into a LakeLogic DataContract."""
    odcs_payload = {
        "kind": "DataContract",
        "apiVersion": "v3.1.0",
        "dataset": "customers_silver",
        "schema": [
            {"name": "customer_id", "type": "integer", "required": True},
            {"name": "email", "type": "string", "pii": True},
            {"name": "created_at", "type": "timestamp"}
        ],
        "customProperties": {
            "lakelogic": {
                "tier": "silver",
                "source": {
                    "type": "file",
                    "path": "s3://bronze/customers",
                    "format": "parquet"
                },
                "materialization": {
                    "strategy": "merge"
                }
            }
        }
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
        "model": {
            "fields": [{"name": "id", "type": "integer"}]
        }
    }
    
    contract = DataContract(**native_payload)
    assert contract.version == "1.0"
    assert contract.info.title == "lakelogic-native"
    assert contract.tier == "bronze"


def test_odcs_partial_payload():
    """Test ODCS payload with missing schema or missing extension blocks."""
    odcs_payload = {
        "kind": "DataContract",
        "apiVersion": "v1.2",
        "dataset": "headless_contract"
    }
    
    # Because there are missing parts, Pydantic should still try its best,
    # but since LakeLogic allows no tier/source in libraries, it should parse.
    contract = DataContract(**odcs_payload)
    assert contract.version == "v1.2"
    assert contract.info.title == "headless_contract"
    assert contract.tier is None
    assert contract.model is None
