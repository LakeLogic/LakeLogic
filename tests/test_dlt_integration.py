"""Tests for the dlt integration layer.

Unit tests (no network, no dlt install required):
- Contract model validation (DltSourceConfig)
- Credential resolution (${ENV_VAR} expansion)
- ImportError guard when dlt is not installed
- Schema reconciliation (extra columns dropped, missing columns fail)

Integration tests (requires dlt + network, CI-gated):
- Marked with @pytest.mark.dlt
"""

import builtins
import json
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# Stable reference to the real module so monkeypatching is robust to other
# tests that stub sys.modules['lakelogic'] (string-path setattr would break).
import lakelogic.adapters.dlt_adapter as _dlt_mod

from lakelogic.core.models import (
    DltEndpointConfig,
    DltSourceConfig,
    SourceConfig,
)

# ─────────────────────────────────────────────────────────────────────────────
# Model validation
# ─────────────────────────────────────────────────────────────────────────────


class TestDltSourceConfig:
    """Validate the DltSourceConfig Pydantic model."""

    def test_verified_source_mode(self):
        cfg = DltSourceConfig(
            source="stripe_analytics",
            resource="charges",
            credentials={"api_key": "${STRIPE_API_KEY}"},
        )
        assert cfg.source == "stripe_analytics"
        assert cfg.resource == "charges"
        assert cfg.credentials["api_key"] == "${STRIPE_API_KEY}"

    def test_rest_api_mode(self):
        cfg = DltSourceConfig(
            base_url="https://api.example.com/v1/",
            endpoints=[
                DltEndpointConfig(name="users", path="users", params={"limit": 100}),
            ],
        )
        assert cfg.base_url == "https://api.example.com/v1/"
        assert len(cfg.endpoints) == 1
        assert cfg.endpoints[0].name == "users"

    def test_requires_source_or_base_url(self):
        with pytest.raises(ValueError, match="source.*base_url"):
            DltSourceConfig()

    def test_defaults(self):
        cfg = DltSourceConfig(source="chess")
        assert cfg.write_disposition == "replace"
        assert cfg.max_table_nesting == 1
        assert cfg.credentials == {}


class TestSourceConfigDlt:
    """Verify SourceConfig accepts type='dlt' and wires DltSourceConfig."""

    def test_dlt_type_with_nested_config(self):
        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(source="chess"),
        )
        assert src.type == "dlt"
        assert src.dlt is not None
        assert src.dlt.source == "chess"

    def test_dlt_type_without_config(self):
        """type=dlt without dlt block should work (dlt field is optional)."""
        src = SourceConfig(type="dlt")
        assert src.dlt is None

    def test_known_keys_includes_dlt(self):
        """Ensure 'dlt' is accepted without unknown-key warnings."""
        # If 'dlt' were not in the known keys set, the SourceConfig
        # model_validator would emit a warning. We verify no warning
        # is emitted when creating a SourceConfig with a dlt block.
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            SourceConfig(
                type="dlt",
                dlt=DltSourceConfig(source="chess"),
            )
            # No unknown-key warnings should have been raised
            dlt_warnings = [x for x in w if "dlt" in str(x.message)]
            assert len(dlt_warnings) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Credential resolution
# ─────────────────────────────────────────────────────────────────────────────


class TestCredentialResolution:
    """Test ${ENV_VAR} expansion in dlt credentials."""

    def test_resolve_env_var(self):
        from lakelogic.adapters.dlt_adapter import _resolve_env_value

        with patch.dict(os.environ, {"MY_API_KEY": "secret123"}):
            assert _resolve_env_value("${MY_API_KEY}") == "secret123"

    def test_resolve_literal_passthrough(self):
        from lakelogic.adapters.dlt_adapter import _resolve_env_value

        assert _resolve_env_value("raw_value") == "raw_value"

    def test_resolve_missing_env_var(self):
        from lakelogic.adapters.dlt_adapter import _resolve_env_value

        # Should return None when env var is not set
        with patch.dict(os.environ, {}, clear=True):
            result = _resolve_env_value("${NONEXISTENT_VAR}")
            assert result is None

    def test_resolve_none(self):
        from lakelogic.adapters.dlt_adapter import _resolve_env_value

        assert _resolve_env_value(None) is None


# ─────────────────────────────────────────────────────────────────────────────
# DltAdapter unit tests (mocked, no dlt required)
# ─────────────────────────────────────────────────────────────────────────────


class TestDltAdapter:
    """Unit tests for DltAdapter (dlt mocked)."""

    def test_adapter_init(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(source="chess"),
        )
        adapter = DltAdapter(src, "test_contract")
        assert adapter.cfg.source == "chess"

    def test_adapter_raises_without_dlt_block(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(type="dlt")
        with pytest.raises(ValueError, match="SourceConfig.dlt is None"):
            DltAdapter(src, "test_contract")

    def test_build_auth_api_key(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com",
                endpoints=[DltEndpointConfig(name="test", path="test")],
            ),
        )
        adapter = DltAdapter(src, "test")
        auth = adapter._build_auth({"api_key": "sk_test_123"})
        assert auth["type"] == "api_key"
        assert auth["api_key"] == "sk_test_123"

    def test_build_auth_bearer(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com",
                endpoints=[DltEndpointConfig(name="test", path="test")],
            ),
        )
        adapter = DltAdapter(src, "test")
        auth = adapter._build_auth({"token": "Bearer xyz"})
        assert auth["type"] == "bearer"

    def test_build_auth_basic(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com",
                endpoints=[DltEndpointConfig(name="test", path="test")],
            ),
        )
        adapter = DltAdapter(src, "test")
        auth = adapter._build_auth({"username": "admin", "password": "pass"})
        assert auth["type"] == "http_basic"

    def test_build_auth_empty(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com",
                endpoints=[DltEndpointConfig(name="test", path="test")],
            ),
        )
        adapter = DltAdapter(src, "test")
        assert adapter._build_auth({}) is None

    def test_build_resources(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com",
                endpoints=[
                    DltEndpointConfig(
                        name="users",
                        path="users",
                        params={"limit": 50},
                        paginator="json_link",
                    ),
                    DltEndpointConfig(name="orders", path="orders"),
                ],
            ),
        )
        adapter = DltAdapter(src, "test")
        resources = adapter._build_resources()
        assert len(resources) == 2
        assert resources[0]["name"] == "users"
        assert resources[0]["endpoint"]["path"] == "users"
        assert resources[0]["endpoint"]["params"] == {"limit": 50}
        assert resources[0]["endpoint"]["paginator"] == "json_link"
        assert resources[1]["name"] == "orders"

    def test_build_resources_raises_when_empty(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com",
            ),
        )
        adapter = DltAdapter(src, "test")
        with pytest.raises(ValueError, match="at least one endpoint"):
            adapter._build_resources()

    def test_resolve_credentials(self):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        with patch.dict(os.environ, {"STRIPE_KEY": "sk_live_xyz"}):
            src = SourceConfig(
                type="dlt",
                dlt=DltSourceConfig(
                    source="stripe",
                    credentials={
                        "api_key": "${STRIPE_KEY}",
                        "literal": "value",
                    },
                ),
            )
            adapter = DltAdapter(src, "test")
            creds = adapter._resolve_credentials()
            assert creds["api_key"] == "sk_live_xyz"
            assert creds["literal"] == "value"

    def test_extract_routes_to_verified_and_rest_and_rejects_missing_mode(self, monkeypatch):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(type="dlt", dlt=DltSourceConfig(source="stripe"))
        adapter = DltAdapter(src, "test")
        monkeypatch.setitem(sys.modules, "dlt", types.SimpleNamespace())
        monkeypatch.setattr(adapter, "_resolve_credentials", lambda: {"api_key": "x"})
        monkeypatch.setattr(
            adapter,
            "_run_verified_source",
            lambda credentials, previous_state=None: ("verified", credentials, previous_state),
        )
        assert adapter.extract("state") == ("verified", {"api_key": "x"}, "state")

        rest_src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com", endpoints=[DltEndpointConfig(name="users", path="users")]
            ),
        )
        rest_adapter = DltAdapter(rest_src, "test")
        monkeypatch.setattr(rest_adapter, "_resolve_credentials", lambda: {"token": "x"})
        monkeypatch.setattr(
            rest_adapter,
            "_run_rest_api",
            lambda credentials, previous_state=None: ("rest", credentials, previous_state),
        )
        assert rest_adapter.extract("state") == ("rest", {"token": "x"}, "state")

        missing_mode = DltAdapter(SourceConfig(type="dlt", dlt=DltSourceConfig(source="placeholder")), "test")
        missing_mode.cfg = types.SimpleNamespace(source=None, base_url=None, credentials={}, endpoints=None)
        with pytest.raises(ValueError, match="must specify either 'source'"):
            missing_mode.extract()

    def test_extract_raises_when_dlt_missing(self, monkeypatch):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(type="dlt", dlt=DltSourceConfig(source="stripe"))
        adapter = DltAdapter(src, "test")
        monkeypatch.delitem(sys.modules, "dlt", raising=False)
        original_import = builtins.__import__

        def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "dlt":
                raise ImportError("missing")
            return original_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", blocked_import)
        with pytest.raises(ImportError, match="dlt integration requires the dlt package"):
            adapter.extract()

    def test_run_verified_source_paths(self, monkeypatch, tmp_path):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(type="dlt", dlt=DltSourceConfig(source="stripe", resource="charges"))
        adapter = DltAdapter(src, "test")
        fake_module = types.SimpleNamespace(
            stripe=lambda **credentials: types.SimpleNamespace(
                with_resources=lambda resource: ("resource", resource, credentials)
            )
        )
        monkeypatch.setattr(
            _dlt_mod.importlib, "import_module", lambda name, package=None: fake_module
        )

        pipeline = types.SimpleNamespace(state={}, run=lambda *args, **kwargs: None)
        fake_dlt = types.ModuleType("dlt")
        fake_dlt.pipeline = lambda **kwargs: pipeline
        fake_dlt.destinations = types.SimpleNamespace(filesystem=lambda bucket_url: {"bucket_url": bucket_url})
        monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
        monkeypatch.setattr(adapter, "_get_tmp_dir", lambda: tmp_path)
        monkeypatch.setattr(
            adapter,
            "_collect_parquet_files",
            lambda tmp_dir, pipeline_obj: {"tmp_dir": tmp_dir, "pipeline": pipeline_obj},
        )

        result = adapter._run_verified_source({"api_key": "x"})
        assert result == {"tmp_dir": tmp_path, "pipeline": pipeline}
        assert adapter.dlt_state_json == "{}"

        def decorated_source(**credentials):
            return types.SimpleNamespace()

        decorated_source._dlt_source = True
        setattr(fake_module, "other", lambda: None)
        setattr(fake_module, "decorated", decorated_source)
        adapter.cfg = DltSourceConfig(source="missing_source")
        monkeypatch.setattr(
            _dlt_mod.importlib, "import_module", lambda name, package=None: fake_module
        )
        assert adapter._run_verified_source({}) == {"tmp_dir": tmp_path, "pipeline": pipeline}

    def test_run_verified_source_import_and_state_restore_failures(self, monkeypatch, tmp_path):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(type="dlt", dlt=DltSourceConfig(source="stripe"))
        adapter = DltAdapter(src, "test")
        monkeypatch.setitem(sys.modules, "dlt", types.ModuleType("dlt"))
        monkeypatch.setattr(
            _dlt_mod.importlib, "import_module",
            lambda name, package=None: (_ for _ in ()).throw(ModuleNotFoundError("missing")),
        )
        with pytest.raises(ImportError):
            adapter._run_verified_source({})

        fake_module = types.ModuleType("stripe")
        fake_module.other = object()
        monkeypatch.setattr(
            _dlt_mod.importlib, "import_module", lambda name, package=None: fake_module
        )
        with pytest.raises(ValueError, match="Could not find a source function"):
            adapter._run_verified_source({})

        pipeline = types.SimpleNamespace(state={}, run=lambda *args, **kwargs: None)
        fake_dlt = types.ModuleType("dlt")
        fake_dlt.pipeline = lambda **kwargs: pipeline
        fake_dlt.destinations = types.SimpleNamespace(filesystem=lambda bucket_url: {"bucket_url": bucket_url})
        monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
        monkeypatch.setattr(adapter, "_get_tmp_dir", lambda: tmp_path)
        monkeypatch.setattr(adapter, "_collect_parquet_files", lambda tmp_dir, pipeline_obj: {})
        warnings = []
        monkeypatch.setattr(_dlt_mod.logger, "warning", warnings.append)
        ok_module = types.ModuleType("stripe")
        ok_module.stripe = lambda **credentials: types.SimpleNamespace()
        monkeypatch.setattr(
            _dlt_mod.importlib, "import_module", lambda name, package=None: ok_module
        )
        adapter._run_verified_source({}, previous_state="not-json")
        assert any("Failed to restore dlt state" in message for message in warnings)

    def test_run_rest_api_and_collect_parquet_paths(self, monkeypatch, tmp_path):
        from lakelogic.adapters.dlt_adapter import DltAdapter

        src = SourceConfig(
            type="dlt",
            dlt=DltSourceConfig(
                base_url="https://api.example.com", endpoints=[DltEndpointConfig(name="users", path="users")]
            ),
        )
        adapter = DltAdapter(src, "test")
        pipeline = types.SimpleNamespace(state={"cursor": 1}, run=lambda *args, **kwargs: None)
        fake_dlt = types.SimpleNamespace(
            pipeline=lambda **kwargs: pipeline,
            destinations=types.SimpleNamespace(filesystem=lambda bucket_url: {"bucket_url": bucket_url}),
        )
        monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
        monkeypatch.setitem(
            sys.modules,
            "dlt.sources.rest_api",
            types.SimpleNamespace(rest_api_source=lambda config: {"config": config}),
        )
        monkeypatch.setattr(adapter, "_get_tmp_dir", lambda: tmp_path)
        monkeypatch.setattr(
            adapter,
            "_collect_parquet_files",
            lambda tmp_dir, pipeline_obj: {"tmp_dir": tmp_dir, "pipeline": pipeline_obj},
        )
        result = adapter._run_rest_api({"api_key": "secret"}, previous_state=json.dumps({"saved": True}))
        assert result == {"tmp_dir": tmp_path, "pipeline": pipeline}
        assert (
            adapter.dlt_state_json == '{"cursor": 1, "saved": true}'
            or adapter.dlt_state_json == '{"saved": true, "cursor": 1}'
        )

        class FakePA:
            class ArrowInvalid(Exception):
                pass

            class ArrowTypeError(Exception):
                pass

            class ArrowNotImplementedError(Exception):
                pass

            @staticmethod
            def table(data):
                return {"table": data}

            @staticmethod
            def string():
                return "string"

            @staticmethod
            def concat_tables(tables, promote_options="default"):
                if any(getattr(table, "force_conflict", False) for table in tables):
                    raise FakePA.ArrowInvalid("conflict")
                return types.SimpleNamespace(num_rows=2, num_columns=2, tables=tables)

        class FakeColumn:
            def __init__(self, name, value):
                self.name = name
                self.value = value

            def cast(self, dtype):
                return f"cast:{self.name}:{dtype}"

        class FakeTable:
            def __init__(self, schema, force_conflict=False):
                self.schema = schema
                self.force_conflict = force_conflict

            def column(self, name):
                return FakeColumn(name, name)

        field_a = types.SimpleNamespace(name="id", type="int")
        field_b = types.SimpleNamespace(name="id", type="string")
        fake_tables = [FakeTable([field_a], force_conflict=True), FakeTable([field_b], force_conflict=True)]
        fake_parquet_module = types.ModuleType("pyarrow.parquet")
        fake_parquet_module.read_table = lambda path: fake_tables.pop(0)
        fake_pyarrow_module = types.ModuleType("pyarrow")
        fake_pyarrow_module.ArrowInvalid = FakePA.ArrowInvalid
        fake_pyarrow_module.ArrowTypeError = FakePA.ArrowTypeError
        fake_pyarrow_module.ArrowNotImplementedError = FakePA.ArrowNotImplementedError
        fake_pyarrow_module.table = FakePA.table
        fake_pyarrow_module.string = FakePA.string
        fake_pyarrow_module.concat_tables = FakePA.concat_tables
        fake_pyarrow_module.parquet = fake_parquet_module
        monkeypatch.setitem(sys.modules, "pyarrow", fake_pyarrow_module)
        monkeypatch.setitem(sys.modules, "pyarrow.parquet", fake_parquet_module)
        info_messages = []
        warning_messages = []
        monkeypatch.setattr(_dlt_mod.logger, "info", info_messages.append)
        monkeypatch.setattr(_dlt_mod.logger, "warning", warning_messages.append)
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir(exist_ok=True)
        (parquet_dir / "a.parquet").write_text("x", encoding="utf-8")
        (parquet_dir / "b.parquet").write_text("y", encoding="utf-8")
        combined = DltAdapter._collect_parquet_files(adapter, parquet_dir, pipeline)
        assert hasattr(combined, "num_rows")
        assert any("auto-casting columns" in message for message in info_messages)

        empty_dir = tmp_path / "empty"
        empty_dir.mkdir(exist_ok=True)
        assert DltAdapter._collect_parquet_files(adapter, empty_dir, pipeline) == {"table": {}}
        assert any("produced no parquet files" in message for message in warning_messages)


# ─────────────────────────────────────────────────────────────────────────────
# Processor integration (mocked)
# ─────────────────────────────────────────────────────────────────────────────


class TestProcessorDltBranch:
    """Verify DataProcessor.run_source routes to _run_dlt_source."""

    def test_run_source_routes_to_dlt(self, tmp_path):
        """Verify that type=dlt calls _run_dlt_source instead of path resolution."""
        import yaml

        contract_yaml = {
            "version": "1.0.0",
            "dataset": "bronze_test",
            "source": {
                "type": "dlt",
                "dlt": {
                    "source": "chess",
                },
            },
            "model": {
                "fields": [
                    {"name": "id", "type": "string"},
                ],
            },
        }
        contract_file = tmp_path / "test.yaml"
        contract_file.write_text(yaml.dump(contract_yaml))

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")

        # Mock _run_dlt_source to verify it's called
        mock_result = MagicMock()
        proc._run_dlt_source = MagicMock(return_value=mock_result)

        result = proc.run_source()
        proc._run_dlt_source.assert_called_once()
        assert result is mock_result


# ─────────────────────────────────────────────────────────────────────────────
# Contract YAML parsing end-to-end
# ─────────────────────────────────────────────────────────────────────────────


class TestContractYamlParsing:
    """Verify that dlt contract YAML is parsed correctly into models."""

    def test_verified_source_contract(self, tmp_path):
        import yaml

        contract = {
            "version": "1.0.0",
            "dataset": "bronze_stripe_charges",
            "source": {
                "type": "dlt",
                "dlt": {
                    "source": "stripe_analytics",
                    "resource": "charges",
                    "credentials": {
                        "api_key": "${STRIPE_API_KEY}",
                    },
                },
            },
            "model": {
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "amount", "type": "integer"},
                ],
            },
        }
        contract_file = tmp_path / "stripe.yaml"
        contract_file.write_text(yaml.dump(contract))

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")
        assert proc.contract.source.type == "dlt"
        assert proc.contract.source.dlt.source == "stripe_analytics"
        assert proc.contract.source.dlt.resource == "charges"
        assert proc.contract.source.dlt.credentials["api_key"] == "${STRIPE_API_KEY}"

    def test_rest_api_contract(self, tmp_path):
        import yaml

        contract = {
            "version": "1.0.0",
            "dataset": "bronze_weather",
            "source": {
                "type": "dlt",
                "dlt": {
                    "base_url": "https://api.openweathermap.org/data/2.5/",
                    "credentials": {
                        "appid": "${OPENWEATHER_KEY}",
                    },
                    "endpoints": [
                        {
                            "name": "forecast",
                            "path": "forecast",
                            "params": {"q": "London", "units": "metric"},
                        },
                    ],
                },
            },
            "model": {
                "fields": [
                    {"name": "dt", "type": "integer"},
                ],
            },
        }
        contract_file = tmp_path / "weather.yaml"
        contract_file.write_text(yaml.dump(contract))

        from lakelogic.core.processor import DataProcessor

        proc = DataProcessor(str(contract_file), engine="polars")
        assert proc.contract.source.type == "dlt"
        assert proc.contract.source.dlt.base_url == "https://api.openweathermap.org/data/2.5/"
        assert len(proc.contract.source.dlt.endpoints) == 1
        assert proc.contract.source.dlt.endpoints[0].name == "forecast"
