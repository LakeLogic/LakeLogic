from __future__ import annotations

import sys
import types

import pytest

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

from lakelogic.core import generator as gen


def _make_generator(monkeypatch, fields=None, faker=None, seed=7):
    monkeypatch.setattr(gen, "_try_faker", lambda: faker)
    monkeypatch.setattr(
        gen.DataGenerator,
        "_contract_from_schema",
        staticmethod(
            lambda schema: {
                "info": {"title": "schema", "version": "0.0.0"},
                "model": {"fields": fields or [{"name": "id", "type": "string"}]},
                "quality": {},
            }
        ),
    )
    monkeypatch.setattr(gen.DataGenerator, "_extract_fields", lambda self: self._contract_raw["model"]["fields"])
    monkeypatch.setattr(gen.DataGenerator, "_extract_unique_integer_fields", lambda self: set())
    monkeypatch.setattr(gen.DataGenerator, "_detect_triplets", lambda self: [])
    monkeypatch.setattr(gen.DataGenerator, "_detect_geo_alignment", lambda self: [])
    return gen.DataGenerator({"id": "string"}, seed=seed, use_faker=faker is not None)


def test_generator_helper_matchers_and_faker_import(monkeypatch):
    assert gen._polars_dtype("string") == pl.Utf8
    assert gen._polars_dtype("INT64") == pl.Int64
    assert gen._polars_dtype("unknown") is None

    fake_faker_module = types.ModuleType("faker")
    fake_faker_module.Faker = lambda: "faker-instance"
    monkeypatch.setitem(sys.modules, "faker", fake_faker_module)
    assert gen._try_faker() == "faker-instance"

    assert gen._match_semantic_hint("user_email") == "email"
    assert gen._match_semantic_hint("email_opt_in") == "email"
    assert gen._match_semantic_hint("ship_date") is None
    assert gen._match_null_probability("customer_deleted_at") == 0.90
    assert gen._match_null_probability("unknown_field") is None
    assert gen._match_distribution("unit_price") == gen._DISTRIBUTION_PROFILES["price"]
    assert gen._match_distribution("unknown_field") is None


def test_schema_input_detection_and_contract_from_schema(monkeypatch):
    StructType = type("StructType", (), {})
    assert gen.DataGenerator._is_schema_input(StructType()) is True
    assert gen.DataGenerator._is_schema_input([("id", "integer")]) is True
    assert gen.DataGenerator._is_schema_input({"id": "integer"}) is True
    assert gen.DataGenerator._is_schema_input("id BIGINT, email STRING") is True
    assert gen.DataGenerator._is_schema_input("contract.yaml") is False
    assert gen.DataGenerator._is_schema_input("folder/contract.yaml") is False

    bootstrap_module = types.ModuleType("lakelogic.core.bootstrap")
    bootstrap_module._parse_schema_to_fields = lambda schema: [{"name": "id", "type": "integer"}]
    monkeypatch.setitem(sys.modules, "lakelogic.core.bootstrap", bootstrap_module)

    contract = gen.DataGenerator._contract_from_schema({"id": "integer"})
    assert contract["info"]["title"] == "_from_schema"
    assert contract["model"]["fields"] == [{"name": "id", "type": "integer"}]


def test_generator_init_with_schema_and_extraction_guard(monkeypatch):
    monkeypatch.setattr(gen, "_try_faker", lambda: None)
    monkeypatch.setattr(
        gen.DataGenerator,
        "_contract_from_schema",
        staticmethod(
            lambda schema: {
                "info": {"title": "schema", "version": "0.0.0"},
                "model": {"fields": [{"name": "id", "type": "integer"}]},
                "quality": {},
            }
        ),
    )
    monkeypatch.setattr(gen.DataGenerator, "_extract_fields", lambda self: self._contract_raw["model"]["fields"])
    monkeypatch.setattr(gen.DataGenerator, "_extract_unique_integer_fields", lambda self: {"id"})
    monkeypatch.setattr(gen.DataGenerator, "_detect_triplets", lambda self: [{"field": "id"}])
    monkeypatch.setattr(gen.DataGenerator, "_detect_geo_alignment", lambda self: [{"city": "lat"}])

    instance = gen.DataGenerator({"id": "integer"}, seed=7, use_faker=False)
    assert instance.contract_path.name == "_from_schema"
    assert instance._fields == [{"name": "id", "type": "integer"}]
    assert instance._unique_integer_fields == {"id"}
    assert instance._triplets == [{"field": "id"}]
    assert instance._geo_alignments == [{"city": "lat"}]

    monkeypatch.setattr(gen.DataGenerator, "_load_yaml", lambda self: {"extraction": {"provider": "llm"}})
    with pytest.raises(ValueError, match="does not support contracts with an 'extraction' section"):
        gen.DataGenerator("contract.yaml", use_faker=False)


def test_from_file_supports_dataframe_and_csv_inputs(tmp_path):
    pandas_df = pd.DataFrame({"id": [1, 2], "name": ["a", None]})
    instance = gen.DataGenerator.from_file(pandas_df, seed=1, use_faker=False)
    assert {field["name"] for field in instance._fields} == {"id", "name"}
    assert instance._auto_sample_pools["id"] == [1, 2]
    assert instance._auto_sample_pools["name"] == ["a"]

    csv_path = tmp_path / "seed.csv"
    pl.DataFrame({"event_id": [10], "active": [True]}).write_csv(csv_path)
    csv_instance = gen.DataGenerator.from_file(csv_path, seed=2, use_faker=False)
    assert {field["name"] for field in csv_instance._fields} == {"event_id", "active"}
    assert csv_instance._auto_sample_pools["event_id"] == [10]
    assert csv_instance._auto_sample_pools["active"] == [True]


def test_from_file_rejects_unknown_extension(tmp_path):
    bad_path = tmp_path / "seed.unknown"
    bad_path.write_text("id|name\n1|alice\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Cannot infer file format"):
        gen.DataGenerator.from_file(bad_path, use_faker=False)


def test_generator_regex_and_filename_helpers(monkeypatch):
    instance = _make_generator(monkeypatch, seed=3)

    assert instance._split_kwargs("text='ORD-####', right_digits=2") == ["text='ORD-####'", " right_digits=2"]
    assert instance._expand_char_class("A-C\\d")[:4] == ["A", "B", "C", "0"]
    assert instance._split_alternation("ab|(cd|ef)|gh") == ["ab", "(cd|ef)", "gh"]
    assert instance._generate_from_regex("^[A-Z]{2}\\d{3}$") is not None
    assert instance._generate_from_regex("(foo|bar)") in {"foo", "bar"}
    assert instance._generate_from_regex("[abc") in {"a", "b", "c"}

    stem = instance._resolve_filename_stem(
        {"listing_id": 101, "property_type": "Semi Detached"},
        "zoopla_{listing_id}_{property_type}_{missing}_{value}",
        "primary",
    )
    assert stem == "zoopla_101_Semi_Detached_{missing}_primary"


def test_generator_string_value_and_entity_id_helpers(monkeypatch):
    instance = _make_generator(monkeypatch, seed=11)

    assert instance._match_entity_id("parent_customer_id") == "CUST-{:06d}"
    assert instance._match_entity_id("unknown_id") is None

    ts_value = instance._string_value("created_at")
    date_value = instance._string_value("ship_date")
    entity_value = instance._string_value("customer_id")

    assert "T" in ts_value
    assert len(date_value.split("-")) == 3
    assert entity_value.startswith("CUST-")


def test_generator_distribution_faker_and_template_helpers(monkeypatch):
    fake_faker = types.SimpleNamespace(
        seed_instance=lambda seed: None,
        email=lambda: "person@example.com",
        bothify=lambda **kwargs: f"bothify::{kwargs['text']}",
    )
    instance = _make_generator(monkeypatch, faker=fake_faker, seed=5)

    assert instance._expand_template("AB##??")[:2] == "AB"
    assert instance._sample_distribution({"distribution": "weighted", "weights": {"active": 1.0}}) == "active"
    lognormal = instance._sample_distribution(
        {"distribution": "lognormal", "mean": 1.0, "std": 0.1, "min": 1.0, "max": 5.0}
    )
    assert 1.0 <= lognormal <= 5.0
    normal = instance._sample_distribution({"distribution": "normal", "mean": 10, "std": 0.1, "min": 1, "max": 20})
    assert isinstance(normal, int)
    beta = instance._sample_distribution({"distribution": "beta", "alpha": 2.0, "beta": 3.0})
    assert 0.0 <= beta <= 1.0
    bimodal = instance._sample_distribution(
        {"distribution": "bimodal", "peak_1": {"value": 9, "weight": 1.0}, "peak_2": {"value": 2, "weight": 0.0}}
    )
    assert 8 <= bimodal <= 10
    assert instance._sample_distribution({}) is None

    assert instance._call_faker("email") == "person@example.com"
    assert instance._call_faker("bothify(text='ORD-####')") == "bothify::ORD-####"
    assert instance._call_faker("missing_method") == "missing_method"
    assert instance._call_faker("missing_method(text='x')") == "missing_method(text='x')"


def test_generator_temporal_consistency_correlation_and_geo_helpers(monkeypatch):
    fields = [
        {"name": "deleted_at", "type": "timestamp"},
        {"name": "total_value", "type": "double"},
    ]
    instance = _make_generator(monkeypatch, fields=fields, seed=13)
    instance._window_start = gen.datetime(2024, 1, 1, 0, 0, 0)
    instance._window_end = gen.datetime(2024, 1, 3, 0, 0, 0)

    date_value = instance._generate_date("invoice_date")
    timestamp_value = instance._generate_timestamp("submitted_at")
    assert "2024-01-" in date_value
    assert timestamp_value.startswith("2024-01-")

    row = {"created_at": "2024-01-05T10:00:00", "updated_at": "2024-01-04T10:00:00"}
    instance._apply_temporal_ordering(row)
    assert row["updated_at"] >= row["created_at"]
    assert instance._parse_temporal("not-a-date") is None

    monkeypatch.setattr(instance, "_make_valid_value", lambda *args, **kwargs: "2024-01-06T00:00:00")
    consistency_row = {
        "status": "deleted",
        "deleted_at": None,
        "subtotal": 100.0,
        "tax_amount": 20.0,
        "discount_amount": 5.0,
        "total_value": 0.0,
        "quantity_committed": 9.0,
        "quantity_on_hand": 4.0,
    }
    instance._apply_field_consistency(consistency_row)
    assert consistency_row["deleted_at"] == "2024-01-06T00:00:00"
    assert consistency_row["total_value"] == 115.0
    assert consistency_row["quantity_committed"] <= consistency_row["quantity_on_hand"]

    correlation_row = {"country_code": "GB", "bank_account": "seed"}
    instance._apply_correlations(correlation_row)
    assert len(correlation_row["bank_account"]) == 8

    instance._geo_alignments = [{"city_field": "city", "lat_field": "latitude", "lng_field": "longitude"}]
    geo_row = {"city": "london", "latitude": 0.0, "longitude": 0.0}
    instance._apply_geo_alignment(geo_row)
    assert 51.4 < geo_row["latitude"] < 51.6
    assert -0.2 < geo_row["longitude"] < 0.0


def test_generator_valid_and_invalid_value_helpers(monkeypatch):
    fields = [{"name": "event_timestamp", "type": "integer", "description": "microseconds epoch"}]
    instance = _make_generator(monkeypatch, fields=fields, seed=17)

    monkeypatch.setattr(
        gen,
        "_match_null_probability",
        lambda name: 1.0 if name == "optional_text" else gen._match_null_probability(name),
    )
    assert instance._make_valid_value("optional_text", "string", {}, nullable=True) is None

    pk_int = instance._make_valid_value(
        "customer_id", "integer", {"primary_key": True}, nullable=False, sample_pools={"customer_id": [10]}
    )
    assert isinstance(pk_int, int)
    assert pk_int >= 10

    pk_string = instance._make_valid_value(
        "order_id", "string", {"primary_key": True}, nullable=False, sample_pools={"order_id": ["ORD-1001"]}
    )
    assert pk_string.startswith("ORD-")
    assert pk_string != "ORD-1001"

    accepted = instance._make_valid_value("status", "string", {"accepted_values": ["active"]}, nullable=False)
    assert accepted == "active"

    regex_value = instance._make_valid_value(
        "code", "string", {"regex_match": "[A-Z]{2}", "min_length": 2, "max_length": 2}, nullable=False
    )
    assert len(regex_value) == 2
    assert regex_value.isupper()

    epoch_value = instance._make_valid_value("event_timestamp", "integer", {}, nullable=False)
    assert epoch_value > 1_000_000

    original_random = instance._rng.random
    instance._rng.random = lambda: 0.5
    try:
        invalid_ai, tc_ai = instance._make_invalid_value(
            "email",
            "string",
            {},
            nullable=False,
            edge_case_pools={"email": ["edge@example"]},
        )
    finally:
        instance._rng.random = original_random
    assert invalid_ai == "edge@example"
    assert tc_ai.type == "EDGE_CASE_AI"

    original_random = instance._rng.random
    original_choice = instance._rng.choice
    instance._rng.random = lambda: 0.99
    instance._rng.choice = lambda seq: seq[0]
    try:
        invalid_generic, tc_generic = instance._make_invalid_value("amount", "double", {"min": 1.0}, nullable=False)
    finally:
        instance._rng.random = original_random
        instance._rng.choice = original_choice
    assert invalid_generic is None
    assert tc_generic.type == "NOT_NULL_VIOLATION"


def test_generator_make_row_runs_postprocessing_only_for_valid_rows(monkeypatch):
    fields = [{"name": "city", "type": "string"}, {"name": "status", "type": "string"}]
    instance = _make_generator(monkeypatch, fields=fields, seed=19)
    instance._build_field_rules = lambda fk_pools=None: {"city": {}, "status": {}}
    instance._make_valid_value = lambda name, ftype, rules, nullable, sample_pools=None: f"valid-{name}"
    instance._make_invalid_value = lambda name, ftype, rules, nullable, edge_case_pools=None: (
        f"invalid-{name}",
        gen.TestCaseInfo("EMPTY_STRING", name, "", "forced invalid", "quality.enforce_required"),
    )

    postprocess_calls = []
    instance._apply_correlations = lambda row: postprocess_calls.append("correlations")
    instance._apply_temporal_ordering = lambda row: postprocess_calls.append("temporal")
    instance._apply_field_consistency = lambda row: postprocess_calls.append("consistency")
    instance._apply_geo_alignment = lambda row: postprocess_calls.append("geo")

    valid_row, valid_cases = instance._make_row(False)
    assert valid_row == {"city": "valid-city", "status": "valid-status", "_is_invalid": False}
    assert valid_cases == []
    assert postprocess_calls == ["correlations", "temporal", "consistency", "geo"]

    postprocess_calls.clear()
    instance._rng.random = lambda: 0.0
    invalid_row, invalid_cases = instance._make_row(True)
    assert invalid_row["city"] == "invalid-city"
    assert invalid_row["status"] == "invalid-status"
    assert invalid_row["_is_invalid"] is True
    assert invalid_row["_test_case_types"] == "EMPTY_STRING"
    assert len(invalid_cases) == 2
    assert postprocess_calls == []


def test_generator_temporal_triplet_generation_covers_valid_and_invalid_formats(monkeypatch):
    instance = _make_generator(monkeypatch, seed=29)
    instance._window_start = gen.datetime(2024, 1, 1, 0, 0, 0)
    instance._window_end = gen.datetime(2024, 1, 2, 0, 0, 0)

    valid_cfg = {
        "start": "started_at",
        "end": "ended_at",
        "duration": "duration_minutes",
        "unit": "minutes",
        "allowed_durations": [15],
    }
    valid_triplet = instance._generate_temporal_triplet(valid_cfg, True)
    started = gen.datetime.fromisoformat(valid_triplet["started_at"])
    ended = gen.datetime.fromisoformat(valid_triplet["ended_at"])
    assert valid_triplet["duration_minutes"] == 15
    assert int((ended - started).total_seconds()) == 900

    original_choice = instance._rng.choice
    instance._rng.choice = lambda seq: (
        "microsecond_precision_mismatch" if isinstance(seq, list) else original_choice(seq)
    )
    try:
        invalid_triplet = instance._generate_temporal_triplet(
            {"start": "started_at", "end": "ended_at", "duration": "duration_seconds", "max_duration": 3600},
            False,
        )
    finally:
        instance._rng.choice = original_choice

    assert "." in invalid_triplet["started_at"]
    assert "." not in invalid_triplet["ended_at"]
    assert invalid_triplet["duration_seconds"] is not None


def test_generator_make_valid_value_covers_pk_string_epoch_and_whole_number_float(monkeypatch):
    fields = [{"name": "event_timestamp", "type": "integer", "description": "milliseconds since epoch"}]
    instance = _make_generator(monkeypatch, fields=fields, seed=31)

    pk_string = instance._make_valid_value(
        "order_id",
        "string",
        {"primary_key": True},
        nullable=False,
        sample_pools={"order_id": ["ORD-1001"]},
    )
    epoch_ms = instance._make_valid_value("event_timestamp", "integer", {}, nullable=False)
    whole_number = instance._make_valid_value("quantity_on_hand", "double", {}, nullable=False)

    assert pk_string.startswith("ORD-")
    assert pk_string != "ORD-1001"
    assert epoch_ms > 1_000_000_000_000
    assert epoch_ms % 1000 == 0
    assert isinstance(whole_number, int)


def test_generator_detect_latest_partition_and_from_dbt(monkeypatch, tmp_path):
    (tmp_path / "year=2024" / "month=01" / "day=02" / "hour=12" / "minute=30").mkdir(parents=True)
    (tmp_path / "year=2024" / "month=01" / "day=03" / "hour=08" / "minute=15").mkdir(parents=True)
    latest = gen.DataGenerator._detect_latest_partition(
        tmp_path,
        "year={Y}/month={m}/day={d}/hour={H}/minute={M}",
        interval_minutes=15,
    )
    assert latest == gen.datetime(2024, 1, 3, 8, 15)
    assert gen.DataGenerator._detect_latest_partition(tmp_path / "missing", "{Y}/{m}/{d}", 60) is None

    fake_contract = types.SimpleNamespace(
        model_dump=lambda exclude_none=True, by_alias=True: {
            "model": {"fields": [{"name": "status", "type": "string"}, {"name": "amount", "type": "double"}]},
            "quality": {
                "row_rules": [
                    {"sql": "status IN ('active', 'inactive')"},
                    {"sql": "amount >= 10"},
                    {"sql": "amount <= 20"},
                ]
            },
        }
    )
    fake_dbt = types.ModuleType("lakelogic.adapters.dbt")
    fake_dbt.load_contract_from_dbt = lambda *args, **kwargs: fake_contract
    monkeypatch.setitem(sys.modules, "lakelogic.adapters.dbt", fake_dbt)

    captured = {}

    def fake_init(self, contract_path, seed=None, use_faker=True):
        import yaml

        captured["path"] = contract_path
        with open(contract_path, encoding="utf-8") as handle:
            self._contract_raw = yaml.safe_load(handle)
        self.contract_path = gen.Path(contract_path)
        self.seed = seed
        self._fields = self._contract_raw["model"]["fields"]
        self._quality = self._contract_raw.get("quality", {})

    monkeypatch.setattr(gen.DataGenerator, "__init__", fake_init)
    instance = gen.DataGenerator.from_dbt("models/schema.yml", model="orders", seed=9, use_faker=False)
    status_field = next(field for field in instance._contract_raw["model"]["fields"] if field["name"] == "status")
    amount_field = next(field for field in instance._contract_raw["model"]["fields"] if field["name"] == "amount")
    assert status_field["accepted_values"] == ["active", "inactive"]
    assert amount_field["min"] == 10.0
    assert amount_field["max"] == 20.0
    assert instance.seed == 9
    assert gen.Path(captured["path"]).suffix == ".yaml"


def test_generator_fk_detection_and_related_generation(monkeypatch):
    relationships = gen.DataGenerator._detect_fk_relationships(
        "orders",
        {
            "model": {
                "fields": [
                    {"name": "customer_id", "description": "FK to customers table"},
                    {"name": "product_id", "type": "integer"},
                ]
            },
            "links": [{"name": "customers", "columns": ["customer_id"]}],
            "transformations": [{"sql": "SELECT * FROM src JOIN customers c ON src.customer_id = c.customer_id"}],
        },
        ["customers", "products", "orders"],
        {"customers": ["customer_id"], "products": ["product_id"]},
    )
    assert {item["fk_column"] for item in relationships} == {"customer_id", "product_id"}

    raw_contracts = {
        "customers": {"primary_key": ["customer_id"], "model": {"fields": [{"name": "customer_id", "type": "string"}]}},
        "orders": {
            "model": {"fields": [{"name": "order_id", "type": "string"}, {"name": "customer_id", "type": "string"}]},
            "links": [{"name": "customers", "columns": ["customer_id"]}],
            "transformations": [{"sql": "SELECT * FROM src JOIN customers c ON src.customer_id = c.customer_id"}],
        },
    }

    def fake_init(self, contract_path, seed=None, use_faker=True):
        self._contract_raw = raw_contracts[contract_path]
        self.seed = seed
        self.contract_path = gen.Path(f"{contract_path}.yaml")

    def fake_generate(self, rows=100, invalid_ratio=0.0, output_format="polars", reference_data=None, **kwargs):
        invalid_count = int(rows * invalid_ratio)
        is_invalid = [False] * (rows - invalid_count) + [True] * invalid_count
        if "customer_id" in (self._contract_raw.get("primary_key") or []):
            return pl.DataFrame({"customer_id": [f"C{i}" for i in range(rows)], "_is_invalid": is_invalid})
        pool = (reference_data or {}).get("customer_id", ["NONE"])
        values = [pool[i % len(pool)] for i in range(rows)]
        return pl.DataFrame(
            {"order_id": [f"O{i}" for i in range(rows)], "customer_id": values, "_is_invalid": is_invalid}
        )

    monkeypatch.setattr(gen.DataGenerator, "__init__", fake_init)
    monkeypatch.setattr(gen.DataGenerator, "generate", fake_generate)

    related = gen.DataGenerator.generate_related(
        contracts={"orders": "orders", "customers": "customers"},
        rows={"customers": 3, "orders": 4},
        invalid_ratio=0.25,
        seed=4,
        output_format="polars",
    )
    assert set(related) == {"customers", "orders"}
    assert related["customers"].height == 3
    assert related["orders"].height == 4
    valid_customer_ids = set(related["customers"]["customer_id"].to_list())
    order_customer_ids = related["orders"]["customer_id"].to_list()
    assert any(value.endswith("_ORPHAN") for value in order_customer_ids)
    assert all(value in valid_customer_ids or value.endswith("_ORPHAN") for value in order_customer_ids)


def test_generator_generation_report_and_save_with_report(monkeypatch, tmp_path):
    instance = _make_generator(monkeypatch, seed=21)
    instance._contract_raw = {"info": {"title": "Orders", "version": "1.2.3"}}
    instance._last_generation_summary = {
        "contract": "Orders",
        "contract_version": "1.2.3",
        "seed": 21,
        "engine": "polars",
        "total_rows": 4,
        "valid_rows": 3,
        "invalid_rows": 1,
        "invalid_ratio": 0.25,
    }
    instance._last_test_case_manifest = [
        gen.TestCaseInfo("NOT_NULL_VIOLATION", "email", None, "missing email", "email is required", row_index=1),
        gen.TestCaseInfo("NOT_NULL_VIOLATION", "email", None, "missing email", "email is required", row_index=2),
        gen.TestCaseInfo("EDGE_CASE_AI", "amount", "-1", "edge", "amount range", row_index=3),
    ]

    report = instance.generation_report()
    assert report["summary"]["test_cases_fired"] == 2
    assert report["summary"]["invalid_rows"] == 1
    assert {item["type"] for item in report["test_cases"]} == {"NOT_NULL_VIOLATION", "EDGE_CASE_AI"}

    saved = []
    monkeypatch.setattr(instance, "save", lambda df, output, format="csv": saved.append((gen.Path(output), format)))
    df = pl.DataFrame(
        {
            "id": [1, 2],
            "_is_invalid": [False, True],
            "_test_case_types": [["ok"], ["NOT_NULL_VIOLATION"]],
        }
    )
    data_path, invalid_path, report_path = instance.save_with_report(df, tmp_path, name="orders", format="json")
    assert data_path.name == "orders_test.json"
    assert invalid_path.name == "orders_invalid.json"
    assert report_path.exists()
    assert saved == [(data_path, "json"), (invalid_path, "json")]


def test_generator_save_partitioned_and_to_frame(monkeypatch, tmp_path):
    instance = _make_generator(
        monkeypatch, fields=[{"name": "id", "type": "integer"}, {"name": "payload", "type": "string"}], seed=23
    )
    json_df = pl.DataFrame(
        {
            "listing_id": [1001, 1002],
            "property_type": ["Semi Detached", "Flat"],
            "payload": ["a", "b"],
        }
    )
    written = instance.save_partitioned(
        json_df,
        tmp_path,
        filename_field="listing_id",
        format="json",
        filename_template="zoopla_{value}_{property_type}",
    )
    assert [path.name for path in written] == ["zoopla_1001_Semi_Detached.json", "zoopla_1002_Flat.json"]

    csv_written = instance.save_partitioned(
        json_df,
        tmp_path / "csv",
        filename_field="listing_id",
        format="csv",
    )
    assert all(path.suffix == ".csv" for path in csv_written)

    with pytest.raises(ValueError, match="filename_field 'missing'"):
        instance.save_partitioned(json_df, tmp_path / "bad", filename_field="missing")

    instance._fields = [
        {"name": "id", "type": "integer"},
        {"name": "flag", "type": "boolean"},
        {"name": "payload", "type": "string"},
    ]
    frame = instance._to_frame(
        [{"id": "10", "flag": "true", "payload": {"name": "alice"}, "extra": "x"}],
        "polars",
    )
    assert frame.columns == ["id", "flag", "payload", "extra"]
    assert frame["id"].to_list() == [10]
    assert frame["flag"].to_list() == [True]
    assert frame["payload"].to_list() == ['{"name": "alice"}']

    pandas_frame = instance._to_frame([{"id": 1, "flag": False, "payload": [1, 2]}], "pandas")
    assert list(pandas_frame.columns)[:3] == ["id", "flag", "payload"]
    assert pandas_frame.iloc[0]["payload"] == "[1, 2]"

    with pytest.raises(ValueError, match="output_format must be 'polars' or 'pandas'"):
        instance._to_frame([{"id": 1}], "duckdb")


def test_generator_generate_stream_writes_micro_batches_and_resumes(monkeypatch, tmp_path):
    instance = _make_generator(monkeypatch, fields=[{"name": "id", "type": "integer"}], seed=29)
    resume_from = gen.datetime(2026, 1, 1, 0, 0, 0)
    monkeypatch.setattr(
        instance, "_detect_latest_partition", lambda output_dir, template, interval_minutes: resume_from
    )

    generated = []

    def fake_generate(rows=100, invalid_ratio=0.0, output_format="polars", reference_data=None, **kwargs):
        generated.append((rows, invalid_ratio, output_format, kwargs["window_start"], kwargs["window_end"]))
        return pl.DataFrame({"id": list(range(rows))})

    saved = []
    monkeypatch.setattr(instance, "generate", fake_generate)
    monkeypatch.setattr(
        instance, "save", lambda frame, path, format="parquet": saved.append((path, format, len(frame)))
    )

    batches = list(
        instance.generate_stream(
            rows_per_batch=5,
            interval_minutes=15,
            batches=3,
            output_dir=tmp_path,
            format="csv",
            invalid_ratio=0.2,
            output_format="polars",
            micro_batches=2,
            resume=True,
            up_to=resume_from + gen.timedelta(minutes=45),
        )
    )

    assert len(batches) == 2
    assert generated[0][3] == resume_from + gen.timedelta(minutes=15)
    assert generated[1][4] == resume_from + gen.timedelta(minutes=45)
    assert all(item[1] == "csv" for item in saved)
    assert len(saved) == 4
    assert sum(item[2] for item in saved[:2]) == 5


def test_generator_detect_latest_partition_parses_valid_directories(tmp_path):
    (tmp_path / "yyyy=2026/mm=01/dd=02/hh=03/mi=15").mkdir(parents=True)
    (tmp_path / "yyyy=2026/mm=01/dd=02/hh=04/mi=30").mkdir(parents=True)
    (tmp_path / "garbage").mkdir()
    (tmp_path / "yyyy=2026/mm=99/dd=99/hh=99/mi=99").mkdir(parents=True)

    latest = gen.DataGenerator._detect_latest_partition(
        tmp_path,
        "yyyy={Y}/mm={m}/dd={d}/hh={H}/mi={M}",
        interval_minutes=15,
    )
    assert latest == gen.datetime(2026, 1, 2, 4, 30)
    assert gen.DataGenerator._detect_latest_partition(tmp_path / "missing", "yyyy={Y}", interval_minutes=5) is None


def test_generator_generate_uses_ai_pools_and_builds_manifest(monkeypatch):
    instance = _make_generator(
        monkeypatch,
        fields=[{"name": "id", "type": "integer"}, {"name": "fk_col", "type": "integer"}],
        seed=31,
    )
    instance._contract_raw = {"info": {"title": "Orders"}, "dataset": "orders"}
    instance._quality = {}

    realistic_calls = []
    edge_calls = []
    fake_ai_data = types.ModuleType("lakelogic.ai.data_generator")
    fake_ai_data.generate_realistic_pools = lambda fields, quality, **kwargs: (
        realistic_calls.append(kwargs) or {"id": [10, 20]}
    )
    fake_ai_edge = types.ModuleType("lakelogic.ai.edge_case_generator")
    fake_ai_edge.generate_edge_cases = lambda fields, quality, **kwargs: edge_calls.append(kwargs) or {"id": [-1, -2]}
    monkeypatch.setitem(sys.modules, "lakelogic.ai.data_generator", fake_ai_data)
    monkeypatch.setitem(sys.modules, "lakelogic.ai.edge_case_generator", fake_ai_edge)

    class PoolWrapper:
        def __init__(self, values):
            self._values = values

        def to_list(self):
            return list(self._values)

    calls = []

    def fake_make_row(invalid=False, fk_pools=None, sample_pools=None, edge_case_pools=None):
        calls.append((invalid, fk_pools, sample_pools, edge_case_pools))
        if invalid:
            row = {"id": -1, "fk_col": fk_pools["fk_col"][0], "_is_invalid": True}
            return row, [gen.TestCaseInfo("EDGE_CASE_AI", "id", -1, "edge", "invalid id")]
        return {"id": sample_pools["id"][0], "fk_col": fk_pools["fk_col"][0], "_is_invalid": False}, []

    monkeypatch.setattr(instance, "_make_row", fake_make_row)

    frame = instance.generate(
        rows=4,
        invalid_ratio=0.25,
        output_format="polars",
        reference_data={"fk_col": PoolWrapper([7, 8])},
        ai=True,
        ai_provider="demo",
        ai_model="mock-model",
        ai_api_key="secret",
        ai_custom_scenario="stress edge cases",
        window_start=gen.datetime(2026, 1, 1, 0, 0, 0),
        window_end=gen.datetime(2026, 1, 1, 1, 0, 0),
    )

    assert frame.height == 4
    assert frame["fk_col"].to_list() == [7, 7, 7, 7]
    assert realistic_calls[0]["dataset_name"] == "Orders"
    assert edge_calls[0]["custom_scenario"] == "stress edge cases"
    assert calls[0][1] == {"fk_col": [7, 8]}
    assert calls[0][2] == {"id": [10, 20]}
    assert calls[-1][3] == {"id": [-1, -2]}
    assert instance._last_generation_summary["total_rows"] == 4
    assert instance._last_generation_summary["invalid_rows"] == 1
    assert instance._last_generation_summary["test_cases_fired"] == 1
    assert instance._last_test_case_manifest[0].row_index >= 0


def test_generator_save_supports_pandas_and_rejects_unsupported_format(monkeypatch, tmp_path):
    instance = _make_generator(monkeypatch, seed=31)
    pandas_df = pd.DataFrame({"id": [1], "name": ["alice"]})

    csv_path = instance.save(pandas_df, tmp_path / "orders.csv", format="csv")
    json_path = instance.save(pandas_df, tmp_path / "orders.json", format="json")

    assert csv_path.exists()
    assert json_path.exists()
    assert "alice" in json_path.read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported format"):
        instance.save(pandas_df, tmp_path / "orders.txt", format="txt")


def test_generator_load_sample_pools_preserves_nested_json_and_ndjson(monkeypatch, tmp_path):
    fields = [
        {"name": "id", "type": "integer"},
        {"name": "payload", "type": "string"},
        {"name": "tags", "type": "string"},
    ]
    instance = _make_generator(monkeypatch, fields=fields, seed=37)

    json_path = tmp_path / "sample.json"
    json_path.write_text(
        '[{"id": 1, "payload": {"city": "london"}, "tags": ["a", "b"]}]',
        encoding="utf-8",
    )
    ndjson_path = tmp_path / "sample.ndjson"
    ndjson_path.write_text('{"id": 2, "payload": {"city": "paris"}, "tags": ["x"]}\n', encoding="utf-8")

    original_read_json = pl.read_json
    original_read_ndjson = pl.read_ndjson
    monkeypatch.setattr(pl, "read_json", lambda path: (_ for _ in ()).throw(RuntimeError("fallback")))
    monkeypatch.setattr(pl, "read_ndjson", lambda path: (_ for _ in ()).throw(RuntimeError("fallback")))
    try:
        json_pools = instance._load_sample_pools(json_path)
        ndjson_pools = instance._load_sample_pools(ndjson_path)
    finally:
        monkeypatch.setattr(pl, "read_json", original_read_json)
        monkeypatch.setattr(pl, "read_ndjson", original_read_ndjson)

    assert json_pools["payload"] == [{"city": "london"}]
    assert json_pools["tags"] == [["a", "b"]]
    assert ndjson_pools["payload"] == [{"city": "paris"}]
    assert ndjson_pools["tags"] == [["x"]]


def test_generator_extract_unique_integer_fields_and_detect_triplets(monkeypatch):
    fields = [
        {"name": "id", "type": "integer"},
        {"name": "order_number", "type": "string"},
        {"name": "booking_start", "type": "timestamp"},
        {"name": "booking_end", "type": "timestamp"},
        {"name": "booking_duration_seconds", "type": "integer"},
    ]
    instance = gen.DataGenerator.__new__(gen.DataGenerator)
    instance._fields = fields
    instance._contract_raw = {"quality": {"dataset_rules": [{"category": "uniqueness", "sql": "id"}]}}

    assert instance._extract_unique_integer_fields() == {"id"}
    triplets = instance._detect_triplets()
    assert triplets[0]["start"] == "booking_start"
    assert triplets[0]["end"] == "booking_end"
    assert triplets[0]["duration"] == "booking_duration_seconds"


def test_generator_string_value_covers_domain_and_format_fallbacks(monkeypatch):
    instance = _make_generator(monkeypatch, seed=43)

    warehouse_name = instance._string_value("warehouse_name")
    vendor_name = instance._string_value("preferred_vendor_name")
    first_name = instance._string_value("first_name")
    last_name = instance._string_value("last_name")
    person_name = instance._string_value("customer_name")
    email = instance._string_value("contact_email")
    phone = instance._string_value("mobile_phone")
    url = instance._string_value("website_url")

    assert warehouse_name in gen._REALISTIC_POOLS["location_name"]
    assert vendor_name in gen._REALISTIC_POOLS["company_name"]
    assert first_name in gen._FIRST_NAMES
    assert last_name in gen._LAST_NAMES
    assert len(person_name.split()) == 2
    assert "@" in email
    assert phone.startswith("+1")
    assert url.startswith("https://www.")
