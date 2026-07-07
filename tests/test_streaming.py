from __future__ import annotations

import builtins
import csv
import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _load_streaming_module(
    module_name: str = "lakelogic.core.streaming",
    *,
    create_registry: bool = True,
    block_registry_import: bool = False,
):
    module_path = Path(__file__).resolve().parents[1] / "lakelogic" / "core" / "streaming.py"
    package = sys.modules.setdefault("lakelogic", types.ModuleType("lakelogic"))
    core_package = sys.modules.setdefault("lakelogic.core", types.ModuleType("lakelogic.core"))

    package.core = core_package

    if create_registry:
        registry_module = sys.modules.setdefault("lakelogic.core.registry", types.ModuleType("lakelogic.core.registry"))
        if not hasattr(registry_module, "DomainRegistry"):
            registry_module.DomainRegistry = object
        core_package.registry = registry_module
    else:
        sys.modules.pop("lakelogic.core.registry", None)
        if hasattr(core_package, "registry"):
            delattr(core_package, "registry")

    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if block_registry_import and name == "lakelogic.core.registry":
            raise ImportError("blocked for test")
        return original_import(name, globals, locals, fromlist, level)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module

    try:
        if block_registry_import:
            builtins.__import__ = guarded_import
        spec.loader.exec_module(module)
        return module
    finally:
        builtins.__import__ = original_import


streaming = _load_streaming_module()


def test_write_csv_splits_rows_across_micro_batches(tmp_path):
    start = datetime(2026, 4, 19, 9, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )
    rows = [{"id": str(index), "value": f"row-{index}"} for index in range(5)]

    files = simulator._write_csv("trip_requests", rows, start, micro_batches=2)

    assert len(files) == 2
    assert all(Path(file_path).exists() for file_path in files)
    assert Path(files[0]).parent == tmp_path / "trip_requests" / "y_2026" / "m_04" / "d_19" / "h_09"

    total_rows = 0
    for file_path in files:
        with open(file_path, newline="", encoding="utf-8") as handle:
            total_rows += sum(1 for _ in csv.DictReader(handle))

    assert total_rows == 5


def test_write_csv_handles_empty_rows_and_excess_micro_batches(tmp_path):
    start = datetime(2026, 4, 19, 9, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )

    assert simulator._write_csv("trip_requests", [], start) == []

    rows = [{"id": str(index)} for index in range(2)]
    files = simulator._write_csv("trip_requests", rows, start, micro_batches=5)

    assert len(files) == 2
    assert all(Path(file_path).exists() for file_path in files)


def test_module_import_falls_back_when_registry_import_fails():
    module = _load_streaming_module(
        module_name="lakelogic.core.streaming_importerror",
        create_registry=False,
        block_registry_import=True,
    )

    assert module.DomainRegistry is None


def test_constructor_defaults_start_time_to_midnight_utc(tmp_path):
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        initial_riders=0,
        initial_drivers=0,
    )

    assert simulator._current_time.tzinfo == timezone.utc
    assert simulator._current_time.hour == 0
    assert simulator._current_time.minute == 0
    assert simulator._current_time.second == 0
    assert simulator._current_time.microsecond == 0


def test_run_yields_seed_and_window_results(tmp_path):
    start = datetime(2026, 4, 19, 6, 0, tzinfo=timezone.utc)
    config = {
        "rider_profiles": streaming.EntityStreamConfig(churn_rate=0.5, entity_type="dimension"),
        "driver_profiles": streaming.EntityStreamConfig(churn_rate=0.5, entity_type="dimension"),
        "trip_requests": streaming.EntityStreamConfig(
            rows_per_window=5,
            peak_multiplier=1.0,
            entity_type="fact",
        ),
    }
    simulator = streaming.StreamingSimulator(
        entity_config=config,
        landing_root=str(tmp_path),
        start_time=start,
        seed=7,
        initial_riders=2,
        initial_drivers=2,
    )

    results = list(simulator.run(num_windows=1, include_seed=True))

    assert len(results) == 2
    assert results[0].index == -1
    assert results[0].entities == {"rider_profiles": 2, "driver_profiles": 2}
    assert results[1].index == 0
    assert results[1].entities["trip_requests"] >= 1
    assert results[1].entities["rider_profiles"] >= 1
    assert simulator._current_time == start + timedelta(hours=1)


def test_run_stops_at_up_to_boundary(tmp_path):
    start = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    config = {
        "rider_profiles": streaming.EntityStreamConfig(churn_rate=0.5, entity_type="dimension"),
        "driver_profiles": streaming.EntityStreamConfig(churn_rate=0.5, entity_type="dimension"),
        "trip_requests": streaming.EntityStreamConfig(rows_per_window=4, entity_type="fact"),
    }
    simulator = streaming.StreamingSimulator(
        entity_config=config,
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=1,
        initial_drivers=1,
    )

    results = list(
        simulator.run(
            num_windows=3,
            include_seed=True,
            up_to=start + timedelta(hours=1),
        )
    )

    assert [result.index for result in results] == [-1, 0]
    assert simulator._current_time == start + timedelta(hours=1)


def test_run_all_resumes_and_trims_active_trips(tmp_path):
    start = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )
    resume_calls = []

    simulator._active_trip_ids = [f"TRP-{index}" for index in range(501)]
    simulator._rebuild_state_from_landing = lambda: resume_calls.append(True)

    results = simulator.run_all(num_windows=1, include_seed=False, resume=True)

    assert len(results) == 1
    assert resume_calls == [True]
    assert len(simulator._active_trip_ids) == 200


def test_trip_request_generators_return_empty_without_required_pools(tmp_path):
    start = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )

    assert simulator._gen_trip_requests(1, start) == []
    assert simulator._gen_trip_completed(1, start) == []
    assert simulator._gen_trip_cancellations(1, start) == []
    assert simulator._gen_driver_telemetry(1, start) == []
    assert simulator._gen_rider_app_events(1, start) == []


def test_trip_completion_generator_creates_completed_trip(tmp_path):
    start = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )
    simulator._driver_ids = ["DRV-001"]
    simulator._pending_requests = [
        {
            "request_id": "REQ-001",
            "rider_id": "RDR-001",
            "trip_type": "ride",
            "city_code": "LON",
            "pickup_lat": "51.500000",
            "pickup_lng": "-0.120000",
            "dropoff_lat": "51.510000",
            "dropoff_lng": "-0.130000",
            "requested_at": "2026-04-19T08:00:00.000Z",
            "surge_multiplier": "1.25",
        }
    ]

    rows = simulator._gen_trip_completed(1, start)

    assert len(rows) == 1
    assert rows[0]["driver_id"] == "DRV-001"
    assert rows[0]["rider_id"] == "RDR-001"
    assert rows[0]["surge_multiplier"] == "1.25"
    assert simulator._pending_requests == []
    assert len(simulator._active_trip_ids) == 1


def test_trip_cancellation_generator_uses_driver_branch(tmp_path):
    start = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )
    simulator._driver_ids = ["DRV-001"]
    simulator._pending_requests = [
        {
            "request_id": "REQ-001",
            "rider_id": "RDR-001",
            "trip_type": "ride",
            "city_code": "LON",
            "pickup_lat": "51.500000",
            "pickup_lng": "-0.120000",
            "dropoff_lat": "51.510000",
            "dropoff_lng": "-0.130000",
            "requested_at": "2026-04-19T08:00:00.000Z",
            "surge_multiplier": "1.25",
        }
    ]
    simulator._rng = types.SimpleNamespace(
        sample=lambda population, k: [0],
        choice=lambda options: "driver" if options == streaming._CANCELLED_BY else options[0],
        randint=lambda start_value, end_value: start_value,
    )

    rows = simulator._gen_trip_cancellations(1, start)

    assert len(rows) == 1
    assert rows[0]["cancelled_by"] == "driver"
    assert rows[0]["driver_id"] == "DRV-001"
    assert simulator._pending_requests == []


def test_telemetry_and_app_event_generators_emit_rows(tmp_path):
    start = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=0,
        initial_drivers=0,
    )
    simulator._driver_ids = ["DRV-001"]
    simulator._driver_cities = {"DRV-001": "LON"}
    simulator._active_trip_ids = ["TRP-001"]
    # _active_trips: per-trip (driver, city) lookup. `_gen_driver_telemetry`
    # reads from this dict (not _active_trip_ids) so on-trip pings keep the
    # same driver_id + city_code throughout a trip's lifetime — FK integrity
    # for downstream silver/gold facts. Stub it alongside the legacy list.
    simulator._active_trips = {"TRP-001": {"driver_id": "DRV-001", "city_code": "LON"}}
    simulator._rider_ids = ["RDR-001"]
    simulator._rider_cities = {"RDR-001": "NYC"}
    simulator._rng = types.SimpleNamespace(
        choice=lambda options: (
            "on_trip" if options == ["on_trip", "on_trip", "on_trip", "idle", "offline"] else options[0]
        ),
        randint=lambda start_value, end_value: start_value,
    )

    telemetry_rows = simulator._gen_driver_telemetry(1, start)
    app_event_rows = simulator._gen_rider_app_events(1, start)

    assert len(telemetry_rows) == 1
    assert telemetry_rows[0]["trip_id"] == "TRP-001"
    assert telemetry_rows[0]["status"] == "on_trip"
    assert telemetry_rows[0]["city_code"] == "LON"

    assert len(app_event_rows) == 1
    assert app_event_rows[0]["rider_id"] == "RDR-001"
    assert app_event_rows[0]["city_code"] == "NYC"


def test_rebuild_state_from_landing_restores_ids_and_clock(tmp_path):
    rider_dir = tmp_path / "rider_profiles" / "y_2026" / "m_04" / "d_19" / "h_10"
    driver_dir = tmp_path / "driver_profiles" / "y_2026" / "m_04" / "d_19" / "h_10"
    latest_dir = tmp_path / "trip_requests" / "y_2026" / "m_04" / "d_19" / "h_11"
    rider_dir.mkdir(parents=True)
    driver_dir.mkdir(parents=True)
    latest_dir.mkdir(parents=True)

    with open(rider_dir / "batch.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["rider_id", "city_code"])
        writer.writeheader()
        writer.writerow({"rider_id": "RDR-ONE", "city_code": "LON"})

    with open(driver_dir / "batch.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["driver_id", "city_code"])
        writer.writeheader()
        writer.writerow({"driver_id": "DRV-ONE", "city_code": "NYC"})

    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        initial_riders=0,
        initial_drivers=0,
    )

    simulator._rebuild_state_from_landing()

    assert simulator._rider_ids == ["RDR-ONE"]
    assert simulator._driver_ids == ["DRV-ONE"]
    assert simulator._rider_cities == {"RDR-ONE": "LON"}
    assert simulator._driver_cities == {"DRV-ONE": "NYC"}
    assert simulator._current_time == datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)


def test_rebuild_state_from_landing_handles_missing_root(tmp_path):
    missing_root = tmp_path / "missing"
    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(missing_root),
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        initial_riders=0,
        initial_drivers=0,
    )

    simulator._rebuild_state_from_landing()

    assert simulator._rider_ids == []
    assert simulator._driver_ids == []
    assert simulator._current_time == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_rebuild_state_from_landing_ignores_bad_files_and_invalid_partitions(tmp_path):
    rider_dir = tmp_path / "rider_profiles" / "bad"
    driver_dir = tmp_path / "driver_profiles" / "bad"
    invalid_partition = tmp_path / "trip_requests" / "y_2026" / "m_13" / "d_40" / "h_25"
    rider_dir.mkdir(parents=True)
    driver_dir.mkdir(parents=True)
    invalid_partition.mkdir(parents=True)

    (rider_dir / "broken.csv").write_bytes(b"\xff\xfe\xff")
    (driver_dir / "broken.csv").write_bytes(b"\xff\xfe\xff")

    simulator = streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        initial_riders=0,
        initial_drivers=0,
    )

    simulator._rebuild_state_from_landing()

    assert simulator._rider_ids == []
    assert simulator._driver_ids == []
    assert simulator._rider_cities == {}
    assert simulator._driver_cities == {}
    assert simulator._current_time == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_factory_configuration_and_fk_summary(tmp_path):
    start = datetime(2026, 4, 19, 0, 0, tzinfo=timezone.utc)
    simulator = streaming.StreamingSimulator.rideflow_marketplace(
        landing_root=str(tmp_path),
        start_time=start,
        initial_riders=3,
        initial_drivers=2,
        seed=3,
    )

    seed_result = simulator._seed_initial_pools()
    summary = simulator.validate_fk_consistency()

    assert set(simulator._config) >= {
        "rider_profiles",
        "driver_profiles",
        "trip_requests",
        "trip_completed",
        "trip_cancellations",
        "driver_telemetry",
        "rider_app_events",
    }
    assert seed_result.entities == {"rider_profiles": 3, "driver_profiles": 2}
    assert summary == {
        "total_riders": 3,
        "total_drivers": 2,
        "active_trip_ids": 0,
        "pending_requests": 0,
        "fk_pools_healthy": True,
    }
