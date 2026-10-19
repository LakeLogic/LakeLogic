"""Headless build of the RideFlow Marketplace lakehouse, for the notebooks that read it.

Generated from cells of examples/colab/08b_rideflow_marketplace.ipynb. If 08b's pipeline cells change,
regenerate this file so the dependent notebooks keep building the same data.
"""


def build_marketplace(engine: str = "duckdb") -> None:
    """Build the RideFlow Marketplace lakehouse (Bronze, Silver, Gold) headlessly.

    The same steps as notebook 08b, minus its dashboards and commentary. Keep in sync with 08b.
    """
    from datetime import datetime, timezone
    from pathlib import Path

    ENGINE = engine
    DOMAIN_ROOT = Path("assets/domains_rideflow/marketplace/rideflow")
    SYSTEM_YAML = str(DOMAIN_ROOT / "_system.yaml")
    LAKEHOUSE = Path("./lakehouse")
    LANDING_ROOT = LAKEHOUSE / "_data" / "landing_marketplace" / "rideflow"
    import polars as pl

    from lakelogic.core.registry import DomainRegistry

    ENV = "local"  # ./lakehouse in Colab too, so every notebook finds the same data
    registry = DomainRegistry.from_yaml(SYSTEM_YAML, environment=ENV, storage_mode="path")

    print(f"Domain:  {registry.domain}")
    print(f"System:  {registry.system}")
    print(f"Env:     {ENV}")
    print(f"\nContracts loaded: {len(registry.get_active_contracts())}")
    for c in registry.get_active_contracts():
        print(f"  [{c.layer:6s}] {c.entity}")

    from datetime import timedelta

    from lakelogic.core.streaming import StreamingSimulator

    sim = StreamingSimulator.rideflow_marketplace(
        landing_root=str(LANDING_ROOT),
        window_minutes=60,
        start_time=datetime.now(timezone.utc) - timedelta(days=3),
        seed=42,
        initial_riders=200,
        initial_drivers=100,
    )

    print("🚀 Streaming simulator configured")
    print(f"   Landing root: {LANDING_ROOT}")
    print("   Window size:  60 minutes")
    print("   Entities:     7 (2 dimensions, 3 facts, 2 events)")

    # Generate 24 hours of simulated streaming data
    NUM_WINDOWS = 24

    total_rows = 0
    windows = []
    for window in sim.run(num_windows=NUM_WINDOWS, micro_batches=10, up_to=datetime.now(timezone.utc), resume=True):
        windows.append(window)
        total_rows += window.total_rows

    print(f"\n{'=' * 60}")
    if not windows:
        print(
            "✅ No new windows generated. The simulator has caught up to the current time (up_to constraint reached)."
        )
    else:
        print(f"✅ Generated {total_rows:,} total rows across {len(windows)} windows")
        print(f"   Seed window:    {windows[0].total_rows:,} rows (initial pool)")
        print(f"   Avg per window: {total_rows // len(windows):,} rows")

    # FK consistency check
    fk = sim.validate_fk_consistency()
    print("\n🔗 FK Consistency:")
    print(f"   Riders:    {fk['total_riders']}")
    print(f"   Drivers:   {fk['total_drivers']}")
    print(f"   Healthy:   {'✅' if fk['fk_pools_healthy'] else '❌'}")

    # ── Deterministic Edge-Case Injection ────────────────────────────────────
    # Injects exactly 800 mutated rows into the trip_completed landing zone
    # so the pipeline quarantine layer can demonstrate 100% reconciliation.

    import random
    import uuid
    from datetime import datetime, timedelta, timezone

    random.seed(42)
    NUM_EDGE_CASES = 800

    CITY_PROFILES = {
        "LON": {"timezone": "Europe/London", "base_trips_per_hour": 500},
        "NYC": {"timezone": "US/Eastern", "base_trips_per_hour": 800},
        "BER": {"timezone": "Europe/Berlin", "base_trips_per_hour": 200},
        "PAR": {"timezone": "Europe/Paris", "base_trips_per_hour": 350},
        "TYO": {"timezone": "Asia/Tokyo", "base_trips_per_hour": 600},
        "SYD": {"timezone": "Australia/Sydney", "base_trips_per_hour": 150},
    }

    cities = list(CITY_PROFILES.keys())
    base_time = datetime(2026, 4, 15, 8, 0, tzinfo=timezone.utc)

    # Generate 800 base rows with valid structure
    records = []
    for i in range(NUM_EDGE_CASES):
        trip_id = str(uuid.uuid4())
        rider_id = f"R-{uuid.uuid4().hex[:8]}"
        driver_id = f"D-{uuid.uuid4().hex[:8]}"
        city = random.choice(cities)
        hr = random.choices(
            range(24), weights=[1, 1, 1, 1, 1, 1, 1, 3, 3, 1, 1, 1, 1, 1, 1, 1, 1, 3, 4, 4, 3, 1, 1, 1]
        )[0]
        req_dt = base_time.replace(hour=hr, minute=random.randint(0, 59))
        pickup_dt = req_dt + timedelta(minutes=random.randint(1, 10))
        dropoff_dt = req_dt + timedelta(minutes=random.randint(15, 60))

        records.append(
            {
                "trip_id": trip_id,
                "rider_id": rider_id,
                "driver_id": driver_id,
                "trip_type": random.choice(["ride", "eats_delivery"]),
                "pickup_lat": str(round(random.uniform(51.4, 51.6), 6)),
                "pickup_lng": str(round(random.uniform(-0.2, 0.1), 6)),
                "dropoff_lat": str(round(random.uniform(51.4, 51.6), 6)),
                "dropoff_lng": str(round(random.uniform(-0.2, 0.1), 6)),
                "city_code": city,
                "requested_at": req_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "pickup_at": pickup_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "dropoff_at": dropoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "distance_km": str(round(random.uniform(1, 25), 1)),
                "duration_minutes": str(random.randint(5, 45)),
                "fare_amount": str(round(random.uniform(5, 80), 2)),
                "surge_multiplier": str(round(random.uniform(1.0, 3.5), 2)),
                "tip_amount": str(round(random.uniform(0, 10), 2)),
                "payment_method": random.choice(["card", "apple_pay", "google_pay", "cash"]),
                "rider_rating": str(random.randint(1, 5)),
                "driver_rating": str(random.randint(1, 5)),
                "notes": "",
            }
        )

    # ── Apply TC-001 to TC-008 mutations ──────────────────────────────────────
    for i in range(0, 120):
        records[i]["fare_amount"] = "-15.50"  # TC-001: Negative fare
    for i in range(120, 200):
        records[i]["driver_id"] = records[i]["rider_id"]  # TC-002: Self-service fraud
    for i in range(200, 350):  # TC-003: Time travel
        records[i]["pickup_at"] = "2026-04-15T12:00:00Z"
        records[i]["dropoff_at"] = "2026-04-15T11:00:00Z"
    for i in range(350, 450):
        records[i]["distance_km"] = "650.5"  # TC-004: Impossible trip
    for i in range(450, 600):
        records[i]["notes"] = "DROP TABLE trips; SELECT * FROM users;"  # TC-005
    for i in range(600, 680):
        records[i]["surge_multiplier"] = "5.8"  # TC-006: Cap violation
    for i in range(680, 740):
        records[i]["city_code"] = "XYZ"  # TC-007: Unknown market
    dup_id = records[0]["trip_id"]  # Reuse first trip's ID
    for i in range(740, 800):
        records[i]["trip_id"] = dup_id  # TC-008: Duplicate ID

    random.shuffle(records)
    edge_df = pl.DataFrame(records)

    # Write to the trip_completed landing directory as a separate partition file
    edge_dir = LANDING_ROOT / "trip_completed" / "y_2026" / "m_04" / "d_15" / "h_99"
    edge_dir.mkdir(parents=True, exist_ok=True)
    edge_file = edge_dir / "edge_cases.csv"
    edge_df.write_csv(str(edge_file))

    print(f"\u2705 Injected {NUM_EDGE_CASES} deterministic edge cases into landing zone")
    print(f"   File: {edge_file}")
    print()
    print("   TC-001  Negative fare:         120 rows")
    print("   TC-002  driver_id=rider_id:      80 rows")
    print("   TC-003  Dropoff before pickup:  150 rows")
    print("   TC-004  Distance > 500km:       100 rows")
    print("   TC-005  SQL injection in notes: 150 rows")
    print("   TC-006  Surge > 5.0x:            80 rows")
    print("   TC-007  Unknown city code:        60 rows")
    print("   TC-008  Duplicate trip_id:        60 rows")
    print(f"   {'=' * 45}")
    print(f"   TOTAL edge cases:               {NUM_EDGE_CASES} rows")

    retry_attempts = 1
    parallel = False
    retry_base_wait_seconds = 5
    engine = ENGINE
    reload_layers = ""

    import warnings

    from lakelogic.pipeline.runner import LakehousePipeline

    warnings.filterwarnings("ignore", category=FutureWarning)

    print("Starting Bronze Ingestion via LakeLogic Pipeline Driver...")
    runner = LakehousePipeline(registry, engine=ENGINE)

    summary = runner.run(
        target_layers="bronze",
        reload_layers=reload_layers,
        dry_run=False,
        environment=ENV,
        retry_attempts=retry_attempts,
        retry_base_wait_seconds=retry_base_wait_seconds,
        parallel=False,
    )
    print(summary)

    print("Starting Silver Processing via LakeLogic Pipeline Driver...")

    summary = runner.run(
        target_layers="silver",
        reload_layers=reload_layers,
        dry_run=False,
        environment=ENV,
        retry_attempts=retry_attempts,
        retry_base_wait_seconds=retry_base_wait_seconds,
        parallel=parallel,
    )
    print(summary)

    # Generate surge inference landing data for the Surge Pricing ML Model contract
    import random
    import uuid
    from datetime import datetime, timedelta, timezone

    import polars as pl

    SURGE_LANDING = LAKEHOUSE / "marketplace" / "surge_inference_landing"
    SURGE_LANDING.mkdir(parents=True, exist_ok=True)

    random.seed(42)
    cities = ["LON", "MAN", "BIR", "LDS", "BRS", "EDI", "GLA"]
    n = 200

    surge_df = pl.DataFrame(
        {
            "inference_id": [str(uuid.uuid4()) for _ in range(n)],
            "model_version": [random.choice(["v2.1.0", "v2.2.0", "v2.3.0-beta"]) for _ in range(n)],
            "city_code": [random.choice(cities) for _ in range(n)],
            "demand_index": [round(random.uniform(0.5, 8.5), 2) for _ in range(n)],
            "surge_multiplier": [round(random.uniform(1.0, 4.2), 2) for _ in range(n)],
            "timestamp": [datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 1440)) for _ in range(n)],
        }
    )

    surge_df.write_parquet(SURGE_LANDING / "inferences.parquet")
    print(f"✅ Generated {n} surge inference records → {SURGE_LANDING}")
    surge_df.head(3)

    print("Starting Gold Processing via LakeLogic Pipeline Driver...\n")

    summary = runner.run(
        target_layers="gold",
        reload_layers=reload_layers,
        dry_run=False,
        environment=ENV,
        retry_attempts=retry_attempts,
        retry_base_wait_seconds=retry_base_wait_seconds,
        parallel=parallel,
    )
    print(summary)
