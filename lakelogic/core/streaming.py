"""
Streaming Simulation for LakeLogic Data Mesh.

Provides a ``StreamingSimulator`` that wraps the existing ``DataGenerator``
to emit time-windowed, PK/FK-consistent micro-batches — simulating real-time
event streams landing in hourly partitioned directories.

Designed for notebook-centric demos (Colab / local) with a clear migration
path to Azure Databricks Structured Streaming.

Usage
-----
>>> from lakelogic.core.streaming import StreamingSimulator
>>> sim = StreamingSimulator.rideflow_marketplace(
...     registry=registry,
...     landing_root="./lakehouse/_data/marketplace/rideflow",
... )
>>> for window in sim.run(num_windows=24):
...     print(f"{window.timestamp}: {window.total_rows:,} rows")
"""

from __future__ import annotations

import csv
import math
import os
import random
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from loguru import logger

try:
    from lakelogic.core.registry import DomainRegistry
except ImportError:
    DomainRegistry = None  # type: ignore[assignment,misc]


# ──────────────────────────────────────────────────────────────────────────────
# Configuration types
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class EntityStreamConfig:
    """Per-entity streaming configuration."""

    rows_per_window: int = 50
    """Base number of rows generated per time window."""

    peak_multiplier: float = 3.0
    """Multiplier applied during rush-hour windows (08:00, 17:00–19:00)."""

    churn_rate: float = 0.02
    """For dimension entities: fraction of existing pool that gets new rows each window."""

    enabled: bool = True
    """Set to False to skip this entity during simulation."""

    entity_type: str = "fact"
    """'dimension' (profiles), 'fact' (trips), or 'event' (telemetry, app events)."""


@dataclass
class WindowResult:
    """Result of a single time-window generation."""

    index: int
    timestamp: datetime
    entities: Dict[str, int]  # entity_name → row count
    files_written: Dict[str, str]  # entity_name → file path

    @property
    def total_rows(self) -> int:
        return sum(self.entities.values())


# ──────────────────────────────────────────────────────────────────────────────
# Demand curves — realistic hour-of-day volume shaping
# ──────────────────────────────────────────────────────────────────────────────

# 24-element list: multiplier for each hour (0–23).
# Models a ride-sharing city: morning commute, lunch bump, evening peak.
_DEMAND_CURVE: List[float] = [
    0.15,
    0.10,
    0.08,
    0.06,
    0.05,
    0.08,  # 00–05: late night / early morning
    0.20,
    0.55,
    0.90,
    0.75,
    0.60,
    0.65,  # 06–11: morning commute + mid-morning
    0.70,
    0.60,
    0.55,
    0.60,
    0.75,
    1.00,  # 12–17: lunch, afternoon, evening start
    0.95,
    0.80,
    0.65,
    0.50,
    0.35,
    0.20,  # 18–23: evening peak → wind-down
]

_CITY_CODES = ["LON", "NYC", "BER", "PAR", "TYO", "SYD"]
_TRIP_TYPES = ["ride", "eats_delivery"]
_PAYMENT_METHODS = ["card", "apple_pay", "google_pay", "cash"]
_PLATFORMS = ["ios", "android"]
_APP_VERSIONS = ["4.12.0", "4.12.1", "4.13.0", "4.13.1", "4.14.0"]
_VEHICLE_TYPES = ["sedan", "suv", "van", "motorcycle", "bicycle"]
_RIDER_STATUSES = ["active", "inactive", "suspended"]
_DRIVER_STATUSES = ["active", "inactive", "suspended", "pending_verification"]
_CANCEL_REASONS = [
    "driver_no_show",
    "rider_changed_mind",
    "wait_too_long",
    "wrong_address",
    "price_too_high",
    "found_alternative",
    "driver_cancelled",
    "safety_concern",
]
_CANCELLED_BY = ["rider", "driver"]
_EVENT_NAMES = [
    "screen_view",
    "button_tap",
    "search",
    "ride_request",
    "payment_update",
    "profile_view",
    "promo_tap",
    "share_ride",
]
_SCREEN_NAMES = [
    "home",
    "ride_options",
    "trip_tracker",
    "payment",
    "profile",
    "promotions",
    "trip_history",
    "settings",
]


def _demand_multiplier(hour: int) -> float:
    """Return the demand curve multiplier for a given hour (0–23)."""
    return _DEMAND_CURVE[hour % 24]


def _generate_id(prefix: str, length: int = 8) -> str:
    """Generate a realistic-looking ID like RDR-A3F8B2C1."""
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return f"{prefix}-{suffix}"


def _random_ts(base: datetime, window_minutes: int) -> str:
    """Generate a random ISO timestamp within the window."""
    offset = random.uniform(0, window_minutes * 60)
    ts = base + timedelta(seconds=offset)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _random_coordinate(center: float, spread: float = 0.05) -> str:
    """Generate a random coordinate near a center point."""
    return f"{center + random.uniform(-spread, spread):.6f}"


# City center coordinates for realistic geo data
_CITY_COORDS = {
    "LON": (51.5074, -0.1278),
    "NYC": (40.7128, -74.0060),
    "BER": (52.5200, 13.4050),
    "PAR": (48.8566, 2.3522),
    "TYO": (35.6762, 139.6503),
    "SYD": (-33.8688, 151.2093),
}


# ──────────────────────────────────────────────────────────────────────────────
# Streaming Simulator
# ──────────────────────────────────────────────────────────────────────────────


class StreamingSimulator:
    """
    Time-windowed batch generator that simulates streaming ingestion.

    Generates PK/FK-consistent micro-batches across related entities,
    writing them to hourly-partitioned landing directories.

    Parameters
    ----------
    entity_config : dict
        Mapping of entity name → ``EntityStreamConfig``.
    landing_root : str
        Root directory for landing zone (e.g. ``./lakehouse/_data/marketplace/rideflow``).
    window_minutes : int
        Simulated time span per window (default 60 = 1 hour).
    start_time : datetime, optional
        Start of the simulation clock (default: today at midnight UTC).
    seed : int
        Random seed for reproducibility.
    initial_riders : int
        Size of the initial rider pool.
    initial_drivers : int
        Size of the initial driver pool.
    """

    def __init__(
        self,
        entity_config: Dict[str, EntityStreamConfig],
        landing_root: str,
        *,
        window_minutes: int = 60,
        start_time: Optional[datetime] = None,
        seed: int = 42,
        initial_riders: int = 200,
        initial_drivers: int = 100,
    ):
        self._config = entity_config
        self._landing_root = Path(landing_root)
        self._window_minutes = window_minutes
        self._seed = seed
        self._rng = random.Random(seed)

        # Simulation clock
        if start_time is None:
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            self._current_time = today
        else:
            self._current_time = start_time

        # ── PK Pools (maintained across windows) ─────────────────────────
        self._rider_ids: List[str] = []
        self._driver_ids: List[str] = []
        self._rider_cities: Dict[str, str] = {}
        self._driver_cities: Dict[str, str] = {}
        self._active_trip_ids: List[str] = []  # trips in-progress
        self._pending_requests: List[dict] = []  # requests awaiting completion

        # City distribution weights (some cities busier than others)
        self._city_weights = {
            "LON": 0.25,
            "NYC": 0.25,
            "BER": 0.15,
            "PAR": 0.15,
            "TYO": 0.12,
            "SYD": 0.08,
        }

        # ── Seed initial dimension pools ─────────────────────────────────
        random.seed(seed)
        self._initial_riders = initial_riders
        self._initial_drivers = initial_drivers

    # ──────────────────────────────────────────────────────────────────
    # Generation order: topological sort by FK dependencies
    # ──────────────────────────────────────────────────────────────────
    _ENTITY_ORDER = [
        # Tier 1: Dimensions (no FKs)
        "rider_profiles",
        "driver_profiles",
        # Tier 2: Facts (FK → dimensions)
        "trip_requests",
        "trip_completed",
        "trip_cancellations",
        # Tier 3: Events (FK → dimensions + facts)
        "driver_telemetry",
        "rider_app_events",
    ]

    def _weighted_city(self) -> str:
        """Pick a weighted random city."""
        cities = list(self._city_weights.keys())
        weights = list(self._city_weights.values())
        return self._rng.choices(cities, weights=weights, k=1)[0]

    def _scale_rows(self, config: EntityStreamConfig, hour: int) -> int:
        """Scale row count by demand curve and peak multiplier."""
        base = config.rows_per_window
        demand = _demand_multiplier(hour)
        # Blend: base rate + (peak-scaled demand)
        scaled = base * (0.3 + 0.7 * demand * config.peak_multiplier / 3.0)
        # Add ±15% jitter
        jitter = self._rng.uniform(0.85, 1.15)
        return max(1, int(scaled * jitter))

    # ──────────────────────────────────────────────────────────────────
    # Entity generators
    # ──────────────────────────────────────────────────────────────────

    def _gen_rider_profiles(self, num: int, ts: datetime) -> List[dict]:
        """Generate new rider profile rows."""
        rows = []
        for _ in range(num):
            rid = _generate_id("RDR")
            self._rider_ids.append(rid)
            city = self._weighted_city()
            self._rider_cities[rid] = city
            rows.append(
                {
                    "rider_id": rid,
                    "name": f"Rider {rid[-4:]}",
                    "email": f"rider.{rid[-6:].lower()}@example.com",
                    "phone": f"+44{self._rng.randint(7000000000, 7999999999)}",
                    "date_of_birth": f"{self._rng.randint(1970, 2003)}-{self._rng.randint(1, 12):02d}-{self._rng.randint(1, 28):02d}",
                    "home_address": f"{self._rng.randint(1, 200)} Example Street, {city}",
                    "city_code": city,
                    "signup_date": _random_ts(ts, self._window_minutes),
                    "status": "active",
                    "preferred_payment_method": self._rng.choice(_PAYMENT_METHODS),
                    "updated_at": _random_ts(ts, self._window_minutes),
                }
            )
        return rows

    def _gen_driver_profiles(self, num: int, ts: datetime) -> List[dict]:
        """Generate new driver profile rows."""
        rows = []
        for _ in range(num):
            did = _generate_id("DRV")
            self._driver_ids.append(did)
            city = self._weighted_city()
            self._driver_cities[did] = city
            rows.append(
                {
                    "driver_id": did,
                    "name": f"Driver {did[-4:]}",
                    "email": f"driver.{did[-6:].lower()}@example.com",
                    "phone": f"+44{self._rng.randint(7000000000, 7999999999)}",
                    "date_of_birth": f"{self._rng.randint(1965, 1998)}-{self._rng.randint(1, 12):02d}-{self._rng.randint(1, 28):02d}",
                    "home_address": f"{self._rng.randint(1, 300)} Driver Lane, {city}",
                    "licence_number": f"LIC-{self._rng.randint(100000, 999999)}",
                    "licence_plate": f"{''.join(self._rng.choices(string.ascii_uppercase, k=2))}{self._rng.randint(10, 99)} {''.join(self._rng.choices(string.ascii_uppercase, k=3))}",
                    "vehicle_type": self._rng.choice(_VEHICLE_TYPES),
                    "bank_account_last_four": f"{self._rng.randint(1000, 9999)}",
                    "city_code": city,
                    "signup_date": _random_ts(ts, self._window_minutes),
                    "status": "active",
                    "rating": f"{self._rng.uniform(4.0, 5.0):.2f}",
                    "updated_at": _random_ts(ts, self._window_minutes),
                }
            )
        return rows

    def _gen_trip_requests(self, num: int, ts: datetime) -> List[dict]:
        """Generate trip request rows with FK to rider_profiles."""
        rows = []
        if not self._rider_ids:
            return rows
        for _ in range(num):
            req_id = _generate_id("REQ")
            rider_id = self._rng.choice(self._rider_ids)
            city = self._rider_cities[rider_id]
            lat, lng = _CITY_COORDS[city]
            trip_type = self._rng.choice(_TRIP_TYPES)
            surge = round(1.0 + self._rng.expovariate(3.0), 2)
            surge = min(surge, 5.0)
            req_ts = _random_ts(ts, self._window_minutes)

            row = {
                "request_id": req_id,
                "rider_id": rider_id,
                "trip_type": trip_type,
                "pickup_lat": _random_coordinate(lat),
                "pickup_lng": _random_coordinate(lng),
                "dropoff_lat": _random_coordinate(lat, 0.08),
                "dropoff_lng": _random_coordinate(lng, 0.08),
                "city_code": city,
                "requested_at": req_ts,
                "estimated_fare": f"{self._rng.uniform(5.0, 85.0):.2f}",
                "estimated_eta_minutes": str(self._rng.randint(2, 20)),
                "surge_multiplier": f"{surge:.2f}",
            }
            rows.append(row)

            # Stash for potential completion/cancellation
            self._pending_requests.append(
                {
                    "request_id": req_id,
                    "rider_id": rider_id,
                    "trip_type": trip_type,
                    "city_code": city,
                    "pickup_lat": row["pickup_lat"],
                    "pickup_lng": row["pickup_lng"],
                    "dropoff_lat": row["dropoff_lat"],
                    "dropoff_lng": row["dropoff_lng"],
                    "requested_at": req_ts,
                    "surge_multiplier": row["surge_multiplier"],
                }
            )
        return rows

    def _gen_trip_completed(self, num: int, ts: datetime) -> List[dict]:
        """Generate trip completed rows with FK to rider + driver + request."""
        rows = []
        if not self._driver_ids or not self._pending_requests:
            return rows

        # Complete up to `num` pending requests
        completable = min(num, len(self._pending_requests))
        to_complete = self._rng.sample(
            range(len(self._pending_requests)), k=min(completable, len(self._pending_requests))
        )
        completed_indices = sorted(to_complete, reverse=True)

        for idx in completed_indices:
            req = self._pending_requests.pop(idx)
            trip_id = _generate_id("TRP")
            driver_id = self._rng.choice(self._driver_ids)

            # Generate realistic timing
            pickup_offset = self._rng.randint(3, 15)  # minutes after request
            duration = self._rng.randint(5, 55)  # trip duration in minutes
            req_dt = datetime.fromisoformat(req["requested_at"].replace("Z", "+00:00"))
            pickup_dt = req_dt + timedelta(minutes=pickup_offset)
            dropoff_dt = pickup_dt + timedelta(minutes=duration)

            distance = round(self._rng.uniform(1.0, 25.0), 1)
            surge = float(req["surge_multiplier"])
            base_fare = distance * self._rng.uniform(1.5, 3.0)
            fare = round(base_fare * surge, 2)
            tip = round(fare * self._rng.choice([0, 0, 0, 0.1, 0.15, 0.2]), 2)

            rows.append(
                {
                    "trip_id": trip_id,
                    "rider_id": req["rider_id"],
                    "driver_id": driver_id,
                    "trip_type": req["trip_type"],
                    "pickup_lat": req["pickup_lat"],
                    "pickup_lng": req["pickup_lng"],
                    "dropoff_lat": req["dropoff_lat"],
                    "dropoff_lng": req["dropoff_lng"],
                    "city_code": req["city_code"],
                    "requested_at": req["requested_at"],
                    "pickup_at": pickup_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "dropoff_at": dropoff_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "distance_km": str(distance),
                    "duration_minutes": str(duration),
                    "fare_amount": f"{fare:.2f}",
                    "surge_multiplier": req["surge_multiplier"],
                    "tip_amount": f"{tip:.2f}",
                    "payment_method": self._rng.choice(_PAYMENT_METHODS),
                    "rider_rating": str(self._rng.randint(3, 5)),
                    "driver_rating": str(self._rng.randint(3, 5)),
                    "notes": "",
                }
            )
            self._active_trip_ids.append(trip_id)

        return rows

    def _gen_trip_cancellations(self, num: int, ts: datetime) -> List[dict]:
        """Generate cancellation rows from pending requests."""
        rows = []
        if not self._pending_requests:
            return rows

        cancellable = min(num, len(self._pending_requests))
        to_cancel = self._rng.sample(
            range(len(self._pending_requests)), k=min(cancellable, len(self._pending_requests))
        )
        cancelled_indices = sorted(to_cancel, reverse=True)

        for idx in cancelled_indices:
            req = self._pending_requests.pop(idx)
            cancel_id = _generate_id("CXL")
            cancelled_by = self._rng.choice(_CANCELLED_BY)
            req_dt = datetime.fromisoformat(req["requested_at"].replace("Z", "+00:00"))
            cancel_dt = req_dt + timedelta(minutes=self._rng.randint(1, 10))

            rows.append(
                {
                    "cancellation_id": cancel_id,
                    "trip_id": "",  # pre-match cancellation
                    "rider_id": req["rider_id"],
                    "driver_id": self._rng.choice(self._driver_ids)
                    if self._driver_ids and cancelled_by == "driver"
                    else "",
                    "cancelled_by": cancelled_by,
                    "cancel_reason_code": self._rng.choice(_CANCEL_REASONS),
                    "city_code": req["city_code"],
                    "requested_at": req["requested_at"],
                    "cancelled_at": cancel_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "cancellation_fee": f"{self._rng.choice([0, 0, 0, 2.50, 5.00]):.2f}",
                }
            )

        return rows

    def _gen_driver_telemetry(self, num: int, ts: datetime) -> List[dict]:
        """Generate GPS telemetry pings."""
        rows = []
        if not self._driver_ids:
            return rows
        for _ in range(num):
            driver_id = self._rng.choice(self._driver_ids)
            city = self._driver_cities[driver_id]
            lat, lng = _CITY_COORDS[city]
            status = self._rng.choice(["on_trip", "on_trip", "on_trip", "idle", "offline"])
            trip_id = self._rng.choice(self._active_trip_ids) if self._active_trip_ids and status == "on_trip" else ""

            rows.append(
                {
                    "telemetry_id": _generate_id("TEL"),
                    "driver_id": driver_id,
                    "gps_lat": _random_coordinate(lat, 0.02),
                    "gps_lng": _random_coordinate(lng, 0.02),
                    "speed_kmh": str(self._rng.randint(0, 80)),
                    "heading": str(self._rng.randint(0, 360)),
                    "accuracy_meters": str(self._rng.randint(3, 25)),
                    "trip_id": trip_id,
                    "status": status,
                    "city_code": city,
                    "timestamp": _random_ts(ts, self._window_minutes),
                }
            )
        return rows

    def _gen_rider_app_events(self, num: int, ts: datetime) -> List[dict]:
        """Generate rider app interaction events."""
        rows = []
        if not self._rider_ids:
            return rows
        for _ in range(num):
            rider_id = self._rng.choice(self._rider_ids)
            city = self._rider_cities[rider_id]
            rows.append(
                {
                    "event_id": _generate_id("EVT"),
                    "rider_id": rider_id,
                    "device_id": f"DEV-{self._rng.randint(100000, 999999)}",
                    "ip_address": f"{self._rng.randint(10, 220)}.{self._rng.randint(0, 255)}.{self._rng.randint(0, 255)}.{self._rng.randint(1, 254)}",
                    "event_name": self._rng.choice(_EVENT_NAMES),
                    "screen_name": self._rng.choice(_SCREEN_NAMES),
                    "event_timestamp": _random_ts(ts, self._window_minutes),
                    "app_version": self._rng.choice(_APP_VERSIONS),
                    "platform": self._rng.choice(_PLATFORMS),
                    "city_code": city,
                    "event_properties_json": "{}",
                }
            )
        return rows

    # ──────────────────────────────────────────────────────────────────
    # Generator dispatch
    # ──────────────────────────────────────────────────────────────────

    _GENERATORS = {
        "rider_profiles": "_gen_rider_profiles",
        "driver_profiles": "_gen_driver_profiles",
        "trip_requests": "_gen_trip_requests",
        "trip_completed": "_gen_trip_completed",
        "trip_cancellations": "_gen_trip_cancellations",
        "driver_telemetry": "_gen_driver_telemetry",
        "rider_app_events": "_gen_rider_app_events",
    }

    def _write_csv(
        self,
        entity: str,
        rows: List[dict],
        ts: datetime,
        micro_batches: int = 1,
    ) -> List[str]:
        """Write rows to partitioned CSV files in the landing zone.

        Parameters
        ----------
        entity : str
            Entity name (used as top-level directory).
        rows : list[dict]
            Row data to write.
        ts : datetime
            Window timestamp (used for partition path).
        micro_batches : int
            Number of files to split the rows across (default 1).
            Set to e.g. 6 for ~10-minute micro-batch files in a
            60-minute window.

        Returns
        -------
        list[str]
            Paths of all files written.
        """
        if not rows:
            return []

        # Build partition path: entity/y_YYYY/m_MM/d_DD/h_HH/
        partition = f"y_{ts.strftime('%Y')}/m_{ts.strftime('%m')}/d_{ts.strftime('%d')}/h_{ts.strftime('%H')}"
        out_dir = self._landing_root / entity / partition
        out_dir.mkdir(parents=True, exist_ok=True)

        fieldnames = list(rows[0].keys())
        files_written: List[str] = []

        if micro_batches <= 1:
            # Single file (original behavior)
            filename = f"batch_{uuid.uuid4().hex[:8]}.csv"
            filepath = out_dir / filename
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            files_written.append(str(filepath))
        else:
            # Split rows across N micro-batch files
            chunk_size = max(1, math.ceil(len(rows) / micro_batches))
            for mb_idx in range(micro_batches):
                start = mb_idx * chunk_size
                end = min(start + chunk_size, len(rows))
                if start >= len(rows):
                    break
                chunk = rows[start:end]
                filename = f"batch_{mb_idx:02d}_{uuid.uuid4().hex[:6]}.csv"
                filepath = out_dir / filename
                with open(filepath, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(chunk)
                files_written.append(str(filepath))

        return files_written

    # ──────────────────────────────────────────────────────────────────
    # Main simulation loop
    # ──────────────────────────────────────────────────────────────────

    def _seed_initial_pools(self) -> WindowResult:
        """Generate the initial dimension pools before streaming begins."""
        ts = self._current_time
        entities: Dict[str, int] = {}
        files: Dict[str, str] = {}

        # Seed riders
        rider_rows = self._gen_rider_profiles(self._initial_riders, ts)
        if rider_rows:
            fps = self._write_csv("rider_profiles", rider_rows, ts)
            entities["rider_profiles"] = len(rider_rows)
            files["rider_profiles"] = fps[0] if fps else ""

        # Seed drivers
        driver_rows = self._gen_driver_profiles(self._initial_drivers, ts)
        if driver_rows:
            fps = self._write_csv("driver_profiles", driver_rows, ts)
            entities["driver_profiles"] = len(driver_rows)
            files["driver_profiles"] = fps[0] if fps else ""

        logger.info(f"🌱 Seeded initial pools: {len(self._rider_ids)} riders, {len(self._driver_ids)} drivers")

        return WindowResult(
            index=-1,
            timestamp=ts,
            entities=entities,
            files_written=files,
        )

    def _generate_window(self, window_index: int, micro_batches: int = 1) -> WindowResult:
        """Generate a single time window of data for all entities."""
        ts = self._current_time
        hour = ts.hour
        entities: Dict[str, int] = {}
        files: Dict[str, str] = {}

        for entity_name in self._ENTITY_ORDER:
            config = self._config.get(entity_name)
            if config is None or not config.enabled:
                continue

            # Determine row count
            if config.entity_type == "dimension":
                # Dimensions: generate churn_rate × pool_size new rows
                pool_size = len(self._rider_ids) if "rider" in entity_name else len(self._driver_ids)
                num = max(1, int(pool_size * config.churn_rate))
            else:
                num = self._scale_rows(config, hour)

            # Call the generator
            gen_method = getattr(self, self._GENERATORS[entity_name])
            rows = gen_method(num, ts)

            if rows:
                fps = self._write_csv(entity_name, rows, ts, micro_batches=micro_batches)
                entities[entity_name] = len(rows)
                files[entity_name] = fps[0] if fps else ""

        return WindowResult(
            index=window_index,
            timestamp=ts,
            entities=entities,
            files_written=files,
        )

    def run(
        self,
        num_windows: int = 24,
        *,
        include_seed: bool = True,
        micro_batches: int = 1,
        up_to: Optional[datetime] = None,
        resume: bool = False,
    ) -> Iterator[WindowResult]:
        """
        Run the streaming simulation for ``num_windows`` time windows.

        Parameters
        ----------
        num_windows : int
            Number of windows to generate (default 24 = one full day).
        include_seed : bool
            If True, yield a seed window (index=-1) with initial dimension pools.
        micro_batches : int
            Number of files to split each entity's rows into per window
            (default 1).  Set to 6 for ~10-minute micro-batch files in a
            60-minute window, or 60 for ~1 file per minute.
        up_to : datetime, optional
            Stop generating windows whose start time exceeds this timestamp.
            Defaults to None (no cap).  Set to
            ``datetime.now(timezone.utc)`` to prevent generating future data.
        resume : bool
            If True, scan existing landing zone partitions to find the latest
            one, rebuild PK pools from dimension CSVs, and start generating
            from the next window.  Allows incremental runs.

        Yields
        ------
        WindowResult
            The result of each generated window.
        """
        # ── Resume: rebuild state from existing data ──────────────────────
        if resume:
            self._rebuild_state_from_landing()

        # Seed initial dimension pools
        if include_seed and not self._rider_ids:
            seed_result = self._seed_initial_pools()
            yield seed_result

        for i in range(num_windows):
            # ── Time cap: stop if current window is past up_to ────────────
            if up_to is not None and self._current_time >= up_to:
                logger.info(f"⏹ Stopping at window {i}: {self._current_time.isoformat()} >= up_to {up_to.isoformat()}")
                break

            result = self._generate_window(i, micro_batches=micro_batches)
            logger.info(
                f"⏱ Window {i:3d} | {self._current_time.strftime('%Y-%m-%d %H:%M')} | "
                f"{result.total_rows:>5,} rows | "
                + " ".join(f"{k}={v}" for k, v in result.entities.items())
                + (f" | {micro_batches} micro-batches/entity" if micro_batches > 1 else "")
            )
            yield result

            # Advance simulation clock
            self._current_time += timedelta(minutes=self._window_minutes)

            # Trim active trips (old trips expire after a few windows)
            if len(self._active_trip_ids) > 500:
                self._active_trip_ids = self._active_trip_ids[-200:]

    def run_all(self, num_windows: int = 24, **kwargs) -> List[WindowResult]:
        """Non-generator version — returns all windows as a list."""
        return list(self.run(num_windows, **kwargs))

    # ──────────────────────────────────────────────────────────────────
    # Resume: rebuild state from existing landing zone
    # ──────────────────────────────────────────────────────────────────

    def _rebuild_state_from_landing(self) -> None:
        """Scan existing landing zone to rebuild PK pools and advance the clock.

        Reads rider_profiles and driver_profiles CSVs to reconstruct
        ``_rider_ids``, ``_driver_ids``, ``_rider_cities``, and
        ``_driver_cities``.  Finds the latest hourly partition across all
        entities and advances ``_current_time`` to the next window.
        """
        import re as _re_mod

        if not self._landing_root.exists():
            logger.info("No existing landing zone found -- starting fresh")
            return

        # ── Rebuild dimension pools from CSVs ─────────────────────────
        rider_dir = self._landing_root / "rider_profiles"
        driver_dir = self._landing_root / "driver_profiles"

        if rider_dir.exists():
            for csv_file in rider_dir.rglob("*.csv"):
                try:
                    with open(csv_file, "r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            rid = row.get("rider_id", "")
                            city = row.get("city_code", "")
                            if rid and rid not in self._rider_cities:
                                self._rider_ids.append(rid)
                                if city:
                                    self._rider_cities[rid] = city
                except Exception:
                    continue

        if driver_dir.exists():
            for csv_file in driver_dir.rglob("*.csv"):
                try:
                    with open(csv_file, "r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            did = row.get("driver_id", "")
                            city = row.get("city_code", "")
                            if did and did not in self._driver_cities:
                                self._driver_ids.append(did)
                                if city:
                                    self._driver_cities[did] = city
                except Exception:
                    continue

        # ── Find latest partition timestamp ───────────────────────────
        hour_pattern = _re_mod.compile(r"y_(\d{4})/m_(\d{2})/d_(\d{2})/h_(\d{2})")
        latest: Optional[datetime] = None

        for dirpath, _, _ in os.walk(self._landing_root):
            rel = str(Path(dirpath).relative_to(self._landing_root)).replace("\\", "/")
            match = hour_pattern.search(rel)
            if match:
                try:
                    ts = datetime(
                        year=int(match.group(1)),
                        month=int(match.group(2)),
                        day=int(match.group(3)),
                        hour=int(match.group(4)),
                        tzinfo=timezone.utc,
                    )
                    if latest is None or ts > latest:
                        latest = ts
                except ValueError:
                    continue

        if latest is not None:
            # Advance to the NEXT window after the latest partition
            self._current_time = latest + timedelta(minutes=self._window_minutes)
            logger.info(
                f"Resumed: {len(self._rider_ids)} riders, "
                f"{len(self._driver_ids)} drivers rebuilt. "
                f"Next window: {self._current_time.isoformat()}"
            )
        else:
            logger.info("No partitions found in landing zone -- starting fresh")

    # ──────────────────────────────────────────────────────────────────
    # Convenience factory
    # ──────────────────────────────────────────────────────────────────

    @classmethod
    def rideflow_marketplace(
        cls,
        landing_root: str,
        *,
        window_minutes: int = 60,
        start_time: Optional[datetime] = None,
        seed: int = 42,
        initial_riders: int = 200,
        initial_drivers: int = 100,
        **overrides: Any,
    ) -> "StreamingSimulator":
        """
        Pre-configured simulator for the marketplace/rideflow domain.

        Parameters
        ----------
        landing_root : str
            Root directory for landing zone CSV files.
        window_minutes : int
            Simulated time span per window (default 60).
        start_time : datetime, optional
            Simulation start (default: today at midnight UTC).
        seed : int
            Random seed.
        initial_riders : int
            Number of riders to seed before streaming.
        initial_drivers : int
            Number of drivers to seed before streaming.

        Returns
        -------
        StreamingSimulator
        """
        config = {
            "rider_profiles": EntityStreamConfig(
                rows_per_window=10,
                churn_rate=0.03,
                entity_type="dimension",
            ),
            "driver_profiles": EntityStreamConfig(
                rows_per_window=5,
                churn_rate=0.02,
                entity_type="dimension",
            ),
            "trip_requests": EntityStreamConfig(
                rows_per_window=80,
                peak_multiplier=4.0,
                entity_type="fact",
            ),
            "trip_completed": EntityStreamConfig(
                rows_per_window=65,
                peak_multiplier=3.5,
                entity_type="fact",
            ),
            "trip_cancellations": EntityStreamConfig(
                rows_per_window=15,
                peak_multiplier=2.0,
                entity_type="fact",
            ),
            "driver_telemetry": EntityStreamConfig(
                rows_per_window=500,
                peak_multiplier=3.0,
                entity_type="event",
            ),
            "rider_app_events": EntityStreamConfig(
                rows_per_window=200,
                peak_multiplier=2.5,
                entity_type="event",
            ),
        }

        return cls(
            entity_config=config,
            landing_root=landing_root,
            window_minutes=window_minutes,
            start_time=start_time,
            seed=seed,
            initial_riders=initial_riders,
            initial_drivers=initial_drivers,
        )

    # ──────────────────────────────────────────────────────────────────
    # FK consistency checks (for verification)
    # ──────────────────────────────────────────────────────────────────

    def validate_fk_consistency(self) -> Dict[str, Any]:
        """
        Return a summary of FK consistency across the generated data.
        Call after ``run()`` to verify referential integrity.
        """
        rider_set = set(self._rider_ids)
        driver_set = set(self._driver_ids)

        return {
            "total_riders": len(rider_set),
            "total_drivers": len(driver_set),
            "active_trip_ids": len(self._active_trip_ids),
            "pending_requests": len(self._pending_requests),
            "fk_pools_healthy": len(rider_set) > 0 and len(driver_set) > 0,
        }
