"""
lakelogic.core.generator
------------------------
Contract-aware synthetic data generator.

Reads a DataContract and generates rows that respect field types, nullability,
accepted_values, and range constraints. Optionally injects a controlled ratio of
invalid records so you can stress-test quarantine logic without touching real data.

Python API
----------
    from lakelogic import DataGenerator

    # Pure synthetic generation from a contract
    gen = DataGenerator("contracts/orders.yaml")
    df  = gen.generate(rows=1_000, invalid_ratio=0.05)          # 5% bad rows
    gen.save(df, "sample_orders.parquet", format="parquet")

    # Contract-free: infer schema AND seed from an existing file — no YAML required
    gen = DataGenerator.from_file("data/zoopla_sample.csv")     # schema inferred
    df  = gen.generate(rows=5_000)                              # values mirror the file
    df  = gen.generate(rows=500, invalid_ratio=0.1)             # 10% bad rows

    # Contract + file: use schema from contract, distributions from file
    gen = DataGenerator("contracts/orders.yaml")
    df  = gen.generate_from_sample("data/orders_sample.csv", rows=5_000)

    # ── Shorthand Schema Initialization ─────────────────────────────────────
    
    # DDL string
    gen = DataGenerator("order_id BIGINT, email STRING, amount DOUBLE, created_at TIMESTAMP")
    df  = gen.generate(rows=1000, invalid_ratio=0.10)

    # List of tuples
    gen = DataGenerator([("user_id", "integer"), ("name", "string"), ("active", "boolean")])
    df  = gen.generate(rows=500)

    # Dictionary
    gen = DataGenerator({"product_id": "integer", "price": "decimal", "in_stock": "boolean"})
    df  = gen.generate(rows=200)

    # Spark StructType
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([StructField("user_id", IntegerType()), StructField("email", StringType())])
    gen = DataGenerator(schema)
    df  = gen.generate(rows=50)

    # ── Reading from Unity Catalog / Database Tables ───────────────────────
    
    from lakelogic import infer_contract
    # Chain directly to generator from a live catalog table
    gen = infer_contract("my_catalog.sales.orders").to_generator(seed=42)
    df  = gen.generate(rows=500, invalid_ratio=0.05)

    # ── Multi-Table Referential Integrity (generate_related) ─────────────

    # Auto-detected FK relationships (column names match across contracts)
    related = DataGenerator.generate_related(
        contracts={
            "customers": "customer_id BIGINT, name STRING, email STRING",
            "orders": "order_id BIGINT, customer_id BIGINT, amount DOUBLE",
        },
        rows={"customers": 50, "orders": 200},
    )
    # Every orders["customer_id"] exists in customers["customer_id"]

    # Explicit FK relationships (when column names differ)
    related = DataGenerator.generate_related(
        contracts={
            "customers": "id BIGINT, name STRING, email STRING",
            "products":  "id BIGINT, product_name STRING, price DOUBLE",
            "sales":     "sale_id BIGINT, cust_id BIGINT, prod_id BIGINT, amount DOUBLE",
        },
        rows={"customers": 50, "products": 30, "sales": 500},
        relationships=[
            {"child": "sales", "child_column": "cust_id",  "parent": "customers", "parent_column": "id"},
            {"child": "sales", "child_column": "prod_id",  "parent": "products",  "parent_column": "id"},
        ],
    )
    # Every sales["cust_id"] exists in customers["id"]
    # Every sales["prod_id"] exists in products["id"]

CLI
---
    lakelogic generate --contract contracts/orders.yaml --rows 1000 \\
                       --invalid-ratio 0.05 --format parquet --output sample.parquet
"""

from __future__ import annotations

import json as _json
import random
import re
import string
from dataclasses import dataclass

# Module-level alias used in _build_field_rules SQL parsing helpers
_re = re

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# Contract type → Polars dtype mapping
# ---------------------------------------------------------------------------


def _polars_dtype(ftype: str):
    """Return the polars DataType for a contract field type string."""
    try:
        import polars as pl
    except ImportError:
        return None

    _MAP = {
        "string": pl.Utf8,
        "str": pl.Utf8,
        "text": pl.Utf8,
        "varchar": pl.Utf8,
        "integer": pl.Int64,
        "int": pl.Int64,
        "int32": pl.Int32,
        "int64": pl.Int64,
        "long": pl.Int64,
        "double": pl.Float64,
        "float": pl.Float64,
        "float32": pl.Float32,
        "float64": pl.Float64,
        "decimal": pl.Float64,
        "number": pl.Float64,
        "boolean": pl.Boolean,
        "bool": pl.Boolean,
        "date": pl.Utf8,  # stored as ISO string; processor parses
        "timestamp": pl.Utf8,
        "datetime": pl.Utf8,
    }
    return _MAP.get(ftype.lower().split("(")[0].strip())


# ---------------------------------------------------------------------------
# Optional deps — only fail at call time, not at import time
# ---------------------------------------------------------------------------


def _try_faker():
    try:
        from faker import Faker

        return Faker()
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Test Case metadata — used by the generation report
# ---------------------------------------------------------------------------

_TEST_CASE_TYPES = {
    "NOT_NULL_VIOLATION": "Required field set to null",
    "ACCEPTED_VALUE_VIOLATION": "Value outside accepted_values list",
    "RANGE_VIOLATION": "Numeric value outside min/max bounds",
    "TYPE_CONFUSION": "Wrong type injected (e.g. string in numeric field)",
    "REGEX_VIOLATION": "Value violates expected format/pattern",
    "TEMPORAL_VIOLATION": "Date/timestamp ordering or consistency broken",
    "BOUNDARY_VALUE": "Numeric boundary value (0, -1, MAX_INT, etc.)",
    "EDGE_CASE_AI": "AI-generated edge case value",
    "EDGE_CASE_BUILTIN": "Built-in edge case from profile index",
    "EMPTY_STRING": "Empty string on a text field",
}


@dataclass
class TestCaseInfo:
    """Metadata about a single invalid value injected during generation.

    Collected by ``_make_invalid_value`` and aggregated into the
    generation report by ``generate()``.
    """

    type: str  # Key from _TEST_CASE_TYPES, e.g. "NOT_NULL_VIOLATION"
    field: str  # Field name, e.g. "email"
    value: Any = None  # The actual invalid value injected
    description: str = ""  # Human-readable explanation
    contract_rule: str = ""  # e.g. "quality.enforce_required"
    row_index: int = -1  # Set after row is assigned its position


# ---------------------------------------------------------------------------
# Fallback pools for synthetic data (when Faker is not available)
# ---------------------------------------------------------------------------

_FIRST_NAMES = [
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "William",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
]

_LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
]

_GEO_DATA = [
    # alpha2, alpha3, numeric, name, currency
    ("US", "USA", "840", "United States of America", "USD"),
    ("GB", "GBR", "826", "United Kingdom", "GBP"),
    ("DE", "DEU", "276", "Germany", "EUR"),
    ("FR", "FRA", "250", "France", "EUR"),
    ("CA", "CAN", "124", "Canada", "CAD"),
    ("AU", "AUS", "036", "Australia", "AUD"),
    ("JP", "JPN", "392", "Japan", "JPY"),
    ("IN", "IND", "356", "India", "INR"),
    ("BR", "BRA", "076", "Brazil", "BRL"),
    ("MX", "MEX", "484", "Mexico", "MXN"),
    ("IT", "ITA", "380", "Italy", "EUR"),
    ("ES", "ESP", "724", "Spain", "EUR"),
    ("NL", "NLD", "528", "Netherlands", "EUR"),
    ("SE", "SWE", "752", "Sweden", "SEK"),
    ("CH", "CHE", "756", "Switzerland", "CHF"),
    ("SG", "SGP", "702", "Singapore", "SGD"),
    ("ZA", "ZAF", "710", "South Africa", "ZAR"),
    ("NZ", "NZL", "554", "New Zealand", "NZD"),
    ("CN", "CHN", "156", "China", "CNY"),
    ("KR", "KOR", "410", "South Korea", "KRW"),
]

_GEO_LOOKUP_BY_NAME = {}
for _a2, _a3, _num, _name, _curr in _GEO_DATA:
    _GEO_LOOKUP_BY_NAME[_name] = {
        "country_code": _a2,
        "country_code_alpha2": _a2,
        "country_code_alpha3": _a3,
        "country_code_numeric": _num,
        "primary_currency_code": _curr,
        "currency_code": _curr,
        "currency": _curr,
    }

# ── City → Coordinates lookup for geo-alignment ──────────────────────────
# Maps city names, codes, and common abbreviations to (lat, lng) center.
# Used by _apply_geo_alignment() to snap lat/lng fields to match a city field.
_CITY_GEO_COORDS: Dict[str, Tuple[float, float]] = {
    # Full names (lowercase for matching)
    "london": (51.5074, -0.1278),
    "new york": (40.7128, -74.0060),
    "new york city": (40.7128, -74.0060),
    "berlin": (52.5200, 13.4050),
    "paris": (48.8566, 2.3522),
    "tokyo": (35.6762, 139.6503),
    "sydney": (-33.8688, 151.2093),
    "los angeles": (34.0522, -118.2437),
    "chicago": (41.8781, -87.6298),
    "san francisco": (37.7749, -122.4194),
    "toronto": (43.6532, -79.3832),
    "mexico city": (19.4326, -99.1332),
    "mumbai": (19.0760, 72.8777),
    "singapore": (1.3521, 103.8198),
    "dubai": (25.2048, 55.2708),
    "amsterdam": (52.3676, 4.9041),
    "madrid": (40.4168, -3.7038),
    "rome": (41.9028, 12.4964),
    "seoul": (37.5665, 126.9780),
    "beijing": (39.9042, 116.4074),
    "shanghai": (31.2304, 121.4737),
    "hong kong": (22.3193, 114.1694),
    "bangkok": (13.7563, 100.5018),
    "istanbul": (41.0082, 28.9784),
    "cape town": (-33.9249, 18.4241),
    "johannesburg": (-26.2041, 28.0473),
    "lagos": (6.5244, 3.3792),
    "nairobi": (-1.2921, 36.8219),
    "cairo": (30.0444, 31.2357),
    "moscow": (55.7558, 37.6173),
    "seattle": (47.6062, -122.3321),
    "boston": (42.3601, -71.0589),
    "miami": (25.7617, -80.1918),
    "atlanta": (33.7490, -84.3880),
    "denver": (39.7392, -104.9903),
    "austin": (30.2672, -97.7431),
    "dallas": (32.7767, -96.7970),
    "houston": (29.7604, -95.3698),
    "phoenix": (33.4484, -112.0740),
    "portland": (45.5152, -122.6784),
    "vancouver": (49.2827, -123.1207),
    "montreal": (45.5017, -73.5673),
    "melbourne": (-37.8136, 144.9631),
    "brisbane": (-27.4698, 153.0251),
    "auckland": (-36.8485, 174.7633),
    "dublin": (53.3498, -6.2603),
    "lisbon": (38.7223, -9.1393),
    "zurich": (47.3769, 8.5417),
    "stockholm": (59.3293, 18.0686),
    "oslo": (59.9139, 10.7522),
    "copenhagen": (55.6761, 12.5683),
    "helsinki": (60.1699, 24.9384),
    "warsaw": (52.2297, 21.0122),
    "prague": (50.0755, 14.4378),
    "vienna": (48.2082, 16.3738),
    "budapest": (47.4979, 19.0402),
    "bucharest": (44.4268, 26.1025),
    # Short codes (e.g. from city_code fields)
    "lon": (51.5074, -0.1278),
    "nyc": (40.7128, -74.0060),
    "ber": (52.5200, 13.4050),
    "par": (48.8566, 2.3522),
    "tyo": (35.6762, 139.6503),
    "syd": (-33.8688, 151.2093),
    "lax": (34.0522, -118.2437),
    "chi": (41.8781, -87.6298),
    "sfo": (37.7749, -122.4194),
    "tor": (43.6532, -79.3832),
    "mex": (19.4326, -99.1332),
    "bom": (19.0760, 72.8777),
    "sin": (1.3521, 103.8198),
    "dxb": (25.2048, 55.2708),
    "ams": (52.3676, 4.9041),
    "mad": (40.4168, -3.7038),
    "rom": (41.9028, 12.4964),
    "sel": (37.5665, 126.9780),
    "pek": (39.9042, 116.4074),
    "sha": (31.2304, 121.4737),
    "hkg": (22.3193, 114.1694),
    "bkk": (13.7563, 100.5018),
    "ist": (41.0082, 28.9784),
    "cpt": (-33.9249, 18.4241),
    "jnb": (-26.2041, 28.0473),
    "los": (6.5244, 3.3792),
    "nbo": (-1.2921, 36.8219),
    "cai": (30.0444, 31.2357),
    "svo": (55.7558, 37.6173),
    "sea": (47.6062, -122.3321),
    "bos": (42.3601, -71.0589),
    "mia": (25.7617, -80.1918),
    "atl": (33.7490, -84.3880),
    "den": (39.7392, -104.9903),
    "aus": (30.2672, -97.7431),
    "dfw": (32.7767, -96.7970),
    "hou": (29.7604, -95.3698),
    "phx": (33.4484, -112.0740),
    "pdx": (45.5152, -122.6784),
    "yvr": (49.2827, -123.1207),
    "yul": (45.5017, -73.5673),
    "mel": (-37.8136, 144.9631),
    "bne": (-27.4698, 153.0251),
    "akl": (-36.8485, 174.7633),
    "dub": (53.3498, -6.2603),
    "lis": (38.7223, -9.1393),
    "zrh": (47.3769, 8.5417),
}

# Field name patterns for geo-alignment detection
_LAT_FIELD_PATTERNS = {
    "latitude",
    "lat",
    "gps_lat",
    "pickup_lat",
    "dropoff_lat",
    "start_lat",
    "end_lat",
    "origin_lat",
    "dest_lat",
    "destination_lat",
}
_LNG_FIELD_PATTERNS = {
    "longitude",
    "lng",
    "lon",
    "gps_lng",
    "gps_lon",
    "pickup_lng",
    "dropoff_lng",
    "pickup_lon",
    "dropoff_lon",
    "start_lng",
    "end_lng",
    "origin_lng",
    "dest_lng",
    "destination_lng",
    "start_lon",
    "end_lon",
    "origin_lon",
    "dest_lon",
    "destination_lon",
}
_CITY_FIELD_PATTERNS = {"city", "city_code", "city_name", "town", "metro", "metro_area", "location_city"}


_TIMESTAMP_SUFFIXES = ("_at", "_time", "_timestamp", "_dt", "_ts")
_DATE_NAME_SUFFIXES = ("_date", "_on")
_DATE_NAME_CONTAINS = ("date_", "day_")


def _match_semantic_hint(name: str) -> Optional[str]:
    """
    Map a field name to a Faker method name using regex/word boundaries.
    """
    # Simplified mapping for common fields
    _HINTS = {
        r"\bemail\b": "email",
        r"\b(first_?name|given_?name)\b": "first_name",
        r"\b(last_?name|family_?name|surname)\b": "last_name",
        r"\bcity\b": "city",
        r"\bcountry\b": "country",
        r"\bpostcode\b": "postcode",
        r"\bphone\b": "phone_number",
        r"\baddress\b": "address",
        r"\bcompany\b": "company",
        r"\bjob\b": "job",
        r"\b(url|website)\b": "url",
        r"\bip\b": "ipv4",
    }
    for pattern, method in _HINTS.items():
        if re.search(pattern, name, re.IGNORECASE):
            return method
    return None


# ---------------------------------------------------------------------------
# Semantic hints — map common field names to Faker generators
# ---------------------------------------------------------------------------

_SEMANTIC_HINTS: Dict[str, str] = {
    # ── Identity & People ─────────────────────────────────────────────────
    "email": "email",
    "email_address": "email",
    "name": "name",
    "full_name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "surname": "last_name",
    "username": "user_name",
    "user_name": "user_name",
    "phone": "phone_number",
    "phone_number": "phone_number",
    "mobile": "phone_number",
    "mobile_number": "phone_number",
    "telephone": "phone_number",
    "ssn": "ssn",
    "social_security": "ssn",
    # ── Address ───────────────────────────────────────────────────────────
    "address": "address",
    "street": "street_address",
    "street_address": "street_address",
    "city": "city",
    "town": "city",
    "state": "state",
    "county": "city",
    "postcode": "postcode",
    "postal_code": "postcode",
    "zip": "zipcode",
    "zipcode": "zipcode",
    "zip_code": "zipcode",
    "zip_code_prefix": "zipcode",
    "latitude": "latitude",
    "lat": "latitude",
    "longitude": "longitude",
    "lng": "longitude",
    "lon": "longitude",
    "country": "country",
    "country_name": "country",
    "country_code": "country_code",
    "country_code_alpha2": "country_code",
    "country_code_alpha3": "bothify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')",
    "country_code_numeric": "bothify(text='###')",
    # ── Business ──────────────────────────────────────────────────────────
    "company": "company",
    "company_name": "company",
    "organisation": "company",
    "organization": "company",
    "job": "job",
    "job_title": "job",
    "title": "sentence",
    # ── Internet ──────────────────────────────────────────────────────────
    "url": "url",
    "website": "url",
    "domain_name": "domain_name",
    "domain": "domain_name",
    "ip": "ipv4",
    "ip_address": "ipv4",
    "ipv4": "ipv4",
    "ipv6": "ipv6",
    "mac_address": "mac_address",
    "user_agent": "user_agent",
    "browser": "user_agent",
    "uuid": "uuid4",
    # ── Text ──────────────────────────────────────────────────────────────
    "description": "sentence",
    "summary": "sentence",
    "notes": "text",
    "comment": "sentence",
    "bio": "text",
    "paragraph": "paragraph",
    # ── Financial (existing) ──────────────────────────────────────────────
    "iban": "iban",
    "credit_card": "credit_card_number",
    "card_number": "credit_card_number",
    "currency_code": "currency_code",
    "isbn": "isbn13",
    # ── Finance & Banking ─────────────────────────────────────────────────
    "account_number": "bban",
    "sort_code": "bban",
    "swift_code": "swift",
    "bic": "swift",
    "routing_number": "aba",
    "tax_id": "ein",
    "vat_number": "ein",
    "invoice_number": "bothify(text='INV-####-????')",
    "transaction_id": "uuid4",
    "amount": "pyfloat(min_value=0.01, max_value=10000, right_digits=2)",
    "balance": "pyfloat(min_value=0, max_value=100000, right_digits=2)",
    "price": "pyfloat(min_value=0.01, max_value=999.99, right_digits=2)",
    "cost": "pyfloat(min_value=0.01, max_value=9999.99, right_digits=2)",
    "discount": "pyfloat(min_value=0, max_value=100, right_digits=2)",
    "tax_rate": "pyfloat(min_value=0, max_value=30, right_digits=2)",
    "quantity": "pyint(min_value=1, max_value=10000)",
    "quantity_on_hand": "pyint(min_value=0, max_value=10000)",
    "quantity_available": "pyint(min_value=0, max_value=10000)",
    "quantity_committed": "pyint(min_value=0, max_value=5000)",
    "stock_level": "pyint(min_value=0, max_value=5000)",
    # ── Healthcare ────────────────────────────────────────────────────────
    "nhs_number": "numerify(text='### ### ####')",
    "patient_id": "bothify(text='PAT-######')",
    "diagnosis_code": "bothify(text='???##.#')",
    "icd_code": "bothify(text='???##.#')",
    "npi": "numerify(text='##########')",
    "drug_name": "word",
    "dosage": "bothify(text='##mg')",
    # ── E-commerce / Retail ───────────────────────────────────────────────
    "order_id": "bothify(text='ORD-######')",
    "order_number": "bothify(text='ORD-######')",
    "tracking_number": "bothify(text='TRK-??########')",
    "barcode": "ean13",
    "ean": "ean13",
    "ean13": "ean13",
    "ean8": "ean8",
    "asin": "bothify(text='B0?????????')",
    "product_code": "bothify(text='PROD-####-??')",
    "sku": "bothify(text='SKU-####')",
    "variant_id": "bothify(text='VAR-####')",
    # ── Logistics / Shipping ──────────────────────────────────────────────
    "shipment_id": "bothify(text='SHP-########')",
    "waybill": "bothify(text='WB##########')",
    "container_id": "bothify(text='CONT-######')",
    # ── HR / People ───────────────────────────────────────────────────────
    "employee_id": "bothify(text='EMP-######')",
    "staff_id": "bothify(text='EMP-######')",
    "badge_number": "numerify(text='#####')",
    "salary": "pyfloat(min_value=20000, max_value=200000, right_digits=2)",
    "national_insurance": "bothify(text='??######?')",
    "passport_number": "bothify(text='??#######')",
    "drivers_licence": "bothify(text='?????##?##??')",
    "emergency_contact": "name",
    "manager_id": "bothify(text='EMP-######')",
    # ── Marketing / Analytics ─────────────────────────────────────────────
    "campaign_id": "bothify(text='CMP-######')",
    "session_id": "uuid4",
    "click_id": "uuid4",
    "impression_id": "uuid4",
    "referrer": "url",
    "utm_campaign": "bothify(text='camp_????_##')",
    "conversion_rate": "pyfloat(min_value=0, max_value=1, right_digits=4)",
    "bounce_rate": "pyfloat(min_value=0, max_value=1, right_digits=4)",
    "click_through_rate": "pyfloat(min_value=0, max_value=1, right_digits=4)",
    # ── IoT / Technical ───────────────────────────────────────────────────
    "device_id": "uuid4",
    "serial_number": "bothify(text='SN-##########')",
    "firmware_version": "bothify(text='#.#.##')",
    "hardware_version": "bothify(text='v#.#')",
    "sensor_id": "bothify(text='SENS-######')",
    "reading": "pyfloat(min_value=0, max_value=1000, right_digits=2)",
    "signal_strength": "pyfloat(min_value=-120, max_value=0, right_digits=1)",
    "battery_level": "pyfloat(min_value=0, max_value=100, right_digits=1)",
    # ── Identifiers (common patterns) ─────────────────────────────────────
    "reference": "bothify(text='REF-########')",
    "reference_number": "bothify(text='REF-########')",
    "ticket_id": "bothify(text='TKT-######')",
    "case_id": "bothify(text='CASE-######')",
    "incident_id": "bothify(text='INC-######')",
    "request_id": "uuid4",
    "correlation_id": "uuid4",
    "external_id": "bothify(text='EXT-########')",
    "record_id": "uuid4",
    "hash": "sha256",
    "checksum": "md5",
    "token": "sha1",
    "api_key": "uuid4",
    # ── Files ─────────────────────────────────────────────────────────────
    "file_name": "file_name",
    "file_path": "file_path",
    "mime_type": "mime_type",
    "content_type": "mime_type",
    # NOTE: 'country' intentionally omitted — Faker returns full names ('United Kingdom')
    # but most dbt accepted_values tests list ISO codes ('GB').  When an accepted_values
    # constraint is present, _make_valid_value picks from it first (before Faker), so
    # removing the hint here means the no-constraint path also uses ISO-safe random strings.
    # ── Telecom Identity & Subscriber ─────────────────────────────────────
    "msisdn": "numerify(text='447#########')",
    "calling_number": "numerify(text='447#########')",
    "called_number": "numerify(text='447#########')",
    "cli": "numerify(text='447#########')",
    "a_number": "numerify(text='447#########')",
    "b_number": "numerify(text='447#########')",
    "iccid": "numerify(text='8944####################')",
    "sim_serial": "numerify(text='8944####################')",
    "imsi": "numerify(text='23430##########')",
    "subscriber_id": "numerify(text='23430##########')",
    "imei": "numerify(text='35######-######-#')",
    "device_imei": "numerify(text='35######-######-#')",
    "tac": "numerify(text='35######')",
    "impi": "bothify(text='???????@ims.mnc###.mcc###.3gppnetwork.org')",
    "impu": "bothify(text='sip:+447#########@ims.mnc###.mcc###.3gppnetwork.org')",
    "min": "numerify(text='##########')",
    "mdn": "numerify(text='447#########')",
    "msin": "numerify(text='##########')",
    "esim_eid": "numerify(text='89##########################')",
    "eid": "numerify(text='89##########################')",
    # ── Telecom Network Infrastructure ────────────────────────────────────
    "cell_id": "numerify(text='######')",
    "cell_global_id": "bothify(text='234-30-####-######')",
    "cgi": "bothify(text='234-30-####-######')",
    "ecgi": "bothify(text='234-30-#######')",
    "enodeb_id": "numerify(text='#######')",
    "gnodeb_id": "numerify(text='#########')",
    "sector_id": "numerify(text='###')",
    "lac": "numerify(text='#####')",
    "rac": "numerify(text='###')",
    "tac_5g": "numerify(text='######')",
    "sac": "numerify(text='####')",
    "ran_node_id": "bothify(text='RAN-##-???-######')",
    "bsc_id": "bothify(text='BSC-####')",
    "rnc_id": "bothify(text='RNC-####')",
    "mme_id": "bothify(text='MME-####')",
    "sgw_id": "bothify(text='SGW-####')",
    "pgw_id": "bothify(text='PGW-####')",
    "amf_id": "bothify(text='AMF-####')",
    "smf_id": "bothify(text='SMF-####')",
    "upf_id": "bothify(text='UPF-####')",
    "plmn": "numerify(text='23430')",
    "mcc": "numerify(text='234')",
    "mnc": "numerify(text='30')",
    "pdp_context_id": "numerify(text='##')",
    # ── Telecom Usage / CDR Metrics ───────────────────────────────────────
    "call_duration_seconds": "pyint(min_value=1, max_value=7200)",
    "call_duration_minutes": "pyint(min_value=0, max_value=120)",
    "data_volume_bytes": "pyint(min_value=1024, max_value=10737418240)",
    "data_volume_kb": "pyint(min_value=1, max_value=10485760)",
    "data_volume_mb": "pyfloat(min_value=0.001, max_value=10240, right_digits=3)",
    "sms_count": "pyint(min_value=0, max_value=500)",
    "mms_count": "pyint(min_value=0, max_value=50)",
    "roaming_data_mb": "pyfloat(min_value=0, max_value=2048, right_digits=2)",
    "setup_time_ms": "pyint(min_value=50, max_value=5000)",
    "call_attempt_count": "pyint(min_value=1, max_value=5)",
    "charging_id": "numerify(text='##########')",
    "rating_group": "numerify(text='###')",
    "service_identifier": "numerify(text='###')",
    "bearer_id": "numerify(text='##')",
    # ── Telecom Billing / Charging ────────────────────────────────────────
    # NOTE: account_number already defined above (Finance & Banking section)
    "ban": "numerify(text='##########')",
    "charge_amount": "pyfloat(min_value=0, max_value=500, right_digits=4)",
    "rated_amount": "pyfloat(min_value=0, max_value=500, right_digits=4)",
    "bundle_id": "bothify(text='BDL-??????-##')",
    "addon_id": "bothify(text='ADD-######')",
    "promo_code": "bothify(text='PROMO-????##')",
    "bolt_on_id": "bothify(text='BOLT-####')",
    "allowance_remaining": "pyfloat(min_value=0, max_value=100000, right_digits=2)",
    "out_of_bundle_charge": "pyfloat(min_value=0, max_value=50, right_digits=4)",
    "roaming_charge": "pyfloat(min_value=0, max_value=200, right_digits=4)",
    # NOTE: invoice_number already defined above (Finance & Banking section)
    "direct_debit_ref": "bothify(text='DD-??########')",
    # ── Telecom RF / QoS Metrics ──────────────────────────────────────────
    "rsrp": "pyfloat(min_value=-140, max_value=-44, right_digits=1)",
    "rsrq": "pyfloat(min_value=-20, max_value=-3, right_digits=1)",
    "rssi": "pyfloat(min_value=-110, max_value=-50, right_digits=1)",
    "sinr": "pyfloat(min_value=-20, max_value=30, right_digits=1)",
    "cqi": "pyint(min_value=0, max_value=15)",
    "throughput_dl_mbps": "pyfloat(min_value=0.1, max_value=1000, right_digits=2)",
    "throughput_ul_mbps": "pyfloat(min_value=0.1, max_value=300, right_digits=2)",
    "latency_ms": "pyfloat(min_value=1, max_value=500, right_digits=1)",
    "jitter_ms": "pyfloat(min_value=0, max_value=50, right_digits=2)",
    "packet_loss_pct": "pyfloat(min_value=0, max_value=10, right_digits=3)",
    "bler": "pyfloat(min_value=0, max_value=0.5, right_digits=4)",
    "handover_count": "pyint(min_value=0, max_value=20)",
}


def _match_semantic_hint(name_lower: str) -> Optional[str]:
    """
    Match a field name to a Faker method using word-boundary-aware matching.

    Priority order:
    1. Exact match: ``ip_address`` matches ``ip_address``
    2. Word-boundary match: ``user_email`` matches ``email`` (splits on ``_``)
    3. No match

    This prevents substring collisions like ``ship_date`` matching ``ip``
    or ``ip_address`` matching ``address``.
    """
    # 1. Exact match — highest confidence
    if name_lower in _SEMANTIC_HINTS:
        return _SEMANTIC_HINTS[name_lower]

    # 2. Word-boundary match — split on underscores and check each word/suffix
    parts = name_lower.split("_")
    # Try progressively longer suffixes: for "user_email_address",
    # try "address", then "email_address", then "user_email_address"
    for i in range(len(parts) - 1, -1, -1):
        suffix = "_".join(parts[i:])
        if suffix in _SEMANTIC_HINTS:
            return _SEMANTIC_HINTS[suffix]

    # 3. Also try prefixes for patterns like "email_opt_in" → "email"
    for i in range(1, len(parts)):
        prefix = "_".join(parts[:i])
        if prefix in _SEMANTIC_HINTS:
            return _SEMANTIC_HINTS[prefix]

    return None


# Patterns that identify date/timestamp fields by name (for string-typed fields)
_DATE_NAME_SUFFIXES = (
    "_at",
    "_on",
    "_date",
    "_time",
    "_timestamp",
    "_ts",
    "_datetime",
    "_dt",
)
_DATE_NAME_CONTAINS = (
    "created",
    "updated",
    "modified",
    "deleted",
    "loaded",
    "ingested",
    "processed",
    "completed",
    "submitted",
    "approved",
    "shipped",
    "delivered",
    "expired",
    "started",
    "ended",
    "registered",
    "published",
    "event_time",
    "order_date",
    "ship_date",
    "birth_date",
    "start_date",
    "end_date",
    "due_date",
    "close_date",
)
_TIMESTAMP_SUFFIXES = ("_at", "_time", "_timestamp", "_ts", "_datetime", "_dt")


# ── Temporal Triplets Configuration ──────────────────────────────────────────
# Defines linked field groups where start/end/duration must be mathematically consistent

_TEMPORAL_TRIPLETS = {
    "session": {
        "start": "session_start",
        "end": "session_end",
        "duration": "session_duration_seconds",
        "unit": "seconds",
        "min_duration": 1,
        "max_duration": 86400,
        "formula": "end - start = duration",
    },
    "call": {
        "start": "call_start_time",
        "end": "call_end_time",
        "duration": "call_duration_seconds",
        "unit": "seconds",
        "min_duration": 1,
        "max_duration": 7200,
        "formula": "end - start = duration",
    },
    "data_session": {
        "start": "data_session_start",
        "end": "data_session_end",
        "duration": "data_volume_mb",
        "unit": "seconds",
        "min_duration": 1,
        "max_duration": 86400,
    },
    "job": {
        "start": "job_start_time",
        "end": "job_end_time",
        "duration": "duration_ms",
        "unit": "milliseconds",
        "min_duration": 100,
        "max_duration": 3600000,
        "formula": "end - start = duration / 1000",
    },
    "appointment": {
        "start": "appointment_start",
        "end": "appointment_end",
        "duration": "appointment_duration_minutes",
        "unit": "minutes",
        "min_duration": 15,
        "max_duration": 480,
        "slot_sizes": [15, 30, 45, 60, 90, 120],
    },
    "incident": {
        "start": "opened_at",
        "end": "resolved_at",
        "duration": "resolution_time_minutes",
        "unit": "minutes",
        "min_duration": 5,
        "max_duration": 20160,
        "nullable_end": True,
    },
    "contract_period": {
        "start": "contract_start_date",
        "end": "contract_end_date",
        "duration": "contract_length_months",
        "unit": "months",
        "allowed_durations": [1, 6, 12, 18, 24, 36],
    },
    "fulfilment": {
        "start": "picked_at",
        "end": "dispatched_at",
        "duration": "pick_duration_minutes",
        "unit": "minutes",
        "min_duration": 1,
        "max_duration": 480,
    },
}

_TRIPLET_GENERATION_STRATEGY = {
    "start_plus_duration": {
        "step_1": "generate session_start randomly",
        "step_2": "generate session_duration_seconds within min/max",
        "step_3": "session_end = session_start + duration",
        "advantage": "duration is always exact integer",
        "use_when": "duration field is the source of truth",
    },
    "start_to_end": {
        "step_1": "generate session_start randomly",
        "step_2": "generate session_end = session_start + random(min, max)",
        "step_3": "session_duration_seconds = (session_end - session_start).seconds",
        "advantage": "timestamps look natural",
        "use_when": "start/end timestamps are the source of truth",
    },
}

_TRIPLET_INVALID_PATTERNS = {
    "end_before_start": {
        "description": "session_end is before session_start",
        "seen_in_screen": True,
        "generate": "end = start - random(1, 86400)",
        "duration": "null or wrong",
        "real_world_cause": "timezone bug, DST switch, clock skew",
    },
    "duration_mismatch": {
        "description": "duration does not match end - start",
        "seen_in_screen": True,
        "generate": "correct timestamps, wrong duration value",
        "real_world_cause": "duration calculated in wrong unit (ms vs s)",
    },
    "null_duration": {
        "description": "session_end exists but duration is null",
        "seen_in_screen": True,
        "generate": "valid start and end, duration = null",
        "real_world_cause": "ETL job failed to calculate derived field",
    },
    "null_end_with_duration": {
        "description": "session_end is null but duration is populated",
        "generate": "valid start, end = null, duration = random",
        "real_world_cause": "session still open but heartbeat sent duration",
    },
    "zero_duration": {
        "description": "start == end, duration == 0",
        "generate": "end = start, duration = 0",
        "real_world_cause": "instant event logged as session",
    },
    "impossibly_long": {
        "description": "duration exceeds max realistic value",
        "generate": "duration > max_duration * 10",
        "real_world_cause": "session not properly closed, runaway counter",
    },
    "future_end": {
        "description": "session_end is in the future",
        "generate": "end = now() + random(1hr, 30 days)",
        "real_world_cause": "wrong year, timezone issue, test data in prod",
    },
    "microsecond_precision_mismatch": {
        "description": "start has microseconds, end does not — or vice versa",
        "seen_in_screen": True,
        "generate": "start=...092257, end=...143995",
        "real_world_cause": "different source systems with different clock precision",
    },
}


# ---------------------------------------------------------------------------
# Realistic fallback pools — used when Faker is NOT installed.
# These produce human-readable values instead of garbage like 'kd83jf2n'.
# Keys are substring patterns matched against field names (lowercase).
# ---------------------------------------------------------------------------

_REALISTIC_POOLS: Dict[str, List[str]] = {
    "city": [
        "London",
        "Manchester",
        "Birmingham",
        "Leeds",
        "Bristol",
        "Glasgow",
        "Edinburgh",
        "Liverpool",
        "Sheffield",
        "Cardiff",
        "Belfast",
        "Oxford",
        "Cambridge",
        "Bath",
        "York",
    ],
    "country": [_name for _, _, _, _name, _ in _GEO_DATA],
    "country_name": [_name for _, _, _, _name, _ in _GEO_DATA],
    "country_code": [_a2 for _a2, _, _, _, _ in _GEO_DATA],
    "device_type": ["mobile", "tablet", "desktop", "smart_tv", "console"],
    "browser": ["Chrome", "Safari", "Firefox", "Edge", "Samsung Browser"],
    "os": ["iOS", "Android", "Windows", "macOS", "Linux"],
    "state": [
        "California",
        "Texas",
        "New York",
        "Florida",
        "Illinois",
        "Pennsylvania",
        "Ohio",
        "Georgia",
        "Michigan",
        "North Carolina",
    ],
    "county": [
        "Surrey",
        "Kent",
        "Essex",
        "Hampshire",
        "Devon",
        "Somerset",
        "Norfolk",
        "Suffolk",
        "Dorset",
        "Wiltshire",
    ],
    "currency": ["GBP", "USD", "EUR", "JPY", "AUD", "CAD", "CHF", "CNY"],
    "color": [
        "Red",
        "Blue",
        "Green",
        "Black",
        "White",
        "Grey",
        "Navy",
        "Burgundy",
        "Teal",
        "Ivory",
        "Charcoal",
        "Silver",
    ],
    "colour": [
        "Red",
        "Blue",
        "Green",
        "Black",
        "White",
        "Grey",
        "Navy",
        "Burgundy",
        "Teal",
        "Ivory",
        "Charcoal",
        "Silver",
    ],
    "status": [
        "active",
        "inactive",
        "pending",
        "completed",
        "cancelled",
        "expired",
        "suspended",
        "archived",
    ],
    "priority": ["low", "medium", "high", "critical", "urgent"],
    "category": [
        "Electronics",
        "Clothing",
        "Home",
        "Sports",
        "Books",
        "Automotive",
        "Health",
        "Food",
        "Travel",
        "Finance",
    ],
    "type": ["standard", "premium", "enterprise", "basic", "pro"],
    "tier": ["free", "starter", "professional", "enterprise"],
    "plan": ["free", "basic", "standard", "premium", "enterprise"],
    "gender": ["Male", "Female", "Non-binary", "Prefer not to say"],
    "language": [
        "English",
        "Spanish",
        "French",
        "German",
        "Portuguese",
        "Chinese",
        "Japanese",
        "Korean",
        "Arabic",
        "Hindi",
    ],
    "region": [
        "North",
        "South",
        "East",
        "West",
        "Central",
        "Northeast",
        "Southeast",
        "Northwest",
        "Southwest",
    ],
    "department": [
        "Engineering",
        "Sales",
        "Marketing",
        "Finance",
        "HR",
        "Operations",
        "Legal",
        "Support",
        "Product",
        "Design",
    ],
    "role": ["Admin", "User", "Moderator", "Editor", "Viewer", "Manager"],
    "industry": [
        "Technology",
        "Healthcare",
        "Finance",
        "Retail",
        "Manufacturing",
        "Education",
        "Energy",
        "Real Estate",
    ],
    "payment_method": [
        "credit_card",
        "debit_card",
        "bank_transfer",
        "paypal",
        "apple_pay",
        "google_pay",
    ],
    "property_type": [
        "detached",
        "semi-detached",
        "terraced",
        "flat",
        "bungalow",
        "maisonette",
        "cottage",
        "townhouse",
    ],
    "listing_status": [
        "for_sale",
        "sold",
        "under_offer",
        "withdrawn",
        "sale_agreed",
        "reduced",
    ],
    "tenure": ["freehold", "leasehold", "share_of_freehold"],
    "condition": ["new", "excellent", "good", "fair", "poor", "refurbished"],
    "size": ["XS", "S", "M", "L", "XL", "XXL"],
    "day_of_week": [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ],
    "month": [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ],
    # ── Inventory / Warehouse / Product ───────────────────────────────────
    "item_name": [
        "Widget Pro",
        "Gadget Ultra",
        "Sensor Module A",
        "Control Board v2",
        "Power Supply 500W",
        "Mounting Bracket",
        "Filter Cartridge",
        "LED Panel 4K",
        "Cooling Fan 120mm",
        "Cable Assembly",
        "Connector Kit",
        "Display Module",
        "Battery Pack Li-Ion",
        "Adapter USB-C",
        "Motor Driver IC",
        "Circuit Board",
    ],
    "product_name": [
        "Widget Pro",
        "Gadget Ultra",
        "Sensor Module A",
        "Control Board v2",
        "Power Supply 500W",
        "Mounting Bracket",
        "Filter Cartridge",
        "LED Panel 4K",
        "Cooling Fan 120mm",
        "Cable Assembly",
        "Connector Kit",
        "Display Module",
    ],
    "display_name": [
        "Widget Pro",
        "Gadget Ultra",
        "Sensor Module A",
        "Control Board v2",
        "Power Supply 500W",
        "Mounting Bracket",
        "Filter Cartridge",
        "LED Panel 4K",
    ],
    "item_display_name": [
        "Widget Pro",
        "Cable Type-C",
        "Bracket M8",
        "Connector RJ45",
        "Filter Cartridge",
        "Sensor Module",
        "Panel Board A3",
        "Gasket Kit",
    ],
    "item_type": ["standard", "premium", "enterprise", "basic", "pro", "custom"],
    "location_name": [
        "Warehouse A",
        "Warehouse B",
        "Distribution Center 1",
        "DC East",
        "Fulfillment Center West",
        "Main Depot",
        "Regional Hub North",
        "Cross-Dock Facility",
        "Storage Unit 12",
        "Cold Storage",
    ],
    "warehouse": [
        "Warehouse A",
        "Warehouse B",
        "DC East",
        "DC West",
        "Main Depot",
        "Regional Hub",
        "Fulfillment Center",
        "Cold Storage",
    ],
    "channel": [
        "online",
        "in-store",
        "wholesale",
        "marketplace",
        "direct",
        "partner",
        "mobile_app",
        "social",
    ],
    "supplier": [
        "Acme Corp",
        "GlobalTech",
        "FastShip Ltd",
        "ElectroParts Inc",
        "Pacific Supply Co",
        "Nordic Components",
        "Alpine Industrial",
    ],
    # ── Finance ────────────────────────────────────────────────────────────
    "payment_status": [
        "pending",
        "authorised",
        "captured",
        "settled",
        "refunded",
        "partially_refunded",
        "voided",
        "failed",
        "disputed",
    ],
    "transaction_type": [
        "purchase",
        "refund",
        "chargeback",
        "adjustment",
        "transfer",
        "withdrawal",
        "deposit",
        "fee",
    ],
    "return_reason": [
        "faulty",
        "not_as_described",
        "changed_mind",
        "wrong_item",
        "arrived_late",
        "damaged_in_transit",
        "duplicate_order",
    ],
    "refund_status": ["requested", "approved", "processed", "denied", "partial"],
    # ── Logistics / Shipping ──────────────────────────────────────────────
    "carrier": [
        "DHL",
        "FedEx",
        "UPS",
        "Royal Mail",
        "DPD",
        "Hermes",
        "Yodel",
        "TNT",
        "Parcelforce",
        "Amazon Logistics",
    ],
    "shipment_status": [
        "pending",
        "picked_up",
        "in_transit",
        "out_for_delivery",
        "delivered",
        "failed_delivery",
        "returned_to_sender",
        "lost",
    ],
    "incoterm": ["EXW", "FCA", "CPT", "CIP", "DAP", "DPU", "DDP", "FOB", "CIF"],
    "port": [
        "Felixstowe",
        "Southampton",
        "Rotterdam",
        "Hamburg",
        "Antwerp",
        "Shanghai",
        "Singapore",
        "Los Angeles",
        "New York",
    ],
    "port_of_origin": [
        "Shanghai",
        "Singapore",
        "Rotterdam",
        "Los Angeles",
        "Hamburg",
        "Busan",
        "Hong Kong",
        "Antwerp",
    ],
    # ── Marketing / Analytics ─────────────────────────────────────────────
    "source": ["organic", "paid", "email", "social", "referral", "direct"],
    "medium": ["cpc", "cpm", "email", "social", "organic", "display", "affiliate"],
    "utm_source": [
        "google",
        "facebook",
        "instagram",
        "twitter",
        "linkedin",
        "email",
        "organic",
        "referral",
        "direct",
        "tiktok",
        "youtube",
    ],
    "utm_medium": [
        "cpc",
        "organic",
        "email",
        "social",
        "referral",
        "display",
        "affiliate",
        "push",
        "sms",
    ],
    "acquisition_channel": [
        "organic_search",
        "paid_search",
        "social_organic",
        "social_paid",
        "email",
        "referral",
        "direct",
        "display",
        "affiliate",
    ],
    "email_status": [
        "subscribed",
        "unsubscribed",
        "bounced",
        "complained",
        "pending_confirmation",
        "archived",
    ],
    # ── Healthcare ────────────────────────────────────────────────────────
    "blood_type": ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"],
    "appointment_type": [
        "consultation",
        "follow_up",
        "emergency",
        "routine_check",
        "specialist_referral",
        "procedure",
        "vaccination",
    ],
    "appointment_status": [
        "scheduled",
        "confirmed",
        "attended",
        "dna",
        "cancelled",
        "rescheduled",
        "walk_in",
    ],
    "ward": [
        "cardiology",
        "oncology",
        "orthopaedics",
        "paediatrics",
        "neurology",
        "A&E",
        "ICU",
        "maternity",
        "radiology",
        "pharmacy",
    ],
    # ── HR ────────────────────────────────────────────────────────────────
    "employment_type": [
        "full_time",
        "part_time",
        "contract",
        "freelance",
        "intern",
        "apprentice",
        "zero_hours",
    ],
    "contract_type": [
        "permanent",
        "fixed_term",
        "temporary",
        "casual",
        "probationary",
    ],
    "leave_type": [
        "annual",
        "sick",
        "maternity",
        "paternity",
        "shared_parental",
        "compassionate",
        "unpaid",
        "study",
    ],
    "absence_reason": [
        "illness",
        "family_emergency",
        "medical_appointment",
        "mental_health",
        "bereavement",
        "unauthorised",
    ],
    "performance_band": ["exceeds", "meets", "developing", "underperforming"],
    # ── Real Estate / Property ────────────────────────────────────────────
    "energy_rating": ["A", "B", "C", "D", "E", "F", "G"],
    "heating_type": [
        "gas_central",
        "electric",
        "oil",
        "heat_pump",
        "underfloor",
        "solar",
        "wood_burning",
    ],
    "parking": ["garage", "driveway", "on_street", "allocated", "none"],
    "garden_type": ["rear", "front", "both", "communal", "roof_terrace", "none"],
    # ── IoT / Technical ───────────────────────────────────────────────────
    "event_type": [
        "click",
        "view",
        "purchase",
        "sign_up",
        "login",
        "logout",
        "error",
        "timeout",
        "heartbeat",
        "alert",
        "threshold_exceeded",
    ],
    "log_level": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"],
    "alert_type": [
        "threshold_breach",
        "anomaly_detected",
        "connection_lost",
        "battery_low",
        "firmware_update",
        "tamper_detected",
    ],
    "protocol": ["HTTP", "HTTPS", "MQTT", "CoAP", "AMQP", "WebSocket", "gRPC"],
    "http_method": ["GET", "POST", "PUT", "PATCH", "DELETE"],
    "http_status": [200, 201, 204, 301, 302, 400, 401, 403, 404, 409, 422, 500, 502, 503],
    # ── Support / CRM ─────────────────────────────────────────────────────
    "ticket_status": [
        "open",
        "pending",
        "in_progress",
        "waiting_on_customer",
        "escalated",
        "resolved",
        "closed",
        "reopened",
    ],
    "resolution_type": [
        "fixed",
        "workaround",
        "by_design",
        "duplicate",
        "cannot_reproduce",
        "wont_fix",
        "user_error",
    ],
    "satisfaction_score": [1, 2, 3, 4, 5],
    "nps_score": list(range(0, 11)),
    "contact_reason": [
        "billing_query",
        "technical_issue",
        "product_question",
        "complaint",
        "cancellation",
        "upgrade_request",
        "general_enquiry",
    ],
    "contact_channel": ["phone", "email", "chat", "social", "in_person", "self_service"],
    # ── Content / Media ───────────────────────────────────────────────────
    "content_type": [
        "article",
        "video",
        "podcast",
        "webinar",
        "whitepaper",
        "case_study",
        "infographic",
        "tutorial",
        "product_page",
    ],
    "content_status": ["draft", "review", "approved", "published", "archived", "deleted"],
    "file_type": ["pdf", "docx", "xlsx", "csv", "jpg", "png", "mp4", "mp3", "zip"],
    # ── Scoring / ML output ───────────────────────────────────────────────
    "risk_band": ["very_low", "low", "medium", "high", "very_high", "critical"],
    "credit_band": ["excellent", "good", "fair", "poor", "very_poor"],
    "segment": [
        "new",
        "active",
        "at_risk",
        "churned",
        "win_back",
        "vip",
        "dormant",
        "high_value",
        "low_value",
    ],
    "model_version": ["v1.0", "v1.1", "v2.0", "v2.1", "v3.0"],
    "prediction_label": ["positive", "negative", "uncertain"],
    # ── Financial Services ────────────────────────────────────────────────
    "account_type": [
        "current",
        "savings",
        "isa",
        "sipp",
        "stocks_and_shares",
        "junior_isa",
        "business_current",
        "fixed_rate_bond",
    ],
    "mortgage_type": [
        "fixed",
        "variable",
        "tracker",
        "discount",
        "offset",
        "interest_only",
        "repayment",
    ],
    "loan_purpose": [
        "home_improvement",
        "debt_consolidation",
        "car_purchase",
        "holiday",
        "wedding",
        "education",
        "medical",
        "business",
    ],
    "kyc_status": [
        "not_started",
        "in_progress",
        "pending_review",
        "approved",
        "rejected",
        "expired",
        "suspended",
    ],
    "aml_risk": ["low", "medium", "high", "pep", "sanctioned"],
    "fund_type": [
        "equity",
        "bond",
        "money_market",
        "mixed",
        "index_tracker",
        "etf",
        "hedge_fund",
        "private_equity",
    ],
    "market": [
        "LSE",
        "NYSE",
        "NASDAQ",
        "EURONEXT",
        "TSX",
        "ASX",
        "HKEX",
        "TSE",
        "BSE",
        "NSE",
    ],
    "asset_class": [
        "equities",
        "fixed_income",
        "commodities",
        "forex",
        "real_estate",
        "crypto",
        "derivatives",
        "cash",
    ],
    "regulatory_status": [
        "FCA_authorised",
        "FCA_registered",
        "exempt",
        "appointed_representative",
        "unregulated",
    ],
    # ── Telecom ───────────────────────────────────────────────────────────
    "plan_type": [
        "pay_as_you_go",
        "sim_only_monthly",
        "sim_only_annual",
        "handset_24m",
        "handset_36m",
        "broadband_only",
        "bundle_broadband_tv",
        "business_unlimited",
    ],
    "network_type": ["2G", "3G", "4G", "4G+", "5G", "5G+"],
    "roaming_zone": ["UK", "EU", "Zone_1", "Zone_2", "Zone_3", "Worldwide"],
    "fault_type": [
        "no_signal",
        "slow_data",
        "call_dropping",
        "billing_error",
        "port_issue",
        "sim_swap",
        "handset_fault",
        "coverage_gap",
    ],
    "churn_reason": [
        "price",
        "coverage",
        "customer_service",
        "handset",
        "competitor_offer",
        "moving_abroad",
        "deceased",
        "unknown",
    ],
    "activation_channel": [
        "store",
        "online",
        "telesales",
        "partner_retail",
        "direct_mail",
        "business_account_manager",
    ],
    "tariff_band": ["budget", "mid", "premium", "unlimited", "enterprise"],
    "number_type": ["mobile", "landline", "voip", "freephone", "premium_rate"],
    # ── Healthcare (extended) ─────────────────────────────────────────────
    "icd10_chapter": [
        "A00-B99",
        "C00-D48",
        "D50-D89",
        "E00-E90",
        "F00-F99",
        "G00-G99",
        "H00-H59",
        "I00-I99",
        "J00-J99",
        "K00-K93",
        "L00-L99",
        "M00-M99",
    ],
    "snomed_concept": [
        "Hypertension",
        "Type 2 Diabetes",
        "Asthma",
        "COPD",
        "Atrial Fibrillation",
        "Heart Failure",
        "Osteoarthritis",
        "Depression",
        "Anxiety",
        "Obesity",
    ],
    "care_setting": [
        "GP",
        "A&E",
        "outpatient",
        "inpatient",
        "day_surgery",
        "ICU",
        "community",
        "telehealth",
    ],
    "referral_source": [
        "GP",
        "self_referral",
        "A&E",
        "111",
        "consultant",
        "community_nurse",
        "social_services",
    ],
    "discharge_destination": [
        "home",
        "care_home",
        "rehabilitation",
        "transfer",
        "deceased",
        "self_discharge",
    ],
    "funding_type": ["NHS", "private", "insurance", "overseas_visitor"],
    # ── Real Estate (extended) ────────────────────────────────────────────
    "property_style": [
        "Victorian",
        "Edwardian",
        "Georgian",
        "Art_Deco",
        "Post_War",
        "1960s",
        "1970s",
        "Modern",
        "New_Build",
        "Contemporary",
    ],
    "sale_type": [
        "private_treaty",
        "auction",
        "tender",
        "shared_ownership",
        "help_to_buy",
        "right_to_buy",
    ],
    "valuation_method": [
        "comparable",
        "income_approach",
        "cost_approach",
        "automated_valuation",
        "RICS_survey",
    ],
    "mortgage_status": [
        "no_mortgage",
        "mortgage_agreed",
        "mortgage_applied",
        "awaiting_valuation",
        "exchanged",
        "completed",
    ],
    "survey_type": [
        "condition_report",
        "homebuyer_report",
        "full_structural",
        "new_build_snagging",
        "valuation_only",
    ],
    # ── Retail / FMCG ─────────────────────────────────────────────────────
    "promotion_type": [
        "percentage_off",
        "fixed_amount_off",
        "buy_one_get_one",
        "multibuy",
        "bundle",
        "free_gift",
        "loyalty_points",
        "flash_sale",
        "clearance",
        "member_exclusive",
    ],
    "return_policy": ["14_day", "28_day", "60_day", "90_day", "no_return"],
    "fulfilment_type": [
        "standard_delivery",
        "next_day",
        "same_day",
        "click_and_collect",
        "locker_pickup",
        "express_international",
        "economy_international",
    ],
    "merchandising_zone": [
        "end_cap",
        "mid_aisle",
        "checkout",
        "entrance",
        "promotional_bay",
        "online_homepage",
        "category_page",
    ],
    "demand_class": ["A", "B", "C", "D"],
    "replenishment_method": [
        "min_max",
        "reorder_point",
        "just_in_time",
        "vendor_managed",
        "consignment",
        "make_to_order",
    ],
    # NOTE: duplicate "item_name" and "product_name" keys removed
    # (already defined in the Inventory/Warehouse/Product section above)
    # NOTE: duplicate "location_name" key removed (already defined earlier)
    "company_name": [
        "Acme Corp",
        "GlobalTech Ltd",
        "Pinnacle Solutions",
        "NextWave Industries",
        "Sterling Partners",
        "Atlas Manufacturing",
        "Summit Logistics",
        "Vanguard Supply Co",
        "Meridian Trading",
        "Pacific Wholesale",
        "Horizon Electronics",
        "Apex Components",
        "Delta Materials",
        "Omega Precision",
        "Quantum Systems",
        "Nordic Supplies",
        "Phoenix Distribution",
        "Titan Enterprises",
        "BlueChip Trading",
        "Pioneer Industrial",
    ],
    "brand_name": [
        "ProLine",
        "TechVault",
        "EcoSmart",
        "PureCore",
        "MaxDrive",
        "SwiftEdge",
        "NovaPrime",
        "ZenithPlus",
        "CoreTech",
        "AquaFlow",
        "SteelGuard",
        "VoltMax",
        "OptiGrade",
        "CloudNine",
        "IronEdge",
    ],
    # ── Telecom Extended Pools ─────────────────────────────────────────────
    "apn": [
        "internet",
        "mms",
        "wap.vodafone.co.uk",
        "ee.co.uk",
        "three.co.uk",
        "o2.co.uk",
        "mobile.bt.com",
        "globaldata.vodafone.com",
        "iot.1nce.net",
        "super.telstra.com",
    ],
    # NOTE: duplicate "network_type" key removed (already defined earlier)
    "call_type": [
        "voice_mo",
        "voice_mt",
        "sms_mo",
        "sms_mt",
        "mms_mo",
        "mms_mt",
        "data_session",
        "volte_mo",
        "volte_mt",
        "vowifi_mo",
        "vowifi_mt",
        "roaming_mo",
        "roaming_mt",
        "premium_rate",
        "international_mo",
        "emergency_call",
    ],
    "termination_reason": [
        "normal_clearing",
        "busy",
        "no_answer",
        "congestion",
        "call_rejected",
        "number_changed",
        "destination_out_of_order",
        "invalid_number",
        "facility_rejected",
        "response_to_status_enquiry",
        "normal_unspecified",
        "radio_link_failure",
        "handover_failure",
    ],
    # NOTE: duplicate "event_type" key removed (already defined earlier)
    "sim_status": [
        "active",
        "inactive",
        "suspended",
        "barred",
        "terminated",
        "stolen",
        "lost",
        "ported_out",
        "replacement_pending",
        "test",
    ],
    "port_status": [
        "not_porting",
        "port_requested",
        "port_in_progress",
        "ported_in",
        "ported_out",
        "port_rejected",
        "port_cancelled",
    ],
    "service_class": [
        "voice",
        "data",
        "sms",
        "mms",
        "roaming",
        "international",
        "premium_rate",
        "directory_enquiry",
        "emergency",
        "iot",
        "m2m",
        "nb_iot",
        "lte_m",
    ],
    "barring_type": [
        "outgoing_all",
        "outgoing_international",
        "outgoing_international_except_home",
        "incoming_all",
        "incoming_when_roaming",
        "premium_rate_outgoing",
        "premium_rate_incoming",
    ],
    # NOTE: duplicate "roaming_zone" key removed (already defined earlier)
    "handset_os": [
        "iOS_17",
        "iOS_16",
        "iOS_15",
        "Android_14",
        "Android_13",
        "Android_12",
        "HarmonyOS_4",
        "KaiOS_3",
    ],
    "handset_manufacturer": [
        "Apple",
        "Samsung",
        "Google",
        "OnePlus",
        "Xiaomi",
        "Huawei",
        "Nokia",
        "Motorola",
        "Sony",
        "OPPO",
        "Vivo",
        "Realme",
        "Nothing",
        "Fairphone",
    ],
    # NOTE: duplicate "churn_reason" key removed (already defined earlier)
    "complaint_category": [
        "billing_error",
        "coverage_complaint",
        "service_quality",
        "data_speed",
        "handset_fault",
        "staff_conduct",
        "mis_selling",
        "port_delay",
        "contract_dispute",
        "roaming_charge_dispute",
        "number_not_working",
    ],
    "drop_call_indicator": ["Y", "N"],
    "codec": [
        "AMR-NB",
        "AMR-WB",
        "EVS",
        "OPUS",
        "G.711",
        "G.722",
        "G.729",
    ],
    "qos_class": ["GBR", "Non-GBR", "Delay-Critical_GBR"],
    "bearer_type": [
        "default",
        "dedicated_GBR",
        "dedicated_Non-GBR",
        "IMS_signalling",
        "IMS_voice",
        "IMS_video",
    ],
}

# ---------------------------------------------------------------------------
# MCC/MNC reference pool — ties network, location, and subscriber identity.
# Used for locale-aware MSISDN/IMSI generation and operator correlation.
# ---------------------------------------------------------------------------

_MCC_MNC_POOL: List[Tuple[str, str, str, str]] = [
    # (MCC, MNC, operator, country)
    ("234", "10", "O2 UK", "GB"),
    ("234", "20", "Three UK", "GB"),
    ("234", "30", "EE", "GB"),
    ("234", "15", "Vodafone UK", "GB"),
    ("310", "410", "AT&T", "US"),
    ("310", "260", "T-Mobile US", "US"),
    ("311", "480", "Verizon", "US"),
    ("262", "01", "T-Mobile DE", "DE"),
    ("262", "02", "Vodafone DE", "DE"),
    ("208", "10", "SFR", "FR"),
    ("208", "20", "Bouygues", "FR"),
    ("404", "20", "Airtel India", "IN"),
    ("404", "45", "Airtel India", "IN"),
    ("505", "01", "Telstra", "AU"),
    ("505", "03", "Vodafone AU", "AU"),
    ("440", "10", "NTT Docomo", "JP"),
    ("440", "20", "SoftBank", "JP"),
]

# Fast lookup: MCC → list of (MNC, operator, country) for correlation
_MCC_INDEX: Dict[str, List[Tuple[str, str, str]]] = {}
for _mcc, _mnc, _op, _cc in _MCC_MNC_POOL:
    _MCC_INDEX.setdefault(_mcc, []).append((_mnc, _op, _cc))

# ---------------------------------------------------------------------------
# Correlated field groups — dependent fields generated based on a "driver"
# field's value in the same row.
#
# Structure: (driver_field, dependent_field) → { driver_value: template }
#
# Templates:
#   - String with '#' → replaced by random digit
#   - String with '?' → replaced by random uppercase letter
#   - Plain string     → literal value
#   - List            → random choice from the list
# ---------------------------------------------------------------------------

_CORRELATED_POOLS: Dict[Tuple[str, str], Dict[str, Any]] = {
    # ── Postcode format by country ────────────────────────────────────────
    ("country_code", "postcode"): {
        "GB": "?#? #??",
        "US": "#####",
        "DE": "#####",
        "FR": "#####",
        "CA": "?#? #?#",
        "AU": "####",
        "JP": "###-####",
        "NL": "#### ??",
        "BR": "#####-###",
        "IN": "######",
    },
    ("country_code", "postal_code"): {
        "GB": "?#? #??",
        "US": "#####",
        "DE": "#####",
        "FR": "#####",
        "CA": "?#? #?#",
        "AU": "####",
        "JP": "###-####",
    },
    ("country_code", "zip_code"): {
        "US": "#####",
        "DE": "#####",
        "FR": "#####",
    },
    # ── Phone format by country ───────────────────────────────────────────
    ("country_code", "phone"): {
        "GB": "+44 7### ######",
        "US": "+1 (###) ###-####",
        "DE": "+49 ### #######",
        "FR": "+33 # ## ## ## ##",
        "AU": "+61 4## ### ###",
        "JP": "+81 ##-####-####",
        "CA": "+1 (###) ###-####",
        "IN": "+91 #####-#####",
    },
    ("country_code", "phone_number"): {
        "GB": "+44 7### ######",
        "US": "+1 (###) ###-####",
        "DE": "+49 ### #######",
        "FR": "+33 # ## ## ## ##",
        "AU": "+61 4## ### ###",
        "JP": "+81 ##-####-####",
        "CA": "+1 (###) ###-####",
    },
    # ── Currency by country ───────────────────────────────────────────────
    ("country_code", "currency"): {
        "GB": "GBP",
        "US": "USD",
        "DE": "EUR",
        "FR": "EUR",
        "ES": "EUR",
        "IT": "EUR",
        "NL": "EUR",
        "AU": "AUD",
        "CA": "CAD",
        "JP": "JPY",
        "BR": "BRL",
        "IN": "INR",
        "CN": "CNY",
        "KR": "KRW",
        "MX": "MXN",
        "SE": "SEK",
        "NO": "NOK",
        "DK": "DKK",
        "CH": "CHF",
    },
    ("country_code", "currency_code"): {
        "GB": "GBP",
        "US": "USD",
        "DE": "EUR",
        "FR": "EUR",
        "AU": "AUD",
        "CA": "CAD",
        "JP": "JPY",
        "IN": "INR",
    },
    # ── State/region by country ───────────────────────────────────────────
    ("country_code", "state"): {
        "US": [
            "California",
            "Texas",
            "New York",
            "Florida",
            "Illinois",
            "Pennsylvania",
            "Ohio",
            "Georgia",
            "Michigan",
            "North Carolina",
        ],
        "AU": ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"],
        "CA": ["Ontario", "Quebec", "British Columbia", "Alberta", "Manitoba"],
        "DE": ["Bavaria", "Berlin", "Hamburg", "Hesse", "Saxony"],
        "IN": ["Maharashtra", "Karnataka", "Tamil Nadu", "Uttar Pradesh", "Gujarat"],
    },
    # ── City by country ───────────────────────────────────────────────────
    ("country_code", "city"): {
        "GB": ["London", "Manchester", "Birmingham", "Leeds", "Bristol", "Edinburgh", "Glasgow"],
        "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Francisco"],
        "DE": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne", "Stuttgart"],
        "FR": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Bordeaux"],
        "AU": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra"],
        "JP": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya", "Sapporo"],
    },
    # ── National ID format by country ─────────────────────────────────────
    ("country_code", "national_id"): {
        "GB": "??######?",  # NI number: AB123456C
        "US": "###-##-####",  # SSN
        "DE": "###########",  # 11-digit
        "IN": "############",  # Aadhaar (12-digit)
    },
    # ── VAT number format by country ──────────────────────────────────────
    ("country_code", "vat_number"): {
        "GB": "GB#########",
        "DE": "DE#########",
        "FR": "FR??#########",
        "IN": "##?????####?#?#",
    },
    # ── Sort code (UK-specific but harmless if absent) ────────────────────
    ("country_code", "sort_code"): {
        "GB": "##-##-##",
    },
    # ── IBAN by country ───────────────────────────────────────────────────
    ("country_code", "iban"): {
        "GB": "GB##????########",
        "DE": "DE####################",
        "FR": "FR#########################",
        "NL": "NL##????##########",
    },
    # ── Bank account by country ───────────────────────────────────────────
    ("country_code", "bank_account"): {
        "GB": "########",
        "US": "#########",
        "DE": "##########",
    },
    # ── Telecom lookups ───────────────────────────────────────────────────
    ("mcc", "country_code"): {mcc: cc for mcc, _, _, cc in _MCC_MNC_POOL},
    ("country_code", "mcc"): {cc: mcc for mcc, _, _, cc in _MCC_MNC_POOL},
    # Format MSISDN based on country code (or MCC):
    ("country_code", "msisdn"): {
        "GB": "447#########",
        "US": "1##########",
        "DE": "4915########",
        "FR": "336########",
        "IN": "91##########",
        "AU": "614########",
        "JP": "8190########",
    },
    ("mcc", "msisdn"): {
        mcc: {
            "GB": "447#########",
            "US": "1##########",
            "DE": "4915########",
            "FR": "336########",
            "IN": "91##########",
            "AU": "614########",
            "JP": "8190########",
        }.get(cc, "##########")
        for mcc, _, _, cc in _MCC_MNC_POOL
    },
}


# ---------------------------------------------------------------------------
# Locale-specific format rules — full reference for each country
# ---------------------------------------------------------------------------

_LOCALE_FORMATS: Dict[str, Dict[str, str]] = {
    "GB": {
        "phone": "+44 7### ######",
        "postcode": "?#? #??",
        "national_id": "??######?",
        "vat_number": "GB#########",
        "bank_account": "########",
        "sort_code": "##-##-##",
        "date_format": "DD/MM/YYYY",
    },
    "US": {
        "phone": "+1 (###) ###-####",
        "postcode": "#####",
        "national_id": "###-##-####",
        "vat_number": "##-#######",
        "date_format": "MM/DD/YYYY",
    },
    "DE": {
        "phone": "+49 ### #######",
        "postcode": "#####",
        "national_id": "###########",
        "vat_number": "DE#########",
        "iban": "DE####################",
        "date_format": "DD.MM.YYYY",
    },
    "FR": {
        "phone": "+33 # ## ## ## ##",
        "postcode": "#####",
        "vat_number": "FR??#########",
        "iban": "FR#########################",
        "date_format": "DD/MM/YYYY",
    },
    "IN": {
        "phone": "+91 #####-#####",
        "postcode": "######",
        "national_id": "############",
        "vat_number": "##?????####?#?#",
        "date_format": "DD/MM/YYYY",
    },
}

# ── Telecom: MSISDN formats by MCC ─────────────────────────────────────────

_LOCALE_FORMATS_BY_MCC: Dict[str, Dict[str, str]] = {
    mcc: {
        "msisdn": {
            "GB": "447#########",
            "US": "1##########",
            "DE": "4915########",
            "FR": "336########",
            "IN": "91##########",
            "AU": "614########",
            "JP": "8190########",
        }.get(cc, "##########")
    }
    for mcc, _, _, cc in _MCC_MNC_POOL
}

# ---------------------------------------------------------------------------
# Currency-country binding
# ---------------------------------------------------------------------------

_COUNTRY_CURRENCY: Dict[str, str] = {
    "GB": "GBP",
    "US": "USD",
    "DE": "EUR",
    "FR": "EUR",
    "ES": "EUR",
    "IT": "EUR",
    "NL": "EUR",
    "AU": "AUD",
    "CA": "CAD",
    "JP": "JPY",
    "IN": "INR",
    "BR": "BRL",
    "MX": "MXN",
    "CN": "CNY",
    "KR": "KRW",
    "SE": "SEK",
    "NO": "NOK",
    "DK": "DKK",
    "CH": "CHF",
    "NZ": "NZD",
}


# Build a fast lookup: dependent_field → [(driver_field, correlation_map), ...]
# so _make_row can quickly check if a generated field has a correlation.
_CORRELATED_INDEX: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
for (_drv, _dep), _map in _CORRELATED_POOLS.items():
    _CORRELATED_INDEX.setdefault(_dep, []).append((_drv, _map))


# ---------------------------------------------------------------------------
# Null probability hints — field-name-aware null injection rates.
#
# In real data, some columns are almost always populated (IDs, timestamps),
# while others are commonly null (soft-delete timestamps, optional fields).
# A flat 3% null rate is unrealistic.  These hints override the default
# when the field name matches (exact or word-boundary, like _SEMANTIC_HINTS).
# ---------------------------------------------------------------------------

_NULL_PROBABILITY_HINTS: Dict[str, float] = {
    # Almost never null (0%)
    "id": 0.0,
    "customer_id": 0.0,
    "order_id": 0.0,
    "user_id": 0.0,
    "created_at": 0.0,
    "status": 0.0,
    "email": 0.0,
    "name": 0.0,
    # Almost never null (1-2%)
    "updated_at": 0.01,
    "type": 0.02,
    # Sometimes null — common optional fields
    "phone": 0.15,
    "phone_number": 0.15,
    "middle_name": 0.60,
    "address_line2": 0.55,
    "company": 0.40,
    "organisation": 0.40,
    "organization": 0.40,
    "notes": 0.45,
    "comment": 0.35,
    "bio": 0.50,
    "description": 0.20,
    "discount": 0.65,
    "referrer": 0.50,
    "utm_source": 0.45,
    "utm_medium": 0.45,
    "utm_campaign": 0.50,
    # Usually null — event-driven timestamps
    "deleted_at": 0.90,
    "resolved_at": 0.70,
    "cancelled_at": 0.85,
    "completed_at": 0.40,
    "shipped_at": 0.30,
    "delivered_at": 0.35,
    "refunded_at": 0.88,
}

_DEFAULT_NULL_PROBABILITY = 0.03


def _match_null_probability(name_lower: str) -> Optional[float]:
    """Look up null probability for a field name (exact then word-boundary match)."""
    if name_lower in _NULL_PROBABILITY_HINTS:
        return _NULL_PROBABILITY_HINTS[name_lower]
    parts = name_lower.split("_")
    for i in range(len(parts) - 1, -1, -1):
        suffix = "_".join(parts[i:])
        if suffix in _NULL_PROBABILITY_HINTS:
            return _NULL_PROBABILITY_HINTS[suffix]
    return None


# ---------------------------------------------------------------------------
# Temporal realism — dates that make sense together
# ---------------------------------------------------------------------------

# Defines which date/timestamp fields must be ordered relative to each other.
# After all fields are independently generated, _apply_temporal_ordering()
# fixes any violations by adjusting the later field to be >= the earlier field.
_TEMPORAL_ORDERING_RULES: List[Tuple[str, str, str]] = [
    # (earlier_field, later_field, relationship)
    ("created_at", "updated_at", "lte"),
    ("created_at", "deleted_at", "lte"),
    ("order_date", "ship_date", "lte"),
    ("ship_date", "delivered_at", "lte"),
    ("order_date", "delivered_at", "lte"),
    ("start_date", "end_date", "lte"),
    ("valid_from", "valid_to", "lte"),
    ("hired_at", "terminated_at", "lte"),
    ("born_at", "hired_at", "lte"),
    ("issued_at", "expires_at", "lte"),
    ("submitted_at", "approved_at", "lte"),
    ("approved_at", "completed_at", "lte"),
    ("opened_at", "closed_at", "lte"),
    ("scheduled_at", "started_at", "lte"),
    ("started_at", "ended_at", "lte"),
    ("first_seen_at", "last_seen_at", "lte"),
    ("first_order_at", "last_order_at", "lte"),
    # ── Telecom ───────────────────────────────────────────────────────────
    ("call_start_time", "call_end_time", "lte"),
    ("data_session_start", "data_session_end", "lte"),
    ("sim_activation_date", "sim_termination_date", "lte"),
    ("contract_start_date", "contract_end_date", "lte"),
    ("port_request_date", "port_completion_date", "lte"),
    ("complaint_raised_at", "complaint_resolved_at", "lte"),
    ("invoice_date", "payment_due_date", "lte"),
    ("payment_due_date", "payment_received_date", "lte"),
]

# How far apart related dates should be: (min_minutes, max_minutes).
# Used to generate a realistic gap when the later field needs adjustment.
_TEMPORAL_GAPS: Dict[Tuple[str, str], Tuple[int, int]] = {
    ("order_date", "ship_date"): (60, 4320),  # 1 hr to 3 days
    ("ship_date", "delivered_at"): (1440, 20160),  # 1 day to 2 weeks
    ("created_at", "updated_at"): (0, 525600),  # 0 to 1 year
    ("submitted_at", "approved_at"): (60, 10080),  # 1 hr to 1 week
    ("started_at", "ended_at"): (5, 480),  # 5 mins to 8 hrs
    ("issued_at", "expires_at"): (43800, 525600),  # 1 month to 1 year
    ("hired_at", "terminated_at"): (43800, 3153600),  # 1 month to 6 years
    ("opened_at", "closed_at"): (30, 43200),  # 30 mins to 30 days
    ("scheduled_at", "started_at"): (0, 1440),  # 0 to 1 day
    # ── Telecom ───────────────────────────────────────────────────────────
    ("call_start_time", "call_end_time"): (0, 120),  # 0 to 2 hrs
    ("data_session_start", "data_session_end"): (0, 1440),  # 0 to 1 day
    ("port_request_date", "port_completion_date"): (1440, 10080),  # 1 to 7 days
    ("invoice_date", "payment_due_date"): (20160, 43200),  # 14 to 30 days
}

# Age-based constraints for date-of-birth style fields.
_AGE_CONSTRAINTS: Dict[str, Dict[str, int]] = {
    "date_of_birth": {"min_age_years": 18, "max_age_years": 85},
    "birth_date": {"min_age_years": 18, "max_age_years": 85},
    "dob": {"min_age_years": 18, "max_age_years": 85},
    "incorporation_date": {"min_age_years": 0, "max_age_years": 50},
}

# Business-hours awareness: True = cluster 08:00-18:00, False = any time.
_BUSINESS_HOURS_FIELDS: Dict[str, bool] = {
    "submitted_at": True,
    "approved_at": True,
    "support_opened": True,
    "invoice_date": True,
    "payment_date": True,
    "order_placed_at": False,
    "login_at": False,
    "event_time": False,
}

# Fields unlikely to fall on weekends.
_WEEKDAY_ONLY_FIELDS: set = {
    "invoice_date",
    "payment_date",
    "approval_date",
    "submitted_at",
    "bank_transfer_date",
    "approved_at",
}

# Pre-built index: earlier_field → [(later_field, relationship), ...]
_TEMPORAL_ORDER_INDEX: Dict[str, List[Tuple[str, str]]] = {}
for _earlier, _later, _rel in _TEMPORAL_ORDERING_RULES:
    _TEMPORAL_ORDER_INDEX.setdefault(_earlier, []).append((_later, _rel))


# ---------------------------------------------------------------------------
# Cross-field consistency rules
# ---------------------------------------------------------------------------

_FIELD_CONSISTENCY_RULES: Dict[str, Dict[str, Any]] = {
    # ── Geographic Consistency ────────────────────────────────────────────
    "country_code": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    "country_code_alpha2": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    "country_code_alpha3": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    "country_code_numeric": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    "primary_currency_code": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    "currency_code": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    "currency": {
        "correlates_with": "country_name",
        "format_lookup": "_GEO_LOOKUP_BY_NAME",
    },
    # If status is 'deleted', deleted_at must not be null
    "deleted_at": {
        "condition_field": "status",
        "condition_value": "deleted",
        "behaviour": "must_be_populated",
    },
    # If status is NOT 'deleted', deleted_at must be null
    "deleted_at_null": {
        "target_field": "deleted_at",
        "condition_field": "status",
        "condition_not_value": "deleted",
        "behaviour": "must_be_null",
    },
    # resolved_at only populated when status is resolved/closed
    "resolved_at": {
        "condition_field": "status",
        "condition_value": ["resolved", "closed", "completed"],
        "behaviour": "must_be_populated",
    },
    # refund_amount only when order_status is refunded
    "refund_amount": {
        "condition_field": "order_status",
        "condition_value": ["refunded", "partially_refunded"],
        "behaviour": "must_be_populated",
    },
    # discount_code only when discount > 0
    "discount_code": {
        "condition_field": "discount",
        "condition_gt": 0,
        "behaviour": "must_be_populated",
    },
    # total_value = subtotal + tax_amount - discount_amount
    "total_value": {
        "derived_from": ["subtotal", "tax_amount", "discount_amount"],
        "formula": "subtotal + tax_amount - discount_amount",
    },
    # quantity_available = quantity_on_hand - quantity_committed
    "quantity_available": {
        "derived_from": ["quantity_on_hand", "quantity_committed"],
        "formula": "quantity_on_hand - quantity_committed",
    },
    # tax_amount = amount * tax_rate / 100
    "tax_amount": {
        "derived_from": ["amount", "tax_rate"],
        "formula": "amount * (tax_rate / 100)",
    },
    # ── Telecom ───────────────────────────────────────────────────────────
    # call_duration must be 0 if call never connected
    "call_duration_seconds": {
        "condition_field": "termination_reason",
        "condition_value": ["busy", "no_answer", "call_rejected"],
        "behaviour": "set_to_zero",
    },
    # data_volume must be 0 if session failed
    "data_volume_bytes": {
        "condition_field": "session_status",
        "condition_value": "failed",
        "behaviour": "set_to_zero",
    },
    # roaming charges only when roaming_indicator is true
    "roaming_charge": {
        "condition_field": "roaming_indicator",
        "condition_value": False,
        "behaviour": "set_to_zero",
    },
    # out_of_bundle_charge only when bundle NOT exhausted
    "out_of_bundle_charge": {
        "condition_field": "in_bundle",
        "condition_value": True,
        "behaviour": "set_to_zero",
    },
    # RSRP only valid for LTE/5G (not 2G)
    "rsrp": {
        "condition_field": "network_type",
        "condition_value": ["2G_GSM", "2G_GPRS", "2G_EDGE"],
        "behaviour": "must_be_null",
    },
    # MSISDN format must match MCC country
    "msisdn": {
        "correlates_with": "mcc",
        "format_lookup": "_LOCALE_FORMATS_BY_MCC",
    },
}

# Numeric range constraints tied to other fields.
# (smaller_field, larger_field): relationship
_NUMERIC_CONSISTENCY: Dict[Tuple[str, str], str] = {
    ("quantity_committed", "quantity_on_hand"): "lte",
    ("quantity_available", "quantity_on_hand"): "lte",
    ("discount_amount", "subtotal"): "lte",
    ("refund_amount", "order_total"): "lte",
    ("paid_amount", "invoice_amount"): "lte",
    ("used_quantity", "total_quantity"): "lte",
}


# ---------------------------------------------------------------------------
# Entity relationships and foreign key realism
# ---------------------------------------------------------------------------

# When generating string ID fields, use these formatted patterns instead of
# random noise.  {:06d} is replaced with a zero-padded random integer.
_ENTITY_ID_PATTERNS: Dict[str, str] = {
    "customer_id": "CUST-{:06d}",
    "order_id": "ORD-{:06d}",
    "product_id": "PROD-{:06d}",
    "employee_id": "EMP-{:06d}",
    "supplier_id": "SUP-{:06d}",
    "invoice_id": "INV-{:06d}",
    "ticket_id": "TKT-{:06d}",
    "campaign_id": "CMP-{:06d}",
    "session_id": "SES-{:06d}",
    "device_id": "DEV-{:06d}",
    "shipment_id": "SHP-{:06d}",
    "patient_id": "PAT-{:06d}",
    "account_id": "ACC-{:06d}",
    "contract_id": "CTR-{:06d}",
    "project_id": "PRJ-{:06d}",
    "user_id": "USR-{:06d}",
}

# Cardinality hints — used when generating multiple related contracts together.
# Prevents unrealistic 1:1 when 1:many is expected.
# (parent_entity, child_entity): (min_children, max_children)
_CARDINALITY_HINTS: Dict[Tuple[str, str], Tuple[int, int]] = {
    ("customer", "order"): (0, 25),
    ("customer", "address"): (1, 3),
    ("order", "order_line"): (1, 10),
    ("invoice", "invoice_line"): (1, 20),
    ("product", "variant"): (1, 8),
    ("employee", "timesheet"): (0, 52),
    ("campaign", "ad_impression"): (100, 10000),
    ("ticket", "comment"): (0, 15),
    ("device", "event"): (10, 1000),
}

# Pre-built index: match entity ID patterns by suffix (word-boundary aware)
_ENTITY_ID_INDEX: Dict[str, str] = {}
for _eid_key, _eid_fmt in _ENTITY_ID_PATTERNS.items():
    _ENTITY_ID_INDEX[_eid_key] = _eid_fmt


# ---------------------------------------------------------------------------
# Value distribution profiles
# ---------------------------------------------------------------------------
# Instead of uniform random, shape the value distribution to mimic real data.
# Supports: weighted, lognormal, normal, beta, bimodal.

_DISTRIBUTION_PROFILES: Dict[str, Dict[str, Any]] = {
    # Status fields — active dominates in live datasets
    "status": {
        "distribution": "weighted",
        "weights": {
            "active": 0.65,
            "inactive": 0.15,
            "pending": 0.12,
            "suspended": 0.05,
            "deleted": 0.03,
        },
    },
    # Order status — most orders are delivered
    "order_status": {
        "distribution": "weighted",
        "weights": {
            "delivered": 0.70,
            "processing": 0.12,
            "shipped": 0.08,
            "cancelled": 0.06,
            "refunded": 0.03,
            "failed": 0.01,
        },
    },
    # Ticket priority — most tickets are low/medium
    "priority": {
        "distribution": "weighted",
        "weights": {"low": 0.45, "medium": 0.35, "high": 0.15, "critical": 0.05},
    },
    # Revenue — right-skewed (most small, few large)
    "revenue": {
        "distribution": "lognormal",
        "mean": 4.5,
        "std": 1.2,
        "min": 0.01,
        "max": 50000,
    },
    # CSAT scores pile at 4 and 5
    "satisfaction_score": {
        "distribution": "weighted",
        "weights": {1: 0.05, 2: 0.08, 3: 0.12, 4: 0.30, 5: 0.45},
    },
    # Churn risk — skewed toward 0 (low risk)
    "churn_risk": {
        "distribution": "beta",
        "alpha": 1.5,
        "beta": 5.0,
    },
    # NPS — bimodal (promoters and detractors)
    "nps_score": {
        "distribution": "bimodal",
        "peak_1": {"value": 9, "weight": 0.40},
        "peak_2": {"value": 2, "weight": 0.25},
        "flat_weight": 0.35,
    },
    # Age — working-age customers
    "age": {
        "distribution": "normal",
        "mean": 38,
        "std": 12,
        "min": 18,
        "max": 80,
    },
    # Amount / price — right-skewed
    "amount": {
        "distribution": "lognormal",
        "mean": 3.5,
        "std": 1.0,
        "min": 0.01,
        "max": 10000,
    },
    "price": {
        "distribution": "lognormal",
        "mean": 3.0,
        "std": 0.8,
        "min": 0.01,
        "max": 999.99,
    },
}


def _match_distribution(name_lower: str) -> Optional[Dict[str, Any]]:
    """Look up a distribution profile for a field name (exact then suffix match)."""
    if name_lower in _DISTRIBUTION_PROFILES:
        return _DISTRIBUTION_PROFILES[name_lower]
    parts = name_lower.split("_")
    for i in range(len(parts) - 1, -1, -1):
        suffix = "_".join(parts[i:])
        if suffix in _DISTRIBUTION_PROFILES:
            return _DISTRIBUTION_PROFILES[suffix]
    return None


# ---------------------------------------------------------------------------
# Edge case injection profiles
# ---------------------------------------------------------------------------
# Injected into invalid_ratio rows. Each profile defines a specific type of
# real-world data quality issue.

_EDGE_CASE_PROFILES: Dict[str, Dict[str, Any]] = {
    # Unicode and special characters — tests encoding robustness
    "unicode_injection": {
        "fields": ["name", "full_name", "description", "notes", "address", "comment"],
        "values": [
            "José García",
            "Müller, Hans",
            "Søren Aaberg",
            "François Dupont",
            "Ανδρέας Παπαδόπουλος",
            "张伟",
            "田中 太郎",
            "محمد علي",
            "O'Brien",
            "St. John-Smith",
            "McDonald's & Co.",
        ],
    },
    # SQL injection attempts — tests sanitisation
    "sql_injection": {
        "fields": ["name", "description", "notes", "search_term", "comment"],
        "values": [
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "admin'--",
            "1; SELECT * FROM contracts",
        ],
        "include_in_invalid_only": True,
    },
    # Boundary values for numeric fields
    "numeric_boundaries": {
        "pattern": "numeric",
        "values": [0, -1, -0.01, 0.001, 999999999, -999999999],
    },
    # Empty string vs null — different failure modes
    "empty_strings": {
        "fields": ["email", "phone", "name", "postcode", "address"],
        "values": ["", " ", "  ", "\t", "\n"],
    },
    # Future dates — common data quality issue
    "future_dates": {
        "fields": ["date_of_birth", "created_at", "order_date", "birth_date"],
        "behaviour": "set_to_future",
        "include_in_invalid_only": True,
    },
    # Type confusion — numeric stored as string
    "type_confusion": {
        "fields": ["amount", "quantity", "age", "price", "total", "balance"],
        "values": ["N/A", "null", "NULL", "None", "n/a", "TBC", "-"],
        "include_in_invalid_only": True,
    },
    # Format violations
    "format_violations": {
        "email": ["notanemail", "@nodomain", "missing@", "spaces in@email.com"],
        "postcode": ["INVALID", "00000", "ABC", "SW1A1A"],
        "phone": ["0000", "not-a-phone", "++44", "123"],
        "iban": ["GB00000", "not-an-iban", "12345"],
    },
}

# Pre-build: field_name → list of edge-case values for fast lookup
_EDGE_CASE_INDEX: Dict[str, List[Any]] = {}
for _ec_name, _ec_profile in _EDGE_CASE_PROFILES.items():
    if "fields" in _ec_profile and "values" in _ec_profile:
        for _field in _ec_profile["fields"]:
            _EDGE_CASE_INDEX.setdefault(_field, []).extend(_ec_profile["values"])
    elif isinstance(_ec_profile, dict) and _ec_name == "format_violations":
        for _fld, _vals in _ec_profile.items():
            if isinstance(_vals, list):
                _EDGE_CASE_INDEX.setdefault(_fld, []).extend(_vals)


class DataGenerator:
    """
    Generate synthetic data from a LakeLogic contract YAML or schema definition.

    Parameters
    ----------
    contract_path : str | Path | list | dict | StructType
        Data source schema.  Accepts:

        - **Contract path** — path to a YAML contract file
        - **DDL string**    — ``"col1 STRING, col2 INT, col3 TIMESTAMP"``
        - **List of tuples** — ``[("col1", "string"), ("col2", "int")]``
        - **Dict**          — ``{"col1": "string", "col2": "int"}``
        - **Spark StructType** — schema-only, no data needed

    seed : int, optional
        Random seed for reproducibility.
    use_faker : bool
        If True (default) and Faker is installed, use semantic generation
        for string fields with recognisable names (email, name, …).

    Examples
    --------
    From a contract file::

        gen = DataGenerator("contracts/orders.yaml")
        df  = gen.generate(rows=1000, invalid_ratio=0.10)

    From a DDL string::

        gen = DataGenerator("order_id BIGINT, email STRING, amount DOUBLE, created_at TIMESTAMP")
        df  = gen.generate(rows=1000, invalid_ratio=0.10)

    From a list of tuples::

        gen = DataGenerator([("user_id", "integer"), ("name", "string"), ("active", "boolean")])
        df  = gen.generate(rows=500)

    From a dict::

        gen = DataGenerator({"product_id": "integer", "price": "decimal", "in_stock": "boolean"})
        df  = gen.generate(rows=200)

    From a Spark StructType::

        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        schema = StructType([
            StructField("user_id", IntegerType()),
            StructField("email", StringType()),
        ])
        gen = DataGenerator(schema)
        df  = gen.generate(rows=50)

    From reading a Databricks Unity Catalog / Database table (chained)::

        from lakelogic import infer_contract
        gen = infer_contract("my_catalog.sales.orders").to_generator(seed=42)
        df  = gen.generate(rows=500, invalid_ratio=0.05)

    From a CSV / Parquet file (chained)::

        from lakelogic import infer_contract
        gen = infer_contract("data/orders.csv").to_generator(seed=0)
        df  = gen.generate(rows=100)
    """

    def __init__(
        self,
        contract_path,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> None:
        self.seed = seed
        self._rng = random.Random(seed)
        self._faker = _try_faker() if use_faker else None
        if self._faker and seed is not None:
            self._faker.seed_instance(seed)

        # ── Schema-only input (DDL string, tuples, dict, StructType) ───────
        if self._is_schema_input(contract_path):
            self.contract_path = Path("_from_schema")
            self._contract_raw = self._contract_from_schema(contract_path)
        else:
            self.contract_path = Path(contract_path)
            self._contract_raw: Dict[str, Any] = self._load_yaml()

        # ── Guard: contracts with LLM extraction are not supported ─────────
        if self._contract_raw.get("extraction"):
            raise ValueError(
                f"DataGenerator does not support contracts with an 'extraction' "
                f"section ({self.contract_path.name}).\n"
                f"Contracts that use LLM-based extraction require real "
                f"unstructured data (text, PDFs, images, audio) that cannot "
                f"be synthesised.\n"
                f"For CI/CD testing, use a sample CSV:\n"
                f"  df = polars.read_csv('_data/samples/your_sample.csv')\n"
                f"  result = DataProcessor.run(contract, df)"
            )

        self._fields: List[Dict[str, Any]] = self._extract_fields()
        self._quality: Dict[str, Any] = self._contract_raw.get("quality", {}) or {}
        # Fields that are integer + flagged unique in quality rules — these need
        # special treatment in generate_from_sample (single-record seed files).
        self._unique_integer_fields: set = self._extract_unique_integer_fields()
        # Populated by from_file(); generate() uses these automatically
        self._auto_sample_pools: Optional[Dict[str, List[Any]]] = None
        self._triplets: List[Dict[str, Any]] = self._detect_triplets()
        self._geo_alignments: List[Dict[str, str]] = self._detect_geo_alignment()

    @staticmethod
    def _is_schema_input(source) -> bool:
        """Return True if *source* is a schema definition (not a file path)."""
        # Spark StructType
        if type(source).__name__ == "StructType":
            return True
        # List/tuple of (name, type) pairs
        if isinstance(source, (list, tuple)) and source and isinstance(source[0], (list, tuple)):
            return True
        # Dict {"col": "type"}
        if isinstance(source, dict) and source:
            return all(isinstance(v, str) for v in source.values())
        # DDL string heuristic
        if isinstance(source, str) and not any(sep in source for sep in ("/", "\\")):
            # Must not end with .yaml/.yml (contract path)
            if source.rstrip().endswith((".yaml", ".yml")):
                return False
            _ddl_types = {
                "string",
                "varchar",
                "char",
                "int",
                "integer",
                "bigint",
                "smallint",
                "tinyint",
                "float",
                "double",
                "decimal",
                "boolean",
                "date",
                "timestamp",
                "binary",
                "long",
                "short",
            }
            tokens = {t.strip().lower().split("(")[0] for t in source.replace(",", " ").split()}
            if len(tokens & _ddl_types) >= 2:
                return True
        return False

    @staticmethod
    def _contract_from_schema(schema) -> Dict[str, Any]:
        """Build an in-memory contract dict from a schema definition."""
        from lakelogic.core.bootstrap import _parse_schema_to_fields

        fields = _parse_schema_to_fields(schema)
        return {
            "info": {
                "title": "_from_schema",
                "version": "0.0.0",
                "description": "Auto-generated contract from schema definition.",
            },
            "model": {"fields": fields},
            "quality": {},
        }

    @classmethod
    def from_file(
        cls,
        source,
        *,
        format: Optional[str] = None,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> "DataGenerator":
        """
        Create a ``DataGenerator`` directly from an existing data file — **no contract needed**.

        The schema (column names and types) is inferred from the file itself and a
        temporary contract is built in memory.  The file's actual values become the
        sampling pool, so ``generate()`` produces rows that mirror the source
        distribution without repeating the file path a second time.

        Parameters
        ----------
        source : str | Path | polars.DataFrame | pandas.DataFrame
            Seed file or in-memory DataFrame.  File formats auto-detected from extension:
            ``.csv``, ``.parquet``, ``.json``, ``.ndjson`` / ``.jsonl``, ``.xlsx`` / ``.xls``.
        format : str, optional
            Explicit file format to use if the file has no extension or a non-standard one (e.g. "csv").
        seed : int, optional
            Random seed for reproducibility.
        use_faker : bool
            If True and Faker is installed, apply semantic generation for string
            fields that have no observed values (all-null columns).

        Returns
        -------
        DataGenerator
            Instance backed by the inferred schema.  Call ``.generate(rows=N)``
            directly — values are sampled from the source file automatically.

        Examples
        --------
        ::

            from lakelogic import DataGenerator

            # No contract YAML required
            gen = DataGenerator.from_file("data/zoopla_sample.csv")
            df  = gen.generate(rows=5_000)

            # With reproducibility seed and bad-row injection
            gen = DataGenerator.from_file("data/zoopla_sample.csv", seed=42)
            df  = gen.generate(rows=500, invalid_ratio=0.1)

            # From an in-memory DataFrame
            import polars as pl
            seed_df = pl.read_parquet("data/seed.parquet")
            gen = DataGenerator.from_file(seed_df)
            df  = gen.generate(rows=1_000)
        """
        import tempfile
        import polars as pl

        # ── 1. Load source into a polars DataFrame ────────────────────────────
        if isinstance(source, pl.DataFrame):
            df = source
        elif hasattr(source, "to_dict"):  # pandas DataFrame
            df = pl.from_pandas(source)
        else:
            p = Path(source)
            ext = f".{format.lower().lstrip('.')}" if format else p.suffix.lower()
            if ext == ".csv":
                df = pl.read_csv(p, infer_schema_length=10_000)
            elif ext == ".parquet":
                df = pl.read_parquet(p)
            elif ext == ".json":
                df = pl.read_json(p)
            elif ext in (".ndjson", ".jsonl"):
                df = pl.read_ndjson(p)
            elif ext in (".xlsx", ".xls"):
                try:
                    import pandas as pd

                    df = pl.from_pandas(pd.read_excel(p))
                except ImportError as exc:
                    raise ImportError(
                        "Excel support requires pandas and openpyxl: pip install pandas openpyxl"
                    ) from exc
            else:
                raise ValueError(
                    f"Cannot infer file format from extension {ext!r}. "
                    "Supported: .csv, .parquet, .json, .ndjson, .jsonl, .xlsx, .xls"
                )

        # ── 2. Map polars dtypes → contract type strings ──────────────────────
        _DTYPE_MAP = {
            pl.Utf8: "string",
            pl.String: "string",
            pl.Int8: "integer",
            pl.Int16: "integer",
            pl.Int32: "integer",
            pl.Int64: "integer",
            pl.UInt8: "integer",
            pl.UInt16: "integer",
            pl.UInt32: "integer",
            pl.UInt64: "integer",
            pl.Float32: "double",
            pl.Float64: "double",
            pl.Boolean: "boolean",
            pl.Date: "date",
            pl.Datetime: "timestamp",
        }

        fields = []
        for col_name in df.columns:
            col_dtype = df[col_name].dtype
            # Match by type class (handles temporal subtypes like Datetime(tu, tz))
            ctype = "string"  # safe default
            for dtype_cls, ctype_str in _DTYPE_MAP.items():
                if isinstance(col_dtype, type(dtype_cls)):
                    ctype = ctype_str
                    break
            fields.append({"name": col_name, "type": ctype})

        # ── 3. Build a minimal contract dict and write to a temp YAML ─────────
        contract_data = {
            "info": {
                "title": "_inferred_from_file",
                "version": "0.0.0",
                "description": "Auto-generated contract inferred from source file.",
            },
            "model": {"fields": fields},
            "quality": {},
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            yaml.dump(contract_data, tmp, sort_keys=False, allow_unicode=True)
            tmp_path = tmp.name

        # ── 4. Instantiate and attach auto-pools ──────────────────────────────
        instance = cls(tmp_path, seed=seed, use_faker=use_faker)

        # Build value pools from non-null unique values in each column
        pools: Dict[str, List[Any]] = {}
        for col_name in df.columns:
            series = df[col_name].drop_nulls()
            if not series.is_empty():
                pools[col_name] = series.unique().to_list()

        instance._auto_sample_pools = pools
        return instance

    @classmethod
    def from_dbt(
        cls,
        schema_path,
        *,
        model: Optional[str] = None,
        source_name: Optional[str] = None,
        source_table: Optional[str] = None,
        seed: Optional[int] = None,
        use_faker: bool = True,
    ) -> "DataGenerator":
        """
        Create a ``DataGenerator`` from a dbt ``schema.yml`` / ``sources.yml``.

        Converts the dbt model/source to a temporary LakeLogic contract and
        returns a generator backed by that schema.  All ``generate()`` and
        ``save()`` methods work identically.

        Parameters
        ----------
        schema_path
            Path to the dbt schema YAML file.
        model
            dbt model name.  May be omitted when the file has exactly one model.
        source_name / source_table
            dbt source identifiers (for ``sources.yml`` files).
        seed
            Random seed for reproducibility.
        use_faker
            If True and Faker is installed, use semantic generation for
            string fields with recognisable names (email, name, …).

        Examples
        --------
        >>> gen = DataGenerator.from_dbt("models/schema.yml", model="customers")
        >>> df  = gen.generate(rows=500, invalid_ratio=0.05)
        """
        import re
        import tempfile
        import yaml

        from lakelogic.adapters.dbt import load_contract_from_dbt

        contract = load_contract_from_dbt(
            schema_path,
            model=model,
            source_name=source_name,
            source_table=source_table,
        )

        # Serialise contract to a mutable dict
        data = contract.model_dump(exclude_none=True, by_alias=True)

        # ── Back-fill field-level hints from SQL row rules ─────────────────
        # The generator's _build_field_rules() only understands structured
        # rule dicts (accepted_values / range).  The dbt adapter emits plain
        # QualityRule SQL strings, so we parse them back here and inject the
        # values directly on each field definition.
        row_rules = (data.get("quality") or {}).get("row_rules") or []
        fields_by_name: Dict[str, Dict] = {}
        for f in (data.get("model") or {}).get("fields") or []:
            fields_by_name[f.get("name", "")] = f

        for rule in row_rules:
            if not isinstance(rule, dict):
                continue
            sql = rule.get("sql", "")
            if not sql:
                continue

            # Pattern: <col> IN ('a', 'b', 'c') or <col> IN (1, 2, 3)
            m_in = re.match(r"^\s*(\w+)\s+IN\s*\((.+)\)\s*$", sql, re.IGNORECASE | re.DOTALL)
            if m_in:
                col = m_in.group(1)
                inner = m_in.group(2)
                # Parse quoted strings or bare numbers
                values = re.findall(r"'([^']*)'|\b(-?\d+(?:\.\d+)?)\b", inner)
                parsed = [s or n for s, n in values]
                if parsed and col in fields_by_name:
                    # Inject as structured accepted_values so _build_field_rules picks it up
                    field_def = fields_by_name[col]
                    field_def.setdefault("accepted_values", parsed)
                continue

            # Pattern: <col> >= <number>   (min constraint from expression_is_true)
            m_gte = re.match(r"^\s*(\w+)\s*>=\s*(-?\d+(?:\.\d+)?)\s*$", sql)
            if m_gte:
                col, val = m_gte.group(1), float(m_gte.group(2))
                if col in fields_by_name:
                    fields_by_name[col].setdefault("min", val)
                continue

            # Pattern: <col> > <number>
            m_gt = re.match(r"^\s*(\w+)\s*>\s*(-?\d+(?:\.\d+)?)\s*$", sql)
            if m_gt:
                col, val = m_gt.group(1), float(m_gt.group(2))
                if col in fields_by_name:
                    fields_by_name[col].setdefault("min", val + 0.00001)
                continue

            # Pattern: <col> <= <number>   (max constraint)
            m_lte = re.match(r"^\s*(\w+)\s*<=\s*(-?\d+(?:\.\d+)?)\s*$", sql)
            if m_lte:
                col, val = m_lte.group(1), float(m_lte.group(2))
                if col in fields_by_name:
                    fields_by_name[col].setdefault("max", val)
                continue

            # Pattern: <col> LIKE '%@%'  (email format) — mark the field name so
            # Faker's semantic hint picks it up (already does via field name "email")
            # No extra action needed.

        # Write enriched contract to temp YAML
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            yaml.dump(data, tmp, sort_keys=False, allow_unicode=True)
            tmp_path = tmp.name

        return cls(tmp_path, seed=seed, use_faker=use_faker)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        rows: int = 100,
        invalid_ratio: float = 0.0,
        output_format: str = "polars",
        reference_data: Optional[Dict[str, List[Any]]] = None,
        ai_edge_cases: Optional[Dict[str, List[Any]]] = None,
        ai: bool = False,
        ai_provider: Optional[str] = None,
        ai_model: Optional[str] = None,
        ai_api_key: Optional[str] = None,
        ai_custom_scenario: Optional[str] = None,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
    ):
        """
        Generate ``rows`` synthetic rows from the contract schema.

        Parameters
        ----------
        rows : int
            Total number of rows to generate.
        invalid_ratio : float
            Fraction of rows (0.0–1.0) that intentionally break quality rules.
            Useful for verifying quarantine logic.  e.g. ``0.1`` = 10% bad rows.
        output_format : str
            ``"polars"``   → returns a ``polars.DataFrame``
            ``"pandas"``   → returns a ``pandas.DataFrame``
        reference_data : dict, optional
            FK-aware generation pool.  Maps FK column names to a list (or
            polars/pandas Series) of valid PK values drawn from the reference
            table.  The generator samples from this pool for FK columns instead
            of generating random values.

            If a field declares ``foreign_key`` in the contract but the column
            is NOT present in ``reference_data``, the generator falls back to a
            synthetic pool of N distinct surrogate integers — useful in CI
            where no live reference table is available.

            Example::

                agents_df = DataGenerator("silver_agents.yaml").generate(rows=20)
                orders_df = DataGenerator("gold_orders.yaml").generate(
                    rows=200,
                    reference_data={"agent_id": agents_df["agent_id"].to_list()},
                )
                # Every orders_df["agent_id"] value exists in agents_df["agent_id"]

        ai_edge_cases : dict, optional
            Pre-computed edge-case pools from ``generate_edge_cases()``.
            Maps field name → list of edge-case values.  When provided,
            invalid rows will preferentially use these instead of heuristic
            strategies.
        ai : bool
            If True and ``invalid_ratio > 0``, call an LLM to generate
            edge-case values for invalid rows.
        ai_provider : str, optional
            LLM provider (openai, azure, anthropic, ollama).
        ai_model : str, optional
            Model name override.
        ai_api_key : str, optional
            API key override.  If not provided, falls back to the
            provider's default env var (e.g. ``OPENAI_API_KEY``).
        ai_custom_scenario : str, optional
            Free-text scenario or test condition to guide the AI's generation.
            (e.g., "Generate users with non-latin characters and ages above 80").
        window_start : datetime, optional
            If provided alongside ``window_end``, all generated timestamps
            and dates are constrained to this window.  Use this to simulate
            continuous / streaming data where each batch covers a specific
            time range (e.g. every 5 minutes)::

                from datetime import datetime, timedelta
                end   = datetime.now()
                start = end - timedelta(minutes=5)
                df = gen.generate(rows=100, window_start=start, window_end=end)

        window_end : datetime, optional
            End of the generation time window (inclusive).  Defaults to
            ``datetime.now()`` if ``window_start`` is set but ``window_end``
            is omitted.

        Returns
        -------
        DataFrame (Polars or Pandas depending on output_format)
        """
        # ── Store time window on instance for all temporal generators ─────
        if window_start is not None:
            self._window_start = window_start
            self._window_end = window_end or datetime.now()
        else:
            self._window_start = None
            self._window_end = None

        # Normalise reference_data values to plain Python lists
        fk_pools: Dict[str, List[Any]] = {}
        if reference_data:
            for col, pool in reference_data.items():
                if hasattr(pool, "to_list"):  # polars / pandas Series
                    fk_pools[col] = pool.to_list()
                elif hasattr(pool, "tolist"):  # numpy array
                    fk_pools[col] = pool.tolist()
                else:
                    fk_pools[col] = list(pool)

        n_invalid = int(rows * invalid_ratio)
        n_valid = rows - n_invalid

        # AI edge-case generation (opt-in)
        edge_pools: Optional[Dict[str, List[Any]]] = ai_edge_cases

        # AI realistic value pools (opt-in) — generates contextually realistic
        # sample values for ALL rows, not just edge cases.
        ai_sample_pools: Optional[Dict[str, List[Any]]] = None
        if ai:
            try:
                from lakelogic.ai.data_generator import generate_realistic_pools

                dataset_name = ""
                info = self._contract_raw.get("info") or {}
                dataset_name = info.get("title") or self._contract_raw.get("dataset") or ""
                ai_sample_pools = generate_realistic_pools(
                    self._fields,
                    self._quality,
                    dataset_name=dataset_name,
                    provider=ai_provider,
                    model=ai_model,
                    api_key=ai_api_key,
                    custom_scenario=ai_custom_scenario or "",
                )
            except Exception as e:
                from loguru import logger

                logger.warning(f"AI data generation failed, falling back to Faker: {e}")
                ai_sample_pools = None

        if ai and n_invalid > 0 and not edge_pools:
            try:
                from lakelogic.ai.edge_case_generator import generate_edge_cases

                dataset_name = ""
                info = self._contract_raw.get("info") or {}
                dataset_name = info.get("title") or self._contract_raw.get("dataset") or ""
                edge_pools = generate_edge_cases(
                    self._fields,
                    self._quality,
                    dataset_name=dataset_name,
                    provider=ai_provider,
                    model=ai_model,
                    api_key=ai_api_key,
                    custom_scenario=ai_custom_scenario or "",
                )
            except Exception as e:
                from loguru import logger

                logger.warning(f"AI edge case generation failed: {e}")
                edge_pools = None

        # from_file() stores auto-pools; pass them so generate() mirrors the source file
        # without the caller needing to specify the file path a second time.
        # AI pools take priority over file-seeded pools.
        auto_pools = ai_sample_pools or getattr(self, "_auto_sample_pools", None) or None

        # ── Generation rationale logging ──────────────────────────────────
        from loguru import logger as _gen_logger

        dataset_label = ""
        info = self._contract_raw.get("info") or {}
        dataset_label = info.get("title") or self._contract_raw.get("dataset") or "unknown"

        _gen_logger.info(f"📋 Generating data for: {dataset_label}")
        _gen_logger.info(f"   Records    : {n_valid:,} valid + {n_invalid:,} invalid = {rows:,} total")

        if self._window_start:
            _gen_logger.info(f"   Window     : {self._window_start.isoformat()} → {self._window_end.isoformat()}")

        # Log data source rationale
        if ai_sample_pools:
            pool_fields = len(ai_sample_pools)
            pool_values = sum(len(v) for v in ai_sample_pools.values())
            _gen_logger.info(
                f"   Source     : AI-generated realistic pools ({pool_fields} fields, {pool_values} values)"
            )
        elif getattr(self, "_auto_sample_pools", None):
            file_fields = len(self._auto_sample_pools)
            _gen_logger.info(f"   Source     : File-seeded sample pools ({file_fields} fields from source data)")
        else:
            _gen_logger.info("   Source     : Faker + heuristic generation (no AI or file seeds)")

        # Log edge case details
        if edge_pools and n_invalid > 0:
            edge_field_count = len(edge_pools)
            edge_total = sum(len(v) for v in edge_pools.values())
            _gen_logger.info(f"   Edge cases : {edge_total} values across {edge_field_count} fields (AI-generated)")
            # Show top 5 fields with most edge cases
            top_fields = sorted(edge_pools.items(), key=lambda x: len(x[1]), reverse=True)[:5]
            for fname, fvals in top_fields:
                sample = str(fvals[:3])
                if len(sample) > 60:
                    sample = sample[:57] + "..."
                _gen_logger.debug(f"     • {fname}: {len(fvals)} cases — e.g. {sample}")
        elif n_invalid > 0:
            _gen_logger.info("   Edge cases : Heuristic-only (no AI edge cases available)")

        valid_records = []
        invalid_records = []
        test_case_manifest: List[TestCaseInfo] = []

        for _ in range(n_valid):
            row, _ = self._make_row(invalid=False, fk_pools=fk_pools, sample_pools=auto_pools)
            valid_records.append(row)

        for _ in range(n_invalid):
            row, test_cases = self._make_row(
                invalid=True,
                fk_pools=fk_pools,
                sample_pools=auto_pools,
                edge_case_pools=edge_pools,
            )
            invalid_records.append(row)
            test_case_manifest.extend(test_cases)

        # Shuffle so bad rows aren't all at the end
        all_records = valid_records + invalid_records
        self._rng.shuffle(all_records)

        # Assign final row indices to test case entries
        # Build lookup: id(row) → final index
        _row_id_to_idx = {id(r): idx for idx, r in enumerate(all_records)}
        for tc in test_case_manifest:
            # Find the row this test case belongs to by matching the field value
            for r in invalid_records:
                if r.get(tc.field) == tc.value and r.get("_is_invalid"):
                    tc.row_index = _row_id_to_idx.get(id(r), -1)
                    break

        _gen_logger.info(f"   Row generation complete: {len(all_records):,} records built")

        # ── Store generation report data on instance ────────────────────────
        info = self._contract_raw.get("info") or {}
        self._last_generation_summary = {
            "total_rows": len(all_records),
            "valid_rows": n_valid,
            "invalid_rows": n_invalid,
            "invalid_ratio": round(n_invalid / max(len(all_records), 1), 4),
            "test_cases_fired": len(test_case_manifest),
            "seed": getattr(self, "_seed", None),
            "contract": info.get("title") or self._contract_raw.get("dataset") or "unknown",
            "contract_version": info.get("version"),
            "engine": "polars",
        }
        self._last_test_case_manifest = test_case_manifest

        # Log test case summary
        if test_case_manifest:
            tc_counts: Dict[str, int] = {}
            for tc in test_case_manifest:
                tc_counts[tc.type] = tc_counts.get(tc.type, 0) + 1
            _gen_logger.info(f"   Test cases : {len(test_case_manifest)} across {len(tc_counts)} categories")
            for tc_type, count in sorted(tc_counts.items(), key=lambda x: -x[1]):
                _gen_logger.info(f"     {tc_type:30s} {count:>4d} injections")

        return self._to_frame(all_records, output_format)

    def generate_stream(
        self,
        rows_per_batch: int = 100,
        interval_minutes: int = 5,
        batches: int = 12,
        output_dir: Optional[str | Path] = None,
        format: str = "parquet",
        partition_template: str = "yyyy={Y}/mm={m}/dd={d}/hh={H}/mi={M}",
        invalid_ratio: float = 0.0,
        output_format: str = "polars",
        start_from: Optional[datetime] = None,
        micro_batches: int = 1,
        up_to: Optional[datetime] = None,
        resume: bool = False,
    ):
        """
        Generate successive batches of time-windowed data, simulating a streaming source.

        Each batch covers ``interval_minutes`` and all timestamps are constrained
        to that specific window.  Use this to stress-test freshness SLOs,
        incremental pipelines, and partition-based ingestion patterns.

        Parameters
        ----------
        rows_per_batch : int
            Number of rows per batch (default 100).
        interval_minutes : int
            Duration of each batch window in minutes (default 5).
        batches : int
            Total number of batches to generate (default 12 = 1 hour).
        output_dir : str | Path, optional
            If provided, each batch is saved to a partitioned subdirectory
            using ``partition_template``.  If None, batches are yielded
            in-memory only.
        format : str
            File format when saving: ``"parquet"`` (default), ``"csv"``, or
            ``"json"``.
        partition_template : str
            Python strftime-compatible template for partition directories.
            Default: ``yyyy={Y}/mm={m}/dd={d}/hh={H}/mi={M}``
        invalid_ratio : float
            Fraction of bad rows per batch.
        output_format : str
            ``"polars"`` (default) or ``"pandas"``.
        start_from : datetime, optional
            Starting timestamp for the first batch.  Defaults to
            ``datetime.now() - interval_minutes * batches``.
        micro_batches : int
            Number of files to split each window's rows into (default 1).
            Set to e.g. 6 to produce ~1 file per 10 minutes in a 60-minute
            window, or 60 for ~1 file per minute.  Each micro-batch is saved
            as a separate file in the same partition directory, simulating
            realistic streaming ingestion patterns.
        up_to : datetime, optional
            Stop generating windows whose start time exceeds this timestamp.
            Defaults to None (no cap).  Set to ``datetime.now()`` to prevent
            generating future data.
        resume : bool
            If True and ``output_dir`` is set, scan existing partitions to
            find the latest one and start from the next window.  This allows
            incremental generation: run once in the morning, run again later
            to fill the gap.  Requires ``output_dir`` to be set.

        Yields
        ------
        tuple[datetime, datetime, DataFrame]
            ``(window_start, window_end, batch_df)`` for each batch.

        Examples
        --------
        In-memory streaming test::

            gen = DataGenerator("contracts/events.yaml")
            for ws, we, df in gen.generate_stream(batches=6, interval_minutes=5):
                print(f"Batch {ws} -> {we}: {len(df)} rows")

        Save to partitioned landing zone with micro-batching::

            gen = DataGenerator("contracts/events.yaml")
            for ws, we, df in gen.generate_stream(
                output_dir="landing/events",
                batches=12,
                interval_minutes=60,
                micro_batches=6,  # 6 files per hour = ~10 min intervals
            ):
                print(f"Wrote batch {ws} -> {we}")

        Time-aware incremental generation::

            gen = DataGenerator("contracts/events.yaml")
            for ws, we, df in gen.generate_stream(
                output_dir="landing/events",
                batches=24,
                interval_minutes=60,
                up_to=datetime.now(),   # never generate future data
                resume=True,            # skip already-generated windows
            ):
                print(f"Wrote batch {ws} -> {we}")
        """
        from loguru import logger
        import uuid as _uuid
        import math as _math

        interval = timedelta(minutes=interval_minutes)

        # ── Resume: detect latest existing partition ──────────────────────
        if resume and output_dir is not None:
            resume_from = self._detect_latest_partition(Path(output_dir), partition_template, interval_minutes)
            if resume_from is not None:
                # Start from the window AFTER the latest existing partition
                cursor = resume_from + interval
                logger.info(
                    f"\u23ed\ufe0f  Resuming from {resume_from.isoformat()} (next window: {cursor.isoformat()})"
                )
            else:
                cursor = start_from if start_from is not None else (datetime.now() - (interval * batches))
        elif start_from is not None:
            cursor = start_from
        else:
            cursor = datetime.now() - (interval * batches)

        effective_up_to = up_to  # None means no cap

        logger.info(
            f"\U0001f504 Streaming {batches} batches \u00d7 {rows_per_batch} rows "
            f"({interval_minutes}min intervals, starting {cursor.isoformat()}"
            + (f", capped at {effective_up_to.isoformat()}" if effective_up_to else "")
            + ")"
        )

        for i in range(batches):
            window_start = cursor
            window_end = cursor + interval

            # ── Time cap: skip windows past up_to ─────────────────────────
            if effective_up_to is not None and window_start >= effective_up_to:
                logger.info(
                    f"   \u23f9 Stopping at window {i}: {window_start.isoformat()} "
                    f">= up_to {effective_up_to.isoformat()}"
                )
                break

            # For partial windows (last window touching up_to), cap the end
            if effective_up_to is not None and window_end > effective_up_to:
                window_end = effective_up_to

            df = self.generate(
                rows=rows_per_batch,
                invalid_ratio=invalid_ratio,
                output_format=output_format,
                window_start=window_start,
                window_end=window_end,
            )

            # Save to partitioned directory if requested
            if output_dir is not None:
                out_root = Path(output_dir)
                partition_path = window_start.strftime(
                    partition_template.replace("{Y}", "%Y")
                    .replace("{m}", "%m")
                    .replace("{d}", "%d")
                    .replace("{H}", "%H")
                    .replace("{M}", "%M")
                    .replace("{S}", "%S")
                )
                batch_dir = out_root / partition_path
                batch_dir.mkdir(parents=True, exist_ok=True)

                # ── Micro-batching: split rows into N files ───────────────
                if micro_batches > 1:
                    total_rows = len(df)
                    chunk_size = max(1, _math.ceil(total_rows / micro_batches))

                    for mb_idx in range(micro_batches):
                        start_row = mb_idx * chunk_size
                        end_row = min(start_row + chunk_size, total_rows)
                        if start_row >= total_rows:
                            break

                        if output_format == "polars":
                            chunk_df = df.slice(start_row, end_row - start_row)
                        else:  # pandas
                            chunk_df = df.iloc[start_row:end_row]

                        batch_file = batch_dir / f"batch_{mb_idx:02d}_{_uuid.uuid4().hex[:6]}.{format}"
                        self.save(chunk_df, batch_file, format=format)

                    logger.info(
                        f"   \U0001f4be Batch {i + 1}/{batches} \u2192 {min(micro_batches, total_rows)} "
                        f"micro-batch files in {batch_dir}"
                    )
                else:
                    batch_file = batch_dir / f"batch_{_uuid.uuid4().hex[:6]}.{format}"
                    self.save(df, batch_file, format=format)
                    logger.info(f"   \U0001f4be Batch {i + 1}/{batches} saved to {batch_file}")

            yield window_start, window_end, df
            cursor = window_end

    @staticmethod
    def _detect_latest_partition(
        output_dir: Path,
        partition_template: str,
        interval_minutes: int,
    ) -> Optional[datetime]:
        """Scan output_dir for the latest existing partition timestamp.

        Walks the directory tree and parses partition folder names using
        the provided template to reconstruct timestamps.  Returns the
        latest timestamp found, or None if the directory is empty.
        """
        import os as _os

        if not output_dir.exists():
            return None

        # Build a regex from the partition template to extract date parts
        import re as _re_mod

        pattern = (
            partition_template.replace("{Y}", r"(?P<Y>\d{4})")
            .replace("{m}", r"(?P<m>\d{2})")
            .replace("{d}", r"(?P<d>\d{2})")
            .replace("{H}", r"(?P<H>\d{2})")
            .replace("{M}", r"(?P<M>\d{2})")
            .replace("{S}", r"(?P<S>\d{2})")
        )
        regex = _re_mod.compile(pattern)

        latest: Optional[datetime] = None

        for dirpath, dirnames, filenames in _os.walk(output_dir):
            rel = str(Path(dirpath).relative_to(output_dir)).replace("\\", "/")
            match = regex.search(rel)
            if match:
                parts = match.groupdict()
                try:
                    ts = datetime(
                        year=int(parts.get("Y", 2026)),
                        month=int(parts.get("m", 1)),
                        day=int(parts.get("d", 1)),
                        hour=int(parts.get("H", 0)),
                        minute=int(parts.get("M", 0)),
                        second=int(parts.get("S", 0)),
                    )
                    if latest is None or ts > latest:
                        latest = ts
                except (ValueError, KeyError):
                    continue

        return latest

    def generate_from_sample(
        self,
        source,
        rows: int = 100,
        invalid_ratio: float = 0.0,
        output_format: str = "polars",
        columns: Optional[List[str]] = None,
    ):
        """
        Generate synthetic rows seeded by the value distribution in an existing file.

        Instead of producing purely random values, each column is sampled from the
        unique values observed in ``source`` (wherever that column exists in both the
        file and the contract schema).  Contract quality rules still apply — so
        ``invalid_ratio`` injects rows that intentionally break them.

        Parameters
        ----------
        source : str | Path | polars.DataFrame | pandas.DataFrame
            Seed file or in-memory DataFrame.  File formats auto-detected from extension:

            - ``.csv``             — comma-separated values
            - ``.parquet``         — Apache Parquet
            - ``.json``            — JSON array
            - ``.ndjson`` / ``.jsonl`` — newline-delimited JSON
            - ``.xlsx`` / ``.xls``    — Excel (requires ``openpyxl``)

        rows : int
            Total number of rows to generate (default 100).
        invalid_ratio : float
            Fraction of rows (0.0–1.0) that intentionally break quality rules.
            Useful for verifying quarantine logic.  e.g. ``0.1`` = 10% bad rows.
        output_format : str
            ``"polars"`` (default) or ``"pandas"``.
        columns : list of str, optional
            Restrict seeding to these specific column names.  Columns in the
            contract but absent from this list fall back to normal synthetic
            generation.  Defaults to all columns present in both file and schema.

        Returns
        -------
        DataFrame (Polars or Pandas depending on output_format)

        Examples
        --------
        Seed from a CSV file::

            from lakelogic import DataGenerator
            gen = DataGenerator("contracts/orders.yaml")

            # Mirror the distribution of a sample CSV
            df = gen.generate_from_sample("data/orders_sample.csv", rows=5_000)

        Mix file-seeded columns with bad rows::

            df = gen.generate_from_sample(
                "data/orders.parquet",
                rows=500,
                invalid_ratio=0.05,
            )

        Pass an existing DataFrame directly::

            import polars as pl
            seed = pl.read_parquet("data/seed.parquet")
            df = gen.generate_from_sample(seed, rows=200)

        Only seed specific columns (others use normal synthetic generation)::

            df = gen.generate_from_sample(
                "data/orders.csv",
                rows=1_000,
                columns=["status", "region"],
            )
        """
        sample_pools = self._load_sample_pools(source, columns=columns)

        n_invalid = int(rows * invalid_ratio)
        n_valid = rows - n_invalid

        valid_rows = [self._make_row(invalid=False, sample_pools=sample_pools) for _ in range(n_valid)]
        invalid_rows = [self._make_row(invalid=True, sample_pools=sample_pools) for _ in range(n_invalid)]

        all_records = valid_rows + invalid_rows
        self._rng.shuffle(all_records)
        return self._to_frame(all_records, output_format)

    # ------------------------------------------------------------------

    def save(
        self,
        df,
        output: str | Path,
        format: str = "parquet",
    ) -> Path:
        """
        Save a generated DataFrame to disk.

        Parameters
        ----------
        df : polars.DataFrame | pandas.DataFrame
        output : str | Path
            Destination file path.
        format : str
            ``"parquet"``, ``"csv"``, or ``"json"``.

        Returns
        -------
        Path
            The resolved output path.
        """
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        fmt = format.lower()

        if hasattr(df, "write_parquet"):  # Polars
            if fmt == "parquet":
                df.write_parquet(output)
            elif fmt == "csv":
                df.write_csv(output)
            elif fmt == "json":
                df.write_ndjson(output)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
        else:  # Pandas
            if fmt == "parquet":
                df.to_parquet(output, index=False)
            elif fmt == "csv":
                df.to_csv(output, index=False)
            elif fmt == "json":
                df.to_json(output, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported format: {fmt}")

        return output

    # ------------------------------------------------------------------
    # Generation Report
    # ------------------------------------------------------------------

    def generation_report(self) -> Dict[str, Any]:
        """Build the structured generation report from the last ``generate()`` call.

        Returns a JSON-serialisable dict documenting every test case that
        fired, grouped by type, with row counts, sample values, and the
        contract rule each test case targets.

        Returns ``{}`` if ``generate()`` has not been called yet.
        """
        summary = getattr(self, "_last_generation_summary", None)
        manifest = getattr(self, "_last_test_case_manifest", None)

        if not summary or manifest is None:
            return {}

        # Group test cases by (type, field) → list of TestCaseInfo
        from collections import defaultdict

        groups: Dict[Tuple[str, str], List[TestCaseInfo]] = defaultdict(list)
        for tc in manifest:
            groups[(tc.type, tc.field)].append(tc)

        # Build test case entries
        test_cases = []
        tc_counter = 0
        for (tc_type, tc_field), entries in sorted(groups.items()):
            tc_counter += 1
            # Unique values used
            values_used = []
            seen_vals = set()
            for e in entries:
                val_repr = repr(e.value)
                if val_repr not in seen_vals:
                    seen_vals.add(val_repr)
                    values_used.append(e.value)

            # Row IDs (capped at 20 for readability)
            row_ids = sorted({e.row_index for e in entries if e.row_index >= 0})[:20]

            test_cases.append(
                {
                    "id": f"TC-{tc_counter:03d}",
                    "type": tc_type,
                    "field": tc_field,
                    "description": entries[0].description,
                    "rows_generated": len(entries),
                    "values_used": values_used[:10],  # cap at 10 for readability
                    "row_ids": row_ids,
                    "expected_quarantine_reason": tc_type,
                    "contract_rule_tested": entries[0].contract_rule,
                }
            )

        report = {
            "generated_at": datetime.now().isoformat(),
            "contract": summary.get("contract", "unknown"),
            "contract_version": summary.get("contract_version"),
            "seed": summary.get("seed"),
            "engine": summary.get("engine", "polars"),
            "summary": {
                "total_rows": summary.get("total_rows", 0),
                "valid_rows": summary.get("valid_rows", 0),
                "invalid_rows": summary.get("invalid_rows", 0),
                "invalid_ratio": summary.get("invalid_ratio", 0),
                "test_cases_fired": len(test_cases),
            },
            "test_cases": test_cases,
        }

        return report

    def save_with_report(
        self,
        df,
        output_dir: str | Path,
        name: Optional[str] = None,
        format: str = "csv",
    ) -> Tuple[Path, Path, Path]:
        """Save the generated data alongside a generation report and invalid manifest.

        Produces three files in ``output_dir``:

        1. ``{name}_test.{format}`` — all rows with ``_is_invalid`` and
           ``_test_case_types`` metadata columns stripped.
        2. ``{name}_invalid.{format}`` — invalid rows only, metadata preserved.
        3. ``{name}_report.json`` — structured generation report.

        Parameters
        ----------
        df : DataFrame
            The DataFrame returned by ``generate()``.
        output_dir : str | Path
            Directory to write files to (created if needed).
        name : str, optional
            Base name for files. Defaults to the contract dataset name.
        format : str
            Output format: ``csv``, ``parquet``, or ``json``.

        Returns
        -------
        tuple of (data_path, invalid_path, report_path)
        """
        import polars as pl

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Resolve base name
        if not name:
            info = self._contract_raw.get("info") or {}
            name = info.get("title") or self._contract_raw.get("dataset") or "test_data"
            # Sanitise for filesystem
            name = re.sub(r"[^\w\-]", "_", name).strip("_").lower()

        # Metadata columns to strip from the "clean" data file
        _meta_cols = {"_is_invalid", "_test_case_types"}

        # ── 1. Clean data file (all rows, metadata stripped) ──────────────
        if isinstance(df, pl.DataFrame):
            clean_cols = [c for c in df.columns if c not in _meta_cols]
            clean_df = df.select(clean_cols)
        else:
            # Pandas fallback
            clean_cols = [c for c in df.columns if c not in _meta_cols]
            clean_df = df[clean_cols]

        data_path = output_dir / f"{name}_test.{format}"
        self.save(clean_df, data_path, format=format)

        # ── 2. Invalid rows manifest (metadata preserved) ─────────────────
        if isinstance(df, pl.DataFrame):
            if "_is_invalid" in df.columns:
                invalid_df = df.filter(pl.col("_is_invalid"))
            else:
                invalid_df = pl.DataFrame()
        else:
            if "_is_invalid" in df.columns:
                invalid_df = df[df["_is_invalid"]]
            else:
                invalid_df = df.iloc[0:0]

        invalid_path = output_dir / f"{name}_invalid.{format}"
        self.save(invalid_df, invalid_path, format=format)

        # ── 3. Generation report JSON ─────────────────────────────────────
        report = self.generation_report()
        report_path = output_dir / f"{name}_report.json"
        with open(report_path, "w", encoding="utf-8") as f:
            _json.dump(report, f, indent=2, default=str)

        from loguru import logger as _save_logger

        _save_logger.info(f"   Saved: {data_path}  ({report.get('summary', {}).get('total_rows', '?')} rows)")
        _save_logger.info(
            f"   Saved: {invalid_path}  ({report.get('summary', {}).get('invalid_rows', '?')} invalid rows)"
        )
        _save_logger.info(f"   Saved: {report_path}  (generation report)")

        return data_path, invalid_path, report_path

    def save_partitioned(
        self,
        df,
        output_dir: str | Path,
        filename_field: Optional[str] = None,
        format: str = "json",
        filename_template: str = "{value}",
        orient: str = "records",
        one_row_per_file: bool = True,
    ) -> List[Path]:
        """
        Save a generated DataFrame as one file per unique key, named by field value(s).

        This replicates landing-zone patterns where each entity (e.g. a property
        listing) arrives as its own file named after its primary key::

            output_dir/
            ├── 10001.json
            ├── 10002.json
            └── 10003.json

        Parameters
        ----------
        df : polars.DataFrame | pandas.DataFrame
            The generated DataFrame (output of ``generate()``).
        output_dir : str | Path
            Directory to write files into (created if it does not exist).
        filename_field : str, optional
            Column whose value is used as the primary key for grouping rows and
            as ``{value}`` in *filename_template*.
            Can be omitted when *filename_template* fully specifies the name via
            ``{column_name}`` placeholders (see below).
        format : str
            ``"json"`` (default), ``"parquet"``, or ``"csv"``.
        filename_template : str
            Template string for the filename stem.  Supports two kinds of placeholders:

            ``{value}``
                Replaced by the value of *filename_field*.  Backward-compatible shorthand.

            ``{column_name}``
                Replaced by the value of *column_name* for each row.  You can mix
                field references and static text freely.

            Examples (``listing_id=10001``, ``postcode="SW1A 1AA"``, ``property_type="flat"``):

            .. code-block:: text

               "{value}"                    → 10001.json          (default)
               "zoopla_{value}"             → zoopla_10001.json    (static prefix)
               "{value}_raw"                → 10001_raw.json       (static suffix)
               "{listing_id}_{postcode}"    → 10001_SW1A 1AA.json  (two fields)
               "listing_{listing_id}_{property_type}"  → listing_10001_flat.json

        orient : str
            JSON orientation when *format* is ``"json"``.
            ``"records"`` (default) writes a single JSON object per file.
            Use ``"lines"`` for NDJSON when ``one_row_per_file=False``.
        one_row_per_file : bool
            When ``True`` (default) each file contains exactly one JSON object
            (the canonical single-entity landing-zone pattern).
            When ``False``, all rows sharing the same key are written to the
            same file as a JSON array.

        Returns
        -------
        list[Path]
            Sorted list of paths written.

        Examples
        --------
        >>> gen = DataGenerator("contracts/bronze_zoopla_listings_v1.0.0.yaml")
        >>> df  = gen.generate(rows=50)

        Simple — just listing_id as the name:

        >>> paths = gen.save_partitioned(
        ...     df,
        ...     output_dir="data/landing/zoopla/listings",
        ...     filename_field="listing_id",
        ... )
        # → 10001.json, 10002.json, …

        Static prefix + field:

        >>> paths = gen.save_partitioned(
        ...     df,
        ...     output_dir="data/landing/zoopla/listings",
        ...     filename_field="listing_id",
        ...     filename_template="zoopla_{value}",
        ... )
        # → zoopla_10001.json, zoopla_10002.json, …

        Composite key from two columns:

        >>> paths = gen.save_partitioned(
        ...     df,
        ...     output_dir="data/landing/zoopla/listings",
        ...     filename_template="{listing_id}_{property_type}",
        ... )
        # → 10001_flat.json, 10002_terraced.json, …
        """
        import json as _json

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        fmt = format.lower()

        # ── Normalise to list-of-dicts for uniform handling ───────────────
        is_polars = hasattr(df, "write_parquet")
        columns = list(df.columns) if is_polars else list(df.columns)

        if filename_field is not None and filename_field not in columns:
            raise ValueError(f"filename_field '{filename_field}' not found in DataFrame. Available columns: {columns}")

        rows_iter = df.to_dicts() if is_polars else df.to_dict(orient="records")

        # ── Group rows by the primary key for file-per-entity splitting ───
        # Key is the filename_field value when provided, otherwise the full
        # resolved stem so each row gets its own file.
        groups: Dict[str, List[Dict]] = {}
        for row in rows_iter:
            key = (
                str(row[filename_field])
                if filename_field is not None
                else self._resolve_filename_stem(row, filename_template, None)
            )
            groups.setdefault(key, []).append(row)

        written: List[Path] = []
        ext = {"json": ".json", "parquet": ".parquet", "csv": ".csv"}.get(fmt, f".{fmt}")

        for key, group_rows in groups.items():
            # Resolve the stem from the first row in the group so per-row
            # field values (e.g. postcode) are reflected in the name.
            stem = self._resolve_filename_stem(group_rows[0], filename_template, key)
            dest = output_dir / f"{stem}{ext}"

            if fmt == "json":
                payload = group_rows[0] if one_row_per_file else group_rows
                dest.write_text(
                    _json.dumps(payload, default=str, indent=2),
                    encoding="utf-8",
                )

            elif fmt == "parquet":
                if is_polars:
                    import polars as pl

                    pl.DataFrame(group_rows).write_parquet(dest)
                else:
                    import pandas as pd

                    pd.DataFrame(group_rows).to_parquet(dest, index=False)

            elif fmt == "csv":
                if is_polars:
                    import polars as pl

                    pl.DataFrame(group_rows).write_csv(dest)
                else:
                    import pandas as pd

                    pd.DataFrame(group_rows).to_csv(dest, index=False)

            else:
                raise ValueError(f"Unsupported format '{fmt}'. Choose 'json', 'parquet', or 'csv'.")

            written.append(dest)

        return sorted(written)

    @staticmethod
    def _resolve_filename_stem(
        row: Dict[str, Any],
        template: str,
        primary_value: Optional[str],
    ) -> str:
        """
        Resolve a filename stem from a row dict and a template string.

        Replacement order
        -----------------
        1. ``{value}`` → *primary_value* (the ``filename_field`` value).
        2. ``{column_name}`` → the value of that column in *row*, cast to str.
           Spaces are replaced with underscores so filenames stay shell-safe.
        3. Any ``{placeholder}`` that does not match a column name is left as-is
           so callers get a clear hint rather than a silent wrong filename.

        Parameters
        ----------
        row : dict
            A single row as a plain Python dict.
        template : str
            Template string, e.g. ``"zoopla_{listing_id}_{property_type}"``.
        primary_value : str | None
            Value of the ``filename_field`` column (used for ``{value}``).
            Pass ``None`` when there is no primary field.
        """
        import re as _re

        stem = template

        # 1. {value} shorthand
        if primary_value is not None:
            stem = stem.replace("{value}", str(primary_value))

        # 2. {column_name} → row value (space → underscore for safety)
        def _sub(match: "re.Match") -> str:  # type: ignore[name-defined]
            field = match.group(1)
            if field in row:
                return str(row[field]).replace(" ", "_")
            return match.group(0)  # leave unresolvable placeholders intact

        stem = _re.sub(r"\{([^}]+)\}", _sub, stem)
        return stem

    # ------------------------------------------------------------------
    # Related data generation (FK/PK referential integrity)
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_fk_relationships(
        entity_name: str,
        contract_raw: Dict[str, Any],
        all_entity_names: List[str],
        all_pk_columns: Dict[str, List[str]],
    ) -> List[Dict[str, str]]:
        """
        Detect FK columns in a contract that reference another entity's PK.

        Detection sources (in priority order):
        1. ``links:`` section — explicit table references with join columns
        2. ``transformations:`` SQL — parses ``JOIN ... ON x.col = y.col``
        3. Field ``description`` — patterns like "FK to customers table"
        4. Shared column names — if ``customer_id`` appears in both ``orders``
           and ``customers`` contracts, and ``customer_id`` is a PK of
           ``customers``, it's inferred as a FK.

        Returns
        -------
        list of dict
            Each dict has: ``fk_column``, ``ref_entity``, ``ref_column``
        """
        relationships: List[Dict[str, str]] = []
        seen_fk_cols: set = set()

        # 1. Parse links: section
        links = contract_raw.get("links") or []
        transformations = contract_raw.get("transformations") or []

        for link in links:
            link_name = link.get("name", "")
            link_columns = link.get("columns") or []

            # Try to match link name to an entity in our generation set
            ref_entity = None
            for ename in all_entity_names:
                if ename == entity_name:
                    continue
                # Match: link name "customers" matches entity "customers"
                # Also match partial: link "customers" matches entity "bronze_customers"
                if link_name == ename or ename.endswith(f"_{link_name}") or link_name.endswith(f"_{ename}"):
                    ref_entity = ename
                    break

            if not ref_entity:
                continue

            # Parse SQL JOINs to find the actual join column
            for tx in transformations:
                sql = tx.get("sql", "")
                if not sql:
                    continue
                # Pattern: JOIN <alias> ON <x>.<col> = <y>.<col>
                join_pattern = re.compile(
                    r"JOIN\s+(\w+)\s+\w+\s+ON\s+\w+\.(\w+)\s*=\s*\w+\.(\w+)",
                    re.IGNORECASE,
                )
                for m in join_pattern.finditer(sql):
                    join_table, left_col, right_col = m.group(1), m.group(2), m.group(3)
                    if join_table.lower() == link_name.lower():
                        fk_col = left_col  # the column on the source (child) side
                        ref_col = right_col  # the column on the joined (parent) side
                        if fk_col not in seen_fk_cols:
                            relationships.append(
                                {
                                    "fk_column": fk_col,
                                    "ref_entity": ref_entity,
                                    "ref_column": ref_col,
                                }
                            )
                            seen_fk_cols.add(fk_col)

            # If no SQL JOIN found, try matching link columns against PKs
            if not any(r["ref_entity"] == ref_entity for r in relationships):
                ref_pks = all_pk_columns.get(ref_entity, [])
                for pk_col in ref_pks:
                    if pk_col in link_columns and pk_col not in seen_fk_cols:
                        relationships.append(
                            {
                                "fk_column": pk_col,
                                "ref_entity": ref_entity,
                                "ref_column": pk_col,
                            }
                        )
                        seen_fk_cols.add(pk_col)

        # 2. Parse field descriptions for "FK to <table>" patterns
        fields = (contract_raw.get("model") or {}).get("fields") or []
        for field in fields:
            fname = field.get("name", "")
            if fname in seen_fk_cols:
                continue
            desc = (field.get("description") or "").lower()
            fk_match = re.search(r"fk\s+to\s+(\w+)", desc, re.IGNORECASE)
            if fk_match:
                ref_table = fk_match.group(1).lower()
                for ename in all_entity_names:
                    if ename == entity_name:
                        continue
                    if ref_table == ename or ename.endswith(f"_{ref_table}") or ref_table.endswith(f"_{ename}"):
                        relationships.append(
                            {
                                "fk_column": fname,
                                "ref_entity": ename,
                                "ref_column": fname,  # assume same column name
                            }
                        )
                        seen_fk_cols.add(fname)
                        break

        # 3. Shared ID column names — if a column ending in _id appears in
        #    both this contract and is a PK of another entity
        for field in fields:
            fname = field.get("name", "")
            if fname in seen_fk_cols:
                continue
            if not fname.endswith("_id"):
                continue
            # Derive the likely parent entity from the column name
            # e.g. customer_id → "customer" or "customers"
            entity_stem = fname.rsplit("_id", 1)[0]
            for ename in all_entity_names:
                if ename == entity_name:
                    continue
                ref_pks = all_pk_columns.get(ename, [])
                # Match: "customer" in "customers" or "customer" in "bronze_customers"
                ename_lower = ename.lower()
                if (
                    entity_stem == ename_lower
                    or entity_stem + "s" == ename_lower
                    or ename_lower.endswith(f"_{entity_stem}")
                    or ename_lower.endswith(f"_{entity_stem}s")
                ):
                    if fname in ref_pks:
                        relationships.append(
                            {
                                "fk_column": fname,
                                "ref_entity": ename,
                                "ref_column": fname,
                            }
                        )
                        seen_fk_cols.add(fname)
                        break

        return relationships

    @staticmethod
    def generate_related(
        contracts: Dict[str, Any],
        rows: Any = 100,
        invalid_ratio: float = 0.0,
        seed: Optional[int] = None,
        use_faker: bool = True,
        output_format: str = "polars",
        relationships: Optional[List[Dict[str, str]]] = None,
        ai: bool = False,
        ai_provider: Optional[str] = None,
        ai_model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate referentially consistent data for multiple related contracts.

        Automatically detects FK/PK relationships between contracts and
        generates parent tables first, then passes parent PK values into
        child tables so FK columns contain valid references.

        Parameters
        ----------
        contracts : dict
            Maps logical entity name → contract schema source.  Each value
            can be any format accepted by ``DataGenerator()``:

            - **Contract YAML path** — ``"contracts/orders_v1.0.yaml"``
            - **DDL string** — ``"order_id BIGINT, customer_id STRING, amount DOUBLE"``
            - **List of tuples** — ``[("order_id", "integer"), ("customer_id", "string")]``
            - **Dict** — ``{"order_id": "integer", "customer_id": "string"}``
            - **Spark StructType** — schema object (no data needed)

            Example (contract paths)::

                {
                    "customers": "contracts/bronze_customers_v1.0.yaml",
                    "orders": "contracts/bronze_orders_v1.0.yaml",
                }

            Example (DDL strings — no YAML files needed)::

                {
                    "customers": "customer_id BIGINT, name STRING, email STRING",
                    "orders": "order_id BIGINT, customer_id BIGINT, amount DOUBLE",
                }

            Example (tuple lists)::

                {
                    "customers": [("customer_id", "integer"), ("name", "string")],
                    "orders": [("order_id", "integer"), ("customer_id", "integer"), ("amount", "double")],
                }

        rows : int or dict
            Number of rows per entity.  If an ``int``, all entities get the
            same count.  If a ``dict``, maps entity name → row count::

                {"customers": 50, "orders": 200}

        invalid_ratio : float
            Fraction of invalid rows (0.0–1.0) injected into each entity.
        seed : int, optional
            Random seed for reproducibility.
        use_faker : bool
            Use Faker for semantic generation (default True).
        output_format : str
            ``"polars"`` (default) or ``"pandas"``.
        relationships : list of dict, optional
            Explicit FK→PK mappings for cases where column names differ
            between parent and child.  Each dict must have::

                {
                    "child": "<child_entity_name>",
                    "child_column": "<FK column in child>",
                    "parent": "<parent_entity_name>",
                    "parent_column": "<PK column in parent>",
                }

            These are merged with auto-detected relationships (explicit
            declarations take priority).
        ai : bool
            Enable AI-generated realistic values.
        ai_provider / ai_model : str, optional
            LLM provider and model overrides.

        Returns
        -------
        dict[str, DataFrame]
            Maps entity name → generated DataFrame with referential integrity.

        Examples
        --------
        From contract YAML files::

            related = DataGenerator.generate_related(
                contracts={
                    "customers": "contracts/bronze_customers_v1.0.yaml",
                    "orders": "contracts/bronze_orders_v1.0.yaml",
                },
                rows={"customers": 50, "orders": 200},
                invalid_ratio=0.05,
                seed=42,
            )

        From DDL strings (zero files required)::

            related = DataGenerator.generate_related(
                contracts={
                    "customers": "customer_id BIGINT, name STRING, email STRING",
                    "orders": "order_id BIGINT, customer_id BIGINT, amount DOUBLE, created_at TIMESTAMP",
                },
                rows={"customers": 20, "orders": 100},
            )

        From tuple lists::

            related = DataGenerator.generate_related(
                contracts={
                    "patients": [("patient_id", "string"), ("ssn", "string"), ("name", "string")],
                    "admissions": [("admission_id", "integer"), ("patient_id", "string"), ("diagnosis_code", "string")],
                },
                rows={"patients": 50, "admissions": 200},
            )

        From a Unity Catalog table (via infer_contract)::

            from lakelogic import infer_contract

            # Infer contracts from live catalog tables, save, then generate
            cust = infer_contract("my_catalog.sales.customers")
            cust.save("contracts/customers.yaml")
            ords = infer_contract("my_catalog.sales.orders")
            ords.save("contracts/orders.yaml")

            related = DataGenerator.generate_related(
                contracts={
                    "customers": "contracts/customers.yaml",
                    "orders": "contracts/orders.yaml",
                },
                rows={"customers": 50, "orders": 200},
            )

        With explicit relationships (when column names differ)::

            # Parent has PK "id", child has FK "cust_id"
            related = DataGenerator.generate_related(
                contracts={
                    "customers": "id BIGINT, name STRING, email STRING",
                    "products": "id BIGINT, product_name STRING, price DOUBLE",
                    "sales": "sale_id BIGINT, cust_id BIGINT, prod_id BIGINT, amount DOUBLE",
                },
                rows={"customers": 50, "products": 30, "sales": 500},
                relationships=[
                    {"child": "sales", "child_column": "cust_id", "parent": "customers", "parent_column": "id"},
                    {"child": "sales", "child_column": "prod_id", "parent": "products", "parent_column": "id"},
                ],
            )
            # Every sales["cust_id"] exists in customers["id"]
            # Every sales["prod_id"] exists in products["id"]
        """
        from loguru import logger as _rel_logger

        # ── 1. Load all contracts and extract metadata ────────────────────
        generators: Dict[str, "DataGenerator"] = {}
        raw_contracts: Dict[str, Dict[str, Any]] = {}
        pk_columns: Dict[str, List[str]] = {}

        for name, schema_source in contracts.items():
            gen = DataGenerator(schema_source, seed=seed, use_faker=use_faker)
            generators[name] = gen
            raw = gen._contract_raw
            raw_contracts[name] = raw

            # Extract primary key columns
            pks: List[str] = []
            # From top-level primary_key list
            if raw.get("primary_key"):
                pks = list(raw["primary_key"])
            # From field-level primary_key flags
            if not pks:
                for f in (raw.get("model") or {}).get("fields") or []:
                    if f.get("primary_key"):
                        pks.append(f["name"])
            pk_columns[name] = pks

        entity_names = list(contracts.keys())

        # ── 2. Detect FK relationships ────────────────────────────────────
        # relationships_map: child_entity → [{"fk_column", "ref_entity", "ref_column"}]
        all_relationships: Dict[str, List[Dict[str, str]]] = {}

        # 2a. Apply explicit relationships first (highest priority)
        if relationships:
            for rel in relationships:
                child = rel["child"]
                entry = {
                    "fk_column": rel["child_column"],
                    "ref_entity": rel["parent"],
                    "ref_column": rel["parent_column"],
                }
                all_relationships.setdefault(child, []).append(entry)
                _rel_logger.info(
                    f"🔗 Explicit FK: {child}.{rel['child_column']} → {rel['parent']}.{rel['parent_column']}"
                )

        # 2b. Auto-detect remaining relationships (skips already-declared FK columns)
        explicit_fk_cols: Dict[str, set] = {}
        for child, rels in all_relationships.items():
            explicit_fk_cols[child] = {r["fk_column"] for r in rels}

        for name in entity_names:
            rels = DataGenerator._detect_fk_relationships(
                name,
                raw_contracts[name],
                entity_names,
                pk_columns,
            )
            # Only add auto-detected relationships for columns not already declared
            existing = explicit_fk_cols.get(name, set())
            for r in rels:
                if r["fk_column"] not in existing:
                    all_relationships.setdefault(name, []).append(r)
                    _rel_logger.info(f"🔗 Detected FK: {name}.{r['fk_column']} → {r['ref_entity']}.{r['ref_column']}")

        # ── 3. Topological sort — parents before children ─────────────────
        # Build dependency graph: child → set of parent entities
        deps: Dict[str, set] = {name: set() for name in entity_names}
        for child, rels in all_relationships.items():
            for r in rels:
                deps[child].add(r["ref_entity"])

        sorted_entities: List[str] = []
        remaining = set(entity_names)
        while remaining:
            # Find entities with no unresolved deps
            ready = [e for e in remaining if not (deps[e] - set(sorted_entities))]
            if not ready:
                # Circular dependency — just add remaining in original order
                _rel_logger.warning(
                    f"Circular FK dependencies detected among {remaining}. Generating in original order."
                )
                sorted_entities.extend(e for e in entity_names if e in remaining)
                break
            sorted_entities.extend(sorted(ready))
            remaining -= set(ready)

        _rel_logger.info(f"📋 Generation order: {' → '.join(sorted_entities)}")

        # ── 4. Generate in dependency order ───────────────────────────────
        results: Dict[str, Any] = {}
        row_counts: Dict[str, int] = rows if isinstance(rows, dict) else {name: rows for name in entity_names}

        for name in sorted_entities:
            gen = generators[name]
            n_rows = row_counts.get(name, 100)

            # Build reference_data from parent PKs
            reference_data: Dict[str, List[Any]] = {}
            for r in all_relationships.get(name, []):
                parent_name = r["ref_entity"]
                parent_col = r["ref_column"]
                if parent_name in results:
                    parent_df = results[parent_name]
                    if hasattr(parent_df, "get_column"):  # polars
                        pk_values = parent_df.get_column(parent_col).drop_nulls().to_list()
                    elif hasattr(parent_df, "__getitem__"):  # pandas
                        pk_values = parent_df[parent_col].dropna().tolist()
                    else:
                        pk_values = []

                    if pk_values:
                        reference_data[r["fk_column"]] = pk_values
                        _rel_logger.info(
                            f"   {name}.{r['fk_column']} ← "
                            f"{len(pk_values)} unique values from "
                            f"{parent_name}.{parent_col}"
                        )

            # Generate with FK pools
            df = gen.generate(
                rows=n_rows,
                invalid_ratio=invalid_ratio,
                output_format=output_format,
                reference_data=reference_data or None,
                ai=ai,
                ai_provider=ai_provider,
                ai_model=ai_model,
            )

            # Intentionally introduce margin of error (orphan FKs) for invalid_ratio > 0
            if invalid_ratio > 0 and reference_data:
                if output_format == "polars":
                    import polars as pl

                    for fk_col in reference_data.keys():
                        if fk_col not in df.columns:
                            continue
                        if df.schema[fk_col] in (pl.Utf8, pl.Categorical):
                            expr = pl.concat_str([pl.col(fk_col), pl.lit("_ORPHAN")])
                        else:
                            expr = pl.col(fk_col).cast(pl.Int64) + 999000

                        df = df.with_columns(
                            pl.when(pl.col("_is_invalid") == True).then(expr).otherwise(pl.col(fk_col)).alias(fk_col)
                        )
                else:  # pandas
                    import pandas as pd

                    for fk_col in reference_data.keys():
                        if fk_col not in df.columns:
                            continue
                        mask = df["_is_invalid"] == True
                        if pd.api.types.is_string_dtype(df[fk_col]):
                            df.loc[mask, fk_col] = df.loc[mask, fk_col].astype(str) + "_ORPHAN"
                        elif pd.api.types.is_numeric_dtype(df[fk_col]):
                            df.loc[mask, fk_col] = df.loc[mask, fk_col] + 999000

            results[name] = df

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_sample_pools(
        self,
        source,
        columns: Optional[List[str]] = None,
    ) -> Dict[str, List[Any]]:
        """
        Read *source* and return a dict mapping column name → list of unique values.

        *source* may be a file path (str / Path) or an existing polars / pandas
        DataFrame.  File format is inferred from the extension.

        For JSON and NDJSON sources the file is parsed twice:
        - once via Polars (for column-list discovery against the contract schema)
        - once via stdlib ``json`` (for the raw pool values)

        The second read is critical: Polars converts nested JSON objects (structs,
        lists) to its own display strings like ``{["val"]}`` when it stores them
        as ``String`` columns.  Reading with ``json.load`` returns proper Python
        dicts/lists, so generated JSON output keeps the original nested structure.
        """
        import polars as pl
        import json as _json

        raw_rows: Optional[List[Dict[str, Any]]] = None  # native Python rows

        # ── Load into a Polars DataFrame for schema/column detection ──────────
        if isinstance(source, pl.DataFrame):
            df = source
        elif hasattr(source, "to_dict"):  # pandas DataFrame
            df = pl.from_pandas(source)
        else:
            p = Path(source)
            ext = p.suffix.lower()
            if ext == ".csv":
                df = pl.read_csv(p, infer_schema_length=10_000)
            elif ext == ".parquet":
                df = pl.read_parquet(p)
            elif ext == ".json":
                # ── raw read preserves nested dicts/lists ─────────────────
                text = p.read_text(encoding="utf-8")
                raw = _json.loads(text)
                raw_rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                # Polars for column names only
                try:
                    df = pl.read_json(p)
                except Exception:
                    df = pl.from_dicts(raw_rows) if raw_rows else pl.DataFrame()
            elif ext in (".ndjson", ".jsonl"):
                # ── raw read preserves nested dicts/lists ─────────────────
                raw_rows = [_json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
                try:
                    df = pl.read_ndjson(p)
                except Exception:
                    df = pl.from_dicts(raw_rows) if raw_rows else pl.DataFrame()
            elif ext in (".xlsx", ".xls"):
                try:
                    import pandas as pd

                    df = pl.from_pandas(pd.read_excel(p))
                except ImportError as exc:
                    raise ImportError(
                        "Excel support requires pandas and openpyxl: pip install pandas openpyxl"
                    ) from exc
            else:
                raise ValueError(
                    f"Cannot infer file format from extension {ext!r}. "
                    "Supported: .csv, .parquet, .json, .ndjson, .jsonl, .xlsx, .xls"
                )

        # ── Identify which columns to pool ────────────────────────────────────
        schema_cols = {f["name"] for f in self._fields if "name" in f}
        file_cols = set(df.columns)
        target_cols = schema_cols & file_cols
        if columns:
            target_cols = target_cols & set(columns)

        # ── Resolve row source ────────────────────────────────────────────────
        # For JSON/NDJSON: use raw_rows (native Python dicts, nested intact).
        # For everything else: df.to_dicts() is fine since CSV/Parquet/Excel
        # don't have nested JSON objects that Polars would stringify.
        all_rows: List[Dict[str, Any]] = raw_rows if raw_rows is not None else df.to_dicts()

        # ── Build pools: unique non-null values per column ────────────────────
        pools: Dict[str, List[Any]] = {}
        for col in target_cols:
            seen: dict = {}  # json-key → original value (preserves nested types)
            for row in all_rows:
                val = row.get(col)
                if val is None:
                    continue
                try:
                    key = _json.dumps(val, sort_keys=True, default=str)
                except Exception:
                    key = str(val)
                if key not in seen:
                    seen[key] = val
            if seen:
                pools[col] = list(seen.values())

        return pools

    def _load_yaml(self) -> Dict[str, Any]:
        with open(self.contract_path, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _extract_fields(self) -> List[Dict[str, Any]]:
        model = self._contract_raw.get("model") or {}
        return model.get("fields") or []

    def _extract_unique_integer_fields(self) -> set:
        """
        Return the set of field names that are:
          - integer type in the contract schema, AND
          - listed as a uniqueness rule in quality.dataset_rules

        These fields need special treatment in generate_from_sample when the
        seed source is a single record — a pool of one value would otherwise
        cause every generated row to carry the same ID.
        """
        integer_types = {"integer", "int", "int32", "int64", "long"}
        integer_fields = {
            f["name"] for f in self._fields if (f.get("type") or "").lower() in integer_types and "name" in f
        }

        quality = self._contract_raw.get("quality") or {}
        unique_rule_fields = {
            rule.get("sql", "").strip()  # dataset_rules store the column name in sql
            for rule in (quality.get("dataset_rules") or [])
            if rule.get("category") == "uniqueness"
        }

        return integer_fields & unique_rule_fields

    def _detect_triplets(self) -> List[Dict[str, Any]]:
        fields = [f.get("name") for f in self._fields if f.get("name")]
        detected = []

        START_SUFFIXES = ["_start", "_start_time", "_start_at", "_begin", "_opened_at"]
        END_SUFFIXES = ["_end", "_end_time", "_end_at", "_close", "_closed_at"]
        DUR_SUFFIXES = ["_duration", "_duration_seconds", "_duration_minutes", "_duration_ms", "_length", "_elapsed"]

        for field in fields:
            for suf in START_SUFFIXES:
                if field.endswith(suf):
                    prefix = field[: -len(suf)]
                    end_field = next((f for f in fields for es in END_SUFFIXES if f == prefix + es), None)
                    dur_field = next(
                        (
                            f
                            for f in fields
                            for ds in DUR_SUFFIXES
                            if f.startswith(prefix) and f.endswith(ds.split("_")[-1])
                        ),
                        None,
                    )

                    if end_field and dur_field:
                        # Match to a config
                        cfg = next(
                            (v for k, v in _TEMPORAL_TRIPLETS.items() if prefix.startswith(k) or k.startswith(prefix)),
                            None,
                        )
                        if not cfg:
                            # Fallback auto-config
                            cfg = {
                                "start": field,
                                "end": end_field,
                                "duration": dur_field,
                                "unit": "seconds",
                                "min_duration": 1,
                                "max_duration": 86400,
                            }
                        else:
                            # Use exact field names from schema, not just the config defaults
                            cfg = dict(cfg)
                            cfg["start"] = field
                            cfg["end"] = end_field
                            cfg["duration"] = dur_field
                        detected.append(cfg)
        return detected

    def _generate_temporal_triplet(self, cfg: dict, is_valid: bool) -> dict:
        now = datetime.now()

        # ── Window-constrained start generation ──────────────────────────
        if getattr(self, "_window_start", None) is not None:
            ws = self._window_start
            we = self._window_end
            delta_secs = max(int((we - ws).total_seconds()), 1)
        else:
            ws = now - timedelta(days=730)  # 2 years
            delta_secs = int((now - ws).total_seconds())

        if is_valid:
            # Valid generation
            start_offset = self._rng.randint(0, delta_secs)
            start = ws + timedelta(seconds=start_offset)

            if "allowed_durations" in cfg:
                duration = self._rng.choice(cfg["allowed_durations"])
            else:
                duration = self._rng.randint(cfg.get("min_duration", 1), cfg.get("max_duration", 86400))

            unit = cfg.get("unit", "seconds")
            if unit == "seconds":
                end = start + timedelta(seconds=duration)
            elif unit == "minutes":
                end = start + timedelta(minutes=duration)
            elif unit == "milliseconds":
                end = start + timedelta(milliseconds=duration)
            elif unit == "months":
                end = start + timedelta(days=duration * 30)
            else:
                end = start + timedelta(seconds=duration)

            if cfg.get("nullable_end") and self._rng.random() < 0.15:
                end = None
                duration = None

            return {
                cfg["start"]: start.isoformat(),
                cfg["end"]: end.isoformat() if end else None,
                cfg["duration"]: duration,
            }
        else:
            # Invalid generation
            pattern = self._rng.choice(list(_TRIPLET_INVALID_PATTERNS.keys()))
            start_offset = self._rng.randint(0, delta_secs)
            start = ws + timedelta(seconds=start_offset)
            end = None
            duration = None

            if pattern == "end_before_start":
                end = start - timedelta(seconds=self._rng.randint(1, 86400))
            elif pattern == "duration_mismatch":
                end = start + timedelta(seconds=self._rng.randint(100, 3600))
                duration = self._rng.randint(1, 100000)
            elif pattern == "null_duration":
                end = start + timedelta(seconds=self._rng.randint(1, 3600))
            elif pattern == "null_end_with_duration":
                duration = self._rng.randint(1, 3600)
            elif pattern == "zero_duration":
                end = start
                duration = 0
            elif pattern == "impossibly_long":
                duration = cfg.get("max_duration", 86400) * self._rng.randint(10, 100)
                end = start + timedelta(seconds=duration)
            elif pattern == "future_end":
                end = now + timedelta(days=self._rng.randint(1, 365))
                duration = int((end - start).total_seconds())
            elif pattern == "microsecond_precision_mismatch":
                start_iso = start.strftime("%Y-%m-%dT%H:%M:%S.%f")
                end = start + timedelta(seconds=self._rng.randint(100, 3600))
                end_iso = end.strftime("%Y-%m-%dT%H:%M:%S")
                duration = int((end - start).total_seconds())
                return {cfg["start"]: start_iso, cfg["end"]: end_iso, cfg["duration"]: duration}

            return {
                cfg["start"]: start.isoformat(),
                cfg["end"]: end.isoformat() if end else None,
                cfg["duration"]: duration,
            }

    def _make_row(
        self,
        invalid: bool,
        fk_pools: Optional[Dict[str, List[Any]]] = None,
        sample_pools: Optional[Dict[str, List[Any]]] = None,
        edge_case_pools: Optional[Dict[str, List[Any]]] = None,
    ) -> Tuple[Dict[str, Any], List[TestCaseInfo]]:
        row: Dict[str, Any] = {}
        test_cases: List[TestCaseInfo] = []
        field_rules = self._build_field_rules(fk_pools=fk_pools)

        # ── Pre-populate temporal triplets ──────────────────────────────────
        # If schema has start/end/duration, generate them holistically.
        if self._triplets:
            for triplet_cfg in self._triplets:
                t_data = self._generate_temporal_triplet(triplet_cfg, not invalid)
                row.update(t_data)

        for field in self._fields:
            name: str = field.get("name", "col")
            if name in row:
                continue  # Skip if already generated (e.g., by triplet logic)

            ftype: str = (field.get("type") or "string").lower()
            required: bool = field.get("required", False)
            nullable: bool = not required

            rules = field_rules.get(name, {})

            if invalid and self._rng.random() < 0.4:
                # 40% chance each field is broken in an invalid row
                val, tc_info = self._make_invalid_value(
                    name,
                    ftype,
                    rules,
                    nullable,
                    edge_case_pools=edge_case_pools,
                )
                row[name] = val
                test_cases.append(tc_info)
            else:
                row[name] = self._make_valid_value(name, ftype, rules, nullable, sample_pools=sample_pools)

        # ── Correlated field override ──────────────────────────────────────
        # After all fields are generated, patch dependent fields whose value
        # should match the driver field. Only for valid rows — invalid rows
        # keep their deliberately broken values.
        if not invalid:
            self._apply_correlations(row)
            self._apply_temporal_ordering(row)
            self._apply_field_consistency(row)
            self._apply_geo_alignment(row)

        # Flag the row for frontend debugging / filtering
        row["_is_invalid"] = invalid
        if test_cases:
            # Add primary test case type to the row for filtering
            row["_test_case_types"] = ",".join(sorted({tc.type for tc in test_cases}))

        return row, test_cases

    # ------------------------------------------------------------------
    # Temporal-aware date/timestamp generation
    # ------------------------------------------------------------------

    def _generate_date(self, name: str) -> str:
        """Generate a date string, respecting age constraints, weekday bias, and time window."""
        name_lower = name.lower()

        # Age constraints (date_of_birth, birth_date, etc.)
        age_rule = _AGE_CONSTRAINTS.get(name_lower)
        if age_rule:
            today = date.today()
            max_birth = today - timedelta(days=age_rule["min_age_years"] * 365)
            min_birth = today - timedelta(days=age_rule["max_age_years"] * 365)
            days_range = (max_birth - min_birth).days
            if days_range <= 0:
                days_range = 1
            return (min_birth + timedelta(days=self._rng.randint(0, days_range))).isoformat()

        # ── Window-constrained generation ───────────────────────────────────
        if getattr(self, "_window_start", None) is not None:
            ws = self._window_start.date()
            we = self._window_end.date()
            days_range = max((we - ws).days, 0)
            d = ws + timedelta(days=self._rng.randint(0, days_range))
        else:
            today = date.today()
            base = today - timedelta(days=90)
            d = base + timedelta(days=self._rng.randint(0, 90))

        # Weekday-only bias: if the field shouldn't fall on weekends, nudge
        if name_lower in _WEEKDAY_ONLY_FIELDS and d.weekday() >= 5:
            # Move to previous Friday
            d = d - timedelta(days=d.weekday() - 4)

        return d.isoformat()

    def _generate_timestamp(self, name: str) -> str:
        """Generate a timestamp string with business-hours, weekday awareness, and time window."""
        name_lower = name.lower()

        # ── Window-constrained generation ───────────────────────────────────
        if getattr(self, "_window_start", None) is not None:
            ws = self._window_start
            we = self._window_end
            delta_secs = max(int((we - ws).total_seconds()), 1)
            dt = ws + timedelta(seconds=self._rng.randint(0, delta_secs))
        else:
            now = datetime.now()
            base = now - timedelta(days=90)
            dt = base + timedelta(seconds=self._rng.randint(0, 60 * 60 * 24 * 90))

        # Weekday-only bias
        if name_lower in _WEEKDAY_ONLY_FIELDS and dt.weekday() >= 5:
            dt = dt - timedelta(days=dt.weekday() - 4)

        # Business-hours awareness
        is_biz = _BUSINESS_HOURS_FIELDS.get(name_lower)
        if is_biz is None:
            # Check suffix match
            parts = name_lower.split("_")
            for i in range(len(parts) - 1, -1, -1):
                suffix = "_".join(parts[i:])
                if suffix in _BUSINESS_HOURS_FIELDS:
                    is_biz = _BUSINESS_HOURS_FIELDS[suffix]
                    break

        if is_biz:
            # Cluster time between 08:00 and 18:00
            biz_hour = self._rng.randint(8, 17)
            biz_minute = self._rng.randint(0, 59)
            biz_second = self._rng.randint(0, 59)
            dt = dt.replace(hour=biz_hour, minute=biz_minute, second=biz_second)

        return dt.isoformat()

    def _apply_temporal_ordering(self, row: Dict[str, Any]) -> None:
        """Enforce temporal ordering rules between date/timestamp fields.

        After all fields are independently generated, this pass ensures
        that e.g. ``created_at <= updated_at``.  When a violation is found,
        the *later* field is adjusted to be the earlier field + a realistic
        gap (from ``_TEMPORAL_GAPS``, or a random 0-7 day offset).
        """
        row_keys = set(row.keys())

        for earlier_field, successors in _TEMPORAL_ORDER_INDEX.items():
            if earlier_field not in row_keys:
                continue
            earlier_val = row[earlier_field]
            if earlier_val is None:
                continue

            # Parse the earlier value to a datetime
            earlier_dt = self._parse_temporal(earlier_val)
            if earlier_dt is None:
                continue

            for later_field, _rel in successors:
                if later_field not in row_keys:
                    continue
                later_val = row[later_field]
                if later_val is None:
                    continue

                later_dt = self._parse_temporal(later_val)
                if later_dt is None:
                    continue

                # Check if ordering is violated
                if later_dt >= earlier_dt:
                    continue  # already valid

                # Fix: set later = earlier + realistic gap
                gap_spec = _TEMPORAL_GAPS.get((earlier_field, later_field))
                if gap_spec:
                    min_mins, max_mins = gap_spec
                else:
                    min_mins, max_mins = 0, 10080  # default: 0 to 7 days

                gap_minutes = self._rng.randint(min_mins, max_mins)
                new_later_dt = earlier_dt + timedelta(minutes=gap_minutes)

                # Preserve the original format (date-only vs full timestamp)
                if isinstance(earlier_val, str) and len(earlier_val) == 10:
                    # Date-only format (YYYY-MM-DD)
                    row[later_field] = new_later_dt.date().isoformat()
                else:
                    row[later_field] = new_later_dt.isoformat()

    @staticmethod
    def _parse_temporal(val: Any) -> Optional[datetime]:
        """Parse a date or timestamp value to a datetime, or None."""
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime(val.year, val.month, val.day)
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val)
            except (ValueError, TypeError):
                return None
        return None

    def _apply_field_consistency(self, row: Dict[str, Any]) -> None:
        """Enforce cross-field consistency rules."""
        row_keys = set(row.keys())

        # ── 1. Conditional + derived rules ─────────────────────────────────
        for rule_key, rule in _FIELD_CONSISTENCY_RULES.items():
            # Determine the actual target field (may differ from rule_key)
            target = rule.get("target_field", rule_key)
            if target not in row_keys:
                continue

            # ── Correlated Format Lookup ───────────────────────────────────
            if "format_lookup" in rule and "correlates_with" in rule:
                driver = rule["correlates_with"]
                if driver in row_keys and row[driver]:
                    lookup_name = rule["format_lookup"]
                    lookup_dict = globals().get(lookup_name)
                    if lookup_dict:
                        driver_val = str(row[driver])
                        if driver_val in lookup_dict and target in lookup_dict[driver_val]:
                            fmt = lookup_dict[driver_val][target]
                            if "?" in fmt or "#" in fmt:
                                row[target] = self._call_faker("bothify", text=fmt)
                            else:
                                row[target] = fmt
                continue

            # ── Derived formula ────────────────────────────────────────────
            derived_from = rule.get("derived_from")
            if derived_from and rule.get("formula"):
                if all(f in row_keys and row[f] is not None for f in derived_from):
                    try:
                        # Build a safe namespace with only the source fields
                        ns = {f: float(row[f]) for f in derived_from}
                        result = eval(rule["formula"], {"__builtins__": {}}, ns)  # noqa: S307
                        row[target] = round(result, 2)
                    except (ValueError, TypeError, ZeroDivisionError):
                        pass  # leave the independently-generated value
                continue

            # ── Conditional population/nulling ─────────────────────────────
            cond_field = rule.get("condition_field")
            if not cond_field or cond_field not in row_keys:
                continue

            cond_val = row[cond_field]
            behaviour = rule.get("behaviour")
            matched = False

            # condition_value: exact match (string or list)
            cv = rule.get("condition_value")
            if cv is not None:
                if isinstance(cv, list):
                    matched = cond_val in cv
                else:
                    matched = cond_val == cv

            # condition_not_value: inverse match
            cnv = rule.get("condition_not_value")
            if cnv is not None:
                if isinstance(cnv, list):
                    matched = cond_val not in cnv
                else:
                    matched = cond_val != cnv

            # condition_gt: numeric comparison
            cgt = rule.get("condition_gt")
            if cgt is not None:
                try:
                    matched = float(cond_val) > float(cgt) if cond_val is not None else False
                except (ValueError, TypeError):
                    matched = False

            if matched and behaviour == "must_be_populated":
                if row[target] is None:
                    # Generate a non-null value for this field
                    for field in self._fields:
                        if field.get("name") == target:
                            ftype = (field.get("type") or "string").lower()
                            row[target] = self._make_valid_value(
                                target,
                                ftype,
                                {},
                                nullable=False,
                            )
                            break
            elif matched and behaviour == "must_be_null":
                row[target] = None
            elif matched and behaviour == "set_to_zero":
                row[target] = 0

        # ── 2. Numeric ordering constraints ────────────────────────────────
        for (smaller, larger), rel in _NUMERIC_CONSISTENCY.items():
            if smaller not in row_keys or larger not in row_keys:
                continue
            s_val, l_val = row[smaller], row[larger]
            if s_val is None or l_val is None:
                continue
            try:
                s_num, l_num = float(s_val), float(l_val)
            except (ValueError, TypeError):
                continue
            if rel == "lte" and s_num > l_num:
                # Clamp the smaller field to be <= the larger field
                row[smaller] = round(self._rng.uniform(0, l_num), 2)

    def _apply_correlations(self, row: Dict[str, Any]) -> None:
        """Override dependent fields based on driver field values.

        Mutates *row* in place.  Only applies when:
        - Both driver and dependent fields exist in the row.
        - The driver value has a correlation entry.
        - The dependent field was not null (preserve null injection).
        """
        row_keys = set(row.keys())
        for dep_field, correlations in _CORRELATED_INDEX.items():
            if dep_field not in row_keys or row[dep_field] is None:
                continue
            for driver_field, mapping in correlations:
                if driver_field not in row_keys:
                    continue
                driver_val = row[driver_field]
                if driver_val is None or str(driver_val) not in mapping:
                    continue
                template = mapping[str(driver_val)]
                if isinstance(template, list):
                    row[dep_field] = self._rng.choice(template)
                elif isinstance(template, str) and ("#" in template or "?" in template):
                    row[dep_field] = self._expand_template(template)
                else:
                    row[dep_field] = template
                break  # first matching correlation wins

    def _detect_geo_alignment(self) -> List[Dict[str, str]]:
        """Detect lat/lng + city field groups that should be geo-aligned.

        Scans the schema fields and pairs up latitude/longitude fields with
        a city field that shares a common prefix (e.g. ``pickup_lat``,
        ``pickup_lng`` → ``city_code``) or falls back to the first city field
        found.

        Returns a list of dicts, each with keys: ``lat_field``, ``lng_field``,
        ``city_field``.
        """
        field_names = [f.get("name", "").lower() for f in self._fields]
        field_name_set = set(field_names)

        # Collect all lat, lng, and city fields
        lat_fields = [n for n in field_names if n in _LAT_FIELD_PATTERNS]
        lng_fields = [n for n in field_names if n in _LNG_FIELD_PATTERNS]
        city_fields = [n for n in field_names if n in _CITY_FIELD_PATTERNS]

        if not lat_fields or not lng_fields or not city_fields:
            return []

        alignments: List[Dict[str, str]] = []
        used_lngs: set = set()

        for lat_f in lat_fields:
            # Try to find a matching lng by prefix (pickup_lat → pickup_lng)
            prefix = lat_f.rsplit("lat", 1)[0]  # e.g. "gps_", "pickup_", ""
            matched_lng = None
            for lng_f in lng_fields:
                if lng_f in used_lngs:
                    continue
                lng_prefix = lng_f.rsplit("lng", 1)[0] if "lng" in lng_f else lng_f.rsplit("lon", 1)[0]
                if lng_prefix == prefix:
                    matched_lng = lng_f
                    break

            if not matched_lng:
                # Fallback: first unused lng field
                for lng_f in lng_fields:
                    if lng_f not in used_lngs:
                        matched_lng = lng_f
                        break

            if matched_lng:
                used_lngs.add(matched_lng)
                # Use first available city field (city_code, city, etc.)
                alignments.append(
                    {
                        "lat_field": lat_f,
                        "lng_field": matched_lng,
                        "city_field": city_fields[0],
                    }
                )

        if alignments:
            try:
                from loguru import logger as _logger

                _logger.debug(
                    "\U0001f30d Geo-alignment detected: "
                    + ", ".join(f"{a['lat_field']}/{a['lng_field']} ← {a['city_field']}" for a in alignments)
                )
            except ImportError:
                pass

        return alignments

    def _apply_geo_alignment(self, row: Dict[str, Any]) -> None:
        """Snap lat/lng fields to match the city field's coordinates.

        When a row contains both a city field and lat/lng fields, this method
        looks up the city in ``_CITY_GEO_COORDS`` and overrides the lat/lng
        with coordinates near the city center (±0.05° jitter for realism).

        Only applies to valid rows. Mutates *row* in place.
        """
        if not self._geo_alignments:
            return

        for alignment in self._geo_alignments:
            city_field = alignment["city_field"]
            lat_field = alignment["lat_field"]
            lng_field = alignment["lng_field"]

            city_val = row.get(city_field)
            if city_val is None:
                continue

            # Lookup city coordinates (case-insensitive)
            coords = _CITY_GEO_COORDS.get(str(city_val).lower().strip())
            if coords is None:
                continue

            center_lat, center_lng = coords
            # Add ±0.05° jitter (~5km) for realistic spread within the city
            jitter_lat = self._rng.uniform(-0.05, 0.05)
            jitter_lng = self._rng.uniform(-0.05, 0.05)

            # Determine output format: match existing value type
            existing_lat = row.get(lat_field)
            if isinstance(existing_lat, str):
                row[lat_field] = f"{center_lat + jitter_lat:.6f}"
                row[lng_field] = f"{center_lng + jitter_lng:.6f}"
            else:
                row[lat_field] = round(center_lat + jitter_lat, 6)
                row[lng_field] = round(center_lng + jitter_lng, 6)

    def _expand_template(self, template: str) -> str:
        """Expand a template string: ``#`` → random digit, ``?`` → random letter."""
        out: List[str] = []
        for ch in template:
            if ch == "#":
                out.append(str(self._rng.randint(0, 9)))
            elif ch == "?":
                out.append(self._rng.choice(string.ascii_uppercase))
            else:
                out.append(ch)
        return "".join(out)

    def _sample_distribution(self, profile: Dict[str, Any]) -> Any:
        """Sample a value from a distribution profile.

        Supported distributions:
        - ``weighted``:  weighted random choice from a dict of value→weight
        - ``lognormal``: right-skewed continuous (revenue, prices)
        - ``normal``:    Gaussian with optional min/max clamp
        - ``beta``:      0-1 range, shaped by alpha/beta params
        - ``bimodal``:   two peaks + flat background (NPS-like)
        """
        dist_type = profile.get("distribution")
        if not dist_type:
            return None

        if dist_type == "weighted":
            weights_map = profile.get("weights", {})
            if not weights_map:
                return None
            values = list(weights_map.keys())
            weights = list(weights_map.values())
            return self._rng.choices(values, weights=weights, k=1)[0]

        if dist_type == "lognormal":
            mu = profile.get("mean", 3.0)
            sigma = profile.get("std", 1.0)
            lo = profile.get("min", 0.01)
            hi = profile.get("max", 100000)
            val = self._rng.lognormvariate(mu, sigma)
            val = max(lo, min(hi, val))
            return round(val, 2)

        if dist_type == "normal":
            mu = profile.get("mean", 50)
            sigma = profile.get("std", 15)
            lo = profile.get("min", 0)
            hi = profile.get("max", 100)
            val = self._rng.gauss(mu, sigma)
            val = max(lo, min(hi, val))
            # Return int if min/max are ints (e.g. age)
            if isinstance(lo, int) and isinstance(hi, int):
                return int(round(val))
            return round(val, 2)

        if dist_type == "beta":
            alpha = profile.get("alpha", 2.0)
            beta_param = profile.get("beta", 5.0)
            val = self._rng.betavariate(alpha, beta_param)
            return round(val, 4)

        if dist_type == "bimodal":
            peak_1 = profile.get("peak_1", {})
            peak_2 = profile.get("peak_2", {})
            # flat_weight not currently used but kept in profile schema

            p1_val = peak_1.get("value", 8)
            p1_wt = peak_1.get("weight", 0.4)
            p2_val = peak_2.get("value", 3)
            p2_wt = peak_2.get("weight", 0.3)

            roll = self._rng.random()
            if roll < p1_wt:
                # Peak 1 cluster: value ± 1
                return p1_val + self._rng.choice([-1, 0, 0, 0, 1])
            elif roll < p1_wt + p2_wt:
                # Peak 2 cluster: value ± 1
                return p2_val + self._rng.choice([-1, 0, 0, 0, 1])
            else:
                # Flat: uniform across full range (0-10 for NPS)
                return self._rng.randint(0, 10)

        return None

    def _make_valid_value(
        self,
        name: str,
        ftype: str,
        rules: Dict[str, Any],
        nullable: bool,
        sample_pools: Optional[Dict[str, List[Any]]] = None,
    ) -> Any:
        # Null injection for nullable fields — use field-aware probability
        if nullable:
            null_prob = _match_null_probability(name.lower())
            if null_prob is None:
                null_prob = _DEFAULT_NULL_PROBABILITY
            if null_prob > 0 and self._rng.random() < null_prob:
                return None

        # File-seeded pool — highest priority after null injection.
        # Sampling real observed values keeps the generated dataset realistic
        # (e.g. real postcodes, real status codes, realistic price ranges).
        #
        # Exception: when the pool has exactly ONE value AND the field looks like
        # a primary-key identifier (integer type + name contains "id"), we spread
        # values around the seed instead of repeating it.
        #
        # Why name-based rather than quality-rule-based: with a single source
        # record, suggest_rules never emits a uniqueness rule (needs >1 row to
        # confirm all values are distinct), so `_unique_integer_fields` is always
        # empty when seeding from a single JSON file.
        #
        # Non-ID integers (bathrooms=3, total_bedrooms=4) correctly repeat.
        if sample_pools and name in sample_pools:
            pool = sample_pools[name]
            if pool:
                is_pk = rules.get("primary_key", False)
                is_int_type = ftype in ("integer", "int", "int32", "int64", "long")
                is_id_field = "id" in name.lower()

                # If it's a primary key, we must avoid duplicates and match formatting
                if is_pk or (is_id_field and is_int_type and len(pool) == 1):
                    if is_int_type:
                        # Spread integers widely to minimise collisions
                        seed_val = int(pool[0])
                        return self._rng.randint(seed_val, seed_val + 10_000_000)
                    else:
                        base_val = str(self._rng.choice(pool))
                        # Infer format: alphanumeric prefix + numeric suffix (e.g., CUST-1001, PRD_005)
                        if match := re.match(r"^(.*?)([\d]+)$", base_val):
                            prefix = match.group(1)
                            num_len = len(match.group(2))
                            new_num = "".join(self._rng.choices(string.digits, k=num_len))
                            return f"{prefix}{new_num}"
                        else:
                            # Fallback: append a random suffix to preserve uniqueness
                            suffix = "".join(self._rng.choices(string.ascii_uppercase + string.digits, k=6))
                            return f"{base_val}-{suffix}"

                # Not a primary key — safely replay observed values
                return self._rng.choice(pool)

        # accepted_values ALWAYS wins — even over Faker semantic hints.
        # This is critical: Faker's country() returns 'United Kingdom', not 'GB'.
        # When a dbt accepted_values test exists, the generator must pick from that list.
        accepted = rules.get("accepted_values")
        if accepted:
            # Check for distribution profile on this field — use weighted
            # sampling from accepted_values if a weight map exists.
            dist = _match_distribution(name.lower())
            if dist and dist.get("distribution") == "weighted":
                weights_map = dist.get("weights", {})
                # Only use if weights match the accepted values
                matched = [w for v in accepted if (w := weights_map.get(v)) is not None]
                if matched and len(matched) == len(accepted):
                    return self._rng.choices(accepted, weights=matched, k=1)[0]
            return self._rng.choice(accepted)

        # Distribution profile — when no accepted_values, use shaped sampling
        dist = _match_distribution(name.lower())
        if dist:
            result = self._sample_distribution(dist)
            if result is not None:
                return result

        min_val = rules.get("min")
        max_val = rules.get("max")

        if ftype in ("integer", "int", "int32", "int64", "long"):
            name_lower = name.lower()

            # ── Epoch timestamp detection for numeric fields ──────────────
            # Fields like event_timestamp (long) should produce realistic
            # Unix epoch values, not random 1..10000.
            is_epoch_ts = any(name_lower.endswith(s) for s in _TIMESTAMP_SUFFIXES) or any(
                kw in name_lower for kw in ("epoch", "unix_time")
            )
            if is_epoch_ts:
                import time as _time

                # ── Window-constrained epoch generation ────────────────
                if getattr(self, "_window_start", None) is not None:
                    base = int(self._window_start.timestamp())
                    now = int(self._window_end.timestamp())
                else:
                    now = int(_time.time())
                    base = now - 90 * 86400  # 90 days ago
                epoch_seconds = self._rng.randint(base, now)
                # Detect granularity from name or description
                if "_us" in name_lower or "micro" in name_lower:
                    return epoch_seconds * 1_000_000
                elif "_ms" in name_lower or "milli" in name_lower:
                    return epoch_seconds * 1_000
                else:
                    # Default: check if description mentions microseconds
                    desc = ""
                    for f in self._fields:
                        if f.get("name", "").lower() == name_lower:
                            desc = (f.get("description") or "").lower()
                            break
                    if "micro" in desc:
                        return epoch_seconds * 1_000_000
                    elif "milli" in desc:
                        return epoch_seconds * 1_000
                    return epoch_seconds

            lo = int(min_val) if min_val is not None else 1
            hi = int(max_val) if max_val is not None else 10_000
            # Single-point range from a single-record source (min == max):
            # spread ID fields so every generated row gets a distinct value.
            # Non-ID integers (bathrooms=3, total_bedrooms=4) stay constant — correct.
            if lo == hi and "id" in name_lower:
                hi = lo + 10_000

            # ── Duration fields (_days, _hours, _minutes, _weeks) ──────────
            # Prevent unreasonable defaults like 7372 days (20 years)
            if name_lower.endswith("_days") or "days" in name_lower:
                if min_val is None:
                    lo = 1
                if max_val is None:
                    hi = 180
            elif name_lower.endswith("_hours") or "hours" in name_lower:
                if min_val is None:
                    lo = 1
                if max_val is None:
                    hi = 720
            elif name_lower.endswith("_weeks") or "weeks" in name_lower:
                if min_val is None:
                    lo = 1
                if max_val is None:
                    hi = 52
            elif name_lower.endswith("_months") or "months" in name_lower:
                if min_val is None:
                    lo = 1
                if max_val is None:
                    hi = 60

            return self._rng.randint(lo, hi)

        if ftype in ("double", "float", "float32", "float64", "decimal", "number"):
            name_lower = name.lower()
            # ── Whole-number override for float-typed counting fields ──────
            # Fields like quantity_on_hand, reorder_point, stock_level are
            # often typed as float/double but represent discrete counts.
            _WHOLE_NUMBER_KEYWORDS = (
                "quantity",
                "qty",
                "count",
                "units",
                "stock_level",
                "reorder_point",
                "reorder_quantity",
                "headcount",
                "num_items",
                "number_of",
                "total_items",
                "preferred_stock",
                "backordered",
            )
            if any(kw in name_lower for kw in _WHOLE_NUMBER_KEYWORDS):
                lo = int(float(min_val)) if min_val is not None else 0
                hi = int(float(max_val)) if max_val is not None else 1000
                return self._rng.randint(lo, hi)

            lo = float(min_val) if min_val is not None else 0.01
            hi = float(max_val) if max_val is not None else 1_000.0
            return round(self._rng.uniform(lo, hi), 4)

        if ftype in ("boolean", "bool"):
            return self._rng.choice([True, False])

        if ftype in ("date",):
            return self._generate_date(name)

        if ftype in ("timestamp", "datetime"):
            return self._generate_timestamp(name)

        # String — try regex_match first if defined, otherwise semantic generation
        regex_pat = rules.get("regex_match")
        if regex_pat:
            val = self._generate_from_regex(regex_pat)
            if val:
                return val

        val = self._string_value(name)

        # Enforce min_length / max_length if defined in rules
        min_len = rules.get("min_length")
        max_len = rules.get("max_length")
        if max_len is not None and len(val) > max_len:
            val = val[:max_len]
        if min_len is not None and len(val) < min_len:
            # Pad with realistic filler to reach minimum
            while len(val) < min_len:
                val += self._rng.choice(string.ascii_lowercase)
        return val

    def _make_invalid_value(
        self,
        name: str,
        ftype: str,
        rules: Dict[str, Any],
        nullable: bool,
        edge_case_pools: Optional[Dict[str, List[Any]]] = None,
    ) -> Tuple[Any, TestCaseInfo]:
        """Generate an invalid value and return ``(value, test_case_info)``.

        Every invalidation strategy now produces structured metadata so the
        generation report can document exactly what was injected and why.
        """
        # AI edge cases get priority — 60% of the time when available
        if edge_case_pools and name in edge_case_pools:
            pool = edge_case_pools[name]
            if pool and self._rng.random() < 0.6:
                val = self._rng.choice(pool)
                return val, TestCaseInfo(
                    type="EDGE_CASE_AI",
                    field=name,
                    value=val,
                    description=f"AI-generated edge case for {name}",
                    contract_rule="quality.row_rules (AI-targeted)",
                )

        # ── Edge case profile injection (30% chance when matching) ───────
        name_lower = name.lower()
        if self._rng.random() < 0.3:
            # Check format violations (field-specific)
            fmt_viol = _EDGE_CASE_PROFILES.get("format_violations", {})
            if name_lower in fmt_viol:
                val = self._rng.choice(fmt_viol[name_lower])
                return val, TestCaseInfo(
                    type="REGEX_VIOLATION",
                    field=name,
                    value=val,
                    description=f"Format violation for {name}",
                    contract_rule="quality.row_rules.regex_match",
                )

            # Check general edge case index
            if name_lower in _EDGE_CASE_INDEX:
                val = self._rng.choice(_EDGE_CASE_INDEX[name_lower])
                return val, TestCaseInfo(
                    type="EDGE_CASE_BUILTIN",
                    field=name,
                    value=val,
                    description=f"Built-in edge case for {name}",
                    contract_rule="quality.row_rules",
                )

            # Word-boundary match
            parts = name_lower.split("_")
            for i in range(len(parts) - 1, -1, -1):
                suffix = "_".join(parts[i:])
                if suffix in _EDGE_CASE_INDEX:
                    val = self._rng.choice(_EDGE_CASE_INDEX[suffix])
                    return val, TestCaseInfo(
                        type="EDGE_CASE_BUILTIN",
                        field=name,
                        value=val,
                        description=f"Built-in edge case for {name} (matched suffix '{suffix}')",
                        contract_rule="quality.row_rules",
                    )
                if suffix in fmt_viol:
                    val = self._rng.choice(fmt_viol[suffix])
                    return val, TestCaseInfo(
                        type="REGEX_VIOLATION",
                        field=name,
                        value=val,
                        description=f"Format violation for {name} (matched suffix '{suffix}')",
                        contract_rule="quality.row_rules.regex_match",
                    )

            # Future date injection
            future_fields = _EDGE_CASE_PROFILES.get("future_dates", {}).get("fields", [])
            if name_lower in future_fields:
                future_days = self._rng.randint(30, 3650)
                future_dt = datetime.now() + timedelta(days=future_days)
                val = future_dt.date().isoformat() if ftype == "date" else future_dt.isoformat()
                return val, TestCaseInfo(
                    type="TEMPORAL_VIOLATION",
                    field=name,
                    value=val,
                    description=f"Future date injected for {name} ({future_days} days ahead)",
                    contract_rule="quality.row_rules.range (temporal)",
                )

            # Numeric boundary injection
            if ftype in (
                "integer",
                "int",
                "int32",
                "int64",
                "long",
                "double",
                "float",
                "float32",
                "float64",
                "decimal",
                "number",
            ):
                boundary_vals = _EDGE_CASE_PROFILES.get("numeric_boundaries", {}).get("values", [])
                if boundary_vals:
                    val = self._rng.choice(boundary_vals)
                    return val, TestCaseInfo(
                        type="BOUNDARY_VALUE",
                        field=name,
                        value=val,
                        description=f"Numeric boundary value for {name}",
                        contract_rule="quality.row_rules.range",
                    )

            # Type confusion injection for numeric-named string fields
            confusion = _EDGE_CASE_PROFILES.get("type_confusion", {})
            if name_lower in confusion.get("fields", []):
                val = self._rng.choice(confusion["values"])
                return val, TestCaseInfo(
                    type="TYPE_CONFUSION",
                    field=name,
                    value=val,
                    description=f"Type confusion injected for {name}",
                    contract_rule="schema.type_enforcement",
                )

        # ── Strategy pool: pick one at random ────────────────────────────
        strategies: List[Tuple[str, str, str, Any]] = []
        # Each entry: (tc_type, description, contract_rule, value_factory)

        # Null on required field — always a valid "bad" strategy
        strategies.append(
            (
                "NOT_NULL_VIOLATION",
                f"{name} set to null — tests required field constraint",
                "quality.enforce_required",
                lambda: None,
            )
        )

        accepted = rules.get("accepted_values")
        if accepted:
            # Value outside accepted list — must stay same Python type
            if ftype not in ("boolean", "bool"):
                strategies.append(
                    (
                        "ACCEPTED_VALUE_VIOLATION",
                        f"{name} set to value outside accepted_values list",
                        "quality.row_rules.accepted_values",
                        lambda: "INVALID_" + "".join(self._rng.choices(string.ascii_uppercase, k=4)),
                    )
                )

        min_val = rules.get("min")
        max_val = rules.get("max")

        if ftype in ("integer", "int", "int32", "int64", "long"):
            if min_val is not None:
                strategies.append(
                    (
                        "RANGE_VIOLATION",
                        f"{name} set below minimum ({min_val})",
                        "quality.row_rules.range",
                        lambda: int(min_val) - self._rng.randint(1, 100),
                    )
                )
            if max_val is not None:
                strategies.append(
                    (
                        "RANGE_VIOLATION",
                        f"{name} set above maximum ({max_val})",
                        "quality.row_rules.range",
                        lambda: int(max_val) + self._rng.randint(1, 100),
                    )
                )
            if len(strategies) == 1:  # only None so far
                strategies.append(
                    (
                        "RANGE_VIOLATION",
                        f"{name} set to negative value",
                        "quality.row_rules.range",
                        lambda: -self._rng.randint(1, 999),
                    )
                )

        elif ftype in ("double", "float", "float32", "float64", "decimal", "number"):
            if min_val is not None:
                strategies.append(
                    (
                        "RANGE_VIOLATION",
                        f"{name} set below minimum ({min_val})",
                        "quality.row_rules.range",
                        lambda: float(min_val) - self._rng.uniform(0.01, 10.0),
                    )
                )
            if max_val is not None:
                strategies.append(
                    (
                        "RANGE_VIOLATION",
                        f"{name} set above maximum ({max_val})",
                        "quality.row_rules.range",
                        lambda: float(max_val) + self._rng.uniform(0.01, 10.0),
                    )
                )
            if len(strategies) == 1:
                strategies.append(
                    (
                        "RANGE_VIOLATION",
                        f"{name} set to negative float",
                        "quality.row_rules.range",
                        lambda: -self._rng.uniform(0.01, 999.0),
                    )
                )

        elif ftype in ("boolean", "bool"):
            # Only valid Python booleans or None — never strings
            # None (already added) is the only way to violate a boolean required rule
            pass

        else:
            # String fields: empty string
            strategies.append(
                (
                    "EMPTY_STRING",
                    f"{name} set to empty string — tests not_null/pattern rules",
                    "quality.enforce_required",
                    lambda: "",
                )
            )

        # Pick a strategy and fire it
        tc_type, description, contract_rule, factory = self._rng.choice(strategies)
        val = factory()
        return val, TestCaseInfo(
            type=tc_type,
            field=name,
            value=val,
            description=description,
            contract_rule=contract_rule,
        )

    def _call_faker(self, hint: str) -> Any:
        """Invoke a Faker method from a hint string.

        Handles two forms:
        - Simple:       ``"email"``                       → ``self._faker.email()``
        - Parameterised: ``"bothify(text='ORD-######')"`` → ``self._faker.bothify(text='ORD-######')``

        For parameterised hints the method name and kwargs are parsed from the
        string.  Only keyword arguments are supported (positional args are not
        used by any of the hints we define).
        """
        if "(" not in hint:
            # Simple method name — most common path
            fn = getattr(self._faker, hint, None)
            if fn:
                return fn()
            return hint  # Unknown method — return hint literal as fallback

        # Parameterised: extract method name and kwargs string
        import ast

        method_name = hint[: hint.index("(")]
        args_str = hint[hint.index("(") + 1 : hint.rindex(")")]

        fn = getattr(self._faker, method_name, None)
        if not fn:
            return hint  # Unknown method — return hint literal as fallback

        # Parse kwargs: "text='ORD-######'" or "min_value=0.01, max_value=10000, right_digits=2"
        kwargs: Dict[str, Any] = {}
        for part in self._split_kwargs(args_str):
            part = part.strip()
            if "=" not in part:
                continue
            key, val_str = part.split("=", 1)
            try:
                kwargs[key.strip()] = ast.literal_eval(val_str.strip())
            except (ValueError, SyntaxError):
                kwargs[key.strip()] = val_str.strip()

        return fn(**kwargs)

    @staticmethod
    def _split_kwargs(s: str) -> List[str]:
        """Split a kwargs string on commas, respecting quoted strings."""
        parts: List[str] = []
        current: List[str] = []
        in_quote: Optional[str] = None
        for ch in s:
            if ch in ("'", '"') and not in_quote:
                in_quote = ch
                current.append(ch)
            elif ch == in_quote:
                in_quote = None
                current.append(ch)
            elif ch == "," and not in_quote:
                parts.append("".join(current))
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current))
        return parts

    def _generate_from_regex(self, pattern: str) -> Optional[str]:
        r"""Generate a string that matches a regex pattern.

        Supports a practical subset of regex syntax:
        - ``[A-Z]``, ``[0-9]``, ``[A-Za-z0-9]`` — character classes
        - ``\d``, ``\w``, ``\s`` — shorthand classes
        - ``{n}``, ``{n,m}`` — exact and range quantifiers
        - ``+``, ``*``, ``?`` — basic quantifiers
        - ``(a|b|c)`` — alternation groups
        - Literal characters

        Returns ``None`` if the pattern is too complex to handle.
        """
        try:
            return self._regex_emit(pattern)
        except (ValueError, IndexError, KeyError):
            return None

    def _regex_emit(self, pattern: str) -> str:
        """Recursive regex-to-string emitter."""
        result: List[str] = []
        i = 0
        n = len(pattern)

        # Strip anchors
        if pattern.startswith("^"):
            pattern = pattern[1:]
            n -= 1
        if pattern.endswith("$") and not pattern.endswith("\\$"):
            pattern = pattern[:-1]
            n -= 1

        while i < n:
            ch = pattern[i]

            # ── Alternation group: (opt1|opt2|opt3) ──
            if ch == "(":
                depth = 1
                j = i + 1
                while j < n and depth > 0:
                    if pattern[j] == "(":
                        depth += 1
                    elif pattern[j] == ")":
                        depth -= 1
                    j += 1
                inner = pattern[i + 1 : j - 1]
                # Split on top-level pipe
                options = self._split_alternation(inner)
                chosen = self._rng.choice(options)
                token = self._regex_emit(chosen)
                i = j

            # ── Character class: [A-Z0-9] ──
            elif ch == "[":
                j = i + 1
                if j < n and pattern[j] == "^":
                    j += 1  # skip negation (unsupported, treat as normal)
                while j < n and pattern[j] != "]":
                    j += 1
                class_str = pattern[i + 1 : j]
                chars = self._expand_char_class(class_str)
                token = self._rng.choice(chars) if chars else "?"
                i = j + 1

            # ── Shorthand: \d, \w, \s ──
            elif ch == "\\" and i + 1 < n:
                esc = pattern[i + 1]
                if esc == "d":
                    token = str(self._rng.randint(0, 9))
                elif esc == "w":
                    token = self._rng.choice(string.ascii_letters + string.digits + "_")
                elif esc == "s":
                    token = " "
                else:
                    token = esc  # literal escape
                i += 2

            # ── Dot: any char ──
            elif ch == ".":
                token = self._rng.choice(string.ascii_letters + string.digits)
                i += 1

            # ── Literal ──
            else:
                token = ch
                i += 1

            # ── Check for quantifier after token ──
            if i < n and pattern[i] == "{":
                j = pattern.index("}", i)
                quant = pattern[i + 1 : j]
                if "," in quant:
                    lo_q, hi_q = quant.split(",", 1)
                    lo_q = int(lo_q.strip()) if lo_q.strip() else 0
                    hi_q = int(hi_q.strip()) if hi_q.strip() else lo_q + 5
                else:
                    lo_q = hi_q = int(quant.strip())
                count = self._rng.randint(lo_q, hi_q)
                i = j + 1
            elif i < n and pattern[i] == "+":
                count = self._rng.randint(1, 5)
                i += 1
            elif i < n and pattern[i] == "*":
                count = self._rng.randint(0, 5)
                i += 1
            elif i < n and pattern[i] == "?":
                count = self._rng.randint(0, 1)
                i += 1
            else:
                count = 1

            result.append(token * count)

        return "".join(result)

    @staticmethod
    def _expand_char_class(class_str: str) -> List[str]:
        """Expand a regex character class like ``A-Z0-9`` into a list of chars."""
        chars: List[str] = []
        i = 0
        # Strip leading ^ (negation — unsupported, just ignore)
        if class_str.startswith("^"):
            class_str = class_str[1:]
        n = len(class_str)
        while i < n:
            if i + 2 < n and class_str[i + 1] == "-":
                lo_c = ord(class_str[i])
                hi_c = ord(class_str[i + 2])
                chars.extend(chr(c) for c in range(lo_c, hi_c + 1))
                i += 3
            elif class_str[i] == "\\" and i + 1 < n:
                esc = class_str[i + 1]
                if esc == "d":
                    chars.extend(string.digits)
                elif esc == "w":
                    chars.extend(string.ascii_letters + string.digits + "_")
                else:
                    chars.append(esc)
                i += 2
            else:
                chars.append(class_str[i])
                i += 1
        return chars if chars else list(string.ascii_letters)

    @staticmethod
    def _split_alternation(inner: str) -> List[str]:
        """Split a regex alternation group on top-level ``|`` characters."""
        options: List[str] = []
        depth = 0
        current: List[str] = []
        for ch in inner:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "|" and depth == 0:
                options.append("".join(current))
                current = []
            else:
                current.append(ch)
        if current:
            options.append("".join(current))
        return options if options else [inner]

    def _match_entity_id(self, name_lower: str) -> Optional[str]:
        """Match a field name to an entity ID format pattern.

        Uses the same word-boundary matching as ``_match_semantic_hint``:
        ``user_customer_id`` matches ``customer_id`` → ``CUST-{:06d}``.
        """
        if name_lower in _ENTITY_ID_INDEX:
            return _ENTITY_ID_INDEX[name_lower]
        parts = name_lower.split("_")
        for i in range(len(parts) - 1, -1, -1):
            suffix = "_".join(parts[i:])
            if suffix in _ENTITY_ID_INDEX:
                return _ENTITY_ID_INDEX[suffix]
        return None

    def _string_value(self, name: str) -> str:
        name_lower = name.lower()

        # ── Date/timestamp detection by field NAME (highest priority) ──────────
        # Must run BEFORE Faker semantic hints to prevent false matches
        # (e.g. "ship_date" matching the "ip" hint in _SEMANTIC_HINTS).
        is_timestamp_name = any(name_lower.endswith(s) for s in _TIMESTAMP_SUFFIXES)
        is_date_name = any(name_lower.endswith(s) for s in _DATE_NAME_SUFFIXES) or any(
            kw in name_lower for kw in _DATE_NAME_CONTAINS
        )
        if is_timestamp_name:
            # ── Window-constrained generation ──────────────────────────
            if getattr(self, "_window_start", None) is not None:
                ws = self._window_start
                we = self._window_end
                delta_secs = max(int((we - ws).total_seconds()), 1)
                return (ws + timedelta(seconds=self._rng.randint(0, delta_secs))).isoformat()
            now = datetime.now()
            base = now - timedelta(days=90)
            return (base + timedelta(seconds=self._rng.randint(0, 60 * 60 * 24 * 90))).isoformat()
        if is_date_name:
            # ── Window-constrained generation ──────────────────────────
            if getattr(self, "_window_start", None) is not None:
                ws = self._window_start.date()
                we = self._window_end.date()
                days_range = max((we - ws).days, 0)
                return (ws + timedelta(days=self._rng.randint(0, days_range))).isoformat()
            today = date.today()
            base = today - timedelta(days=90)
            return (base + timedelta(days=self._rng.randint(0, 90))).isoformat()

        # ── Entity ID patterns (before Faker, so string-typed IDs get formatted) ──
        entity_fmt = self._match_entity_id(name_lower)
        if entity_fmt:
            return entity_fmt.format(self._rng.randint(1, 999999))

        # ── Non-person name routing ─────────────────────────────────────────
        # Fields like item_display_name, location_name, preferred_vendor_name
        # contain "name" but should NOT generate person names.
        _NON_PERSON_NAME_KEYWORDS = {
            "item",
            "product",
            "display",
            "sku",
            "location",
            "warehouse",
            "store",
            "facility",
            "site",
            "depot",
            "vendor",
            "supplier",
            "company",
            "org",
            "organisation",
            "brand",
            "manufacturer",
            "distributor",
            "partner",
            "merchant",
            "category",
            "channel",
            "campaign",
            "project",
            "plan",
            "policy",
            "account",
            "file",
            "table",
            "column",
            "field",
            "metric",
            "event",
            "model",
            "template",
        }
        if "name" in name_lower:
            parts = name_lower.replace("_name", "").replace("name_", "").split("_")
            matched_keyword = None
            for part in parts:
                if part in _NON_PERSON_NAME_KEYWORDS:
                    matched_keyword = part
                    break

            if matched_keyword:
                # Route to domain-specific pool
                _DOMAIN_POOL_MAP = {
                    "item": "item_name",
                    "product": "item_name",
                    "sku": "item_name",
                    "display": "item_name",
                    "location": "location_name",
                    "warehouse": "location_name",
                    "store": "location_name",
                    "facility": "location_name",
                    "site": "location_name",
                    "depot": "location_name",
                    "vendor": "company_name",
                    "supplier": "company_name",
                    "company": "company_name",
                    "org": "company_name",
                    "organisation": "company_name",
                    "brand": "brand_name",
                    "manufacturer": "company_name",
                    "distributor": "company_name",
                    "partner": "company_name",
                    "merchant": "company_name",
                }
                pool_key = _DOMAIN_POOL_MAP.get(matched_keyword)
                if pool_key and pool_key in _REALISTIC_POOLS:
                    return self._rng.choice(_REALISTIC_POOLS[pool_key])
                # Fallback for unmapped non-person names: code-like reference
                prefix = matched_keyword[:3].upper()
                return f"{prefix}-{self._rng.randint(1000, 9999)}"

        # Check realistic fallback pools BEFORE Faker — produces real-world values
        # Exact match first, then word-boundary match (like _match_semantic_hint)
        if name_lower in _REALISTIC_POOLS:
            return self._rng.choice(_REALISTIC_POOLS[name_lower])
        parts = name_lower.split("_")
        for i in range(len(parts) - 1, -1, -1):
            suffix = "_".join(parts[i:])
            if suffix in _REALISTIC_POOLS:
                return self._rng.choice(_REALISTIC_POOLS[suffix])

        # ── Faker semantic hints (word-boundary matching) ──────────────────────
        if self._faker:
            method = _match_semantic_hint(name_lower)
            if method:
                return str(self._call_faker(method))

        # Format-aware fallbacks for common field names — avoids Faker dependency.
        # These must produce values that pass the most common dbt expression_is_true checks.
        if "email" in name_lower or name_lower == "mail":
            user = "".join(self._rng.choices(string.ascii_lowercase, k=self._rng.randint(4, 10)))
            domain = "".join(self._rng.choices(string.ascii_lowercase, k=self._rng.randint(3, 8)))
            tld = self._rng.choice(["com", "org", "net", "io", "co"])
            return f"{user}@{domain}.{tld}"

        if "phone" in name_lower or "mobile" in name_lower:
            digits = "".join(self._rng.choices(string.digits, k=10))
            return f"+1{digits}"

        if "url" in name_lower or "website" in name_lower or "link" in name_lower:
            slug = "".join(self._rng.choices(string.ascii_lowercase, k=self._rng.randint(4, 10)))
            return f"https://www.{slug}.com"

        # Realistic name fallback (no Faker required)
        if name_lower in ("first_name", "firstname", "given_name"):
            return self._rng.choice(_FIRST_NAMES)
        if name_lower in ("last_name", "lastname", "surname", "family_name"):
            return self._rng.choice(_LAST_NAMES)
        if "name" in name_lower:
            return f"{self._rng.choice(_FIRST_NAMES)} {self._rng.choice(_LAST_NAMES)}"

        # Generic readable fallback — produces a code-like reference instead of
        # meaningless alphanumeric noise (e.g. "CUS-4821" instead of "kd83jf2n")
        prefix = name_lower[:3].upper() if len(name_lower) >= 3 else name_lower.upper()
        num = self._rng.randint(1000, 9999)
        return f"{prefix}-{num}"

    def _build_field_rules(self, fk_pools: Optional[Dict[str, List[Any]]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Parse quality rules into a per-field lookup of { min, max, accepted_values }.

        Seeds from field-level definitions first (so injected accepted_values /
        min / max from DataGenerator.from_dbt() are always honoured), then
        overlays any structured quality row_rules on top.

        FK columns: if ``fk_pools`` contains a pool for the column, that pool
        becomes the ``accepted_values`` for generation.  If no pool is supplied
        but the field declares a ``foreign_key``, a surrogate integer pool of
        1–N is used so the generated data is at least type-correct.
        """
        result: Dict[str, Dict[str, Any]] = {}

        # ── 0. Model-level primary key list ───────────────────────────────────
        _pk_fields: set = set()
        for pk_col in self._contract_raw.get("primary_key") or []:
            _pk_fields.add(pk_col)

        # ── 1. Seed from field-level definitions ──────────────────────────────
        for field in self._fields:
            fname = field.get("name", "")
            if not fname:
                continue
            entry = result.setdefault(fname, {})

            # Propagate primary_key flag (field-level or model-level)
            if field.get("primary_key") or fname in _pk_fields:
                entry["primary_key"] = True
            has_range = "min" in field or "max" in field
            av = field.get("accepted_values")
            # accepted_values from a single-record infer_contract produces a
            # degenerate one-item list (e.g. [10001]).  When the caller also
            # sets min/max on the same field (via fields.append upsert), treat
            # the range constraint as authoritative and ignore the single-value
            # pool — otherwise accepted_values always wins and min/max is ignored.
            if av is not None and not (has_range and isinstance(av, list) and len(av) == 1):
                entry.setdefault("accepted_values", av)
            if "min" in field:
                entry["min"] = field["min"]  # always write — range overrides
            if "max" in field:
                entry["max"] = field["max"]  # always write — range overrides
            if "min_length" in field:
                entry["min_length"] = int(field["min_length"])
            if "max_length" in field:
                entry["max_length"] = int(field["max_length"])
            if "regex_match" in field:
                entry["regex_match"] = field["regex_match"]
            if "pattern" in field:
                entry.setdefault("regex_match", field["pattern"])

            # ── FK hint — inject caller-supplied pool or a surrogate fallback ──
            fk = field.get("foreign_key")  # dict with keys: contract, column (+ severity)
            if fk and isinstance(fk, dict):
                if fk_pools and fname in fk_pools:
                    # Caller provided a real pool from the reference table
                    entry["accepted_values"] = fk_pools[fname]
                    entry["_fk_contract"] = fk.get("contract")
                    entry["_fk_column"] = fk.get("column")
                elif "accepted_values" not in entry:
                    # No pool supplied — generate surrogate integers for CI safety
                    # (valid type, referentially meaningless, but won't break type checks)
                    surrogate_n = max(10, len(self._fields) * 5)
                    entry["accepted_values"] = list(range(1, surrogate_n + 1))
                    entry["_fk_contract"] = fk.get("contract")
                    entry["_fk_column"] = fk.get("column")
                    entry["_fk_surrogate"] = True  # flag: values are synthetic, not real

            # ── FK pool catch-all — apply caller-supplied pool even without
            #    a formal foreign_key declaration on the field.  This enables
            #    generate_related() to inject parent PK pools based on naming
            #    conventions and field descriptions alone.  ─────────────────
            if fk_pools and fname in fk_pools and "accepted_values" not in entry:
                entry["accepted_values"] = fk_pools[fname]

        # ── 2. Overlay structured quality row_rules ───────────────────────────
        quality = self._quality

        for rule_item in quality.get("row_rules", []):
            if not isinstance(rule_item, dict):
                continue

            # accepted_values structured rule
            if "accepted_values" in rule_item:
                av = rule_item["accepted_values"]
                field = av.get("field") if isinstance(av, dict) else None
                values = av.get("values") if isinstance(av, dict) else None
                if field and values:
                    result.setdefault(field, {})["accepted_values"] = values

            # range structured rule
            if "range" in rule_item:
                rng = rule_item["range"]
                field = rng.get("field") if isinstance(rng, dict) else None
                if field:
                    result.setdefault(field, {})
                    if "min" in rng:
                        result[field]["min"] = rng["min"]
                    if "max" in rng:
                        result[field]["max"] = rng["max"]

            # regex_match structured rule
            if "regex_match" in rule_item:
                rm = rule_item["regex_match"]
                field = rm.get("field") if isinstance(rm, dict) else None
                pattern = rm.get("pattern") if isinstance(rm, dict) else None
                if field and pattern:
                    result.setdefault(field, {})["regex_match"] = pattern

            # min_length / max_length structured rules
            if "min_length" in rule_item:
                ml = rule_item["min_length"]
                field = ml.get("field") if isinstance(ml, dict) else None
                value = ml.get("value") if isinstance(ml, dict) else None
                if field and value is not None:
                    result.setdefault(field, {})["min_length"] = int(value)

            if "max_length" in rule_item:
                ml = rule_item["max_length"]
                field = ml.get("field") if isinstance(ml, dict) else None
                value = ml.get("value") if isinstance(ml, dict) else None
                if field and value is not None:
                    result.setdefault(field, {})["max_length"] = int(value)

            # referential_integrity structured rule — overlay the caller pool
            # (quality rule takes same pool as field-level hint; already seeded above)
            if "referential_integrity" in rule_item:
                ri = rule_item["referential_integrity"]
                if isinstance(ri, dict):
                    field = ri.get("field")
                    if field and fk_pools and field in fk_pools:
                        result.setdefault(field, {})["accepted_values"] = fk_pools[field]

            # ── SQL-variant FK/RI rules ────────────────────────────────────────
            # Handles QualityRule entries written as raw SQL rather than the
            # structured referential_integrity block.  Two sub-cases:
            #
            #  (a) col IN ('x', 'y')  — literal values in SQL
            #      → extract literals and use directly as accepted_values
            #      → works even without fk_pools (no runtime reference data needed)
            #      Example contract YAML:
            #        - name: valid_status
            #          sql: "status IN ('active', 'inactive')"
            #          category: validity
            #
            #  (b) col IN (SELECT col FROM table)  — subquery
            #      → cannot statically evaluate; apply fk_pools if caller provided it
            #      Example contract YAML:
            #        - name: valid_agent
            #          sql: "agent_id IN (SELECT agent_id FROM silver.agents)"
            #          category: integrity    # ← this category flags it as FK/RI
            #
            #      At generate() call site:
            #        agents = DataGenerator("silver_agents.yaml").generate(rows=20)
            #        orders = DataGenerator("gold_orders.yaml").generate(
            #            rows=200,
            #            reference_data={"agent_id": agents["agent_id"]},
            #        )
            sql_str = rule_item.get("sql", "")
            rule_category = rule_item.get("category", "")

            if sql_str:
                # Sub-case (a): col IN (literal, values) — no subquery
                m_lit = _re.match(r"^\s*(\w+)\s+IN\s*\(([^)]+)\)\s*$", sql_str, _re.IGNORECASE)
                if m_lit:
                    col = m_lit.group(1)
                    inner = m_lit.group(2)
                    if not _re.search(r"\bSELECT\b", inner, _re.IGNORECASE):
                        # Extract quoted strings or bare numbers
                        parsed = [s or n for s, n in _re.findall(r"'([^']*)'|\b(-?\d+(?:\.\d+)?)\b", inner)]
                        if parsed:
                            # setdefault: field-level accepted_values takes priority
                            result.setdefault(col, {}).setdefault("accepted_values", parsed)

                # Sub-case (b): subquery or category == integrity → apply fk_pools
                m_sub = _re.match(r"^\s*(\w+)\s+IN\s*\(\s*SELECT\b", sql_str, _re.IGNORECASE)
                is_integrity_category = rule_category in (
                    "integrity",
                    "referential_integrity",
                    "referential",
                )
                if (m_sub or is_integrity_category) and fk_pools:
                    # Primary: column named before IN in the SQL
                    col_from_sql = m_sub.group(1) if m_sub else None
                    # Fallback: any fk_pool key whose name appears verbatim in SQL
                    candidates = (
                        [col_from_sql]
                        if col_from_sql
                        else [c for c in fk_pools if _re.search(r"\b" + _re.escape(c) + r"\b", sql_str)]
                    )
                    for col in candidates:
                        if col and col in fk_pools:
                            # Override — fk_pools always wins over surrogate fallback
                            result.setdefault(col, {})["accepted_values"] = fk_pools[col]
                            result[col]["_fk_from_sql"] = True  # audit flag

        return result

    def _to_frame(self, records: List[Dict[str, Any]], fmt: str):
        import json as _json

        # Serialise any nested dict/list values to JSON strings before converting
        # to a DataFrame.  Without this, Polars tries to infer a Struct dtype and
        # then stringify it in its own display format (e.g. {["val"]}) rather than
        # proper JSON.  Fields with nested objects are typed as "string" in the
        # contract (bronze layer), so storing them as JSON strings is correct.
        def _normalise(val: Any) -> Any:
            if isinstance(val, (dict, list)):
                return _json.dumps(val, ensure_ascii=False)
            return val

        clean_records = [{k: _normalise(v) for k, v in row.items()} for row in records]

        if fmt == "polars":
            import polars as pl

            try:
                df = pl.DataFrame(clean_records, infer_schema_length=None)
            except Exception:
                # Type conflicts from invalid rows (e.g. "invalid_id" in a long column).
                # Convert all values to strings, then cast back to contract types below.
                str_records = [{k: str(v) if v is not None else None for k, v in row.items()} for row in clean_records]
                df = pl.DataFrame(str_records)

            # Cast every column to its contract-declared dtype so the processor
            # never sees Utf8View where it expects Boolean / Int64 / Float64.
            cast_exprs = []
            for field in self._fields:
                col_name = field.get("name", "")
                ftype = (field.get("type") or "string").lower()
                dtype = _polars_dtype(ftype)
                if dtype is not None and col_name in df.columns:
                    current = df[col_name].dtype
                    if current != dtype:
                        if dtype == pl.Boolean:
                            cast_exprs.append(
                                pl.when(
                                    pl.col(col_name)
                                    .cast(pl.Utf8)
                                    .str.to_lowercase()
                                    .is_in(["true", "t", "yes", "y", "1"])
                                )
                                .then(True)
                                .when(
                                    pl.col(col_name)
                                    .cast(pl.Utf8)
                                    .str.to_lowercase()
                                    .is_in(["false", "f", "no", "n", "0"])
                                )
                                .then(False)
                                .otherwise(None)
                                .alias(col_name)
                            )
                        else:
                            cast_exprs.append(pl.col(col_name).cast(dtype, strict=False).alias(col_name))
            if cast_exprs:
                df = df.with_columns(cast_exprs)

            # Reorder columns to match the contract schema exactly
            ordered_cols = [f.get("name") for f in self._fields if f.get("name")]
            valid_cols = [c for c in ordered_cols if c in df.columns]

            # Add any extra columns (like _invalid_reason) to the end
            extra_cols = [c for c in df.columns if c not in valid_cols]
            final_cols = valid_cols + extra_cols

            return df.select(final_cols)

        elif fmt == "pandas":
            import pandas as pd

            df = pd.DataFrame(clean_records)

            # Reorder columns to match the contract schema exactly
            ordered_cols = [f.get("name") for f in self._fields if f.get("name")]
            valid_cols = [c for c in ordered_cols if c in df.columns]

            # Add any extra columns (like _invalid_reason) to the end
            extra_cols = [c for c in df.columns if c not in valid_cols]
            final_cols = valid_cols + extra_cols

            return df[final_cols]
        else:
            raise ValueError(f"output_format must be 'polars' or 'pandas', got: {fmt!r}")
