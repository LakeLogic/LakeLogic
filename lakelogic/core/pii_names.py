"""Name-based PII detection -- the one definition the inferrer and the profiler share.

A column NAME signals personal data only through whole tokens, never substrings.
Names are split on ``_ - . space`` and camelCase (``riderEmail`` -> rider, email).

* Strong tokens trigger alone (``email``, ``phone``, ``dob``, ``ip``, ``postcode`` ...).
* Phrases trigger as consecutive tokens (``first name``, ``date of birth``, ``card number``).
* Generic tokens never trigger alone: name, code, type, city, id, amount, rating,
  multiplier, status. Substring matching flagged ``trip_id``/``tip_amount`` ("ip"),
  ``rider_rating`` ("tin"), ``cancelled_by`` ("cell"), ``city_code`` and ``event_name``.
* A bare ``name`` column counts only when another column of the same table is
  person PII by name (``name`` beside ``email`` is a person; beside ``price`` it is not).
* Coordinates (lat/lng) are not flagged by name: on trips, vehicles and venues they
  locate a thing, not a person. Classify them in the contract where they do.

Value-based detection (regex patterns, Presidio) is separate and unchanged.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional

PII_NAME_TOKENS: Dict[str, str] = {
    "email": "email",
    "e_mail": "email",
    "phone": "phone",
    "mobile": "phone",
    "telephone": "phone",
    "cellphone": "phone",
    "ssn": "ssn",
    "nin": "national_id",
    "passport": "passport",
    "dob": "date_of_birth",
    "birthdate": "date_of_birth",
    "birthday": "date_of_birth",
    "iban": "bank_account",
    "ip": "ip_address",
    "ipaddress": "ip_address",
    "ipv4": "ip_address",
    "ipv6": "ip_address",
    "postcode": "address",
    "zip": "address",
    "zipcode": "address",
    "postal": "address",
    "address": "address",
    "street": "address",
    "surname": "person_name",
    "firstname": "person_name",
    "lastname": "person_name",
    "fullname": "person_name",
    "tin": "tax_id",
    "pan": "credit_card",
}

PII_NAME_PHRASES: Dict[tuple, str] = {
    **{
        (q, "name"): "person_name"
        for q in (
            "first",
            "last",
            "full",
            "given",
            "family",
            "middle",
            "maiden",
            "customer",
            "rider",
            "driver",
            "user",
            "contact",
            "person",
        )
    },
    ("date", "of", "birth"): "date_of_birth",
    ("birth", "date"): "date_of_birth",
    ("credit", "card"): "credit_card",
    ("card", "number"): "credit_card",
    ("cc", "number"): "credit_card",
    ("national", "id"): "national_id",
    ("tax", "id"): "tax_id",
    ("social", "security"): "ssn",
    ("cell", "number"): "phone",
    ("cell", "phone"): "phone",
    ("license", "number"): "drivers_license",
    ("licence", "number"): "drivers_license",
    ("drivers", "license"): "drivers_license",
    ("driver", "license"): "drivers_license",
    ("drivers", "licence"): "drivers_license",
    ("driving", "licence"): "drivers_license",
    ("account", "number"): "bank_account",
    ("sort", "code"): "bank_account",
    ("routing", "number"): "bank_account",
}

_SPLIT = re.compile(r"[^0-9a-zA-Z]+")
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def name_tokens(name: str) -> List[str]:
    """``riderEmail`` / ``rider-email`` / ``rider.email`` -> ``["rider", "email"]``."""
    return [t.lower() for part in _SPLIT.split(str(name)) for t in _CAMEL.split(part) if t]


def pii_type_for_name(name: str) -> Optional[str]:
    """PII type the column NAME alone signals, or None (token match, not substring)."""
    toks = name_tokens(name)
    for t in toks:
        if t in PII_NAME_TOKENS:
            return PII_NAME_TOKENS[t]
    n = len(toks)
    for ph, kind in PII_NAME_PHRASES.items():
        k = len(ph)
        if any(tuple(toks[i : i + k]) == ph for i in range(n - k + 1)):
            return kind
    return None


def pii_columns_by_name(columns: Iterable[str]) -> Dict[str, str]:
    """``{column: pii_type}`` for a table's columns, including the bare-``name`` rule."""
    cols = list(columns)
    out = {c: t for c in cols if (t := pii_type_for_name(c))}
    if out:
        out.update({c: "person_name" for c in cols if name_tokens(c) == ["name"]})
    return out
