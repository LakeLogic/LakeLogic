"""The loader must not warn about a key it injected itself.

`DomainRegistry` writes a merged `ownership` block onto every contract as the third
step of the ownership chain (domain -> system -> data product). `DataContract` had no
`ownership` field, so its unknown-key validator fired on it — once per contract, and
saying:

    Unknown key 'ownership' in 'contract' block - this key will be ignored by LakeLogic.

Both halves of that are wrong. The key is not the user's: the loader put it there. And
it is not ignored: the merged value is retained on the model with its provenance. On a
66-contract registry it produced 66 warnings telling an operator their governance
config was being dropped when it was not.
"""

from __future__ import annotations

import pytest
from loguru import logger

from lakelogic.core.models import DataContract

OWNERSHIP = {
    "domain_owner": "Platform Data Engineering",
    "team": "data_engineering",
    "contacts": [{"name": "Platform Data Lead", "role": "owner"}],
}


def _contract(**extra):
    base = {
        "version": "1.0.0",
        "dataset": "orders",
        "model": {"fields": [{"name": "id", "type": "int"}]},
    }
    base.update(extra)
    return base


def _warnings_for(contract_dict):
    """Capture via a loguru sink rather than capfd: loguru's own buffering means a
    mid-test capfd.readouterr() can miss a line that pytest later shows at teardown."""
    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="WARNING")
    try:
        DataContract.model_validate(contract_dict)
    finally:
        logger.remove(sink)
    return [ln for ln in lines if "Unknown key" in ln]


def test_ownership_does_not_warn():
    assert _warnings_for(_contract(ownership=OWNERSHIP)) == []


def test_the_ownership_value_is_retained_not_ignored():
    """The warning claimed it would be ignored — prove the opposite is true."""
    contract = DataContract.model_validate(_contract(ownership=OWNERSHIP))
    extra = getattr(contract, "__pydantic_extra__", {}) or {}
    assert extra.get("ownership") == OWNERSHIP


def test_a_genuinely_unknown_key_still_warns():
    """Silencing one injected key must not silence the check itself."""
    warnings = _warnings_for(_contract(ownershp=OWNERSHIP))
    assert warnings, "a misspelled key must still be reported"
    assert "ownershp" in warnings[0]


@pytest.mark.parametrize("key", ["ownership", "quality", "materialization"])
def test_supported_top_level_keys_are_silent(key):
    payload = OWNERSHIP if key == "ownership" else {}
    assert _warnings_for(_contract(**{key: payload})) == []
