"""Strict canonical OLC contract model — re-exported from the public standard.

The canonical definition now lives in the public ``olc`` package (the Open Lakehouse
Contract repository), which LakeLogic installs natively. This module used to hold a
duplicate; it is kept only as a stable import path so existing
``lakelogic.core.contracts.olc_v1`` imports keep working, while OLC is the single
source of truth for the contract spec.
"""
from __future__ import annotations

from olc.models import OLCContractV1, StrictServer
from olc.models._strict_keys import collect_unknown_nested_keys

__all__ = ["OLCContractV1", "StrictServer", "collect_unknown_nested_keys"]
