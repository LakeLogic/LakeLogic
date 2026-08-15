"""RuntimeContract adapter: strict canonical OLC -> lenient runtime contract.

The strict ``OLCContractV1`` is the *public standard*; the engine runs on the
lenient ``DataContract`` (with its stage overrides, fact governance, CDC
defaults, and the many runtime-only affordances). This module is the one-way
bridge between them, so a contract that passes the public standard can be handed
to the existing runtime unchanged.

Phase 4 (opt-in): nothing here is on by default. ``DataProcessor(strict=True)``
routes through :func:`load_strict` to *gate* on the standard, then builds the
runtime object as usual. The default (``strict=False``) path is untouched.
"""
from __future__ import annotations

from typing import Any

from lakelogic.core.contracts.normalise import normalise_contract
from lakelogic.core.contracts.olc_v1 import OLCContractV1


def load_strict(document: Any) -> OLCContractV1:
    """Normalise then strictly validate an input against the public OLC standard.

    Raises ``pydantic.ValidationError`` (or ``ValueError`` for nested-unknown
    keys) if the document is not a canonical OLC contract.
    """
    return OLCContractV1.model_validate(normalise_contract(document))


def to_runtime(olc: OLCContractV1) -> "Any":
    """Adapt a validated :class:`OLCContractV1` into a runtime ``DataContract``.

    Imported lazily to keep the ``models <-> contracts`` import graph acyclic.
    """
    from lakelogic.core.models import DataContract

    # exclude_none keeps the dict tight so the runtime model re-applies its own
    # defaults rather than inheriting explicit ``None``s for absent blocks.
    data = olc.model_dump(exclude_none=True)
    # Vendor ``extensions`` are part of the public standard but carry no runtime
    # meaning — the engine ignores them (and warns). Drop them at the boundary
    # so the runtime object is clean.
    data.pop("extensions", None)
    return DataContract.model_validate(data)
