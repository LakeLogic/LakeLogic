"""Reading Delta tables without depending on polars' Delta integration.

``pl.read_delta()`` and ``pl.scan_delta()`` both raise against current deltalake::

    TypeError: 'deltalake._internal.Schema' object is not iterable

with polars 1.40.1 and deltalake 1.6.2 — and polars itself declares
``deltalake>=1.0.0``, so this is the *supported* combination, not a mispin. The
same class of breakage was hit once before at deltalake 0.17.x and worked around
locally in ``core/slo.py``; it recurred at 1.x because the workaround lived in one
module while four others kept calling ``pl.read_delta`` directly.

deltalake itself is fine — ``DeltaTable.to_pyarrow_table()`` works. Only polars'
bridge is broken, so this reads through deltalake and hands polars Arrow, which it
consumes natively. That also makes the read independent of whether polars ever
fixes its integration.
"""

from __future__ import annotations

from typing import Any, Optional

from loguru import logger

_CLOUD_PREFIXES = ("abfss://", "s3://", "s3a://", "gs://", "gcs://", "adl://", "https://")


def is_cloud_path(path: str) -> bool:
    return any(str(path).startswith(p) for p in _CLOUD_PREFIXES)


def read_delta(path: str, storage_options: Optional[dict] = None) -> Any:
    """Read a Delta table into a Polars DataFrame.

    Goes through deltalake -> Arrow -> polars rather than ``pl.read_delta``. Cloud
    paths take the same route (``storage_options`` is forwarded), because the
    previous helper fell back to ``pl.read_delta`` for them and so stayed broken
    exactly where the data is biggest.

    Falls back to ``pl.read_delta`` only if the Arrow route fails, so a future
    polars fix or an unforeseen path still has a chance rather than hard-failing.
    """
    import polars as pl

    try:
        from deltalake import DeltaTable

        table = DeltaTable(str(path), storage_options=storage_options) if storage_options else DeltaTable(str(path))
        return pl.from_arrow(table.to_pyarrow_table())
    except Exception as exc:
        logger.debug(f"Arrow route failed for {path} ({exc}); trying pl.read_delta.")
        return pl.read_delta(str(path), storage_options=storage_options)
