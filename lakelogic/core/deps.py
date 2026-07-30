"""Lazy-import guard with friendly install hints.

Usage in any module:

    from lakelogic.core.deps import require

    # Single package
    require("snowflake.connector", extra="cloud")

    # Multiple at once
    require("pyspark", extra="enterprise")
"""

from __future__ import annotations

import importlib
from typing import Optional

# Maps import names → (pip package, extra group)
_PACKAGE_EXTRAS: dict[str, tuple[str, str]] = {
    # cloud
    "snowflake.connector": ("snowflake-connector-python", "cloud"),
    "google.cloud.bigquery": ("google-cloud-bigquery", "cloud"),
    "google.cloud.storage": ("google-cloud-storage", "cloud"),
    "google.cloud.secretmanager": ("google-cloud-secret-manager", "cloud"),
    "google.cloud.pubsub_v1": ("google-cloud-pubsub", "cloud"),
    "azure.servicebus": ("azure-servicebus", "cloud"),
    "azure.eventgrid": ("azure-eventgrid", "cloud"),
    # enterprise
    "pyspark": ("pyspark", "enterprise"),
    "dataprofiler": ("dataprofiler", "enterprise"),
    "presidio_analyzer": ("presidio-analyzer", "enterprise"),
    "nbclient": ("nbclient", "enterprise"),
    "nbformat": ("nbformat", "enterprise"),
}


class MissingExtra(ImportError):
    """Raised when an optional dependency is not installed."""


def require(
    module: str,
    *,
    extra: Optional[str] = None,
    pip_name: Optional[str] = None,
):
    """Try to import *module*; raise a helpful error if missing.

    Parameters
    ----------
    module : str
        Dotted module name, e.g. ``"snowflake.connector"``.
    extra : str, optional
        Which ``pip install lakelogic[extra]`` to recommend.
        Auto-detected from ``_PACKAGE_EXTRAS`` if omitted.
    pip_name : str, optional
        PyPI package name to show in the error message.
        Auto-detected from ``_PACKAGE_EXTRAS`` if omitted.

    Returns
    -------
    module
        The imported module object.

    Raises
    ------
    MissingExtra
        With a human-friendly install instruction.
    """
    try:
        return importlib.import_module(module)
    except ImportError:
        # Resolve from registry
        _pip, _extra = _PACKAGE_EXTRAS.get(module, (None, None))
        pip_name = pip_name or _pip or module
        extra = extra or _extra

        if extra:
            hint = (
                f'\n\n  pip install "lakelogic[{extra}]"\n\n'
                f"or install the package directly:\n\n"
                f"  pip install {pip_name}\n"
            )
        else:
            hint = f"\n\n  pip install {pip_name}\n"

        raise MissingExtra(f"'{module}' is required but not installed. Install it with:{hint}") from None
