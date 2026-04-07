"""
Backward-compatibility shim — this module has been renamed to
``lakelogic.engines.catalog_resolver``.

All public symbols are re-exported here so that existing ``import``
statements continue to work, but new code should import from
``catalog_resolver`` directly.
"""

from lakelogic.engines.catalog_resolver import (  # noqa: F401
    UC_RESOLVE_TIMEOUT_SECONDS,
    UnityCatalogResolver,
    get_unity_catalog_resolver,
    resolve_catalog_path,
    resolve_unity_catalog_path,
)
