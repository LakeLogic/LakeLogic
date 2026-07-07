"""
LakeLogic Scanner — metadata-based lakehouse observability without pipeline instrumentation.

Connects directly to Delta Lake, Unity Catalog, or DuckDB and runs SLO checks
(freshness, volume, schema drift, retention) using only table metadata and
lightweight SQL aggregates. No contracts, no DataProcessor, no YAML authoring.

Usage — Python API::

    from lakelogic.scanner import Scanner

    report = Scanner.from_yaml("scanner.yaml").run()
    print(f"Passed: {report.passed}  Failures: {len(report.failures)}")

Usage — CLI::

    lakelogic scan --config scanner.yaml
    lakelogic scan --connection delta --path ./lakehouse/

"""

from lakelogic.scanner.config import (
    ConnectionConfig,
    DiscoveryConfig,
    ObservatoryConfig,
    OutputConfig,
    ScannerConfig,
    SLODefaults,
)
from lakelogic.scanner.connector import (
    BaseConnector,
    DeltaConnector,
    DuckDBConnector,
    ScannedTable,
    TableMetadata,
    UnityCatalogConnector,
    build_connector,
)
from lakelogic.scanner.schema_drift import (
    BaselineStore,
    LocalBaselineStore,
    SchemaDiff,
    compare_schemas,
)
from lakelogic.scanner.validator import ScannerValidator


class Scanner:
    """
    Convenience façade — wires ScannerConfig + connector + validator together.

    Preferred entry point for both the CLI and Python API.
    """

    def __init__(self, config: ScannerConfig, baseline_store: BaselineStore = None):
        self.config = config
        self._connector: BaseConnector = build_connector(config.connection)
        self._validator = ScannerValidator(
            config=config,
            connector=self._connector,
            baseline_store=baseline_store or LocalBaselineStore(),
        )

    @classmethod
    def from_yaml(cls, path: str, baseline_store: BaselineStore = None) -> "Scanner":
        """Load configuration from a scanner.yaml file."""
        return cls(ScannerConfig.from_yaml(path), baseline_store=baseline_store)

    @classmethod
    def from_args(
        cls,
        connection_type: str,
        path: str = None,
        host: str = None,
        catalog: str = None,
        token: str = None,
        baseline_store: BaselineStore = None,
    ) -> "Scanner":
        """Build a scanner from CLI arguments for quick one-shot scans."""
        config = ScannerConfig.from_args(
            connection_type=connection_type,
            path=path,
            host=host,
            catalog=catalog,
            token=token,
        )
        return cls(config, baseline_store=baseline_store)

    def connect(self) -> "Scanner":
        """Validate connectivity. Called automatically by run() if not called manually."""
        self._connector.connect()
        return self

    def run(self, pipeline_run_id: str = None):
        """
        Discover tables, run all checks, write results, return SLOReport.

        Args:
            pipeline_run_id: Optional — links scan results to a pipeline run in run_log.
        """
        self._connector.connect()
        return self._validator.run(pipeline_run_id=pipeline_run_id)


__all__ = [
    # Façade
    "Scanner",
    # Config
    "ScannerConfig",
    "ConnectionConfig",
    "DiscoveryConfig",
    "SLODefaults",
    "OutputConfig",
    "ObservatoryConfig",
    # Connector
    "BaseConnector",
    "DeltaConnector",
    "DuckDBConnector",
    "UnityCatalogConnector",
    "ScannedTable",
    "TableMetadata",
    "build_connector",
    # Schema drift
    "SchemaDiff",
    "compare_schemas",
    "BaselineStore",
    "LocalBaselineStore",
    # Validator
    "ScannerValidator",
]
