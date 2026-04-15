from typing import Optional, Dict

from lakelogic.adapters.dbt import DbtAdapter, load_contract_from_dbt
from lakelogic.core.bootstrap import ContractDraft, ContractInferrer, infer_contract
from lakelogic.core.describe_columns import describe_columns
from lakelogic.core.generator import DataGenerator
from lakelogic.core.incremental import Boundary, IncrementalBoundary
from lakelogic.core.models import (
    DataContract,
    FieldDefinition,
    QualityRule,
    TIER_CANONICAL_MAP as TIER_CANONICAL_MAP,
    Transformation,
)
from lakelogic.core.processor import DataProcessor
from lakelogic.core.schema_api import (
    ValidationError,
    ValidationResult,
    contract_schema,
    contract_schema_json,
    validate_contract,
)
from lakelogic.engines.cloud_credentials import (
    CloudCredentialResolver,
    DatabricksSecretResolver,
    from_databricks,
    resolve_storage_options,
)
from lakelogic.engines.base import ENGINE_DIALECT_MAP
from lakelogic.engines.generic_sql import GenericSQLAdapter

__version__ = "1.16.0"


class HelpTopic:
    """
    Help topic helper (dbutils-style).
    """

    def __init__(self, name: str, text: str) -> None:
        self.name = name
        self._text = text

    def help(self) -> None:
        print(self._text)

    def __call__(self) -> None:
        self.help()


class HelpIndex:
    """
    Top-level help dispatcher (dbutils-style).
    """

    def __init__(self, topics: Dict[str, HelpTopic]) -> None:
        self._topics = topics

    def __getattr__(self, name: str) -> HelpTopic:
        if name in self._topics:
            return self._topics[name]
        raise AttributeError(name)

    def __call__(self, topic: Optional[str] = None, full: bool = False) -> None:
        base = """LakeLogic Help

Topics:
  run         Run a contract against a source file.
  bootstrap   Generate contracts and a registry from a landing zone.
  driver      Registry-driven pipeline driver (Bronze -> Silver -> Gold).
  import_dbt  Import dbt schema.yml / sources.yml as LakeLogic contracts.

Examples:
  lakelogic.help()
  lakelogic.help("bootstrap")
  lakelogic.help("import_dbt")
  lakelogic.driver.help()
"""
        if full:
            print(base)
            for item in self._topics.values():
                print(item._text)
            return
        if not topic:
            print(base)
            return
        topic = topic.lower()
        if topic in self._topics:
            print(self._topics[topic]._text)
            return
        print(base)


_driver_text = """LakeLogic Driver Help

Examples:
  lakelogic-driver --registry contracts/_registry.yaml --layers bronze
  lakelogic-driver --window range --window-start-date 2026-02-01 --window-end-date 2026-02-05
  lakelogic-driver --policy-pack baseline_silver --policy-pack-dir policy_packs

Flags:
  --set                      Override contract fields at runtime (repeatable).
  --policy-pack              Apply a policy pack by name.
  --policy-pack-dir          Directory containing policy packs.
  --state-path               State file for partial resume.
  --resume                   Resume from last successful state.
  --retries                  Retry count for transient failures.
  --retry-backoff            Initial retry backoff in seconds.
  --retry-max-delay          Max retry delay in seconds.
  --approval-required        Require approvals on drift/quarantine thresholds.
  --approval-file            Approval file path to bypass approval gates.
  --cache-references         Cache reference datasets across runs.
  --backfill-start-date      Backfill start date (YYYY-MM-DD).
  --backfill-end-date        Backfill end date (YYYY-MM-DD).
  --backfill-granularity     Backfill granularity (day or week).
"""

_bootstrap_text = """LakeLogic Bootstrap Help

Example:
  lakelogic bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml

Sync mode:
  lakelogic bootstrap --landing data/landing --output-dir contracts/new \
    --registry contracts/new/_registry.yaml --sync
  lakelogic bootstrap --landing data/landing --output-dir contracts/new \
    --registry contracts/new/_registry.yaml --sync --sync-update-schema
  lakelogic bootstrap --landing data/landing --output-dir contracts/new \
    --registry contracts/new/_registry.yaml --sync --sync-overwrite

Flags:
  --sync               Add new entities to the registry without changing existing contracts.
  --sync-update-schema Append newly discovered columns to existing contracts.
  --sync-overwrite     Regenerate existing contracts from landing files.
"""

_run_text = """LakeLogic Run Help

Example:
  lakelogic run --contract contract.yaml --source data.csv
"""

_policy_pack_text = """LakeLogic Policy Packs Help

Apply standardized rules and defaults across contracts.

Example:
  lakelogic-driver --policy-pack baseline_silver --policy-pack-dir policy_packs
"""

_observability_text = """LakeLogic Observability Help

Examples:
  lakelogic-driver --summary-table lakelogic.pipeline_runs --summary-backend duckdb
  lakelogic-driver --metrics-backend prometheus --metrics-host 0.0.0.0 --metrics-port 9100
"""

_import_dbt_text = """LakeLogic import-dbt Help

Import dbt schema.yml / sources.yml into LakeLogic contracts.

Examples:
  lakelogic import-dbt --schema models/schema.yml --model customers --output contracts/
  lakelogic import-dbt --schema models/schema.yml --output contracts/          # all models
  lakelogic import-dbt --schema models/sources.yml --source-name raw --source-table orders --output contracts/
  lakelogic import-dbt --schema models/schema.yml --model customers --dry-run  # preview

Python API:
  from lakelogic import DataProcessor, DataGenerator
  proc = DataProcessor.from_dbt("models/schema.yml", model="customers")
  gen  = DataGenerator.from_dbt("models/schema.yml", model="customers")
"""

help = HelpIndex(
    {
        "driver": HelpTopic("driver", _driver_text),
        "bootstrap": HelpTopic("bootstrap", _bootstrap_text),
        "run": HelpTopic("run", _run_text),
        "policy_packs": HelpTopic("policy_packs", _policy_pack_text),
        "observability": HelpTopic("observability", _observability_text),
        "import_dbt": HelpTopic("import_dbt", _import_dbt_text),
    }
)

# dbutils-style submodule access
driver = help.driver
bootstrap = help.bootstrap
run = help.run
policy_packs = help.policy_packs
observability = help.observability


__all__ = [
    "DataProcessor",
    "DataGenerator",
    "DataContract",
    "FieldDefinition",
    "QualityRule",
    "Transformation",
    "IncrementalBoundary",
    "Boundary",
    "DbtAdapter",
    "load_contract_from_dbt",
    # Contract inference
    "infer_contract",
    "ContractInferrer",
    "ContractDraft",
    "describe_columns",
    # Schema API
    "validate_contract",
    "contract_schema",
    "contract_schema_json",
    "ValidationResult",
    "ValidationError",
    # Cloud credentials
    "CloudCredentialResolver",
    "DatabricksSecretResolver",
    "from_databricks",
    "resolve_storage_options",
    # Generic SQL adapter
    "GenericSQLAdapter",
    "ENGINE_DIALECT_MAP",
    # Help
    "help",
    "driver",
    "bootstrap",
    "run",
    "policy_packs",
    "observability",
]
