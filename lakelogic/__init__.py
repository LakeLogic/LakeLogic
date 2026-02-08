from typing import Optional, Dict

from lakeguard.core.processor import DataProcessor
from lakeguard.core.models import DataContract, FieldDefinition, QualityRule, Transformation

__version__ = "0.1.0"


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
        base = """LakeGuard Help

Topics:
  run         Run a contract against a source file.
  bootstrap   Generate contracts and a registry from a landing zone.
  driver      Registry-driven pipeline driver (Bronze -> Silver -> Gold).

Examples:
  lakeguard.help()
  lakeguard.help("bootstrap")
  lakeguard.driver.help()
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


_driver_text = """LakeGuard Driver Help

Examples:
  lakeguard-driver --registry contracts/_registry.yaml --layers bronze
  lakeguard-driver --window range --window-start-date 2026-02-01 --window-end-date 2026-02-05
  lakeguard-driver --policy-pack baseline_silver --policy-pack-dir policy_packs

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

_bootstrap_text = """LakeGuard Bootstrap Help

Example:
  lakeguard bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml

Sync mode:
  lakeguard bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml --sync
  lakeguard bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml --sync --sync-update-schema
  lakeguard bootstrap --landing data/landing --output-dir contracts/new --registry contracts/new/_registry.yaml --sync --sync-overwrite

Flags:
  --sync               Add new entities to the registry without changing existing contracts.
  --sync-update-schema Append newly discovered columns to existing contracts.
  --sync-overwrite     Regenerate existing contracts from landing files.
"""

_run_text = """LakeGuard Run Help

Example:
  lakeguard run --contract contract.yaml --source data.csv
"""

_policy_pack_text = """LakeGuard Policy Packs Help

Apply standardized rules and defaults across contracts.

Example:
  lakeguard-driver --policy-pack baseline_silver --policy-pack-dir policy_packs
"""

_observability_text = """LakeGuard Observability Help

Examples:
  lakeguard-driver --summary-table lakeguard.pipeline_runs --summary-backend duckdb
  lakeguard-driver --metrics-backend prometheus --metrics-host 0.0.0.0 --metrics-port 9100
"""

help = HelpIndex(
    {
        "driver": HelpTopic("driver", _driver_text),
        "bootstrap": HelpTopic("bootstrap", _bootstrap_text),
        "run": HelpTopic("run", _run_text),
        "policy_packs": HelpTopic("policy_packs", _policy_pack_text),
        "observability": HelpTopic("observability", _observability_text),
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
    "DataContract",
    "FieldDefinition",
    "QualityRule",
    "Transformation",
    "help",
    "driver",
    "bootstrap",
    "run",
    "policy_packs",
    "observability",
]
