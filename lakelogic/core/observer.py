import httpx
import os
from typing import Dict, Any, Optional
from loguru import logger

# Avoid circular import by defining version here
__version__ = "0.1.0"


class RemoteObserver:
    """
    Sends run metadata to a remote endpoint for centralized observability.

    This is completely optional and disabled by default. To enable:
    - Set LAKELOGIC_REMOTE_OBSERVER=true
    - Set LINEAGELOGIC_REPORT_URL to your endpoint
    - Optionally set LINEAGELOGIC_API_KEY for authentication

    LakeLogic SaaS users: This enables Weekly Trust Reports and quality dashboards.
    """

    def __init__(self, api_url: Optional[str] = None):
        self.enabled = os.getenv("LAKELOGIC_REMOTE_OBSERVER", "false").lower() == "true"
        self.api_url = os.getenv("LINEAGELOGIC_REPORT_URL", api_url)
        self.api_key = os.getenv("LINEAGELOGIC_API_KEY")  # Optional

    def report(self, report: Dict[str, Any]):
        """
        Sends the run report to the remote endpoint (opt-in only).
        """
        # Skip if remote observer not enabled
        if not self.enabled:
            return

        # Skip if offline mode enabled (legacy support)
        if os.getenv("LAKELOGIC_OFFLINE", "false").lower() == "true":
            return

        # Skip if no endpoint configured
        if not self.api_url:
            logger.debug("Remote observer enabled but LINEAGELOGIC_REPORT_URL not set")
            return

        counts = report.get("counts", {})
        slos = report.get("slos", {})

        payload = {
            # Identity
            "run_id": report.get("run_id"),
            "pipeline_run_id": report.get("pipeline_run_id"),
            "contract": report.get("contract"),
            "dataset": report.get("dataset"),
            "stage": report.get("stage"),
            "engine": report.get("engine"),
            "timestamp": report.get("timestamp"),
            # Metadata
            "domain": report.get("domain"),
            "system": report.get("system"),
            "data_layer": report.get("data_layer"),
            "source_path": report.get("source_path"),
            # Row counts
            "metrics": {
                "source": counts.get("source"),
                "total": counts.get("total"),
                "good": counts.get("good"),
                "quarantined": counts.get("quarantined"),
                "pre_transform_dropped": counts.get("pre_transform_dropped"),
                "aggregated_rows": counts.get("aggregated_rows"),
                "ratio": counts.get("quarantine_ratio"),
            },
            # Per-rule failures
            "row_rule_failures": report.get("row_rule_failures", []),
            "dataset_rules": report.get("dataset_rules", []),
            # Schema drift
            "schema_drift": report.get("schema_drift", {}),
            # SLOs
            "slos": slos,
            # Duration
            "duration_ms": report.get("duration_ms"),
            # Cost observability
            "cost": {
                "estimated": report.get("estimated_cost"),
                "currency": report.get("cost_currency"),
                "confidence": report.get("cost_confidence"),
            },
        }

        try:
            # We use a short timeout to not block the ETL pipeline
            headers = {"X-LakeLogic-Version": __version__}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            with httpx.Client(timeout=2.0) as client:
                response = client.post(
                    self.api_url,
                    json=payload,
                    headers=headers,
                )
                if response.status_code == 200:
                    logger.debug("Successfully reported metrics to LakeLogic")
        except Exception as e:
            # Silent fail to ensure ETL process continues regardless of internet/SaaS status
            logger.debug(f"Remote reporting skipped: {e}")
