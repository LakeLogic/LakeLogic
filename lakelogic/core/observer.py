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

    LineageLogic SaaS users: This enables Weekly Trust Reports and quality dashboards.
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

        payload = {
            "run_id": report.get("run_id"),
            "contract": report.get("contract"),
            "engine": report.get("engine"),
            "timestamp": report.get("timestamp"),
            "metrics": {
                "total": report.get("counts", {}).get("total"),
                "quarantined": report.get("counts", {}).get("quarantined"),
                "ratio": report.get("counts", {}).get("quarantine_ratio"),
            },
        }

        try:
            # We use a short timeout to not block the ETL pipeline
            with httpx.Client(timeout=2.0) as client:
                response = client.post(
                    self.api_url,
                    json=payload,
                    headers={"X-LakeLogic-Version": __version__},
                )
                if response.status_code == 200:
                    logger.debug("Successfully reported metrics to LineageLogic")
        except Exception as e:
            # Silent fail to ensure ETL process continues regardless of internet/SaaS status
            logger.debug(f"Remote reporting skipped: {e}")
