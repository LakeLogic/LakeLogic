# lib_platform_helpers/notifications.py

import requests
import json
from datetime import datetime
from loguru import logger


def send_teams_alert(
    webhook_url: str, title: str, message: str, alert_type: str = "info"
) -> None:
    """
    Send a formatted alert message to a Microsoft Teams channel via an Incoming Webhook.

    This utility is typically used within Databricks or data pipeline scripts to notify
    a Teams channel about job events such as pipeline failures, data quality warnings,
    or successful job completions. The message supports Markdown formatting and basic
    styling through Teams' MessageCard schema.

    Parameters
    ----------
    webhook_url : str
        The Microsoft Teams Incoming Webhook URL for the target channel.
        Example:
            "https://outlook.office.com/webhook/your-webhook-guid"

    title : str
        The title or headline of the alert message.
        Example:
            "Pipeline Failure - engine_status"

    message : str
        The main body of the Teams message.
        Supports Markdown and emoji.
        Example:
            "### 🚨 Databricks Pipeline Alert\n"
            "- **Dataset:** engine_status\n"
            "- **Status:** Failed during Silver transformation\n"
            "- **Time:** 2025-10-09 16:10 UTC"

    alert_type : str, optional
        The severity level of the alert.
        Determines the color of the Teams message card.
        Supported values:
            - `"info"` (default) → Blue
            - `"warning"` → Orange
            - `"error"` → Red
            - `"success"` → Green

    Returns
    -------
    None
        This function logs the result of the Teams API call (success or failure).

    Raises
    ------
    requests.exceptions.RequestException
        If the POST request to Teams fails (e.g., invalid URL, connection error).

    Example
    -------
    >>> from lib_platform_helpers.notifications import send_teams_alert
    >>> teams_url = "https://outlook.office.com/webhook/..."
    >>> msg = (
    ...     "### ✅ Pipeline Completed\n"
    ...     "- **Dataset:** engine_status\n"
    ...     "- **Environment:** dev\n"
    ...     "- **Time:** 2025-10-09 16:25 UTC"
    ... )
    >>> send_teams_alert(
    ...     webhook_url=teams_url,
    ...     title="✅ Pipeline Success - engine_status",
    ...     message=msg,
    ...     alert_type="success"
    ... )
    INFO | ✅ Teams alert sent: ✅ Pipeline Success - engine_status

    Notes
    -----
    - Microsoft Teams Incoming Webhooks must be configured in the target channel first.
    - Message payload follows the [MessageCard schema](https://docs.microsoft.com/en-us/outlook/actionable-messages/message-card-reference).
    - The function uses `requests.post()` and logs outcomes using the `logging` module.
    - Exceptions are caught and logged, not re-raised, to prevent job termination.
    """

    color_map = {
        "info": "0078D7",  # Blue
        "warning": "FFA500",  # Orange
        "error": "FF0000",  # Red
        "success": "008000",  # Green
    }

    payload = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": color_map.get(alert_type.lower(), "0078D7"),
        "summary": title,
        "sections": [
            {
                "activityTitle": f"**{title}**",
                "activitySubtitle": f"_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_",
                "text": message,
            }
        ],
    }

    try:
        response = requests.post(
            webhook_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=10,
        )
        response.raise_for_status()
        logger.info(f"✅ Teams alert sent: {title}")
    except requests.exceptions.RequestException as e:
        logger.error(f"⚠️ Failed to send Teams alert ({alert_type.upper()}): {e}")
