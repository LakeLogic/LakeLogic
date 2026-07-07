"""
Apprise-based universal notification adapter for LakeLogic.

Apprise supports 90+ notification services through URL-based configuration.
This adapter wraps Apprise to provide a drop-in replacement for the
hand-rolled SMTP, Slack, Teams, SendGrid, and Webhook adapters, while also
unlocking every other channel Apprise supports (Discord, PagerDuty,
Telegram, Pushover, etc.).

Contract YAML usage::

    quarantine:
      notifications:
        # Single target
        - type: apprise
          target: "msteams://webhook_url"
          on_events: [quarantine, failure]

        # Multiple targets — fan-out to several channels at once
        - type: apprise
          targets:
            - "mailto://user:pass@smtp.gmail.com?to=alerts@co.com"
            - "slack://token_a/token_b/token_c/#channel"
            - "json://monitoring.internal/api/alerts"
          on_events: [failure, sla_breach]

        # With Jinja2 templates
        - type: apprise
          target: "slack://token/#channel"
          on_events: [quarantine]
          subject_template: "[{{ event | upper }}] {{ contract.title }}"
          message_template: |
            *{{ contract.title }}* quarantined rows.
            Run: `{{ run_id }}` | Engine: {{ engine }}

Apprise URL reference: https://github.com/caronc/apprise/wiki
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.notifications.base import NotificationAdapter, _resolve_value


class AppriseAdapter(NotificationAdapter):
    """
    Universal notification adapter powered by `Apprise <https://github.com/caronc/apprise>`_.

    Resolves ``target`` (single URL) or ``targets`` (list of URLs) from the
    notification config, creates an ``apprise.Apprise`` instance, and sends
    the rendered message + subject to all configured channels.

    Each target URL can use any scheme Apprise supports — ``mailto://``,
    ``slack://``, ``msteams://``, ``discord://``, ``json://``, etc.

    Secret placeholders (``env:``, ``keyvault:``, ``aws:``, ``gcp:``,
    ``vault:``, ``local:``) in target URLs are resolved before being added
    to Apprise.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._targets = self._collect_targets(config)

    # ------------------------------------------------------------------
    # Target resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_targets(config: Dict[str, Any]) -> List[str]:
        """
        Build a flat list of Apprise URLs from the config.

        Supports both ``target`` (single string) and ``targets`` (list).
        Each value is run through the LakeLogic secret resolver so
        ``env:SLACK_WEBHOOK`` or ``keyvault:my-webhook`` work transparently.
        """
        raw_targets: List[str] = []

        single = config.get("target")
        if single:
            raw_targets.append(str(single))

        multi = config.get("targets")
        if isinstance(multi, list):
            raw_targets.extend(str(t) for t in multi if t)

        if not raw_targets:
            raise ValueError(
                "AppriseAdapter requires at least one target URL. "
                "Set 'target' (single) or 'targets' (list) in your notification config."
            )

        # Resolve secret placeholders in each URL
        resolved: List[str] = []
        for url in raw_targets:
            val = _resolve_value(url, config)
            if val:
                resolved.append(str(val))
            else:
                logger.warning(f"Apprise target resolved to empty — skipping: {url}")

        return resolved

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    def send(
        self,
        message: str,
        subject: str = "LakeLogic Alert",
        *,
        notify_type: Optional[str] = None,
        attach: Optional[List[str]] = None,
    ) -> None:
        """
        Send a notification through all configured Apprise channels.

        Args:
            message: Rendered notification body (plain text or markdown).
            subject: Rendered notification subject / title.
            notify_type: Apprise notify type override — ``info``, ``success``,
                ``warning``, or ``failure``. Auto-detected from subject if omitted.
            attach: Optional list of file paths to attach (e.g. run-log JSON).
        """
        try:
            import apprise
        except ImportError as exc:
            raise ImportError(
                "Apprise is required for the 'apprise' notification type. "
                "Install with: pip install 'lakelogic[notifications]'"
            ) from exc

        ap = apprise.Apprise()

        for url in self._targets:
            ap.add(url)

        # Auto-detect notify type from event keywords in subject
        if notify_type is None:
            _subj = subject.lower()
            if any(kw in _subj for kw in ("fail", "error", "breach")):
                notify_type = apprise.NotifyType.FAILURE
            elif any(kw in _subj for kw in ("warn", "quarantine", "drift")):
                notify_type = apprise.NotifyType.WARNING
            elif any(kw in _subj for kw in ("success", "complet")):
                notify_type = apprise.NotifyType.SUCCESS
            else:
                notify_type = apprise.NotifyType.INFO

        # Build attach list if provided
        ap_attach = None
        if attach:
            ap_attach = apprise.AppriseAttachment()
            for path in attach:
                ap_attach.add(path)

        logger.info(f"Sending Apprise notification to {len(self._targets)} target(s) [type={notify_type}]")

        result = ap.notify(
            title=subject,
            body=message,
            notify_type=notify_type,
            attach=ap_attach,
        )

        if not result:
            logger.warning(
                "Apprise returned failure for one or more targets. Check target URLs and network connectivity."
            )
