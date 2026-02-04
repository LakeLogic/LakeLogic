from abc import ABC, abstractmethod
from typing import Any, Dict
from loguru import logger

class NotificationAdapter(ABC):
    """
    Base class for all notification adapters.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def send(self, message: str, subject: str = "LakeGuard Alert"):
        pass

class SMTPAdapter(NotificationAdapter):
    def send(self, message: str, subject: str = "LakeGuard Alert"):
        logger.info(f"📧 Sending SMTP email to {self.config.get('target')} [Subject: {subject}]")
        # In a real implementation:
        # import smtplib
        # ... send email ...

class SendGridAdapter(NotificationAdapter):
    def send(self, message: str, subject: str = "LakeGuard Alert"):
        logger.info(f"⚡ Sending SendGrid email to {self.config.get('target')} [Subject: {subject}]")
        # In a real implementation:
        # from sendgrid import SendGridAPIClient
        # ...

class SlackAdapter(NotificationAdapter):
    def send(self, message: str, subject: str = "LakeGuard Alert"):
        logger.info(f"💬 Sending Slack message to {self.config.get('target')}")

class TeamsAdapter(NotificationAdapter):
    def send(self, message: str, subject: str = "LakeGuard Alert"):
        logger.info(f"👥 Sending Teams message to {self.config.get('target')}")

class WebhookAdapter(NotificationAdapter):
    def send(self, message: str, subject: str = "LakeGuard Alert"):
        logger.info(f"🔗 Sending Webhook to {self.config.get('target')}")

def get_notification_adapter(notif_type: str, config: Dict[str, Any]) -> NotificationAdapter:
    adapters = {
        "smtp": SMTPAdapter,
        "sendgrid": SendGridAdapter,
        "slack": SlackAdapter,
        "teams": TeamsAdapter,
        "email": SMTPAdapter, # fallback
        "webhook": WebhookAdapter
    }
    adapter_class = adapters.get(notif_type.lower())
    if not adapter_class:
        raise ValueError(f"Unsupported notification type: {notif_type}")
    return adapter_class(config)
