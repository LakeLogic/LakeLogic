import smtplib
from email.mime.text import MIMEText
from loguru import logger


def send_email_alert(
    subject: str,
    html_message: str,
    recipients: list[str],
    smtp_server: str = "smtp.office365.com",
    smtp_port: int = 587,
    smtp_username: str = "noreply@dataplatform.valstream.com",
    smtp_password: str = "app_password",
) -> None:
    """
    Send an HTML-formatted email notification via SMTP.

    This function is designed for use within Databricks or Python-based
    data pipelines to send alerts such as pipeline failures, success confirmations,
    or data quality warnings. It uses Office 365 SMTP with TLS encryption by default.

    Parameters
    ----------
    subject : str
        The email subject line.
        Example: `"Pipeline Failure - engine_status"`

    html_message : str
        The HTML-formatted message body. Supports inline styling and emojis.
        Example:
        ```html
        <h3>🚨 Databricks Pipeline Alert</h3>
        <p><strong>Status:</strong> Failed during Silver transformation</p>
        <p><strong>Dataset:</strong> engine_status</p>
        <p>Check the quarantine table for details.</p>
        ```

    recipients : list[str]
        List of email addresses to receive the alert.
        Example: `["ce@valstream.com", "dataops@valstream.com"]`

    smtp_server : str, optional
        SMTP server address. Defaults to `"smtp.office365.com"`.

    smtp_port : int, optional
        SMTP port number. Defaults to `587` (TLS).

    smtp_username : str, optional
        Sender email address used to authenticate with the SMTP server.
        Defaults to `"noreply@dataplatform.valstream.com"`.

    smtp_password : str, optional
        Password or app-specific token for SMTP authentication.
        It is **strongly recommended** to retrieve this securely from
        Databricks Secret Scopes or Azure Key Vault, not hardcoded.

    Returns
    -------
    None
        This function prints a confirmation message when the email is sent successfully.

    Raises
    ------
    smtplib.SMTPAuthenticationError
        If authentication fails due to incorrect username or password.
    smtplib.SMTPConnectError
        If the connection to the SMTP server cannot be established.
    Exception
        Any other error that occurs during email composition or sending.

    Example
    -------
    >>> html_msg = '''
    ... <h3>✅ Pipeline Completed</h3>
    ... <p>Dataset: engine_status</p>
    ... <p>Status: Success</p>
    ... <p>Time: 2025-10-09 16:05 UTC</p>
    ... '''
    >>> send_email_alert(
    ...     subject="✅ Pipeline Success - engine_status",
    ...     html_message=html_msg,
    ...     recipients=["ce@valstream.com", "ops@valstream.com"]
    ... )
    📧 Email alert sent: ✅ Pipeline Success - engine_status

    Notes
    -----
    - All traffic is encrypted using TLS (`starttls()`).
    - Store credentials securely using Databricks Secrets or Azure Key Vault.
    - For multiple recipients, pass a list of addresses.
    - HTML formatting is supported; plain text can also be used if preferred.
    """

    msg = MIMEText(html_message, "html")
    msg["Subject"] = subject
    msg["From"] = smtp_username
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(msg["From"], recipients, msg.as_string())
        logger.info(f"📧 Email alert sent: {subject}")
