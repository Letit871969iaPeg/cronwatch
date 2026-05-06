"""Alert dispatcher for cronwatch — sends notifications on job drift or failure."""

import logging
import smtplib
from email.message import EmailMessage
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class AlertEvent:
    job_name: str
    reason: str  # 'failure' | 'drift' | 'missing'
    details: str
    severity: str = "warning"  # 'warning' | 'critical'


@dataclass
class SmtpConfig:
    host: str
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = True
    from_addr: str = "cronwatch@localhost"
    to_addrs: list = field(default_factory=list)


class Alerter:
    """Dispatches alerts via configured channels."""

    def __init__(self, smtp_config: Optional[SmtpConfig] = None):
        self.smtp_config = smtp_config

    def send(self, event: AlertEvent) -> None:
        """Send an alert for the given event through all configured channels."""
        logger.warning(
            "[cronwatch] ALERT job=%s reason=%s severity=%s — %s",
            event.job_name,
            event.reason,
            event.severity,
            event.details,
        )
        if self.smtp_config and self.smtp_config.to_addrs:
            self._send_email(event)

    def _send_email(self, event: AlertEvent) -> None:
        cfg = self.smtp_config
        msg = EmailMessage()
        msg["Subject"] = (
            f"[cronwatch/{event.severity.upper()}] {event.job_name}: {event.reason}"
        )
        msg["From"] = cfg.from_addr
        msg["To"] = ", ".join(cfg.to_addrs)
        msg.set_content(
            f"Job:      {event.job_name}\n"
            f"Reason:   {event.reason}\n"
            f"Severity: {event.severity}\n\n"
            f"{event.details}\n"
        )
        try:
            with smtplib.SMTP(cfg.host, cfg.port) as smtp:
                if cfg.use_tls:
                    smtp.starttls()
                if cfg.username and cfg.password:
                    smtp.login(cfg.username, cfg.password)
                smtp.send_message(msg)
            logger.info("Alert email sent for job '%s'", event.job_name)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to send alert email: %s", exc)
