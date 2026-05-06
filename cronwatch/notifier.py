"""Webhook notifier for cronwatch alerts (Slack / generic HTTP)."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from cronwatch.alerter import AlertEvent

log = logging.getLogger(__name__)


@dataclass
class WebhookConfig:
    url: str
    method: str = "POST"
    headers: Dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})
    timeout: int = 10
    # If True, wrap payload in Slack-compatible {"text": ...} envelope
    slack_format: bool = False


class WebhookNotifier:
    """Send alert events to an HTTP webhook endpoint."""

    def __init__(self, cfg: WebhookConfig) -> None:
        self._cfg = cfg

    # ------------------------------------------------------------------
    def _build_payload(self, event: AlertEvent) -> Dict[str, Any]:
        base: Dict[str, Any] = {
            "job": event.job_name,
            "kind": event.kind,
            "message": event.message,
            "ts": event.ts.isoformat(),
        }
        if self._cfg.slack_format:
            return {"text": f"*[cronwatch]* `{event.job_name}` — {event.message}"}
        return base

    # ------------------------------------------------------------------
    def notify(self, event: AlertEvent) -> bool:
        """POST *event* to the configured webhook.  Returns True on success."""
        payload = json.dumps(self._build_payload(event)).encode()
        req = urllib.request.Request(
            self._cfg.url,
            data=payload,
            headers=self._cfg.headers,
            method=self._cfg.method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self._cfg.timeout) as resp:
                status = resp.status
                log.debug("Webhook %s responded %s for job '%s'", self._cfg.url, status, event.job_name)
                return 200 <= status < 300
        except urllib.error.URLError as exc:
            log.error("Webhook delivery failed for job '%s': %s", event.job_name, exc)
            return False
