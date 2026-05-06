"""Parse webhook notifier configuration from the main CronwatchConfig dict."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from cronwatch.notifier import WebhookConfig, WebhookNotifier


def load_webhook_notifiers(raw: Dict[str, Any]) -> List[WebhookNotifier]:
    """Build :class:`WebhookNotifier` instances from the 'webhooks' section.

    Expected YAML shape::

        webhooks:
          - url: https://hooks.slack.com/services/XXX
            slack_format: true
          - url: https://example.com/cronwatch
            headers:
              Authorization: Bearer secret
            timeout: 5
    """
    entries = raw.get("webhooks", []) or []
    notifiers: List[WebhookNotifier] = []
    for entry in entries:
        cfg = WebhookConfig(
            url=entry["url"],
            method=entry.get("method", "POST"),
            headers=entry.get("headers", {"Content-Type": "application/json"}),
            timeout=int(entry.get("timeout", 10)),
            slack_format=bool(entry.get("slack_format", False)),
        )
        notifiers.append(WebhookNotifier(cfg))
    return notifiers
