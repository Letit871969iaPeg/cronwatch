"""Tests for WebhookNotifier."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from cronwatch.alerter import AlertEvent
from cronwatch.notifier import WebhookConfig, WebhookNotifier
from cronwatch.notifier_config import load_webhook_notifiers


EVENT = AlertEvent(
    job_name="backup",
    kind="drift",
    message="Job ran 120s late",
    ts=datetime(2024, 6, 1, 3, 0, 0, tzinfo=timezone.utc),
)


@pytest.fixture()
def cfg() -> WebhookConfig:
    return WebhookConfig(url="https://example.com/hook")


@pytest.fixture()
def notifier(cfg: WebhookConfig) -> WebhookNotifier:
    return WebhookNotifier(cfg)


# ---------------------------------------------------------------------------

def _mock_response(status: int = 200):
    resp = MagicMock()
    resp.status = status
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def test_notify_success(notifier: WebhookNotifier) -> None:
    with patch("urllib.request.urlopen", return_value=_mock_response(200)) as mock_open:
        result = notifier.notify(EVENT)
    assert result is True
    mock_open.assert_called_once()


def test_notify_server_error(notifier: WebhookNotifier) -> None:
    with patch("urllib.request.urlopen", return_value=_mock_response(500)):
        result = notifier.notify(EVENT)
    assert result is False


def test_notify_network_error(notifier: WebhookNotifier) -> None:
    import urllib.error
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("timeout")):
        result = notifier.notify(EVENT)
    assert result is False


def test_slack_format_payload() -> None:
    cfg = WebhookConfig(url="https://hooks.slack.com/x", slack_format=True)
    n = WebhookNotifier(cfg)
    payload = n._build_payload(EVENT)
    assert "text" in payload
    assert "backup" in payload["text"]


def test_standard_payload(notifier: WebhookNotifier) -> None:
    payload = notifier._build_payload(EVENT)
    assert payload["job"] == "backup"
    assert payload["kind"] == "drift"
    assert "message" in payload
    assert "ts" in payload


def test_load_webhook_notifiers_empty() -> None:
    notifiers = load_webhook_notifiers({})
    assert notifiers == []


def test_load_webhook_notifiers_multiple() -> None:
    raw = {
        "webhooks": [
            {"url": "https://a.example.com/hook"},
            {"url": "https://b.example.com/hook", "slack_format": True, "timeout": 5},
        ]
    }
    notifiers = load_webhook_notifiers(raw)
    assert len(notifiers) == 2
    assert notifiers[1]._cfg.slack_format is True
    assert notifiers[1]._cfg.timeout == 5
