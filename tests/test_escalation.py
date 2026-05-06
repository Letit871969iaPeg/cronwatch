"""Tests for EscalationManager."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.escalation import EscalationManager, EscalationPolicy


UTC = timezone.utc


def _event(job_name: str = "backup", kind: str = "FAILURE") -> AlertEvent:
    return AlertEvent(
        job_name=job_name,
        kind=kind,
        message="job failed",
        timestamp=datetime(2024, 1, 1, 2, 0, tzinfo=UTC),
        duration_seconds=None,
    )


@pytest.fixture()
def alerter() -> MagicMock:
    return MagicMock(spec=Alerter)


@pytest.fixture()
def manager(alerter: MagicMock) -> EscalationManager:
    mgr = EscalationManager(alerter)
    mgr.set_policy("backup", EscalationPolicy(threshold=3, repeat_every=2))
    return mgr


def test_no_escalation_below_threshold(manager, alerter):
    for _ in range(2):
        result = manager.record_failure("backup", _event())
        assert result is False
    alerter.send.assert_not_called()


def test_escalation_at_threshold(manager, alerter):
    for _ in range(3):
        manager.record_failure("backup", _event())
    assert alerter.send.call_count == 1
    sent: AlertEvent = alerter.send.call_args[0][0]
    assert "ESCALATED" in sent.kind
    assert "x3" in sent.message


def test_repeat_every_respected(manager, alerter):
    for _ in range(5):
        manager.record_failure("backup", _event())
    # threshold=3 → escalate at 3; repeat_every=2 → escalate again at 5
    assert alerter.send.call_count == 2


def test_no_extra_alert_between_repeats(manager, alerter):
    for _ in range(4):
        manager.record_failure("backup", _event())
    # escalated at 3, not yet at 5
    assert alerter.send.call_count == 1


def test_success_resets_streak(manager, alerter):
    for _ in range(3):
        manager.record_failure("backup", _event())
    manager.record_success("backup")
    assert manager.consecutive_failures("backup") == 0
    alerter.send.reset_mock()
    # Two more failures should NOT escalate (below threshold again)
    for _ in range(2):
        manager.record_failure("backup", _event())
    alerter.send.assert_not_called()


def test_default_policy_applied_for_unknown_job(alerter):
    mgr = EscalationManager(alerter)
    # default threshold=3
    for _ in range(3):
        mgr.record_failure("unknown_job", _event("unknown_job"))
    assert alerter.send.call_count == 1


def test_consecutive_failures_counter(manager):
    assert manager.consecutive_failures("backup") == 0
    manager.record_failure("backup", _event())
    assert manager.consecutive_failures("backup") == 1
