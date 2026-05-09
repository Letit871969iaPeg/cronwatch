"""Tests for cronwatch.job_retry and cronwatch.retry_config."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.job_retry import RetryManager, RetryPolicy
from cronwatch.retry_config import load_retry_policies
from cronwatch.tracker import JobTracker


@pytest.fixture()
def alerter() -> MagicMock:
    return MagicMock(spec=Alerter)


@pytest.fixture()
def tracker() -> MagicMock:
    return MagicMock(spec=JobTracker)


@pytest.fixture()
def manager(alerter: MagicMock, tracker: MagicMock) -> RetryManager:
    return RetryManager(alerter=alerter, tracker=tracker)


# ---------------------------------------------------------------------------
# RetryManager tests
# ---------------------------------------------------------------------------

def test_success_resets_failure_count(manager: RetryManager) -> None:
    manager.set_policy("job_a", RetryPolicy(max_retries=3))
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=True)
    assert manager.failure_count("job_a") == 0


def test_no_alert_below_threshold(manager: RetryManager, alerter: MagicMock) -> None:
    manager.set_policy("job_a", RetryPolicy(max_retries=3))
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=False)
    alerter.send.assert_not_called()


def test_alert_at_threshold(manager: RetryManager, alerter: MagicMock) -> None:
    manager.set_policy("job_a", RetryPolicy(max_retries=3))
    for _ in range(3):
        manager.record_outcome("job_a", success=False)
    alerter.send.assert_called_once()
    event: AlertEvent = alerter.send.call_args[0][0]
    assert event.job_name == "job_a"
    assert event.event_type == "retry_exhausted"


def test_alert_fires_only_once_when_exhausted(manager: RetryManager, alerter: MagicMock) -> None:
    manager.set_policy("job_a", RetryPolicy(max_retries=2))
    for _ in range(5):
        manager.record_outcome("job_a", success=False)
    assert alerter.send.call_count == 1


def test_alert_resets_after_success(manager: RetryManager, alerter: MagicMock) -> None:
    manager.set_policy("job_a", RetryPolicy(max_retries=2))
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=True)
    # Fail again — should alert again
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=False)
    assert alerter.send.call_count == 2


def test_no_policy_no_alert(manager: RetryManager, alerter: MagicMock) -> None:
    for _ in range(10):
        manager.record_outcome("unknown_job", success=False)
    alerter.send.assert_not_called()


def test_is_exhausted_true(manager: RetryManager) -> None:
    manager.set_policy("job_a", RetryPolicy(max_retries=2))
    manager.record_outcome("job_a", success=False)
    manager.record_outcome("job_a", success=False)
    assert manager.is_exhausted("job_a") is True


def test_is_exhausted_false_no_policy(manager: RetryManager) -> None:
    assert manager.is_exhausted("no_policy_job") is False


# ---------------------------------------------------------------------------
# load_retry_policies tests
# ---------------------------------------------------------------------------

def test_load_retry_policies_basic() -> None:
    data = {
        "jobs": [
            {"name": "job_a", "retry": {"max_retries": 5, "alert_on_exhaustion": True}},
            {"name": "job_b"},
        ]
    }
    policies = load_retry_policies(data)
    assert "job_a" in policies
    assert policies["job_a"].max_retries == 5
    assert "job_b" not in policies


def test_load_retry_policies_defaults() -> None:
    data = {"jobs": [{"name": "job_a", "retry": {}}]}
    policies = load_retry_policies(data)
    assert policies["job_a"].max_retries == 3
    assert policies["job_a"].alert_on_exhaustion is True


def test_load_retry_policies_empty_jobs() -> None:
    assert load_retry_policies({"jobs": []}) == {}


def test_load_retry_policies_missing_name_skipped() -> None:
    data = {"jobs": [{"retry": {"max_retries": 2}}]}
    assert load_retry_policies(data) == {}
