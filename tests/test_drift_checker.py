"""Tests for DriftChecker alert logic."""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.config import JobConfig
from cronwatch.drift_checker import DriftChecker
from cronwatch.tracker import JobRecord


def make_job_cfg(**kwargs):
    defaults = dict(
        name="backup",
        schedule="0 2 * * *",
        expected_duration_seconds=60,
        drift_tolerance_pct=20,
        max_interval_seconds=90000,
    )
    defaults.update(kwargs)
    return JobConfig(**defaults)


def make_record(**kwargs):
    defaults = dict(
        last_start=datetime.now(tz=timezone.utc) - timedelta(seconds=30),
        last_end=datetime.now(tz=timezone.utc),
        last_duration_seconds=55.0,
        last_status="success",
    )
    defaults.update(kwargs)
    return JobRecord(**defaults)


@pytest.fixture()
def alerter():
    return MagicMock(spec=Alerter)


@pytest.fixture()
def tracker():
    return MagicMock()


def test_no_record_emits_missing(tracker, alerter):
    tracker.get.return_value = None
    checker = DriftChecker(tracker, alerter)
    events = checker.check_job(make_job_cfg())
    assert len(events) == 1
    assert events[0].reason == "missing"
    alerter.send.assert_called_once()


def test_failure_status_emits_critical(tracker, alerter):
    tracker.get.return_value = make_record(last_status="failure")
    checker = DriftChecker(tracker, alerter)
    events = checker.check_job(make_job_cfg())
    reasons = {e.reason for e in events}
    assert "failure" in reasons
    severities = {e.severity for e in events}
    assert "critical" in severities


def test_duration_drift_emits_warning(tracker, alerter):
    tracker.get.return_value = make_record(last_duration_seconds=120.0, last_status="success")
    checker = DriftChecker(tracker, alerter)
    events = checker.check_job(make_job_cfg(expected_duration_seconds=60, drift_tolerance_pct=20))
    reasons = {e.reason for e in events}
    assert "drift" in reasons


def test_within_tolerance_no_drift_alert(tracker, alerter):
    tracker.get.return_value = make_record(last_duration_seconds=70.0, last_status="success")
    checker = DriftChecker(tracker, alerter)
    events = checker.check_job(make_job_cfg(expected_duration_seconds=60, drift_tolerance_pct=20))
    assert not any(e.reason == "drift" for e in events)


def test_overdue_job_emits_missing_critical(tracker, alerter):
    old_start = datetime.now(tz=timezone.utc) - timedelta(seconds=200000)
    tracker.get.return_value = make_record(last_start=old_start, last_status="success")
    checker = DriftChecker(tracker, alerter)
    events = checker.check_job(make_job_cfg(max_interval_seconds=86400))
    reasons = {e.reason for e in events}
    assert "missing" in reasons


def test_healthy_job_no_alerts(tracker, alerter):
    tracker.get.return_value = make_record(last_status="success", last_duration_seconds=58.0)
    checker = DriftChecker(tracker, alerter)
    events = checker.check_job(make_job_cfg())
    assert events == []
    alerter.send.assert_not_called()
