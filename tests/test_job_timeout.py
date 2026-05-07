"""Tests for cronwatch.job_timeout.TimeoutChecker."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from cronwatch.alerter import AlertEvent, Alerter
from cronwatch.config import JobConfig
from cronwatch.job_timeout import TimeoutChecker
from cronwatch.tracker import JobRecord, JobTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _job_cfg(name: str = "backup", max_dur: int = 60) -> JobConfig:
    cfg = MagicMock(spec=JobConfig)
    cfg.name = name
    cfg.max_duration_seconds = max_dur
    return cfg


def _running_record(started_seconds_ago: float) -> JobRecord:
    record = MagicMock(spec=JobRecord)
    record.is_running.return_value = True
    record.started_at = datetime.now(timezone.utc) - timedelta(seconds=started_seconds_ago)
    return record


def _finished_record() -> JobRecord:
    record = MagicMock(spec=JobRecord)
    record.is_running.return_value = False
    return record


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def alerter() -> MagicMock:
    return MagicMock(spec=Alerter)


@pytest.fixture()
def tracker() -> MagicMock:
    return MagicMock(spec=JobTracker)


@pytest.fixture()
def checker(tracker: MagicMock, alerter: MagicMock) -> TimeoutChecker:
    return TimeoutChecker(tracker=tracker, alerter=alerter)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_no_alert_when_within_limit(checker, tracker, alerter):
    tracker.get.return_value = _running_record(started_seconds_ago=30)
    checker.check_job(_job_cfg(max_dur=60))
    alerter.send.assert_not_called()


def test_alert_when_exceeded(checker, tracker, alerter):
    tracker.get.return_value = _running_record(started_seconds_ago=90)
    checker.check_job(_job_cfg(max_dur=60))
    alerter.send.assert_called_once()
    event: AlertEvent = alerter.send.call_args[0][0]
    assert event.event_type == "timeout"
    assert "backup" in event.message


def test_no_duplicate_alert_for_same_run(checker, tracker, alerter):
    record = _running_record(started_seconds_ago=120)
    tracker.get.return_value = record
    cfg = _job_cfg(max_dur=60)
    checker.check_job(cfg)
    checker.check_job(cfg)  # second tick — should NOT fire again
    assert alerter.send.call_count == 1


def test_no_alert_when_no_record(checker, tracker, alerter):
    tracker.get.return_value = None
    checker.check_job(_job_cfg(max_dur=60))
    alerter.send.assert_not_called()


def test_no_alert_when_job_finished(checker, tracker, alerter):
    tracker.get.return_value = _finished_record()
    checker.check_job(_job_cfg(max_dur=60))
    alerter.send.assert_not_called()


def test_no_alert_when_max_duration_none(checker, tracker, alerter):
    cfg = MagicMock(spec=JobConfig)
    cfg.name = "nolimit"
    cfg.max_duration_seconds = None
    checker.check_job(cfg)
    alerter.send.assert_not_called()
    tracker.get.assert_not_called()


def test_alert_state_cleared_after_job_finishes(checker, tracker, alerter):
    """After a job finishes, a new over-long run should trigger a fresh alert."""
    cfg = _job_cfg(max_dur=60)
    # First run: over-time
    record1 = _running_record(started_seconds_ago=120)
    tracker.get.return_value = record1
    checker.check_job(cfg)
    assert alerter.send.call_count == 1

    # Job finishes
    tracker.get.return_value = _finished_record()
    checker.check_job(cfg)

    # Second run: over-time again — should alert again
    record2 = _running_record(started_seconds_ago=90)
    tracker.get.return_value = record2
    checker.check_job(cfg)
    assert alerter.send.call_count == 2
