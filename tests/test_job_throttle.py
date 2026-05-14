"""Tests for cronwatch.job_throttle."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from cronwatch.job_throttle import JobThrottle, ThrottlePolicy

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def throttle(tmp_path):
    return JobThrottle(str(tmp_path / "throttle.db"))


def test_no_policy_never_throttled(throttle):
    assert throttle.is_throttled("backup") is False


def test_no_prior_run_not_throttled(throttle):
    throttle.set_policy("backup", ThrottlePolicy(min_interval_seconds=300))
    assert throttle.is_throttled("backup") is False


def test_throttled_within_interval(throttle):
    throttle.set_policy("backup", ThrottlePolicy(min_interval_seconds=300))
    # record a run 60 seconds ago
    past = _NOW - timedelta(seconds=60)
    with patch("cronwatch.job_throttle._utcnow", return_value=past):
        throttle.record_run("backup")
    with patch("cronwatch.job_throttle._utcnow", return_value=_NOW):
        assert throttle.is_throttled("backup") is True


def test_not_throttled_after_interval(throttle):
    throttle.set_policy("backup", ThrottlePolicy(min_interval_seconds=300))
    past = _NOW - timedelta(seconds=400)
    with patch("cronwatch.job_throttle._utcnow", return_value=past):
        throttle.record_run("backup")
    with patch("cronwatch.job_throttle._utcnow", return_value=_NOW):
        assert throttle.is_throttled("backup") is False


def test_record_run_updates_timestamp(throttle):
    throttle.set_policy("sync", ThrottlePolicy(min_interval_seconds=60))
    t1 = _NOW - timedelta(seconds=500)
    t2 = _NOW - timedelta(seconds=10)
    with patch("cronwatch.job_throttle._utcnow", return_value=t1):
        throttle.record_run("sync")
    with patch("cronwatch.job_throttle._utcnow", return_value=t2):
        throttle.record_run("sync")
    last = throttle.last_allowed_at("sync")
    assert last is not None
    assert abs((last - t2).total_seconds()) < 1


def test_last_allowed_at_unknown_job_returns_none(throttle):
    assert throttle.last_allowed_at("ghost") is None


def test_independent_jobs_throttled_separately(throttle):
    throttle.set_policy("jobA", ThrottlePolicy(min_interval_seconds=300))
    throttle.set_policy("jobB", ThrottlePolicy(min_interval_seconds=300))
    past = _NOW - timedelta(seconds=60)
    with patch("cronwatch.job_throttle._utcnow", return_value=past):
        throttle.record_run("jobA")
    with patch("cronwatch.job_throttle._utcnow", return_value=_NOW):
        assert throttle.is_throttled("jobA") is True
        assert throttle.is_throttled("jobB") is False


def test_schema_persists_across_instances(tmp_path):
    db = str(tmp_path / "t.db")
    t1 = JobThrottle(db)
    t1.set_policy("nightly", ThrottlePolicy(min_interval_seconds=3600))
    past = _NOW - timedelta(seconds=100)
    with patch("cronwatch.job_throttle._utcnow", return_value=past):
        t1.record_run("nightly")

    t2 = JobThrottle(db)
    t2.set_policy("nightly", ThrottlePolicy(min_interval_seconds=3600))
    with patch("cronwatch.job_throttle._utcnow", return_value=_NOW):
        assert t2.is_throttled("nightly") is True
