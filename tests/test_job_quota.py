"""Tests for cronwatch.job_quota."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from cronwatch.job_quota import JobQuota, QuotaPolicy


@pytest.fixture()
def quota() -> JobQuota:
    q = JobQuota(db_path=":memory:")
    q.set_policy("backup", QuotaPolicy(max_runs=3, window_seconds=3600))
    return q


def _ts(offset_seconds: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)


# ---------------------------------------------------------------------------
# no policy
# ---------------------------------------------------------------------------

def test_no_policy_never_exceeded(quota: JobQuota) -> None:
    quota.record_run("unknown_job")
    assert quota.is_quota_exceeded("unknown_job") is False


def test_no_policy_runs_in_window_is_zero(quota: JobQuota) -> None:
    assert quota.runs_in_window("unknown_job") == 0


# ---------------------------------------------------------------------------
# below quota
# ---------------------------------------------------------------------------

def test_below_quota_not_exceeded(quota: JobQuota) -> None:
    quota.record_run("backup")
    quota.record_run("backup")
    assert quota.is_quota_exceeded("backup") is False


def test_runs_in_window_counts_recent(quota: JobQuota) -> None:
    quota.record_run("backup")
    quota.record_run("backup")
    assert quota.runs_in_window("backup") == 2


# ---------------------------------------------------------------------------
# at / above quota
# ---------------------------------------------------------------------------

def test_at_quota_is_exceeded(quota: JobQuota) -> None:
    for _ in range(3):
        quota.record_run("backup")
    assert quota.is_quota_exceeded("backup") is True


def test_above_quota_is_exceeded(quota: JobQuota) -> None:
    for _ in range(5):
        quota.record_run("backup")
    assert quota.is_quota_exceeded("backup") is True


# ---------------------------------------------------------------------------
# rolling window — old runs do not count
# ---------------------------------------------------------------------------

def test_old_runs_outside_window_not_counted(quota: JobQuota) -> None:
    old = _ts(-7200)  # 2 hours ago — outside 1-hour window
    for _ in range(3):
        quota.record_run("backup", ran_at=old)
    # quota should NOT be exceeded because all runs are outside the window
    assert quota.is_quota_exceeded("backup") is False


def test_mixed_old_and_recent_runs(quota: JobQuota) -> None:
    old = _ts(-7200)
    quota.record_run("backup", ran_at=old)
    quota.record_run("backup", ran_at=old)
    quota.record_run("backup")  # recent
    assert quota.runs_in_window("backup") == 1
    assert quota.is_quota_exceeded("backup") is False


# ---------------------------------------------------------------------------
# multiple jobs are independent
# ---------------------------------------------------------------------------

def test_quota_is_per_job(quota: JobQuota) -> None:
    quota.set_policy("sync", QuotaPolicy(max_runs=2, window_seconds=3600))
    for _ in range(3):
        quota.record_run("backup")
    quota.record_run("sync")
    assert quota.is_quota_exceeded("backup") is True
    assert quota.is_quota_exceeded("sync") is False
