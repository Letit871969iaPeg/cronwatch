"""Tests for cronwatch.retention."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from cronwatch.history import HistoryStore
from cronwatch.retention import RetentionManager, RetentionPolicy
from cronwatch.tracker import JobRecord


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def store(tmp_path):
    return HistoryStore(str(tmp_path / "test.db"))


def _make_record(job_name: str, age_days: float = 0, exit_code: int = 0) -> JobRecord:
    started = datetime.now(tz=timezone.utc) - timedelta(days=age_days)
    finished = started + timedelta(seconds=5)
    return JobRecord(
        job_name=job_name,
        started_at=started,
        finished_at=finished,
        exit_code=exit_code,
        stdout="",
        stderr="",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_prune_by_age_removes_old_records(store):
    policy = RetentionPolicy(max_age_days=7, max_records_per_job=None)
    manager = RetentionManager(store, policy)

    store.record(_make_record("job_a", age_days=10))
    store.record(_make_record("job_a", age_days=3))

    result = manager.prune()

    assert result["by_age"] == 1
    rows = store.fetch("job_a", limit=10)
    assert len(rows) == 1


def test_prune_by_age_keeps_recent_records(store):
    policy = RetentionPolicy(max_age_days=30, max_records_per_job=None)
    manager = RetentionManager(store, policy)

    for _ in range(3):
        store.record(_make_record("job_b", age_days=1))

    result = manager.prune()

    assert result["by_age"] == 0
    assert len(store.fetch("job_b", limit=10)) == 3


def test_prune_by_count_trims_excess(store):
    policy = RetentionPolicy(max_age_days=365, max_records_per_job=3)
    manager = RetentionManager(store, policy)

    for i in range(6):
        rec = _make_record("job_c", age_days=6 - i)  # oldest first
        store.record(rec)
        time.sleep(0.01)  # ensure distinct timestamps

    result = manager.prune()

    assert result["by_count"] == 3
    rows = store.fetch("job_c", limit=10)
    assert len(rows) == 3


def test_prune_by_count_none_skips(store):
    policy = RetentionPolicy(max_age_days=365, max_records_per_job=None)
    manager = RetentionManager(store, policy)

    for _ in range(10):
        store.record(_make_record("job_d"))

    result = manager.prune()

    assert result["by_count"] == 0
    assert len(store.fetch("job_d", limit=20)) == 10


def test_prune_returns_totals(store):
    policy = RetentionPolicy(max_age_days=5, max_records_per_job=2)
    manager = RetentionManager(store, policy)

    store.record(_make_record("job_e", age_days=10))
    for i in range(4):
        store.record(_make_record("job_e", age_days=1))

    result = manager.prune()

    assert result["total"] == result["by_age"] + result["by_count"]
    assert result["total"] > 0
