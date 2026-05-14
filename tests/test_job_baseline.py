"""Tests for BaselineStore and BaselineCollector."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from cronwatch.job_baseline import BaselineStore
from cronwatch.baseline_collector import BaselineCollector
from cronwatch.tracker import JobRecord, JobTracker


@pytest.fixture
def store(tmp_path):
    return BaselineStore(db_path=str(tmp_path / "baseline.db"))


@pytest.fixture
def tracker():
    return JobTracker()


def _ts(offset_seconds: float = 0.0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        seconds=offset_seconds
    )


# ---------------------------------------------------------------------------
# BaselineStore
# ---------------------------------------------------------------------------

def test_add_and_fetch_sample(store):
    store.add_sample("backup", 30.0, "2024-06-01T12:00:00+00:00")
    samples = store.fetch_samples("backup")
    assert len(samples) == 1
    assert samples[0].job_name == "backup"
    assert samples[0].duration_seconds == 30.0


def test_fetch_empty_returns_empty(store):
    assert store.fetch_samples("nonexistent") == []


def test_average_duration_single_sample(store):
    store.add_sample("sync", 60.0, "2024-06-01T12:00:00+00:00")
    assert store.average_duration("sync") == pytest.approx(60.0)


def test_average_duration_multiple_samples(store):
    for d in [10.0, 20.0, 30.0]:
        store.add_sample("etl", d, "2024-06-01T12:00:00+00:00")
    assert store.average_duration("etl") == pytest.approx(20.0)


def test_average_duration_no_samples_returns_none(store):
    assert store.average_duration("ghost") is None


def test_fetch_respects_limit(store):
    for i in range(10):
        store.add_sample("job", float(i), "2024-06-01T12:00:00+00:00")
    samples = store.fetch_samples("job", limit=3)
    assert len(samples) == 3


def test_clear_removes_samples(store):
    store.add_sample("tmp", 5.0, "2024-06-01T12:00:00+00:00")
    store.clear("tmp")
    assert store.fetch_samples("tmp") == []


# ---------------------------------------------------------------------------
# BaselineCollector
# ---------------------------------------------------------------------------

def _finished_record(name: str, duration_sec: float) -> JobRecord:
    started = _ts(0)
    finished = _ts(duration_sec)
    rec = JobRecord(
        job_name=name,
        run_id=str(uuid.uuid4()),
        started_at=started,
        finished_at=finished,
        exit_code=0,
    )
    return rec


def test_collector_ingests_finished_run(store, tracker):
    rec = _finished_record("backup", 45.0)
    tracker._records["backup"] = rec
    collector = BaselineCollector(tracker, store)
    count = collector.collect()
    assert count == 1
    assert store.average_duration("backup") == pytest.approx(45.0)


def test_collector_skips_running_job(store, tracker):
    rec = JobRecord(
        job_name="live",
        run_id=str(uuid.uuid4()),
        started_at=_ts(0),
        finished_at=None,
        exit_code=None,
    )
    tracker._records["live"] = rec
    collector = BaselineCollector(tracker, store)
    assert collector.collect() == 0


def test_collector_deduplicates_on_second_call(store, tracker):
    rec = _finished_record("daily", 20.0)
    tracker._records["daily"] = rec
    collector = BaselineCollector(tracker, store)
    collector.collect()
    second = collector.collect()
    assert second == 0
    assert len(store.fetch_samples("daily")) == 1
