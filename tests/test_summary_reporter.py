"""Tests for cronwatch.summary_reporter."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from cronwatch.history import HistoryStore
from cronwatch.summary_reporter import build_full_report, build_job_summary
from cronwatch.tracker import JobRecord, JobTracker


DT_START = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
DT_END = datetime(2024, 1, 10, 12, 0, 30, tzinfo=timezone.utc)


@pytest.fixture
def history(tmp_path):
    db = tmp_path / "hist.db"
    return HistoryStore(str(db))


@pytest.fixture
def tracker():
    return JobTracker()


def _make_record(name, exit_code=0, start=DT_START, end=DT_END):
    return JobRecord(job_name=name, start_time=start, end_time=end, exit_code=exit_code, output="")


def test_build_summary_no_data(tracker, history):
    entry = build_job_summary("backup", tracker, history)
    assert entry.name == "backup"
    assert entry.last_run is None
    assert entry.run_count == 0
    assert entry.failure_count == 0
    assert entry.avg_duration_s is None


def test_build_summary_with_history(tracker, history):
    r1 = _make_record("backup", exit_code=0)
    r2 = _make_record("backup", exit_code=1)
    history.record(r1)
    history.record(r2)
    tracker.update(r1)

    entry = build_job_summary("backup", tracker, history)
    assert entry.run_count == 2
    assert entry.failure_count == 1
    assert entry.last_status == "success"
    assert entry.last_duration_s == pytest.approx(30.0)
    assert entry.avg_duration_s == pytest.approx(30.0)


def test_build_summary_running_job(tracker, history):
    r = JobRecord(job_name="sync", start_time=DT_START, end_time=None, exit_code=None, output="")
    tracker.update(r)
    entry = build_job_summary("sync", tracker, history)
    assert entry.last_status == "running"
    assert entry.last_duration_s is None


def test_build_full_report_structure(tracker, history):
    r = _make_record("job_a")
    history.record(r)
    tracker.update(r)

    report = build_full_report(["job_a", "job_b"], tracker, history)
    assert "generated_at" in report
    assert len(report["jobs"]) == 2
    names = {j["name"] for j in report["jobs"]}
    assert names == {"job_a", "job_b"}


def test_failure_count_accuracy(tracker, history):
    for code in [0, 0, 1, 1, 1]:
        history.record(_make_record("nightly", exit_code=code))
    entry = build_job_summary("nightly", tracker, history)
    assert entry.failure_count == 3
    assert entry.run_count == 5
