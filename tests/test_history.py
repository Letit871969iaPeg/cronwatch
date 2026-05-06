"""Tests for cronwatch.history.HistoryStore."""

import pytest
from datetime import datetime, timedelta
from pathlib import Path

from cronwatch.history import HistoryStore
from cronwatch.tracker import JobRecord


@pytest.fixture
def store(tmp_path: Path) -> HistoryStore:
    db = tmp_path / "test_history.db"
    s = HistoryStore(db_path=db)
    yield s
    s.close()


def _make_record(exit_code: int = 0, duration_s: float = 30.0) -> JobRecord:
    now = datetime(2024, 1, 15, 12, 0, 0)
    rec = JobRecord()
    rec.started_at = now
    rec.finished_at = now + timedelta(seconds=duration_s)
    rec.exit_code = exit_code
    return rec


def test_record_and_fetch(store: HistoryStore) -> None:
    rec = _make_record(exit_code=0, duration_s=45.0)
    store.record("backup", rec)

    rows = store.fetch("backup")
    assert len(rows) == 1
    assert rows[0]["job_name"] == "backup"
    assert rows[0]["exit_code"] == 0
    assert abs(rows[0]["duration_s"] - 45.0) < 0.01


def test_fetch_empty(store: HistoryStore) -> None:
    rows = store.fetch("nonexistent")
    assert rows == []


def test_fetch_respects_limit(store: HistoryStore) -> None:
    for _ in range(5):
        store.record("job", _make_record())
    rows = store.fetch("job", limit=3)
    assert len(rows) == 3


def test_average_duration_success_only(store: HistoryStore) -> None:
    store.record("job", _make_record(exit_code=0, duration_s=20.0))
    store.record("job", _make_record(exit_code=1, duration_s=5.0))  # failure, excluded
    store.record("job", _make_record(exit_code=0, duration_s=40.0))

    avg = store.average_duration("job", window=10)
    assert avg is not None
    assert abs(avg - 30.0) < 0.01  # (20 + 40) / 2


def test_average_duration_no_data(store: HistoryStore) -> None:
    avg = store.average_duration("missing_job")
    assert avg is None


def test_multiple_jobs_isolated(store: HistoryStore) -> None:
    store.record("jobA", _make_record(duration_s=10.0))
    store.record("jobB", _make_record(duration_s=99.0))

    rows_a = store.fetch("jobA")
    rows_b = store.fetch("jobB")
    assert len(rows_a) == 1
    assert len(rows_b) == 1
    assert rows_a[0]["duration_s"] != rows_b[0]["duration_s"]


def test_schema_created_on_init(tmp_path: Path) -> None:
    db = tmp_path / "subdir" / "cronwatch.db"
    s = HistoryStore(db_path=db)
    assert db.exists()
    s.close()
