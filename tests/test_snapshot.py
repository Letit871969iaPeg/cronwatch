"""Tests for SnapshotStore and SnapshotCollector."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from cronwatch.snapshot import JobSnapshot, SnapshotStore
from cronwatch.snapshot_collector import SnapshotCollector


@pytest.fixture()
def store(tmp_path):
    return SnapshotStore(str(tmp_path / "snaps.db"))


def _snap(name: str = "backup", status: str = "ok") -> JobSnapshot:
    return JobSnapshot(
        job_name=name,
        captured_at="2024-01-01T00:00:00+00:00",
        last_status=status,
        last_run_ts="2024-01-01T00:00:00+00:00",
        last_duration_s=1.5,
        consecutive_failures=0,
    )


# ---------------------------------------------------------------------------
# SnapshotStore
# ---------------------------------------------------------------------------

def test_save_and_fetch(store):
    snap = _snap()
    store.save(snap)
    results = store.fetch("backup")
    assert len(results) == 1
    assert results[0].last_status == "ok"
    assert results[0].last_duration_s == pytest.approx(1.5)


def test_fetch_empty_returns_empty_list(store):
    assert store.fetch("nonexistent") == []


def test_fetch_respects_limit(store):
    for i in range(10):
        store.save(_snap())
    assert len(store.fetch("backup", limit=3)) == 3


def test_fetch_ordered_newest_first(store):
    store.save(
        JobSnapshot(
            job_name="j",
            captured_at="2024-01-01T01:00:00+00:00",
            last_status="ok",
            last_run_ts=None,
            last_duration_s=None,
            consecutive_failures=0,
        )
    )
    store.save(
        JobSnapshot(
            job_name="j",
            captured_at="2024-01-01T02:00:00+00:00",
            last_status="fail",
            last_run_ts=None,
            last_duration_s=None,
            consecutive_failures=1,
        )
    )
    results = store.fetch("j")
    assert results[0].last_status == "fail"


def test_prune_removes_old_entries(store):
    for _ in range(5):
        store.save(_snap())
    removed = store.prune("backup", keep=2)
    assert removed == 3
    assert len(store.fetch("backup")) == 2


# ---------------------------------------------------------------------------
# SnapshotCollector
# ---------------------------------------------------------------------------

def _make_tracker(record=None):
    tracker = MagicMock()
    tracker.get.return_value = record
    return tracker


def test_collector_no_record_saves_null_snapshot(store):
    collector = SnapshotCollector(_make_tracker(None), store)
    snaps = collector.collect(["missing_job"])
    assert len(snaps) == 1
    assert snaps[0].last_status is None
    assert snaps[0].consecutive_failures == 0


def test_collector_with_record_saves_correct_fields(store):
    record = MagicMock()
    record.last_status.return_value = "ok"
    record.last_start = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    record.last_duration_s = 3.7
    record.consecutive_failures = 0

    collector = SnapshotCollector(_make_tracker(record), store)
    snaps = collector.collect(["myjob"])
    assert snaps[0].last_status == "ok"
    assert snaps[0].last_duration_s == pytest.approx(3.7)
    assert "2024-06-01" in snaps[0].last_run_ts


def test_collector_persists_to_store(store):
    record = MagicMock()
    record.last_status.return_value = "fail"
    record.last_start = None
    record.last_duration_s = None
    record.consecutive_failures = 2

    collector = SnapshotCollector(_make_tracker(record), store)
    collector.collect(["failjob"])
    results = store.fetch("failjob")
    assert len(results) == 1
    assert results[0].consecutive_failures == 2
