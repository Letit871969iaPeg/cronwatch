"""Tests for cronwatch.audit_log.AuditLog."""

import pytest
from datetime import datetime, timezone

from cronwatch.audit_log import AuditLog


@pytest.fixture
def store(tmp_path):
    db = AuditLog(db_path=str(tmp_path / "audit_test.db"))
    yield db
    db.close()


def _ts(year=2024, month=1, day=1, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def test_record_and_fetch_all(store):
    store.record("alert", "backup", "Job missed", occurred_at=_ts())
    store.record("escalation", "backup", "Escalated after 3 failures", occurred_at=_ts(hour=1))
    entries = store.fetch()
    assert len(entries) == 2


def test_fetch_by_job(store):
    store.record("alert", "backup", "missed", occurred_at=_ts())
    store.record("alert", "cleanup", "missed", occurred_at=_ts())
    entries = store.fetch(job_name="backup")
    assert all(e.job_name == "backup" for e in entries)
    assert len(entries) == 1


def test_fetch_by_event_type(store):
    store.record("alert", "backup", "missed", occurred_at=_ts())
    store.record("escalation", "backup", "escalated", occurred_at=_ts(hour=1))
    entries = store.fetch(event_type="escalation")
    assert len(entries) == 1
    assert entries[0].event_type == "escalation"


def test_fetch_empty(store):
    assert store.fetch() == []


def test_fetch_respects_limit(store):
    for i in range(10):
        store.record("alert", "job", f"msg {i}", occurred_at=_ts(minute=i))
    entries = store.fetch(limit=4)
    assert len(entries) == 4


def test_extra_roundtrip(store):
    extra = {"exit_code": 1, "duration": 42.5}
    store.record("alert", "myjob", "failed", extra=extra, occurred_at=_ts())
    entries = store.fetch()
    assert entries[0].extra == extra


def test_no_extra_is_none(store):
    store.record("alert", "myjob", "failed", occurred_at=_ts())
    entries = store.fetch()
    assert entries[0].extra is None


def test_entries_ordered_newest_first(store):
    store.record("alert", "job", "first", occurred_at=_ts(hour=0))
    store.record("alert", "job", "second", occurred_at=_ts(hour=2))
    entries = store.fetch()
    assert entries[0].message == "second"
    assert entries[1].message == "first"
