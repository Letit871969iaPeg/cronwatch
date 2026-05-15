"""Tests for cronwatch.job_incident."""

import pytest
from cronwatch.job_incident import IncidentStore


@pytest.fixture
def store():
    return IncidentStore()


def test_open_creates_incident(store):
    inc = store.open("backup", "job failed")
    assert inc.incident_id
    assert inc.job_name == "backup"
    assert inc.reason == "job failed"
    assert inc.status == "open"
    assert inc.resolved_at is None


def test_get_returns_incident(store):
    inc = store.open("backup", "timeout")
    fetched = store.get(inc.incident_id)
    assert fetched is not None
    assert fetched.incident_id == inc.incident_id


def test_get_unknown_returns_none(store):
    assert store.get("nonexistent-id") is None


def test_resolve_marks_resolved(store):
    inc = store.open("sync", "drift detected")
    result = store.resolve(inc.incident_id, notes="manually resolved")
    assert result is True
    updated = store.get(inc.incident_id)
    assert updated.status == "resolved"
    assert updated.resolved_at is not None
    assert updated.notes == "manually resolved"


def test_resolve_already_resolved_returns_false(store):
    inc = store.open("sync", "drift detected")
    store.resolve(inc.incident_id)
    result = store.resolve(inc.incident_id)
    assert result is False


def test_resolve_unknown_id_returns_false(store):
    assert store.resolve("no-such-id") is False


def test_fetch_all(store):
    store.open("jobA", "reason1")
    store.open("jobB", "reason2")
    incidents = store.fetch()
    assert len(incidents) == 2


def test_fetch_filter_by_job(store):
    store.open("jobA", "reason1")
    store.open("jobB", "reason2")
    incidents = store.fetch(job_name="jobA")
    assert len(incidents) == 1
    assert incidents[0].job_name == "jobA"


def test_fetch_filter_by_status(store):
    inc1 = store.open("jobA", "reason1")
    store.open("jobA", "reason2")
    store.resolve(inc1.incident_id)
    open_incidents = store.fetch(status="open")
    resolved_incidents = store.fetch(status="resolved")
    assert len(open_incidents) == 1
    assert len(resolved_incidents) == 1


def test_fetch_empty_returns_empty_list(store):
    assert store.fetch() == []


def test_open_stores_notes(store):
    inc = store.open("jobX", "missing", notes="check crontab")
    fetched = store.get(inc.incident_id)
    assert fetched.notes == "check crontab"


def test_incident_ids_are_unique(store):
    ids = {store.open("job", "reason").incident_id for _ in range(5)}
    assert len(ids) == 5
