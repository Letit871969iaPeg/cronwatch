"""Tests for cronwatch.job_correlation."""

from __future__ import annotations

import pytest

from cronwatch.job_correlation import CorrelationEntry, CorrelationStore


@pytest.fixture()
def store() -> CorrelationStore:
    return CorrelationStore(db_path=":memory:")


def test_new_correlation_id_is_unique(store: CorrelationStore) -> None:
    ids = {store.new_correlation_id() for _ in range(50)}
    assert len(ids) == 50


def test_link_and_fetch(store: CorrelationStore) -> None:
    cid = store.new_correlation_id()
    store.link(cid, "backup", "run-001")
    store.link(cid, "cleanup", "run-002")

    entries = store.fetch(cid)
    assert len(entries) == 2
    job_names = {e.job_name for e in entries}
    assert job_names == {"backup", "cleanup"}


def test_fetch_returns_correct_types(store: CorrelationStore) -> None:
    cid = store.new_correlation_id()
    store.link(cid, "report", "run-abc")
    entries = store.fetch(cid)
    assert len(entries) == 1
    e = entries[0]
    assert isinstance(e, CorrelationEntry)
    assert e.correlation_id == cid
    assert e.job_name == "report"
    assert e.run_id == "run-abc"
    assert e.created_at  # non-empty timestamp


def test_fetch_unknown_id_returns_empty(store: CorrelationStore) -> None:
    entries = store.fetch("nonexistent-id")
    assert entries == []


def test_fetch_by_job(store: CorrelationStore) -> None:
    cid1 = store.new_correlation_id()
    cid2 = store.new_correlation_id()
    store.link(cid1, "etl", "run-1")
    store.link(cid2, "etl", "run-2")
    store.link(cid1, "notify", "run-3")

    entries = store.fetch_by_job("etl")
    assert len(entries) == 2
    assert all(e.job_name == "etl" for e in entries)


def test_fetch_by_job_unknown_returns_empty(store: CorrelationStore) -> None:
    assert store.fetch_by_job("ghost") == []


def test_delete_removes_entries(store: CorrelationStore) -> None:
    cid = store.new_correlation_id()
    store.link(cid, "job-a", "run-x")
    store.link(cid, "job-b", "run-y")

    deleted = store.delete(cid)
    assert deleted == 2
    assert store.fetch(cid) == []


def test_delete_nonexistent_returns_zero(store: CorrelationStore) -> None:
    assert store.delete("no-such-id") == 0


def test_multiple_correlations_independent(store: CorrelationStore) -> None:
    cid1 = store.new_correlation_id()
    cid2 = store.new_correlation_id()
    store.link(cid1, "alpha", "r1")
    store.link(cid2, "beta", "r2")

    assert len(store.fetch(cid1)) == 1
    assert len(store.fetch(cid2)) == 1
    store.delete(cid1)
    assert store.fetch(cid1) == []
    assert len(store.fetch(cid2)) == 1
