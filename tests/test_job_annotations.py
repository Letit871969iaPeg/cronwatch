"""Tests for cronwatch.job_annotations."""

import pytest

from cronwatch.job_annotations import Annotation, AnnotationStore


@pytest.fixture
def store() -> AnnotationStore:
    return AnnotationStore(db_path=":memory:")


def test_set_and_get_single_key(store: AnnotationStore) -> None:
    store.set("backup", "run-1", "exit_code", "0")
    result = store.get("backup", "run-1")
    assert result == {"exit_code": "0"}


def test_get_returns_all_keys_for_run(store: AnnotationStore) -> None:
    store.set("backup", "run-2", "exit_code", "1")
    store.set("backup", "run-2", "host", "worker-01")
    result = store.get("backup", "run-2")
    assert result["exit_code"] == "1"
    assert result["host"] == "worker-01"


def test_get_unknown_run_returns_empty(store: AnnotationStore) -> None:
    result = store.get("backup", "nonexistent")
    assert result == {}


def test_set_overwrites_existing_key(store: AnnotationStore) -> None:
    store.set("sync", "run-3", "status", "ok")
    store.set("sync", "run-3", "status", "warning")
    result = store.get("sync", "run-3")
    assert result["status"] == "warning"
    # Only one entry for that key should exist
    all_ann = store.fetch_all("sync")
    status_entries = [a for a in all_ann if a.key == "status"]
    assert len(status_entries) == 1


def test_fetch_all_returns_annotations_for_job(store: AnnotationStore) -> None:
    store.set("cleanup", "run-10", "note", "first run")
    store.set("cleanup", "run-11", "note", "second run")
    results = store.fetch_all("cleanup")
    assert len(results) == 2
    assert all(isinstance(a, Annotation) for a in results)
    assert all(a.job_name == "cleanup" for a in results)


def test_fetch_all_empty_when_no_annotations(store: AnnotationStore) -> None:
    results = store.fetch_all("ghost_job")
    assert results == []


def test_fetch_all_does_not_return_other_jobs(store: AnnotationStore) -> None:
    store.set("job_a", "run-1", "k", "v")
    store.set("job_b", "run-1", "k", "v")
    results = store.fetch_all("job_a")
    assert all(a.job_name == "job_a" for a in results)


def test_delete_run_removes_annotations(store: AnnotationStore) -> None:
    store.set("prune", "run-99", "k1", "v1")
    store.set("prune", "run-99", "k2", "v2")
    deleted = store.delete_run("prune", "run-99")
    assert deleted == 2
    assert store.get("prune", "run-99") == {}


def test_delete_run_returns_zero_when_nothing_to_delete(store: AnnotationStore) -> None:
    deleted = store.delete_run("noop", "run-000")
    assert deleted == 0


def test_annotation_created_at_is_populated(store: AnnotationStore) -> None:
    store.set("ts_job", "run-ts", "key", "val")
    results = store.fetch_all("ts_job")
    assert len(results) == 1
    ann = results[0]
    assert ann.created_at  # non-empty
    assert "T" in ann.created_at  # ISO format
