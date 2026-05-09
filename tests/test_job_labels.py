"""Tests for cronwatch.job_labels.LabelStore."""

from __future__ import annotations

import pytest

from cronwatch.job_labels import LabelStore, JobLabel


@pytest.fixture()
def store(tmp_path):
    return LabelStore(tmp_path / "labels.db")


def test_set_and_get_single_label(store):
    store.set("backup", "env", "prod")
    labels = store.get("backup")
    assert labels == {"env": "prod"}


def test_set_multiple_labels(store):
    store.set("backup", "env", "prod")
    store.set("backup", "team", "infra")
    labels = store.get("backup")
    assert labels["env"] == "prod"
    assert labels["team"] == "infra"


def test_set_overwrites_existing_key(store):
    store.set("backup", "env", "staging")
    store.set("backup", "env", "prod")
    assert store.get("backup")["env"] == "prod"


def test_get_unknown_job_returns_empty(store):
    assert store.get("nonexistent") == {}


def test_delete_removes_label(store):
    store.set("cleanup", "env", "prod")
    store.set("cleanup", "team", "ops")
    store.delete("cleanup", "env")
    labels = store.get("cleanup")
    assert "env" not in labels
    assert labels["team"] == "ops"


def test_delete_nonexistent_is_safe(store):
    # Should not raise
    store.delete("ghost", "missing")


def test_find_by_label_returns_matching_jobs(store):
    store.set("job_a", "env", "prod")
    store.set("job_b", "env", "prod")
    store.set("job_c", "env", "staging")
    result = store.find_by_label("env", "prod")
    assert sorted(result) == ["job_a", "job_b"]


def test_find_by_label_no_match_returns_empty(store):
    assert store.find_by_label("env", "unknown") == []


def test_all_labels_returns_every_entry(store):
    store.set("job_a", "env", "prod")
    store.set("job_b", "team", "infra")
    entries = store.all_labels()
    assert len(entries) == 2
    assert all(isinstance(e, JobLabel) for e in entries)


def test_all_labels_empty_db(store):
    assert store.all_labels() == []
