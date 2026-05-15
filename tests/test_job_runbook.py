"""Tests for RunbookStore."""

from __future__ import annotations

import pytest

from cronwatch.job_runbook import RunbookStore, RunbookEntry


@pytest.fixture
def store(tmp_path):
    return RunbookStore(str(tmp_path / "runbook.db"))


def test_set_and_get(store):
    store.set("backup", url="https://wiki.example.com/backup", notes="Check S3 bucket")
    entry = store.get("backup")
    assert entry is not None
    assert entry.job_name == "backup"
    assert entry.url == "https://wiki.example.com/backup"
    assert entry.notes == "Check S3 bucket"


def test_get_unknown_returns_none(store):
    assert store.get("nonexistent") is None


def test_set_overwrites_existing(store):
    store.set("backup", url="https://old.example.com", notes="old notes")
    store.set("backup", url="https://new.example.com", notes="new notes")
    entry = store.get("backup")
    assert entry.url == "https://new.example.com"
    assert entry.notes == "new notes"


def test_set_url_only(store):
    store.set("deploy", url="https://wiki.example.com/deploy")
    entry = store.get("deploy")
    assert entry.url == "https://wiki.example.com/deploy"
    assert entry.notes is None


def test_set_notes_only(store):
    store.set("cleanup", notes="Remove temp files older than 7 days")
    entry = store.get("cleanup")
    assert entry.url is None
    assert entry.notes == "Remove temp files older than 7 days"


def test_delete_existing(store):
    store.set("backup", url="https://wiki.example.com/backup")
    removed = store.delete("backup")
    assert removed is True
    assert store.get("backup") is None


def test_delete_nonexistent_returns_false(store):
    assert store.delete("ghost") is False


def test_all_returns_all_entries(store):
    store.set("job_a", url="https://a.example.com")
    store.set("job_b", notes="some note")
    entries = store.all()
    names = [e.job_name for e in entries]
    assert "job_a" in names
    assert "job_b" in names


def test_all_empty_returns_empty_list(store):
    assert store.all() == []


def test_all_sorted_by_name(store):
    store.set("zzz")
    store.set("aaa")
    store.set("mmm")
    names = [e.job_name for e in store.all()]
    assert names == sorted(names)
