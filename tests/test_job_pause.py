"""Tests for cronwatch.job_pause."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from cronwatch.job_pause import PauseStore


@pytest.fixture()
def store(tmp_path):
    return PauseStore(str(tmp_path / "pause.db"))


_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_pause_and_is_paused(store):
    store.pause("backup")
    assert store.is_paused("backup") is True


def test_resume_clears_pause(store):
    store.pause("backup")
    store.resume("backup")
    assert store.is_paused("backup") is False


def test_unknown_job_not_paused(store):
    assert store.is_paused("nonexistent") is False


def test_pause_stores_reason(store):
    store.pause("sync", reason="weekly maintenance")
    entry = store.get("sync")
    assert entry is not None
    assert entry.reason == "weekly maintenance"


def test_pause_stores_until(store):
    future = _NOW + timedelta(hours=2)
    store.pause("sync", paused_until=future)
    entry = store.get("sync")
    assert entry is not None
    assert entry.paused_until == future


def test_auto_expire_when_until_passed(store):
    past = _NOW - timedelta(hours=1)
    store.pause("sync", paused_until=past)
    with patch("cronwatch.job_pause._utcnow", return_value=_NOW):
        assert store.is_paused("sync") is False
    # row should have been cleaned up
    assert store.get("sync") is None


def test_not_expired_when_until_in_future(store):
    future = _NOW + timedelta(hours=1)
    store.pause("sync", paused_until=future)
    with patch("cronwatch.job_pause._utcnow", return_value=_NOW):
        assert store.is_paused("sync") is True


def test_list_paused_returns_all(store):
    store.pause("job_a")
    store.pause("job_b", reason="testing")
    entries = store.list_paused()
    names = {e.job_name for e in entries}
    assert names == {"job_a", "job_b"}


def test_list_paused_empty(store):
    assert store.list_paused() == []


def test_pause_overwrite_updates_reason(store):
    store.pause("backup", reason="first")
    store.pause("backup", reason="second")
    entry = store.get("backup")
    assert entry.reason == "second"


def test_resume_nonexistent_is_noop(store):
    """Resuming a job that was never paused should not raise."""
    store.resume("ghost")  # should not raise
    assert store.is_paused("ghost") is False
