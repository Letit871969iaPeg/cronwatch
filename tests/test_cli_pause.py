"""Tests for cronwatch.cli_pause."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from cronwatch.cli_pause import main
from cronwatch.job_pause import PauseStore


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "pause.db")


def test_pause_command_creates_entry(db_path):
    rc = main(["--db", db_path, "pause", "backup", "--reason", "maint"])
    assert rc == 0
    store = PauseStore(db_path)
    assert store.is_paused("backup") is True


def test_resume_command_clears_entry(db_path):
    store = PauseStore(db_path)
    store.pause("backup")
    rc = main(["--db", db_path, "resume", "backup"])
    assert rc == 0
    assert store.is_paused("backup") is False


def test_resume_not_paused_returns_nonzero(db_path):
    rc = main(["--db", db_path, "resume", "ghost"])
    assert rc == 1


def test_list_empty(db_path, capsys):
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No jobs" in out


def test_list_shows_paused_jobs(db_path, capsys):
    store = PauseStore(db_path)
    store.pause("job_a", reason="testing")
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "job_a" in out
    assert "testing" in out


def test_pause_with_valid_until(db_path):
    future = (datetime.now(timezone.utc) + timedelta(hours=3)).isoformat()
    rc = main(["--db", db_path, "pause", "sync", "--until", future])
    assert rc == 0
    store = PauseStore(db_path)
    entry = store.get("sync")
    assert entry is not None
    assert entry.paused_until is not None


def test_pause_with_invalid_until_returns_error(db_path):
    rc = main(["--db", db_path, "pause", "sync", "--until", "not-a-date"])
    assert rc == 2
