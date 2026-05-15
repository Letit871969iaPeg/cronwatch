"""Tests for cli_runbook.main."""

from __future__ import annotations

import pytest

from cronwatch.cli_runbook import main


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "runbook.db")


def test_set_command_creates_entry(db_path, capsys):
    rc = main(["--db", db_path, "set", "backup", "--url", "https://wiki/backup", "--notes", "Check logs"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "backup" in out


def test_get_existing_entry(db_path, capsys):
    main(["--db", db_path, "set", "backup", "--url", "https://wiki/backup"])
    rc = main(["--db", db_path, "get", "backup"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "https://wiki/backup" in out


def test_get_missing_returns_nonzero(db_path, capsys):
    rc = main(["--db", db_path, "get", "ghost"])
    assert rc == 1


def test_delete_existing_entry(db_path, capsys):
    main(["--db", db_path, "set", "cleanup", "--notes", "temp files"])
    rc = main(["--db", db_path, "delete", "cleanup"])
    assert rc == 0
    rc2 = main(["--db", db_path, "get", "cleanup"])
    assert rc2 == 1


def test_delete_nonexistent_returns_nonzero(db_path):
    rc = main(["--db", db_path, "delete", "ghost"])
    assert rc == 1


def test_list_empty(db_path, capsys):
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No runbook" in out


def test_list_shows_all_entries(db_path, capsys):
    main(["--db", db_path, "set", "job_a", "--url", "https://a.example.com"])
    main(["--db", db_path, "set", "job_b", "--notes", "note b"])
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "job_a" in out
    assert "job_b" in out
