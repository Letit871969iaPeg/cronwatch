"""Tests for cronwatch.cli_grouping."""
from __future__ import annotations

import pytest

from cronwatch.cli_grouping import main


@pytest.fixture()
def db_path(tmp_path) -> str:
    return str(tmp_path / "groups.db")


def test_add_command(db_path: str, capsys) -> None:
    rc = main(["--db", db_path, "add", "nightly", "backup"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "backup" in out
    assert "nightly" in out


def test_remove_command(db_path: str, capsys) -> None:
    main(["--db", db_path, "add", "g", "job1"])
    rc = main(["--db", db_path, "remove", "g", "job1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "job1" in out


def test_show_existing_group(db_path: str, capsys) -> None:
    main(["--db", db_path, "add", "daily", "sync", "--description", "Daily sync jobs"])
    rc = main(["--db", db_path, "show", "daily"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "daily" in out
    assert "sync" in out
    assert "Daily sync jobs" in out


def test_show_unknown_group_returns_nonzero(db_path: str, capsys) -> None:
    rc = main(["--db", db_path, "show", "ghost"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "ghost" in err


def test_list_all_groups(db_path: str, capsys) -> None:
    main(["--db", db_path, "add", "alpha", "j1"])
    main(["--db", db_path, "add", "beta", "j2"])
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" in out


def test_list_empty(db_path: str, capsys) -> None:
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    assert "No groups" in capsys.readouterr().out


def test_jobs_command(db_path: str, capsys) -> None:
    main(["--db", db_path, "add", "g1", "shared"])
    main(["--db", db_path, "add", "g2", "shared"])
    rc = main(["--db", db_path, "jobs", "shared"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "g1" in out
    assert "g2" in out


def test_jobs_unknown_job(db_path: str, capsys) -> None:
    rc = main(["--db", db_path, "jobs", "nobody"])
    assert rc == 0
    assert "no groups" in capsys.readouterr().out
