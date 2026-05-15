"""Tests for job_ownership and cli_ownership."""

from __future__ import annotations

import pytest

from cronwatch.job_ownership import OwnerEntry, OwnershipStore
from cronwatch.cli_ownership import main


@pytest.fixture()
def store(tmp_path):
    return OwnershipStore(db_path=str(tmp_path / "test.db"))


def test_set_and_get(store):
    entry = OwnerEntry(job_name="backup", owner="alice", team="ops", email="alice@example.com")
    store.set(entry)
    result = store.get("backup")
    assert result is not None
    assert result.owner == "alice"
    assert result.team == "ops"
    assert result.email == "alice@example.com"
    assert result.slack_channel is None


def test_get_unknown_returns_none(store):
    assert store.get("nonexistent") is None


def test_set_overwrites_existing(store):
    store.set(OwnerEntry(job_name="backup", owner="alice"))
    store.set(OwnerEntry(job_name="backup", owner="bob", team="platform"))
    result = store.get("backup")
    assert result.owner == "bob"
    assert result.team == "platform"


def test_delete_existing(store):
    store.set(OwnerEntry(job_name="backup", owner="alice"))
    removed = store.delete("backup")
    assert removed is True
    assert store.get("backup") is None


def test_delete_unknown_returns_false(store):
    assert store.delete("ghost") is False


def test_all_returns_sorted(store):
    store.set(OwnerEntry(job_name="z_job", owner="z"))
    store.set(OwnerEntry(job_name="a_job", owner="a"))
    store.set(OwnerEntry(job_name="m_job", owner="m"))
    names = [e.job_name for e in store.all()]
    assert names == ["a_job", "m_job", "z_job"]


def test_all_empty(store):
    assert store.all() == []


# --- CLI tests ---


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "cli_test.db")


def test_cli_set_and_get(db_path, capsys):
    rc = main(["--db", db_path, "set", "deploy", "--owner", "carol", "--team", "infra"])
    assert rc == 0
    rc = main(["--db", db_path, "get", "deploy"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "carol" in out
    assert "infra" in out


def test_cli_get_missing_returns_nonzero(db_path, capsys):
    rc = main(["--db", db_path, "get", "unknown"])
    assert rc == 1


def test_cli_delete_existing(db_path, capsys):
    main(["--db", db_path, "set", "job1", "--owner", "dave"])
    rc = main(["--db", db_path, "delete", "job1"])
    assert rc == 0
    rc = main(["--db", db_path, "get", "job1"])
    assert rc == 1


def test_cli_delete_missing_returns_nonzero(db_path):
    rc = main(["--db", db_path, "delete", "ghost"])
    assert rc == 1


def test_cli_list_empty(db_path, capsys):
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    assert "No ownership" in capsys.readouterr().out


def test_cli_list_multiple(db_path, capsys):
    main(["--db", db_path, "set", "job_a", "--owner", "alice"])
    main(["--db", db_path, "set", "job_b", "--owner", "bob"])
    rc = main(["--db", db_path, "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "bob" in out
