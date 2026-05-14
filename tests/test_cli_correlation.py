"""Tests for cronwatch.cli_correlation."""

from __future__ import annotations

import pytest

from cronwatch.cli_correlation import main
from cronwatch.job_correlation import CorrelationStore


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "corr.db")


def _populate(db_path: str) -> tuple[str, str]:
    store = CorrelationStore(db_path=db_path)
    cid = store.new_correlation_id()
    store.link(cid, "job-alpha", "run-001")
    store.link(cid, "job-beta", "run-002")
    return cid, store.new_correlation_id()  # second id is unused


def test_show_existing_correlation(db_path, capsys):
    cid, _ = _populate(db_path)
    rc = main(["--db", db_path, "show", cid])
    assert rc == 0
    out = capsys.readouterr().out
    assert "job-alpha" in out
    assert "job-beta" in out


def test_show_unknown_correlation_returns_nonzero(db_path, capsys):
    rc = main(["--db", db_path, "show", "does-not-exist"])
    assert rc == 1
    assert "No entries" in capsys.readouterr().out


def test_by_job_shows_entries(db_path, capsys):
    cid, _ = _populate(db_path)
    rc = main(["--db", db_path, "by-job", "job-alpha"])
    assert rc == 0
    assert cid in capsys.readouterr().out


def test_by_job_unknown_returns_nonzero(db_path, capsys):
    rc = main(["--db", db_path, "by-job", "ghost-job"])
    assert rc == 1


def test_delete_existing_correlation(db_path, capsys):
    cid, _ = _populate(db_path)
    rc = main(["--db", db_path, "delete", cid])
    assert rc == 0
    assert "Deleted 2" in capsys.readouterr().out


def test_delete_nonexistent_returns_nonzero(db_path, capsys):
    rc = main(["--db", db_path, "delete", "no-such-id"])
    assert rc == 1
