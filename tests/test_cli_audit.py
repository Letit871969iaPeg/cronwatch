"""Tests for cronwatch.cli_audit.main."""

import pytest
from datetime import datetime, timezone

from cronwatch.audit_log import AuditLog
from cronwatch.cli_audit import main


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "audit.db")
    log = AuditLog(db_path=path)
    ts = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    log.record("alert", "backup", "Job missed deadline", occurred_at=ts)
    log.record("escalation", "cleanup", "Escalated", occurred_at=ts)
    log.close()
    return path


def test_main_no_filters_shows_all(db_path, capsys):
    rc = main(["--db", db_path])
    out = capsys.readouterr().out
    assert rc == 0
    assert "backup" in out
    assert "cleanup" in out


def test_main_filter_by_job(db_path, capsys):
    rc = main(["--db", db_path, "--job", "backup"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "backup" in out
    assert "cleanup" not in out


def test_main_filter_by_event_type(db_path, capsys):
    rc = main(["--db", db_path, "--event-type", "escalation"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "escalation" in out
    assert "alert" not in out.split("\n", 2)[2]  # skip header lines


def test_main_empty_db(tmp_path, capsys):
    empty = str(tmp_path / "empty.db")
    log = AuditLog(db_path=empty)
    log.close()
    rc = main(["--db", empty])
    out = capsys.readouterr().out
    assert rc == 0
    assert "No audit entries found" in out


def test_main_limit(db_path, capsys):
    rc = main(["--db", db_path, "--limit", "1"])
    out = capsys.readouterr().out
    lines = [l for l in out.strip().splitlines() if l and not l.startswith("-") and not l.startswith("ID")]
    assert rc == 0
    assert len(lines) == 1
