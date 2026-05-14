"""Tests for cronwatch.job_checkpoint and cronwatch.cli_checkpoint."""
from __future__ import annotations

import pytest

from cronwatch.job_checkpoint import Checkpoint, CheckpointStore
from cronwatch.cli_checkpoint import main as cli_main


@pytest.fixture()
def store(tmp_path):
    return CheckpointStore(str(tmp_path / "cp.db"))


# ── CheckpointStore unit tests ────────────────────────────────────────────────

def test_set_and_get_checkpoints(store):
    store.set("backup", "run-1", "start")
    store.set("backup", "run-1", "halfway", "50%")
    cps = store.get("backup", "run-1")
    assert len(cps) == 2
    assert cps[0].name == "start"
    assert cps[1].name == "halfway"
    assert cps[1].value == "50%"


def test_get_unknown_run_returns_empty(store):
    result = store.get("no-job", "no-run")
    assert result == []


def test_latest_returns_most_recent(store):
    store.set("sync", "r1", "step-1")
    store.set("sync", "r1", "step-2", "done")
    cp = store.latest("sync", "r1")
    assert cp is not None
    assert cp.name == "step-2"
    assert cp.value == "done"


def test_latest_unknown_run_returns_none(store):
    assert store.latest("ghost", "run-0") is None


def test_checkpoints_are_isolated_by_run(store):
    store.set("job", "run-A", "cp1")
    store.set("job", "run-B", "cp1")
    store.set("job", "run-B", "cp2")
    assert len(store.get("job", "run-A")) == 1
    assert len(store.get("job", "run-B")) == 2


def test_prune_removes_only_target_run(store):
    store.set("job", "run-1", "step")
    store.set("job", "run-2", "step")
    removed = store.prune("job", "run-1")
    assert removed == 1
    assert store.get("job", "run-1") == []
    assert len(store.get("job", "run-2")) == 1


def test_prune_nonexistent_returns_zero(store):
    assert store.prune("nope", "nope") == 0


def test_checkpoint_dataclass_fields(store):
    store.set("myjob", "r99", "ping", "pong")
    cp = store.latest("myjob", "r99")
    assert isinstance(cp, Checkpoint)
    assert cp.job_name == "myjob"
    assert cp.run_id == "r99"
    assert cp.recorded_at  # non-empty ISO string


# ── CLI tests ─────────────────────────────────────────────────────────────────

def test_cli_list_shows_checkpoints(tmp_path, capsys):
    db = str(tmp_path / "cp.db")
    s = CheckpointStore(db)
    s.set("backup", "r1", "start")
    s.set("backup", "r1", "end", "ok")
    rc = cli_main(["--db", db, "list", "backup", "r1"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "start" in out
    assert "end" in out


def test_cli_list_empty_returns_zero(tmp_path, capsys):
    db = str(tmp_path / "cp.db")
    CheckpointStore(db)  # create schema
    rc = cli_main(["--db", db, "list", "ghost", "r0"])
    assert rc == 0
    assert "No checkpoints" in capsys.readouterr().out


def test_cli_latest_shows_last(tmp_path, capsys):
    db = str(tmp_path / "cp.db")
    s = CheckpointStore(db)
    s.set("job", "r2", "alpha")
    s.set("job", "r2", "beta", "val")
    rc = cli_main(["--db", db, "latest", "job", "r2"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "beta" in out
    assert "alpha" not in out


def test_cli_latest_missing_returns_nonzero(tmp_path):
    db = str(tmp_path / "cp.db")
    CheckpointStore(db)
    rc = cli_main(["--db", db, "latest", "none", "r0"])
    assert rc == 1


def test_cli_prune_reports_count(tmp_path, capsys):
    db = str(tmp_path / "cp.db")
    s = CheckpointStore(db)
    s.set("job", "r3", "step1")
    s.set("job", "r3", "step2")
    rc = cli_main(["--db", db, "prune", "job", "r3"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "2" in out
