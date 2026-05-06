"""Tests for cronwatch.cli_tag."""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from cronwatch.cli_tag import main


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    cfg = tmp_path / "cronwatch.yaml"
    cfg.write_text(
        textwrap.dedent(
            """\
            jobs:
              - name: nightly-backup
                schedule: "0 2 * * *"
                command: /usr/bin/backup
                tags: [nightly, backup]
              - name: hourly-sync
                schedule: "0 * * * *"
                command: /usr/bin/sync
                tags: [hourly]
              - name: daily-report
                schedule: "0 6 * * *"
                command: /usr/bin/report
                tags: [nightly, report]
            """
        )
    )
    return cfg


def test_matching_jobs_printed(config_file: Path, capsys):
    rc = main(["--config", str(config_file), "--tags", "nightly"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "nightly-backup" in out
    assert "daily-report" in out
    assert "hourly-sync" not in out


def test_no_match_returns_nonzero(config_file: Path, capsys):
    rc = main(["--config", str(config_file), "--tags", "nonexistent"])
    assert rc == 1


def test_quiet_flag_suppresses_header(config_file: Path, capsys):
    rc = main(["--config", str(config_file), "--tags", "nightly", "--quiet"])
    out = capsys.readouterr().out
    assert rc == 0
    # quiet mode: no header line, just job names
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    assert all(not l.startswith("Jobs matching") for l in lines)
    assert "nightly-backup" in out


def test_multi_tag_filter(config_file: Path, capsys):
    rc = main(["--config", str(config_file), "--tags", "nightly,backup"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "nightly-backup" in out
    assert "daily-report" not in out
