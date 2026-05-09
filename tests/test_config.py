"""Tests for cronwatch configuration loading."""

import os
import textwrap
import pytest

from cronwatch.config import load_config, CronwatchConfig, JobConfig


VALID_YAML = textwrap.dedent("""
    log_file: /tmp/cronwatch.log
    state_dir: /tmp/cronwatch_state
    check_interval_seconds: 60
    smtp_host: smtp.test.com
    jobs:
      - name: test_job
        schedule: "*/5 * * * *"
        max_duration_seconds: 120
        alert_on_drift_seconds: 15
        notify:
          - admin@test.com
        enabled: true
""")


@pytest.fixture
def config_file(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(VALID_YAML)
    return str(p)


def test_load_valid_config(config_file):
    cfg = load_config(config_file)
    assert isinstance(cfg, CronwatchConfig)
    assert cfg.log_file == "/tmp/cronwatch.log"
    assert cfg.check_interval_seconds == 60
    assert cfg.smtp_host == "smtp.test.com"
    assert len(cfg.jobs) == 1


def test_job_fields(config_file):
    cfg = load_config(config_file)
    job = cfg.jobs[0]
    assert isinstance(job, JobConfig)
    assert job.name == "test_job"
    assert job.schedule == "*/5 * * * *"
    assert job.max_duration_seconds == 120
    assert job.alert_on_drift_seconds == 15
    assert job.notify == ["admin@test.com"]
    assert job.enabled is True


def test_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


def test_invalid_yaml_raises(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("- just a list\n- not a mapping\n")
    with pytest.raises(ValueError, match="YAML mapping"):
        load_config(str(p))


def test_job_missing_required_field_raises(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("jobs:\n  - name: no_schedule_job\n")
    with pytest.raises(ValueError, match="missing required fields"):
        load_config(str(p))


def test_defaults_applied(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text("jobs:\n  - name: minimal\n    schedule: '0 * * * *'\n")
    cfg = load_config(str(p))
    assert cfg.log_file == "/var/log/cronwatch.log"
    assert cfg.check_interval_seconds == 30
    job = cfg.jobs[0]
    assert job.max_duration_seconds == 3600
    assert job.enabled is True


def test_multiple_jobs_loaded(tmp_path):
    """Verify that multiple job entries are all parsed and returned."""
    yaml_content = textwrap.dedent("""
        jobs:
          - name: job_one
            schedule: '0 * * * *'
          - name: job_two
            schedule: '30 * * * *'
          - name: job_three
            schedule: '*/15 * * * *'
    """)
    p = tmp_path / "config.yaml"
    p.write_text(yaml_content)
    cfg = load_config(str(p))
    assert len(cfg.jobs) == 3
    job_names = [job.name for job in cfg.jobs]
    assert job_names == ["job_one", "job_two", "job_three"]
