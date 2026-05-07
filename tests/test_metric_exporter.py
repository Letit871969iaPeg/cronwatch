"""Tests for cronwatch.metric_exporter."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from cronwatch.history import HistoryStore
from cronwatch.metric_exporter import MetricExporter, _label_str, _render_samples, MetricSample
from cronwatch.tracker import JobRecord, JobTracker


UTC = timezone.utc


@pytest.fixture()
def tracker():
    t = JobTracker()
    t.ensure("backup")
    t.ensure("cleanup")
    return t


@pytest.fixture()
def history(tmp_path):
    return HistoryStore(str(tmp_path / "test.db"))


@pytest.fixture()
def exporter(tracker, history):
    return MetricExporter(tracker, history, limit=50)


def _make_record(job_name, exit_code=0, duration=30.0):
    from cronwatch.tracker import JobRecord
    r = MagicMock()
    r.job_name = job_name
    r.exit_code = exit_code
    r.duration_seconds = duration
    r.started_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    r.finished_at = datetime(2024, 1, 1, 0, 0, 30, tzinfo=UTC)
    return r


def test_label_str_empty():
    assert _label_str({}) == ""


def test_label_str_single():
    assert _label_str({"job": "backup"}) == '{job="backup"}'


def test_label_str_sorted():
    result = _label_str({"z": "1", "a": "2"})
    assert result == '{a="2",z="1"}'


def test_collect_returns_samples_for_each_job(exporter, tracker):
    samples = exporter.collect()
    job_names = {s.labels["job"] for s in samples}
    assert "backup" in job_names
    assert "cleanup" in job_names


def test_collect_includes_exit_code_sample(exporter, tracker):
    record = tracker._records["backup"]
    record.last_exit_code = 0
    samples = exporter.collect()
    ec_samples = [s for s in samples if s.name == "cronwatch_job_last_exit_code" and s.labels["job"] == "backup"]
    assert len(ec_samples) == 1
    assert ec_samples[0].value == 0.0


def test_collect_exit_code_minus_one_when_never_run(exporter):
    samples = exporter.collect()
    ec = next(s for s in samples if s.name == "cronwatch_job_last_exit_code" and s.labels["job"] == "backup")
    assert ec.value == -1.0


def test_collect_is_running_zero_by_default(exporter):
    samples = exporter.collect()
    running = next(s for s in samples if s.name == "cronwatch_job_is_running" and s.labels["job"] == "backup")
    assert running.value == 0.0


def test_render_text_contains_help_and_type(exporter):
    text = exporter.render_text()
    assert "# HELP" in text
    assert "# TYPE" in text


def test_render_text_no_duplicate_help_lines(exporter):
    text = exporter.render_text()
    help_lines = [l for l in text.splitlines() if l.startswith("# HELP cronwatch_job_last_exit_code")]
    assert len(help_lines) == 1


def test_render_samples_ends_with_newline():
    s = MetricSample(name="foo", labels={"job": "x"}, value=1.0, help_text="h")
    out = _render_samples([s])
    assert out.endswith("\n")
