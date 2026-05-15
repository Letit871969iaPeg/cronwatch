"""Tests for cronwatch.job_trend."""

import pytest

from cronwatch.job_trend import JobTrendAnalyzer, TrendResult


@pytest.fixture
def analyzer(tmp_path):
    db = str(tmp_path / "trend.db")
    a = JobTrendAnalyzer(db, z_threshold=2.5, min_samples=5)
    yield a
    a.close()


def _populate(analyzer: JobTrendAnalyzer, job: str, durations, base_ts="2024-01-01T00:00:00"):
    for i, d in enumerate(durations):
        analyzer.record_duration(job, d, f"2024-01-01T{i:02d}:00:00")


# ── fetch_durations ────────────────────────────────────────────────────────────

def test_fetch_durations_empty(analyzer):
    assert analyzer.fetch_durations("backup") == []


def test_fetch_durations_returns_most_recent_first(analyzer):
    for val in [1.0, 2.0, 3.0]:
        analyzer.record_duration("job", val, "2024-01-01T00:00:00")
    result = analyzer.fetch_durations("job")
    assert result[0] == 3.0


def test_fetch_durations_respects_limit(analyzer):
    for v in range(20):
        analyzer.record_duration("job", float(v), "2024-01-01T00:00:00")
    assert len(analyzer.fetch_durations("job", limit=10)) == 10


# ── analyze – insufficient data ───────────────────────────────────────────────

def test_analyze_returns_none_when_too_few_samples(analyzer):
    _populate(analyzer, "job", [10.0, 11.0, 12.0])  # only 3, min=5
    assert analyzer.analyze("job") is None


def test_analyze_returns_none_for_unknown_job(analyzer):
    assert analyzer.analyze("ghost") is None


# ── analyze – normal run ──────────────────────────────────────────────────────

def test_analyze_returns_trend_result(analyzer):
    _populate(analyzer, "job", [10.0, 10.5, 9.8, 10.2, 10.1, 10.3])
    result = analyzer.analyze("job")
    assert isinstance(result, TrendResult)
    assert result.job_name == "job"


def test_analyze_no_anomaly_for_stable_job(analyzer):
    _populate(analyzer, "job", [10.0, 10.0, 10.0, 10.0, 10.0, 10.0])
    result = analyzer.analyze("job")
    assert result is not None
    assert not result.is_anomaly
    assert result.z_score == 0.0


# ── analyze – anomaly detection ───────────────────────────────────────────────

def test_analyze_flags_anomaly_on_spike(analyzer):
    # stable history then a huge spike as the latest entry
    stable = [10.0] * 9
    _populate(analyzer, "job", stable)
    analyzer.record_duration("job", 999.0, "2024-01-02T00:00:00")
    result = analyzer.analyze("job")
    assert result is not None
    assert result.is_anomaly
    assert result.latest == 999.0


def test_analyze_z_score_positive(analyzer):
    _populate(analyzer, "job", [5.0, 5.0, 5.0, 5.0, 5.0, 100.0])
    result = analyzer.analyze("job")
    assert result is not None
    assert result.z_score >= 0.0


# ── custom threshold ──────────────────────────────────────────────────────────

def test_custom_z_threshold_suppresses_anomaly(tmp_path):
    db = str(tmp_path / "t.db")
    a = JobTrendAnalyzer(db, z_threshold=100.0, min_samples=5)
    _populate(a, "job", [10.0] * 9)
    a.record_duration("job", 999.0, "2024-01-02T00:00:00")
    result = a.analyze("job")
    assert result is not None
    assert not result.is_anomaly
    a.close()
