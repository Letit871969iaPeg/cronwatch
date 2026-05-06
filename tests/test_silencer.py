"""Tests for cronwatch.silencer."""

from datetime import datetime, time

import pytest

from cronwatch.silencer import SilenceWindow, Silencer, load_silencer


# ---------------------------------------------------------------------------
# SilenceWindow.is_active
# ---------------------------------------------------------------------------

class TestSilenceWindowIsActive:
    def _window(self, start: str, end: str) -> SilenceWindow:
        return SilenceWindow(job_name="job", start=time.fromisoformat(start), end=time.fromisoformat(end))

    def test_active_within_normal_window(self):
        w = self._window("02:00", "04:00")
        assert w.is_active(datetime(2024, 1, 1, 3, 0))

    def test_inactive_outside_normal_window(self):
        w = self._window("02:00", "04:00")
        assert not w.is_active(datetime(2024, 1, 1, 5, 0))

    def test_active_overnight_window_before_midnight(self):
        w = self._window("23:00", "01:00")
        assert w.is_active(datetime(2024, 1, 1, 23, 30))

    def test_active_overnight_window_after_midnight(self):
        w = self._window("23:00", "01:00")
        assert w.is_active(datetime(2024, 1, 1, 0, 30))

    def test_inactive_overnight_window_midday(self):
        w = self._window("23:00", "01:00")
        assert not w.is_active(datetime(2024, 1, 1, 12, 0))

    def test_boundary_start_is_inclusive(self):
        w = self._window("03:00", "05:00")
        assert w.is_active(datetime(2024, 1, 1, 3, 0))

    def test_boundary_end_is_inclusive(self):
        w = self._window("03:00", "05:00")
        assert w.is_active(datetime(2024, 1, 1, 5, 0))


# ---------------------------------------------------------------------------
# Silencer
# ---------------------------------------------------------------------------

@pytest.fixture()
def silencer() -> Silencer:
    s = Silencer()
    s.add_window(SilenceWindow(
        job_name="backup",
        start=time(2, 0),
        end=time(4, 0),
        reason="nightly maintenance",
    ))
    return s


def test_is_silenced_returns_true_during_window(silencer):
    assert silencer.is_silenced("backup", now=datetime(2024, 6, 1, 3, 0))


def test_is_silenced_returns_false_outside_window(silencer):
    assert not silencer.is_silenced("backup", now=datetime(2024, 6, 1, 10, 0))


def test_is_silenced_returns_false_for_unknown_job(silencer):
    assert not silencer.is_silenced("other_job", now=datetime(2024, 6, 1, 3, 0))


# ---------------------------------------------------------------------------
# load_silencer
# ---------------------------------------------------------------------------

def test_load_silencer_builds_windows():
    raw = [
        {
            "name": "report",
            "silence_windows": [
                {"start": "01:00", "end": "02:00", "reason": "deploy"},
            ],
        },
        {"name": "cleanup", "silence_windows": []},
    ]
    s = load_silencer(raw)
    assert s.is_silenced("report", now=datetime(2024, 1, 1, 1, 30))
    assert not s.is_silenced("report", now=datetime(2024, 1, 1, 5, 0))
    assert not s.is_silenced("cleanup", now=datetime(2024, 1, 1, 1, 30))


def test_load_silencer_no_windows():
    s = load_silencer([{"name": "job"}])
    assert not s.is_silenced("job")
