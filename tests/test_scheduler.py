"""Tests for the Scheduler class."""

import time
from unittest.mock import MagicMock, call, patch

import pytest

from cronwatch.config import CronwatchConfig, JobConfig
from cronwatch.scheduler import Scheduler


@pytest.fixture()
def job_cfg():
    return JobConfig(name="backup", schedule="0 2 * * *", max_duration=300, drift_threshold=120)


@pytest.fixture()
def config(job_cfg):
    return CronwatchConfig(jobs=[job_cfg])


@pytest.fixture()
def tracker():
    return MagicMock()


@pytest.fixture()
def alerter():
    return MagicMock()


@pytest.fixture()
def scheduler(config, tracker, alerter):
    return Scheduler(config, tracker, alerter, interval=60)


def test_initial_state_not_running(scheduler):
    assert not scheduler.is_running


def test_start_sets_running(scheduler):
    scheduler.start()
    try:
        assert scheduler.is_running
    finally:
        scheduler.stop()


def test_stop_ends_thread(scheduler):
    scheduler.start()
    scheduler.stop(timeout=2.0)
    assert not scheduler.is_running


def test_double_start_raises(scheduler):
    scheduler.start()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            scheduler.start()
    finally:
        scheduler.stop()


def test_tick_calls_check_for_each_job(scheduler, job_cfg):
    with patch.object(scheduler._checker, "check_job") as mock_check:
        scheduler._tick()
        mock_check.assert_called_once_with(job_cfg)


def test_tick_continues_on_exception(config, tracker, alerter):
    extra_job = JobConfig(name="report", schedule="0 6 * * *", max_duration=60, drift_threshold=30)
    config.jobs.append(extra_job)
    sched = Scheduler(config, tracker, alerter, interval=60)
    with patch.object(sched._checker, "check_job", side_effect=RuntimeError("boom")) as mock_check:
        sched._tick()  # should not raise
        assert mock_check.call_count == 2


def test_tick_invoked_during_run(config, tracker, alerter):
    sched = Scheduler(config, tracker, alerter, interval=1)
    with patch.object(sched, "_tick") as mock_tick:
        sched.start()
        time.sleep(0.2)
        sched.stop(timeout=2.0)
        assert mock_tick.call_count >= 1
