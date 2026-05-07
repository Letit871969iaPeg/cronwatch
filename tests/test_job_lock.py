"""Tests for cronwatch.job_lock."""

import os
import time
import pytest
from pathlib import Path
from cronwatch.job_lock import JobLock, LockInfo


@pytest.fixture
def lock_dir(tmp_path):
    return str(tmp_path / "locks")


@pytest.fixture
def lock(lock_dir):
    return JobLock("backup", lock_dir=lock_dir)


def test_acquire_creates_lock_file(lock, lock_dir):
    assert lock.acquire() is True
    lock_file = Path(lock_dir) / "backup.lock"
    assert lock_file.exists()


def test_acquire_returns_false_when_already_locked(lock):
    assert lock.acquire() is True
    assert lock.acquire() is False


def test_release_removes_lock_file(lock, lock_dir):
    lock.acquire()
    lock.release()
    lock_file = Path(lock_dir) / "backup.lock"
    assert not lock_file.exists()


def test_release_no_error_when_not_locked(lock):
    # Should not raise even if lock file doesn't exist
    lock.release()


def test_read_returns_none_when_no_lock(lock):
    assert lock.read() is None


def test_read_returns_lock_info_after_acquire(lock):
    lock.acquire()
    info = lock.read()
    assert info is not None
    assert isinstance(info, LockInfo)
    assert info.pid == os.getpid()
    assert info.job_name == "backup"
    assert info.acquired_at <= time.time()


def test_is_locked_true_when_active(lock):
    lock.acquire()
    assert lock.is_locked() is True


def test_is_locked_false_when_released(lock):
    lock.acquire()
    lock.release()
    assert lock.is_locked() is False


def test_stale_lock_is_replaced(lock_dir):
    stale_lock = JobLock("stale_job", lock_dir=lock_dir, stale_after=0.01)
    stale_lock.acquire()
    time.sleep(0.05)

    # A new lock with same name should detect stale and replace it
    new_lock = JobLock("stale_job", lock_dir=lock_dir, stale_after=0.01)
    result = new_lock.acquire()
    assert result is True
    info = new_lock.read()
    assert info is not None
    assert info.pid == os.getpid()


def test_lock_dir_created_automatically(tmp_path):
    deep_dir = str(tmp_path / "a" / "b" / "c")
    lock = JobLock("autojob", lock_dir=deep_dir)
    assert lock.acquire() is True
    assert Path(deep_dir).exists()


def test_lock_info_age_seconds():
    past = time.time() - 10.0
    info = LockInfo(job_name="x", pid=1234, acquired_at=past)
    assert info.age_seconds() >= 10.0
