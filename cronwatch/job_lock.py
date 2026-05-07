"""Lightweight file-based lock to prevent overlapping cron job runs."""

import os
import time
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_LOCK_DIR = "/tmp/cronwatch/locks"


@dataclass
class LockInfo:
    job_name: str
    pid: int
    acquired_at: float

    def age_seconds(self) -> float:
        return time.time() - self.acquired_at


class JobLock:
    """File-based lock for a single cron job."""

    def __init__(self, job_name: str, lock_dir: str = _DEFAULT_LOCK_DIR, stale_after: float = 3600.0):
        self.job_name = job_name
        self.stale_after = stale_after
        self._lock_dir = Path(lock_dir)
        self._lock_file = self._lock_dir / f"{job_name}.lock"

    def _lock_dir_ensure(self) -> None:
        self._lock_dir.mkdir(parents=True, exist_ok=True)

    def acquire(self) -> bool:
        """Try to acquire the lock. Returns True on success, False if already locked."""
        self._lock_dir_ensure()
        existing = self.read()
        if existing is not None:
            if existing.age_seconds() < self.stale_after:
                logger.warning(
                    "Job '%s' is already locked (pid=%d, age=%.1fs)",
                    self.job_name, existing.pid, existing.age_seconds()
                )
                return False
            logger.warning(
                "Removing stale lock for '%s' (pid=%d, age=%.1fs)",
                self.job_name, existing.pid, existing.age_seconds()
            )
            self.release()

        pid = os.getpid()
        acquired_at = time.time()
        self._lock_file.write_text(f"{pid}\n{acquired_at}\n", encoding="utf-8")
        logger.debug("Lock acquired for '%s' (pid=%d)", self.job_name, pid)
        return True

    def release(self) -> None:
        """Release the lock by removing the lock file."""
        try:
            self._lock_file.unlink()
            logger.debug("Lock released for '%s'", self.job_name)
        except FileNotFoundError:
            pass

    def read(self) -> Optional[LockInfo]:
        """Read current lock info without modifying it."""
        try:
            text = self._lock_file.read_text(encoding="utf-8")
            lines = text.strip().splitlines()
            pid = int(lines[0])
            acquired_at = float(lines[1])
            return LockInfo(job_name=self.job_name, pid=pid, acquired_at=acquired_at)
        except (FileNotFoundError, ValueError, IndexError):
            return None

    def is_locked(self) -> bool:
        info = self.read()
        if info is None:
            return False
        return info.age_seconds() < self.stale_after
