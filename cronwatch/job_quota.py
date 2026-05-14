"""Job execution quota enforcement — limits how many times a job may run
within a rolling time window."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class QuotaPolicy:
    max_runs: int          # maximum allowed executions
    window_seconds: int    # rolling window length in seconds


class JobQuota:
    """SQLite-backed rolling-window quota tracker."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._policies: dict[str, QuotaPolicy] = {}
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quota_runs (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                job     TEXT NOT NULL,
                ran_at  TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quota_job_time ON quota_runs(job, ran_at)"
        )
        self._conn.commit()

    def set_policy(self, job_name: str, policy: QuotaPolicy) -> None:
        self._policies[job_name] = policy

    def record_run(self, job_name: str, ran_at: Optional[datetime] = None) -> None:
        ts = (ran_at or _utcnow()).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO quota_runs (job, ran_at) VALUES (?, ?)", (job_name, ts)
            )
            self._conn.commit()

    def is_quota_exceeded(self, job_name: str) -> bool:
        policy = self._policies.get(job_name)
        if policy is None:
            return False
        cutoff = (_utcnow() - timedelta(seconds=policy.window_seconds)).isoformat()
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM quota_runs WHERE job = ? AND ran_at >= ?",
                (job_name, cutoff),
            ).fetchone()
        return row[0] >= policy.max_runs

    def runs_in_window(self, job_name: str) -> int:
        policy = self._policies.get(job_name)
        if policy is None:
            return 0
        cutoff = (_utcnow() - timedelta(seconds=policy.window_seconds)).isoformat()
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM quota_runs WHERE job = ? AND ran_at >= ?",
                (job_name, cutoff),
            ).fetchone()
        return row[0]
