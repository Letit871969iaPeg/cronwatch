"""Job execution throttling — prevent a job from running more frequently than allowed."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ThrottlePolicy:
    min_interval_seconds: int  # minimum seconds between successful runs


class JobThrottle:
    """Persists last-run timestamps and enforces minimum intervals between job executions."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._policies: dict[str, ThrottlePolicy] = {}
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_throttle (
                job_name TEXT PRIMARY KEY,
                last_allowed_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def set_policy(self, job_name: str, policy: ThrottlePolicy) -> None:
        with self._lock:
            self._policies[job_name] = policy

    def is_throttled(self, job_name: str) -> bool:
        """Return True if the job should be suppressed due to throttling."""
        with self._lock:
            policy = self._policies.get(job_name)
            if policy is None:
                return False
            row = self._conn.execute(
                "SELECT last_allowed_at FROM job_throttle WHERE job_name = ?",
                (job_name,),
            ).fetchone()
            if row is None:
                return False
            last = datetime.fromisoformat(row[0])
            elapsed = (_utcnow() - last).total_seconds()
            return elapsed < policy.min_interval_seconds

    def record_run(self, job_name: str) -> None:
        """Record that the job was allowed to run right now."""
        with self._lock:
            now = _utcnow().isoformat()
            self._conn.execute(
                """
                INSERT INTO job_throttle (job_name, last_allowed_at)
                VALUES (?, ?)
                ON CONFLICT(job_name) DO UPDATE SET last_allowed_at = excluded.last_allowed_at
                """,
                (job_name, now),
            )
            self._conn.commit()

    def last_allowed_at(self, job_name: str) -> Optional[datetime]:
        row = self._conn.execute(
            "SELECT last_allowed_at FROM job_throttle WHERE job_name = ?",
            (job_name,),
        ).fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(row[0])
