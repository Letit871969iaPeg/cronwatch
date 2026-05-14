"""Job cooldown enforcement — prevents a job from running again too soon after completion."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CooldownPolicy:
    job_name: str
    min_interval_seconds: int  # minimum seconds between completions


class JobCooldown:
    """Tracks last completion times and enforces per-job cooldown intervals."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._policies: dict[str, CooldownPolicy] = {}
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cooldown_log (
                    job_name TEXT NOT NULL,
                    completed_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cooldown_job ON cooldown_log(job_name)"
            )
            self._conn.commit()

    def set_policy(self, policy: CooldownPolicy) -> None:
        self._policies[policy.job_name] = policy

    def record_completion(self, job_name: str, completed_at: Optional[datetime] = None) -> None:
        ts = (completed_at or _utcnow()).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO cooldown_log (job_name, completed_at) VALUES (?, ?)",
                (job_name, ts),
            )
            self._conn.commit()

    def is_cooling_down(self, job_name: str, now: Optional[datetime] = None) -> bool:
        """Return True if the job is still within its cooldown window."""
        policy = self._policies.get(job_name)
        if policy is None:
            return False
        now = now or _utcnow()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT completed_at FROM cooldown_log
                WHERE job_name = ?
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (job_name,),
            ).fetchone()
        if row is None:
            return False
        last = datetime.fromisoformat(row[0])
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (now - last).total_seconds()
        return elapsed < policy.min_interval_seconds

    def seconds_remaining(self, job_name: str, now: Optional[datetime] = None) -> float:
        """Return seconds left in cooldown, or 0.0 if not cooling down."""
        policy = self._policies.get(job_name)
        if policy is None:
            return 0.0
        now = now or _utcnow()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT completed_at FROM cooldown_log
                WHERE job_name = ?
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (job_name,),
            ).fetchone()
        if row is None:
            return 0.0
        last = datetime.fromisoformat(row[0])
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        remaining = policy.min_interval_seconds - (now - last).total_seconds()
        return max(0.0, remaining)
