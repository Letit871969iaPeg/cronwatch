"""Baseline duration tracking for cron jobs.

Records rolling average execution durations so the drift checker
can compare against a learned baseline rather than a static threshold.
"""

from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class BaselineSample:
    job_name: str
    duration_seconds: float
    recorded_at: str  # ISO-8601 UTC


class BaselineStore:
    """Persist and query per-job duration samples."""

    def __init__(self, db_path: str = "cronwatch_baseline.db") -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS baseline_samples (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name      TEXT    NOT NULL,
                duration_sec  REAL    NOT NULL,
                recorded_at   TEXT    NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_baseline_job ON baseline_samples(job_name)"
        )
        self._conn.commit()

    def add_sample(self, job_name: str, duration_seconds: float, recorded_at: str) -> None:
        """Insert a new duration sample for *job_name*."""
        self._conn.execute(
            "INSERT INTO baseline_samples (job_name, duration_sec, recorded_at) VALUES (?, ?, ?)",
            (job_name, duration_seconds, recorded_at),
        )
        self._conn.commit()

    def fetch_samples(
        self, job_name: str, limit: int = 50
    ) -> List[BaselineSample]:
        """Return the most recent *limit* samples for *job_name*."""
        rows = self._conn.execute(
            """
            SELECT job_name, duration_sec, recorded_at
            FROM baseline_samples
            WHERE job_name = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (job_name, limit),
        ).fetchall()
        return [BaselineSample(r[0], r[1], r[2]) for r in rows]

    def average_duration(self, job_name: str, limit: int = 50) -> Optional[float]:
        """Return the mean duration over the last *limit* samples, or None."""
        samples = self.fetch_samples(job_name, limit)
        if not samples:
            return None
        return statistics.mean(s.duration_seconds for s in samples)

    def clear(self, job_name: str) -> None:
        """Remove all baseline samples for *job_name*."""
        self._conn.execute(
            "DELETE FROM baseline_samples WHERE job_name = ?", (job_name,)
        )
        self._conn.commit()
