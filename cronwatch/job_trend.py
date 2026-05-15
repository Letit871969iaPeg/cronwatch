"""Tracks execution duration trends for cron jobs and detects anomalies."""

from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class TrendResult:
    job_name: str
    mean: float
    stddev: float
    latest: float
    z_score: float
    is_anomaly: bool


class JobTrendAnalyzer:
    """Stores recent durations and flags jobs whose latest run deviates significantly."""

    def __init__(self, db_path: str, z_threshold: float = 2.5, min_samples: int = 5) -> None:
        self._db_path = db_path
        self._z_threshold = z_threshold
        self._min_samples = min_samples
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_durations (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT    NOT NULL,
                duration REAL    NOT NULL,
                recorded TEXT    NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jd_job ON job_durations(job_name)"
        )
        self._conn.commit()

    def record_duration(self, job_name: str, duration: float, recorded: str) -> None:
        """Persist a single duration sample."""
        self._conn.execute(
            "INSERT INTO job_durations (job_name, duration, recorded) VALUES (?, ?, ?)",
            (job_name, duration, recorded),
        )
        self._conn.commit()

    def fetch_durations(self, job_name: str, limit: int = 50) -> List[float]:
        """Return up to *limit* most-recent durations for the given job."""
        rows = self._conn.execute(
            "SELECT duration FROM job_durations WHERE job_name = ? "
            "ORDER BY id DESC LIMIT ?",
            (job_name, limit),
        ).fetchall()
        return [r[0] for r in rows]

    def analyze(self, job_name: str) -> Optional[TrendResult]:
        """Compute trend statistics; returns None when too few samples exist."""
        durations = self.fetch_durations(job_name)
        if len(durations) < self._min_samples:
            return None

        latest = durations[0]
        historical = durations[1:]
        mean = statistics.mean(historical)
        stddev = statistics.pstdev(historical)

        if stddev == 0.0:
            z_score = 0.0
        else:
            z_score = abs(latest - mean) / stddev

        return TrendResult(
            job_name=job_name,
            mean=mean,
            stddev=stddev,
            latest=latest,
            z_score=z_score,
            is_anomaly=z_score >= self._z_threshold,
        )

    def close(self) -> None:
        self._conn.close()
