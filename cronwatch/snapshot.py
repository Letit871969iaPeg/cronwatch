"""Snapshot module: capture and persist point-in-time job status snapshots."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class JobSnapshot:
    job_name: str
    captured_at: str  # ISO-8601
    last_status: Optional[str]
    last_run_ts: Optional[str]
    last_duration_s: Optional[float]
    consecutive_failures: int


class SnapshotStore:
    """Persists periodic snapshots of job state to SQLite."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name        TEXT    NOT NULL,
                captured_at     TEXT    NOT NULL,
                last_status     TEXT,
                last_run_ts     TEXT,
                last_duration_s REAL,
                consecutive_failures INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snap_job ON snapshots(job_name)"
        )
        self._conn.commit()

    def save(self, snap: JobSnapshot) -> None:
        self._conn.execute(
            """
            INSERT INTO snapshots
                (job_name, captured_at, last_status, last_run_ts,
                 last_duration_s, consecutive_failures)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snap.job_name,
                snap.captured_at,
                snap.last_status,
                snap.last_run_ts,
                snap.last_duration_s,
                snap.consecutive_failures,
            ),
        )
        self._conn.commit()

    def fetch(self, job_name: str, limit: int = 50) -> List[JobSnapshot]:
        cur = self._conn.execute(
            """
            SELECT job_name, captured_at, last_status, last_run_ts,
                   last_duration_s, consecutive_failures
            FROM snapshots
            WHERE job_name = ?
            ORDER BY captured_at DESC
            LIMIT ?
            """,
            (job_name, limit),
        )
        return [
            JobSnapshot(
                job_name=row[0],
                captured_at=row[1],
                last_status=row[2],
                last_run_ts=row[3],
                last_duration_s=row[4],
                consecutive_failures=row[5],
            )
            for row in cur.fetchall()
        ]

    def prune(self, job_name: str, keep: int = 200) -> int:
        """Remove oldest snapshots beyond *keep* for a given job."""
        cur = self._conn.execute(
            """
            DELETE FROM snapshots
            WHERE job_name = ?
              AND id NOT IN (
                SELECT id FROM snapshots
                WHERE job_name = ?
                ORDER BY captured_at DESC
                LIMIT ?
              )
            """,
            (job_name, job_name, keep),
        )
        self._conn.commit()
        return cur.rowcount
