"""Persistent checkpoint store for cron jobs.

Allows jobs to record named checkpoints (progress markers) so that
long-running jobs can report intermediate state and cronwatch can
detect stalled jobs that stopped advancing.
"""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Checkpoint:
    job_name: str
    run_id: str
    name: str
    value: str
    recorded_at: str  # ISO-8601


class CheckpointStore:
    """SQLite-backed store for job checkpoints."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        with self._connect() as conn:
            self._init_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name    TEXT    NOT NULL,
                run_id      TEXT    NOT NULL,
                name        TEXT    NOT NULL,
                value       TEXT    NOT NULL DEFAULT '',
                recorded_at TEXT    NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cp_job_run "
            "ON checkpoints (job_name, run_id)"
        )
        conn.commit()

    def set(self, job_name: str, run_id: str, name: str, value: str = "") -> None:
        """Insert or replace a checkpoint for a specific run."""
        ts = _utcnow().isoformat()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints (job_name, run_id, name, value, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_name, run_id, name, value, ts),
            )
            conn.commit()

    def get(self, job_name: str, run_id: str) -> List[Checkpoint]:
        """Return all checkpoints for a run, oldest first."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT job_name, run_id, name, value, recorded_at
                FROM checkpoints
                WHERE job_name = ? AND run_id = ?
                ORDER BY id ASC
                """,
                (job_name, run_id),
            ).fetchall()
        return [Checkpoint(**dict(r)) for r in rows]

    def latest(self, job_name: str, run_id: str) -> Optional[Checkpoint]:
        """Return the most recent checkpoint for a run, or None."""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_name, run_id, name, value, recorded_at
                FROM checkpoints
                WHERE job_name = ? AND run_id = ?
                ORDER BY id DESC LIMIT 1
                """,
                (job_name, run_id),
            ).fetchone()
        return Checkpoint(**dict(row)) if row else None

    def prune(self, job_name: str, run_id: str) -> int:
        """Delete all checkpoints for a run. Returns number of rows removed."""
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM checkpoints WHERE job_name = ? AND run_id = ?",
                (job_name, run_id),
            )
            conn.commit()
            return cur.rowcount
