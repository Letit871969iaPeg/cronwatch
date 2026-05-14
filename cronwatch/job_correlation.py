"""Track correlated job runs — link related jobs by a shared correlation ID."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CorrelationEntry:
    correlation_id: str
    job_name: str
    run_id: str
    created_at: str


class CorrelationStore:
    """SQLite-backed store for job correlation entries."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS correlations (
                correlation_id TEXT NOT NULL,
                job_name       TEXT NOT NULL,
                run_id         TEXT NOT NULL,
                created_at     TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_corr_id ON correlations(correlation_id)"
        )
        self._conn.commit()

    def new_correlation_id(self) -> str:
        """Generate a fresh correlation ID."""
        return str(uuid.uuid4())

    def link(self, correlation_id: str, job_name: str, run_id: str) -> None:
        """Associate a job run with a correlation ID."""
        self._conn.execute(
            "INSERT INTO correlations VALUES (?, ?, ?, ?)",
            (correlation_id, job_name, run_id, _utcnow()),
        )
        self._conn.commit()

    def fetch(self, correlation_id: str) -> List[CorrelationEntry]:
        """Return all entries for a given correlation ID."""
        cur = self._conn.execute(
            "SELECT correlation_id, job_name, run_id, created_at "
            "FROM correlations WHERE correlation_id = ? ORDER BY created_at",
            (correlation_id,),
        )
        return [
            CorrelationEntry(correlation_id=r[0], job_name=r[1], run_id=r[2], created_at=r[3])
            for r in cur.fetchall()
        ]

    def fetch_by_job(self, job_name: str) -> List[CorrelationEntry]:
        """Return all correlation entries for a specific job."""
        cur = self._conn.execute(
            "SELECT correlation_id, job_name, run_id, created_at "
            "FROM correlations WHERE job_name = ? ORDER BY created_at DESC",
            (job_name,),
        )
        return [
            CorrelationEntry(correlation_id=r[0], job_name=r[1], run_id=r[2], created_at=r[3])
            for r in cur.fetchall()
        ]

    def delete(self, correlation_id: str) -> int:
        """Remove all entries for a correlation ID. Returns number of rows deleted."""
        cur = self._conn.execute(
            "DELETE FROM correlations WHERE correlation_id = ?", (correlation_id,)
        )
        self._conn.commit()
        return cur.rowcount
