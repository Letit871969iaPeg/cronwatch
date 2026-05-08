"""Job annotation store: attach arbitrary key-value metadata to job runs."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Annotation:
    job_name: str
    run_id: str
    key: str
    value: str
    created_at: str


class AnnotationStore:
    """SQLite-backed store for job run annotations."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS annotations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name    TEXT NOT NULL,
                run_id      TEXT NOT NULL,
                key         TEXT NOT NULL,
                value       TEXT NOT NULL,
                created_at  TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ann_job_run "
            "ON annotations (job_name, run_id)"
        )
        self._conn.commit()

    def set(self, job_name: str, run_id: str, key: str, value: str) -> None:
        """Insert or replace an annotation for a specific run."""
        self._conn.execute(
            "DELETE FROM annotations "
            "WHERE job_name=? AND run_id=? AND key=?",
            (job_name, run_id, key),
        )
        self._conn.execute(
            "INSERT INTO annotations (job_name, run_id, key, value, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (job_name, run_id, key, value, _utcnow()),
        )
        self._conn.commit()

    def get(self, job_name: str, run_id: str) -> Dict[str, str]:
        """Return all annotations for a given job run as a dict."""
        cur = self._conn.execute(
            "SELECT key, value FROM annotations "
            "WHERE job_name=? AND run_id=?",
            (job_name, run_id),
        )
        return {row[0]: row[1] for row in cur.fetchall()}

    def fetch_all(self, job_name: str) -> List[Annotation]:
        """Return all annotations for every run of a job, newest first."""
        cur = self._conn.execute(
            "SELECT job_name, run_id, key, value, created_at "
            "FROM annotations WHERE job_name=? ORDER BY id DESC",
            (job_name,),
        )
        return [
            Annotation(job_name=r[0], run_id=r[1], key=r[2], value=r[3], created_at=r[4])
            for r in cur.fetchall()
        ]

    def delete_run(self, job_name: str, run_id: str) -> int:
        """Remove all annotations for a run. Returns number of rows deleted."""
        cur = self._conn.execute(
            "DELETE FROM annotations WHERE job_name=? AND run_id=?",
            (job_name, run_id),
        )
        self._conn.commit()
        return cur.rowcount
