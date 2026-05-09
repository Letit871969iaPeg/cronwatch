"""Attach and retrieve arbitrary key-value labels on jobs for grouping/filtering."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class JobLabel:
    job_name: str
    key: str
    value: str


class LabelStore:
    """Persist job labels in a SQLite database."""

    def __init__(self, db_path: str | Path) -> None:
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_labels (
                job_name TEXT NOT NULL,
                key      TEXT NOT NULL,
                value    TEXT NOT NULL,
                PRIMARY KEY (job_name, key)
            )
            """
        )
        self._conn.commit()

    def set(self, job_name: str, key: str, value: str) -> None:
        """Insert or replace a label for a job."""
        self._conn.execute(
            "INSERT OR REPLACE INTO job_labels (job_name, key, value) VALUES (?, ?, ?)",
            (job_name, key, value),
        )
        self._conn.commit()

    def get(self, job_name: str) -> Dict[str, str]:
        """Return all labels for *job_name* as a plain dict."""
        cur = self._conn.execute(
            "SELECT key, value FROM job_labels WHERE job_name = ?",
            (job_name,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}

    def delete(self, job_name: str, key: str) -> None:
        """Remove a single label from a job."""
        self._conn.execute(
            "DELETE FROM job_labels WHERE job_name = ? AND key = ?",
            (job_name, key),
        )
        self._conn.commit()

    def find_by_label(self, key: str, value: str) -> List[str]:
        """Return job names that have a matching key=value label."""
        cur = self._conn.execute(
            "SELECT DISTINCT job_name FROM job_labels WHERE key = ? AND value = ?",
            (key, value),
        )
        return [row[0] for row in cur.fetchall()]

    def all_labels(self) -> List[JobLabel]:
        """Return every stored label across all jobs."""
        cur = self._conn.execute(
            "SELECT job_name, key, value FROM job_labels ORDER BY job_name, key"
        )
        return [JobLabel(job_name=r[0], key=r[1], value=r[2]) for r in cur.fetchall()]
