"""Per-job runbook links and notes stored in SQLite."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass
class RunbookEntry:
    job_name: str
    url: Optional[str]
    notes: Optional[str]


class RunbookStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runbooks (
                    job_name TEXT PRIMARY KEY,
                    url      TEXT,
                    notes    TEXT
                )
                """
            )

    def set(self, job_name: str, url: Optional[str] = None, notes: Optional[str] = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runbooks (job_name, url, notes)
                VALUES (?, ?, ?)
                ON CONFLICT(job_name) DO UPDATE SET url=excluded.url, notes=excluded.notes
                """,
                (job_name, url, notes),
            )

    def get(self, job_name: str) -> Optional[RunbookEntry]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT job_name, url, notes FROM runbooks WHERE job_name = ?",
                (job_name,),
            ).fetchone()
        if row is None:
            return None
        return RunbookEntry(job_name=row["job_name"], url=row["url"], notes=row["notes"])

    def delete(self, job_name: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM runbooks WHERE job_name = ?", (job_name,))
        return cur.rowcount > 0

    def all(self) -> list[RunbookEntry]:
        with self._connect() as conn:
            rows = conn.execute("SELECT job_name, url, notes FROM runbooks ORDER BY job_name").fetchall()
        return [RunbookEntry(job_name=r["job_name"], url=r["url"], notes=r["notes"]) for r in rows]
