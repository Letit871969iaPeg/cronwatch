"""Job ownership registry — maps jobs to owner/team metadata."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass
class OwnerEntry:
    job_name: str
    owner: str
    team: Optional[str] = None
    email: Optional[str] = None
    slack_channel: Optional[str] = None


class OwnershipStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_ownership (
                job_name      TEXT PRIMARY KEY,
                owner         TEXT NOT NULL,
                team          TEXT,
                email         TEXT,
                slack_channel TEXT
            )
            """
        )
        self._conn.commit()

    def set(self, entry: OwnerEntry) -> None:
        self._conn.execute(
            """
            INSERT INTO job_ownership (job_name, owner, team, email, slack_channel)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(job_name) DO UPDATE SET
                owner         = excluded.owner,
                team          = excluded.team,
                email         = excluded.email,
                slack_channel = excluded.slack_channel
            """,
            (
                entry.job_name,
                entry.owner,
                entry.team,
                entry.email,
                entry.slack_channel,
            ),
        )
        self._conn.commit()

    def get(self, job_name: str) -> Optional[OwnerEntry]:
        row = self._conn.execute(
            "SELECT * FROM job_ownership WHERE job_name = ?", (job_name,)
        ).fetchone()
        if row is None:
            return None
        return OwnerEntry(
            job_name=row["job_name"],
            owner=row["owner"],
            team=row["team"],
            email=row["email"],
            slack_channel=row["slack_channel"],
        )

    def delete(self, job_name: str) -> bool:
        cur = self._conn.execute(
            "DELETE FROM job_ownership WHERE job_name = ?", (job_name,)
        )
        self._conn.commit()
        return cur.rowcount > 0

    def all(self) -> list[OwnerEntry]:
        rows = self._conn.execute(
            "SELECT * FROM job_ownership ORDER BY job_name"
        ).fetchall()
        return [
            OwnerEntry(
                job_name=r["job_name"],
                owner=r["owner"],
                team=r["team"],
                email=r["email"],
                slack_channel=r["slack_channel"],
            )
            for r in rows
        ]
