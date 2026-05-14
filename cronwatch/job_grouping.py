"""Job grouping — organise jobs into named groups and query by group."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class JobGroup:
    name: str
    jobs: List[str] = field(default_factory=list)
    description: str = ""


class GroupStore:
    """Persists job-to-group mappings in SQLite."""

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
                CREATE TABLE IF NOT EXISTS job_groups (
                    group_name TEXT NOT NULL,
                    job_name   TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (group_name, job_name)
                )
                """
            )

    def add(self, group_name: str, job_name: str, description: str = "") -> None:
        """Add *job_name* to *group_name*, upserting the description."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_groups (group_name, job_name, description)
                VALUES (?, ?, ?)
                ON CONFLICT (group_name, job_name) DO UPDATE SET description=excluded.description
                """,
                (group_name, job_name, description),
            )

    def remove(self, group_name: str, job_name: str) -> None:
        """Remove *job_name* from *group_name*."""
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM job_groups WHERE group_name=? AND job_name=?",
                (group_name, job_name),
            )

    def get_group(self, group_name: str) -> Optional[JobGroup]:
        """Return a :class:`JobGroup` or *None* if unknown."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT job_name, description FROM job_groups WHERE group_name=? ORDER BY job_name",
                (group_name,),
            ).fetchall()
        if not rows:
            return None
        desc = rows[0]["description"] if rows else ""
        return JobGroup(
            name=group_name,
            jobs=[r["job_name"] for r in rows],
            description=desc,
        )

    def list_groups(self) -> List[str]:
        """Return sorted list of all known group names."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT group_name FROM job_groups ORDER BY group_name"
            ).fetchall()
        return [r["group_name"] for r in rows]

    def groups_for_job(self, job_name: str) -> List[str]:
        """Return all group names that contain *job_name*."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT group_name FROM job_groups WHERE job_name=? ORDER BY group_name",
                (job_name,),
            ).fetchall()
        return [r["group_name"] for r in rows]

    def all_groups(self) -> Dict[str, JobGroup]:
        """Return a mapping of group_name -> :class:`JobGroup` for all groups."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT group_name, job_name, description FROM job_groups ORDER BY group_name, job_name"
            ).fetchall()
        result: Dict[str, JobGroup] = {}
        for row in rows:
            gname = row["group_name"]
            if gname not in result:
                result[gname] = JobGroup(name=gname, description=row["description"])
            result[gname].jobs.append(row["job_name"])
        return result
