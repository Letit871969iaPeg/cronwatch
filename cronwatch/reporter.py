"""CLI-friendly summary reporter that reads from HistoryStore."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from cronwatch.history import DEFAULT_DB_PATH, HistoryStore


def _fmt_ts(ts: Optional[float]) -> str:
    if ts is None:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_dur(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def print_job_summary(
    job_name: str,
    limit: int = 10,
    db_path: Path = DEFAULT_DB_PATH,
    file=sys.stdout,
) -> None:
    """Print a human-readable table of recent runs for *job_name*."""
    store = HistoryStore(db_path=db_path)
    rows = store.fetch(job_name, limit=limit)
    avg = store.average_duration(job_name, window=limit)
    store.close()

    if not rows:
        print(f"No history found for job: {job_name}", file=file)
        return

    header = f"{'#':<4}  {'Started':<20}  {'Finished':<20}  {'Duration':<10}  {'Exit':>4}"
    separator = "-" * len(header)
    print(f"\nJob: {job_name}  (showing last {len(rows)} runs)", file=file)
    print(separator, file=file)
    print(header, file=file)
    print(separator, file=file)

    for i, row in enumerate(rows, start=1):
        print(
            f"{i:<4}  "
            f"{_fmt_ts(row['started_at']):<20}  "
            f"{_fmt_ts(row['finished_at']):<20}  "
            f"{_fmt_dur(row['duration_s']):<10}  "
            f"{(row['exit_code'] if row['exit_code'] is not None else '?'):>4}",
            file=file,
        )

    print(separator, file=file)
    print(f"Average duration (successful): {_fmt_dur(avg)}", file=file)
    print(file=file)


def print_all_jobs(db_path: Path = DEFAULT_DB_PATH, file=sys.stdout) -> None:
    """Print a one-line summary for every distinct job in the database."""
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT job_name, COUNT(*) as runs,"
        " SUM(CASE WHEN exit_code = 0 THEN 1 ELSE 0 END) as ok,"
        " MAX(started_at) as last_run"
        " FROM job_history GROUP BY job_name ORDER BY job_name"
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No job history found.", file=file)
        return

    print(f"\n{'Job':<30}  {'Runs':>5}  {'OK':>5}  {'Last Run':<20}", file=file)
    print("-" * 65, file=file)
    for row in rows:
        print(
            f"{row['job_name']:<30}  {row['runs']:>5}  {row['ok']:>5}  "
            f"{_fmt_ts(row['last_run']):<20}",
            file=file,
        )
    print(file=file)
