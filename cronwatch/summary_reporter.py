"""Generates a structured summary report of all monitored cron jobs."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import List, Optional

from cronwatch.history import HistoryStore
from cronwatch.tracker import JobTracker


@dataclass
class JobSummaryEntry:
    name: str
    last_run: Optional[str]
    last_status: Optional[str]
    last_duration_s: Optional[float]
    run_count: int
    failure_count: int
    avg_duration_s: Optional[float]


def build_job_summary(name: str, tracker: JobTracker, history: HistoryStore, limit: int = 50) -> JobSummaryEntry:
    record = tracker.get(name)
    records = history.fetch(name, limit=limit)

    run_count = len(records)
    failure_count = sum(1 for r in records if r.exit_code != 0)

    durations = [
        (r.end_time - r.start_time).total_seconds()
        for r in records
        if r.end_time is not None
    ]
    avg_duration = sum(durations) / len(durations) if durations else None

    last_run = None
    last_status = None
    last_duration = None

    if record is not None:
        last_run = record.start_time.isoformat() if record.start_time else None
        last_status = "success" if record.exit_code == 0 else ("failure" if record.exit_code is not None else "running")
        if record.end_time and record.start_time:
            last_duration = (record.end_time - record.start_time).total_seconds()

    return JobSummaryEntry(
        name=name,
        last_run=last_run,
        last_status=last_status,
        last_duration_s=last_duration,
        run_count=run_count,
        failure_count=failure_count,
        avg_duration_s=round(avg_duration, 3) if avg_duration is not None else None,
    )


def build_full_report(
    job_names: List[str],
    tracker: JobTracker,
    history: HistoryStore,
    limit: int = 50,
) -> dict:
    entries = [build_job_summary(n, tracker, history, limit) for n in job_names]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "jobs": [asdict(e) for e in entries],
    }


def print_json_report(report: dict) -> None:
    print(json.dumps(report, indent=2))
