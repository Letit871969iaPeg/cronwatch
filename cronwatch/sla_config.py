"""Load SLA policies from CronwatchConfig job entries."""
from __future__ import annotations

from typing import List

from cronwatch.job_sla import SLAPolicy


def load_sla_policies(config) -> List[SLAPolicy]:
    """Extract SLA policies from a CronwatchConfig (or similar namespace).

    Each job entry may carry an ``sla`` sub-key with:
        max_duration_seconds: float
        deadline_time: "HH:MM"  (optional)
    """
    policies: List[SLAPolicy] = []
    jobs = getattr(config, "jobs", []) or []
    for job in jobs:
        name = getattr(job, "name", None)
        if not name:
            continue
        sla_raw = getattr(job, "sla", None)
        if sla_raw is None:
            continue
        if isinstance(sla_raw, dict):
            max_dur = sla_raw.get("max_duration_seconds")
            deadline = sla_raw.get("deadline_time")
        else:
            max_dur = getattr(sla_raw, "max_duration_seconds", None)
            deadline = getattr(sla_raw, "deadline_time", None)
        if max_dur is None:
            continue
        policies.append(
            SLAPolicy(
                job_name=name,
                max_duration_seconds=float(max_dur),
                deadline_time=deadline,
            )
        )
    return policies
