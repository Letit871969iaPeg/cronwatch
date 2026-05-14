"""Load cooldown policies from the cronwatch config object."""

from __future__ import annotations

from typing import Any

from cronwatch.job_cooldown import CooldownPolicy, JobCooldown


def load_cooldown_manager(config: Any, db_path: str = ":memory:") -> JobCooldown:
    """Build a JobCooldown instance populated with policies from *config*.

    Expected config shape (per job)::

        jobs:
          - name: backup
            cooldown_seconds: 300

    Jobs without a ``cooldown_seconds`` field are skipped.
    """
    manager = JobCooldown(db_path=db_path)
    jobs = getattr(config, "jobs", None) or []
    for job in jobs:
        name = getattr(job, "name", None) or (job.get("name") if isinstance(job, dict) else None)
        if not name:
            continue
        cooldown = (
            job.get("cooldown_seconds")
            if isinstance(job, dict)
            else getattr(job, "cooldown_seconds", None)
        )
        if cooldown is None:
            continue
        try:
            seconds = int(cooldown)
        except (TypeError, ValueError):
            continue
        manager.set_policy(CooldownPolicy(job_name=name, min_interval_seconds=seconds))
    return manager
