"""Load QuotaPolicy entries from a CronwatchConfig object."""

from __future__ import annotations

from typing import Any

from cronwatch.job_quota import JobQuota, QuotaPolicy


def load_quota_manager(config: Any, db_path: str = ":memory:") -> JobQuota:
    """Build a JobQuota from the parsed config.

    Expected YAML shape per job::

        jobs:
          - name: my_job
            quota:
              max_runs: 5
              window_seconds: 3600
    """
    quota = JobQuota(db_path=db_path)
    jobs = getattr(config, "jobs", []) or []
    for job in jobs:
        raw = getattr(job, "quota", None)
        if raw is None:
            continue
        if isinstance(raw, dict):
            max_runs = int(raw.get("max_runs", 0))
            window_seconds = int(raw.get("window_seconds", 3600))
        else:
            max_runs = int(getattr(raw, "max_runs", 0))
            window_seconds = int(getattr(raw, "window_seconds", 3600))
        if max_runs > 0:
            quota.set_policy(
                job.name,
                QuotaPolicy(max_runs=max_runs, window_seconds=window_seconds),
            )
    return quota
