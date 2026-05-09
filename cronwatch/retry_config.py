"""Load retry policies from the cronwatch YAML config."""
from __future__ import annotations

from typing import Dict

from cronwatch.job_retry import RetryPolicy


def load_retry_policies(config_data: dict) -> Dict[str, RetryPolicy]:
    """Parse retry policies from the top-level config dict.

    Expected YAML shape (per job)::

        jobs:
          - name: my_job
            retry:
              max_retries: 5
              alert_on_exhaustion: true

    Returns a mapping of job_name -> RetryPolicy.
    Jobs without a ``retry`` block are omitted.
    """
    policies: Dict[str, RetryPolicy] = {}

    for job in config_data.get("jobs", []):
        name = job.get("name")
        if not name:
            continue

        retry_block = job.get("retry")
        if not retry_block:
            continue

        policies[name] = RetryPolicy(
            max_retries=int(retry_block.get("max_retries", 3)),
            alert_on_exhaustion=bool(retry_block.get("alert_on_exhaustion", True)),
        )

    return policies
