"""Load escalation policies from the cronwatch config dict."""
from __future__ import annotations

from typing import Dict

from cronwatch.escalation import EscalationPolicy


def load_escalation_policies(
    raw_jobs: list[dict],
) -> Dict[str, EscalationPolicy]:
    """Parse escalation blocks from the raw job config list.

    Each job entry may contain an optional ``escalation`` key::

        jobs:
          - name: backup
            schedule: "0 2 * * *"
            escalation:
              threshold: 3
              repeat_every: 2
    """
    policies: Dict[str, EscalationPolicy] = {}
    for job in raw_jobs:
        name = job.get("name")
        if not name:
            continue
        esc = job.get("escalation")
        if not esc:
            continue
        policies[name] = EscalationPolicy(
            threshold=int(esc.get("threshold", 3)),
            repeat_every=int(esc.get("repeat_every", 1)),
        )
    return policies
