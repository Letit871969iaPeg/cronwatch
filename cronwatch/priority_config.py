"""Load job priority policies from the cronwatch YAML config."""
from __future__ import annotations

from typing import Any, Dict, List

from cronwatch.job_priority import Priority, PriorityManager, PriorityPolicy


def load_priority_manager(raw_jobs: List[Dict[str, Any]]) -> PriorityManager:
    """Build a :class:`PriorityManager` from the *jobs* list in the config dict.

    Each job entry may contain::

        name: backup
        priority:
          level: high          # low | normal | high | critical  (default: normal)
          alert_threshold: high  # minimum level to trigger an alert (default: normal)
    """
    manager = PriorityManager()
    for job in raw_jobs:
        name = job.get("name", "").strip()
        if not name:
            continue
        priority_block: Dict[str, Any] = job.get("priority") or {}
        try:
            level = Priority.from_str(priority_block.get("level", "normal"))
        except ValueError:
            level = Priority.NORMAL
        try:
            threshold = Priority.from_str(priority_block.get("alert_threshold", "normal"))
        except ValueError:
            threshold = Priority.NORMAL
        manager.set_policy(PriorityPolicy(job_name=name, priority=level, alert_threshold=threshold))
    return manager
