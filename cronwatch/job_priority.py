"""Job priority levels and priority-aware alert routing."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, Optional


class Priority(IntEnum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

    @classmethod
    def from_str(cls, value: str) -> "Priority":
        mapping = {
            "low": cls.LOW,
            "normal": cls.NORMAL,
            "high": cls.HIGH,
            "critical": cls.CRITICAL,
        }
        key = value.strip().lower()
        if key not in mapping:
            raise ValueError(f"Unknown priority level: {value!r}")
        return mapping[key]


@dataclass
class PriorityPolicy:
    job_name: str
    priority: Priority = Priority.NORMAL
    # Minimum priority required before an alert is actually dispatched
    alert_threshold: Priority = Priority.NORMAL


@dataclass
class PriorityManager:
    _policies: Dict[str, PriorityPolicy] = field(default_factory=dict)

    def set_policy(self, policy: PriorityPolicy) -> None:
        self._policies[policy.job_name] = policy

    def get_priority(self, job_name: str) -> Priority:
        policy = self._policies.get(job_name)
        return policy.priority if policy else Priority.NORMAL

    def should_alert(self, job_name: str, event_priority: Optional[Priority] = None) -> bool:
        """Return True when *event_priority* meets or exceeds the job's alert threshold."""
        policy = self._policies.get(job_name)
        if policy is None:
            return True
        effective = event_priority if event_priority is not None else policy.priority
        return effective >= policy.alert_threshold

    def jobs_at_or_above(self, min_priority: Priority) -> list[str]:
        """Return job names whose configured priority is >= *min_priority*."""
        return [
            name
            for name, pol in self._policies.items()
            if pol.priority >= min_priority
        ]
