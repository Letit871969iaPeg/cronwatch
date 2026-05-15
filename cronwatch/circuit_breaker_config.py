"""Load circuit-breaker policies from a CronwatchConfig object."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cronwatch.job_circuit_breaker import CircuitBreaker, CircuitBreakerPolicy

if TYPE_CHECKING:
    from cronwatch.config import CronwatchConfig


def load_circuit_breaker(config: "CronwatchConfig") -> CircuitBreaker:
    """Build a :class:`CircuitBreaker` from job configurations.

    Each job may carry an optional ``circuit_breaker`` mapping with keys:
    - ``failure_threshold`` (int, default 3)
    - ``recovery_timeout``  (int seconds, default 300)
    """
    cb = CircuitBreaker()
    for job in config.jobs:
        raw = getattr(job, "circuit_breaker", None)
        if raw is None:
            continue
        if isinstance(raw, dict):
            policy = CircuitBreakerPolicy(
                failure_threshold=int(raw.get("failure_threshold", 3)),
                recovery_timeout=int(raw.get("recovery_timeout", 300)),
            )
        else:
            # namespace / dataclass-like object
            policy = CircuitBreakerPolicy(
                failure_threshold=int(getattr(raw, "failure_threshold", 3)),
                recovery_timeout=int(getattr(raw, "recovery_timeout", 300)),
            )
        cb.set_policy(job.name, policy)
    return cb
