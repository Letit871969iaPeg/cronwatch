"""Configuration loading and validation for cronwatch."""

import os
import yaml
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class JobConfig:
    name: str
    schedule: str
    max_duration_seconds: int = 3600
    alert_on_drift_seconds: int = 60
    notify: List[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class CronwatchConfig:
    jobs: List[JobConfig] = field(default_factory=list)
    log_file: str = "/var/log/cronwatch.log"
    state_dir: str = "/var/lib/cronwatch"
    check_interval_seconds: int = 30
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_from: Optional[str] = None


def load_config(path: str) -> CronwatchConfig:
    """Load and parse a YAML configuration file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config file must be a YAML mapping")

    jobs = []
    for job_data in raw.get("jobs", []):
        if "name" not in job_data or "schedule" not in job_data:
            raise ValueError(f"Job entry missing required fields: {job_data}")
        jobs.append(JobConfig(**{k: v for k, v in job_data.items() if k in JobConfig.__dataclass_fields__}))

    global_cfg = {k: v for k, v in raw.items() if k != "jobs" and k in CronwatchConfig.__dataclass_fields__}
    return CronwatchConfig(jobs=jobs, **global_cfg)
