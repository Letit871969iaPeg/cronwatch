"""Prometheus-compatible metrics exporter for cronwatch job statistics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from cronwatch.history import HistoryStore
from cronwatch.tracker import JobTracker


@dataclass
class MetricSample:
    name: str
    labels: dict
    value: float
    help_text: str = ""
    metric_type: str = "gauge"


def _label_str(labels: dict) -> str:
    if not labels:
        return ""
    parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
    return "{" + ",".join(parts) + "}"


def _render_samples(samples: List[MetricSample]) -> str:
    lines: List[str] = []
    seen: set = set()
    for s in samples:
        if s.name not in seen:
            lines.append(f"# HELP {s.name} {s.help_text}")
            lines.append(f"# TYPE {s.name} {s.metric_type}")
            seen.add(s.name)
        lines.append(f"{s.name}{_label_str(s.labels)} {s.value}")
    return "\n".join(lines) + "\n"


class MetricExporter:
    """Collects job metrics and renders them in Prometheus text format."""

    def __init__(self, tracker: JobTracker, history: HistoryStore, limit: int = 100) -> None:
        self._tracker = tracker
        self._history = history
        self._limit = limit

    def collect(self) -> List[MetricSample]:
        samples: List[MetricSample] = []
        job_names = list(self._tracker._records.keys())

        for name in job_names:
            record = self._tracker._records[name]
            labels = {"job": name}

            samples.append(MetricSample(
                name="cronwatch_job_last_exit_code",
                labels=labels,
                value=float(record.last_exit_code if record.last_exit_code is not None else -1),
                help_text="Exit code of the last job run (-1 if never run)",
            ))

            samples.append(MetricSample(
                name="cronwatch_job_is_running",
                labels=labels,
                value=1.0 if record.is_running() else 0.0,
                help_text="1 if the job is currently running, 0 otherwise",
            ))

            rows = self._history.fetch(name, limit=self._limit)
            durations = [
                r.duration_seconds for r in rows if r.duration_seconds is not None
            ]
            if durations:
                avg_dur = sum(durations) / len(durations)
                samples.append(MetricSample(
                    name="cronwatch_job_avg_duration_seconds",
                    labels=labels,
                    value=round(avg_dur, 3),
                    help_text="Average job duration in seconds over recent history",
                    metric_type="gauge",
                ))

            samples.append(MetricSample(
                name="cronwatch_job_run_total",
                labels=labels,
                value=float(len(rows)),
                help_text="Total number of recorded job runs in history",
                metric_type="counter",
            ))

        return samples

    def render_text(self) -> str:
        return _render_samples(self.collect())
