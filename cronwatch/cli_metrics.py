"""CLI entry point: export cronwatch metrics in Prometheus text format."""

from __future__ import annotations

import argparse
import sys

from cronwatch.config import load_config
from cronwatch.history import HistoryStore
from cronwatch.tracker import JobTracker
from cronwatch.metric_exporter import MetricExporter


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-metrics",
        description="Print Prometheus-compatible metrics for all tracked cron jobs.",
    )
    p.add_argument("--config", default="cronwatch.yaml", help="Path to config file")
    p.add_argument("--db", default="cronwatch.db", help="Path to SQLite history database")
    p.add_argument(
        "--history-limit",
        type=int,
        default=100,
        dest="history_limit",
        help="Number of recent history records to use for averages (default: 100)",
    )
    p.add_argument(
        "--job",
        metavar="JOB_NAME",
        default=None,
        help="Restrict output to a single job name",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"error: config file not found: {args.config}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 2

    tracker = JobTracker()
    for job in cfg.jobs:
        tracker.ensure(job.name)

    history = HistoryStore(args.db)
    exporter = MetricExporter(tracker, history, limit=args.history_limit)

    samples = exporter.collect()
    if args.job:
        samples = [s for s in samples if s.labels.get("job") == args.job]
        if not samples:
            print(f"error: no metrics found for job '{args.job}'", file=sys.stderr)
            return 1

    from cronwatch.metric_exporter import _render_samples
    print(_render_samples(samples), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
