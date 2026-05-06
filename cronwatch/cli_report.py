"""CLI entry point: `cronwatch-report` — print a JSON summary of all jobs."""
from __future__ import annotations

import argparse
import sys

from cronwatch.config import load_config
from cronwatch.history import HistoryStore
from cronwatch.summary_reporter import build_full_report, print_json_report
from cronwatch.tracker import JobTracker


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-report",
        description="Print a JSON summary report for all configured cron jobs.",
    )
    p.add_argument("-c", "--config", default="cronwatch.yaml", help="Path to config file")
    p.add_argument("-d", "--db", default="cronwatch_history.db", help="Path to history DB")
    p.add_argument("-n", "--limit", type=int, default=100, help="Max history records per job")
    p.add_argument("-j", "--job", dest="jobs", action="append", help="Filter to specific job(s)")
    return p


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        print(f"[cronwatch-report] Config file not found: {args.config}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"[cronwatch-report] Failed to load config: {exc}", file=sys.stderr)
        return 1

    history = HistoryStore(args.db)
    tracker = JobTracker()

    job_names = args.jobs if args.jobs else [j.name for j in cfg.jobs]

    report = build_full_report(job_names, tracker, history, limit=args.limit)
    print_json_report(report)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
