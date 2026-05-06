"""CLI entry-point: show current escalation state for all monitored jobs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from cronwatch.config import load_config
from cronwatch.escalation import EscalationManager, EscalationPolicy
from cronwatch.escalation_config import load_escalation_policies
from cronwatch.history import HistoryStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-escalation",
        description="Show escalation state derived from recent job history.",
    )
    p.add_argument("--config", default="cronwatch.yaml", help="Config file path")
    p.add_argument("--db", default="cronwatch.db", help="SQLite history DB path")
    p.add_argument(
        "--lookback", type=int, default=20,
        help="Number of recent records to inspect per job (default: 20)",
    )
    p.add_argument("--json", dest="as_json", action="store_true",
                   help="Output as JSON")
    return p


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    args = _build_parser().parse_args(argv)

    cfg = load_config(Path(args.config))
    store = HistoryStore(args.db)

    raw_jobs = [
        {"name": j.name, **({"escalation": j.extra.get("escalation")} if hasattr(j, "extra") and j.extra else {})}
        for j in cfg.jobs
    ]
    policies = load_escalation_policies(raw_jobs)

    rows = []
    for job in cfg.jobs:
        records = store.fetch(job.name, limit=args.lookback)
        consecutive = 0
        for rec in reversed(records):
            if rec.exit_code != 0:
                consecutive += 1
            else:
                break
        policy = policies.get(job.name, EscalationPolicy())
        escalated = consecutive >= policy.threshold
        rows.append({
            "job": job.name,
            "consecutive_failures": consecutive,
            "threshold": policy.threshold,
            "escalated": escalated,
        })

    if args.as_json:
        print(json.dumps(rows, indent=2))
    else:
        fmt = "{:<30} {:>6}  {:>9}  {}"
        print(fmt.format("JOB", "CONSEC", "THRESHOLD", "ESCALATED"))
        print("-" * 58)
        for r in rows:
            print(fmt.format(
                r["job"], r["consecutive_failures"],
                r["threshold"], "YES" if r["escalated"] else "no",
            ))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
