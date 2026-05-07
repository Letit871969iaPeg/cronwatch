"""CLI entry-point: cronwatch-snapshot — view historical job snapshots."""

from __future__ import annotations

import argparse
import sys
from typing import List

from cronwatch.snapshot import SnapshotStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-snapshot",
        description="Display historical state snapshots for a cron job.",
    )
    p.add_argument("job", help="Job name to query")
    p.add_argument(
        "--db",
        default="cronwatch.db",
        metavar="PATH",
        help="Path to the SQLite database (default: cronwatch.db)",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=20,
        metavar="N",
        help="Maximum number of snapshots to display (default: 20)",
    )
    p.add_argument(
        "--prune",
        type=int,
        default=None,
        metavar="KEEP",
        help="Prune old snapshots, keeping only the N most recent, then exit.",
    )
    return p


def _fmt(value) -> str:
    return "-" if value is None else str(value)


def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    store = SnapshotStore(args.db)

    if args.prune is not None:
        removed = store.prune(args.job, keep=args.prune)
        print(f"Pruned {removed} snapshot(s) for '{args.job}' (kept {args.prune}).")
        return 0

    snapshots = store.fetch(args.job, limit=args.limit)
    if not snapshots:
        print(f"No snapshots found for job '{args.job}'.")
        return 1

    header = f"{'captured_at':<30} {'status':<10} {'duration_s':<12} {'failures':<8}"
    print(header)
    print("-" * len(header))
    for s in snapshots:
        print(
            f"{_fmt(s.captured_at):<30} "
            f"{_fmt(s.last_status):<10} "
            f"{_fmt(s.last_duration_s):<12} "
            f"{s.consecutive_failures:<8}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
