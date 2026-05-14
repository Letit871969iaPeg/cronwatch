"""CLI for inspecting job checkpoints.

Usage examples:
    cronwatch-checkpoint list  backup_job  run-42
    cronwatch-checkpoint latest backup_job run-42
    cronwatch-checkpoint prune  backup_job  run-42
"""
from __future__ import annotations

import argparse
import sys
from typing import List

from cronwatch.job_checkpoint import CheckpointStore


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-checkpoint",
        description="Inspect or manage job checkpoints.",
    )
    p.add_argument(
        "--db",
        default="cronwatch.db",
        metavar="PATH",
        help="SQLite database path (default: cronwatch.db)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    lst = sub.add_parser("list", help="List all checkpoints for a run")
    lst.add_argument("job", help="Job name")
    lst.add_argument("run_id", help="Run ID")

    lat = sub.add_parser("latest", help="Show the latest checkpoint for a run")
    lat.add_argument("job", help="Job name")
    lat.add_argument("run_id", help="Run ID")

    prune = sub.add_parser("prune", help="Delete all checkpoints for a run")
    prune.add_argument("job", help="Job name")
    prune.add_argument("run_id", help="Run ID")

    return p


def _print_checkpoint(cp) -> None:
    print(f"  [{cp.recorded_at}] {cp.name!r} = {cp.value!r}")


def main(argv: List[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    store = CheckpointStore(args.db)

    if args.cmd == "list":
        checkpoints = store.get(args.job, args.run_id)
        if not checkpoints:
            print(f"No checkpoints found for {args.job!r} run {args.run_id!r}.")
            return 0
        print(f"Checkpoints for {args.job!r} run {args.run_id!r}:")
        for cp in checkpoints:
            _print_checkpoint(cp)
        return 0

    if args.cmd == "latest":
        cp = store.latest(args.job, args.run_id)
        if cp is None:
            print(f"No checkpoints found for {args.job!r} run {args.run_id!r}.")
            return 1
        print(f"Latest checkpoint for {args.job!r} run {args.run_id!r}:")
        _print_checkpoint(cp)
        return 0

    if args.cmd == "prune":
        removed = store.prune(args.job, args.run_id)
        print(f"Pruned {removed} checkpoint(s) for {args.job!r} run {args.run_id!r}.")
        return 0

    return 1  # unreachable


if __name__ == "__main__":
    sys.exit(main())
