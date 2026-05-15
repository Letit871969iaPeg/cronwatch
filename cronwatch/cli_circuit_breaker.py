"""CLI: inspect and reset job circuit-breaker state."""

from __future__ import annotations

import argparse
import sys

from cronwatch.circuit_breaker_config import load_circuit_breaker
from cronwatch.config import load_config
from cronwatch.job_circuit_breaker import CircuitState


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-circuit",
        description="Inspect or reset circuit-breaker state for cron jobs.",
    )
    p.add_argument("--config", default="cronwatch.yaml", help="Config file path")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("list", help="List circuit states for all configured jobs")

    rst = sub.add_parser("reset", help="Manually close the circuit for a job")
    rst.add_argument("job", help="Job name")

    return p


def main(argv: list[str] | None = None) -> int:  # pragma: no cover
    parser = _build_parser()
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    cb = load_circuit_breaker(cfg)

    if args.cmd == "list" or args.cmd is None:
        job_names = [j.name for j in cfg.jobs]
        if not job_names:
            print("No jobs configured.")
            return 0
        print(f"{'JOB':<30} {'STATE':<12} {'FAILURES':<10}")
        print("-" * 54)
        for name in job_names:
            state = cb.get_state(name)
            failures = cb._state(name).consecutive_failures
            print(f"{name:<30} {state.value:<12} {failures:<10}")
        return 0

    if args.cmd == "reset":
        cb.record_success(args.job)
        state = cb.get_state(args.job)
        if state != CircuitState.CLOSED:
            print(f"Warning: circuit for '{args.job}' is still {state.value}",
                  file=sys.stderr)
            return 1
        print(f"Circuit for '{args.job}' reset to CLOSED.")
        return 0

    parser.print_help()
    return 1
