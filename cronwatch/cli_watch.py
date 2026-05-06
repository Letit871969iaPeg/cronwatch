"""CLI entry-point: ``cronwatch-run <job_name> -- <command...>``

Usage examples::

    cronwatch-run nightly-backup -- /usr/local/bin/backup.sh --full
    cronwatch-run db-vacuum --timeout 120 -- psql -c 'VACUUM ANALYZE'
"""

import argparse
import logging
import sys
from pathlib import Path

from cronwatch.config import load_config
from cronwatch.history import HistoryStore
from cronwatch.tracker import JobTracker
from cronwatch.watcher import JobWatcher

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-run",
        description="Wrap a cron command and record its execution in cronwatch.",
    )
    p.add_argument("job_name", help="Logical job name (must match cronwatch config).")
    p.add_argument(
        "--config",
        default="cronwatch.yaml",
        metavar="FILE",
        help="Path to cronwatch config file (default: cronwatch.yaml).",
    )
    p.add_argument(
        "--db",
        default="cronwatch.db",
        metavar="FILE",
        help="Path to SQLite history database (default: cronwatch.db).",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Kill the job after this many seconds.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    p.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to execute (separate with -- if needed).",
    )
    return p


def main(argv=None) -> int:  # pragma: no cover — integration entry-point
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Strip a leading '--' separator if present.
    command = args.command
    if command and command[0] == "--":
        command = command[1:]

    if not command:
        parser.error("No command specified.")

    cfg = load_config(args.config)
    history = HistoryStore(args.db)
    tracker = JobTracker()

    # Resolve per-job timeout: CLI flag overrides config.
    timeout = args.timeout
    if timeout is None:
        job_cfgs = {j.name: j for j in cfg.jobs}
        if args.job_name in job_cfgs:
            timeout = getattr(job_cfgs[args.job_name], "timeout", None)

    watcher = JobWatcher(args.job_name, tracker, history, timeout=timeout)
    exit_code = watcher.run(command)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
