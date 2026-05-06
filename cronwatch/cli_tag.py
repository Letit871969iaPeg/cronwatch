"""CLI entry-point: list jobs matching a given set of tags."""

from __future__ import annotations

import argparse
import sys
from typing import List

from cronwatch.config import load_config
from cronwatch.tag_filter import TagFilter, parse_tags


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cronwatch-tags",
        description="List configured jobs that match the supplied tags.",
    )
    p.add_argument(
        "--config",
        default="cronwatch.yaml",
        metavar="FILE",
        help="Path to cronwatch config file (default: cronwatch.yaml)",
    )
    p.add_argument(
        "--tags",
        required=True,
        metavar="TAG[,TAG…]",
        help="Comma-separated list of tags; only jobs with ALL tags are shown.",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress header; print job names only.",
    )
    return p


def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    tags = parse_tags(args.tags)
    tf = TagFilter(tags=tags)
    matched = tf.filter_jobs(cfg.jobs)

    if not matched:
        if not args.quiet:
            print(f"No jobs match tags: {tags}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"Jobs matching {tags}:")
    for job in matched:
        print(f"  {job.name}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
