"""CLI: ``python -m lakehouse.generator cdc --brands 3 --events 1000 --seed 42 --dry-run``."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from lakehouse.common.logging import configure_logging
from lakehouse.generator.cdc import InfeasibleDefectPlan, generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.io import write_run


DEFAULT_OUT_DIR = Path("build/generated/cdc")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m lakehouse.generator")
    sub = parser.add_subparsers(dest="command", required=True)
    cdc = sub.add_parser("cdc", help="generate the customer CDC stream and its manifest")
    cdc.add_argument("--brands", type=int, required=True, help="first N tenants of GEN_TENANTS")
    cdc.add_argument("--events", type=int, required=True, help="clean base events (before defects)")
    cdc.add_argument("--seed", type=int, default=None, help="overrides GEN_SEED")
    cdc.add_argument("--dry-run", action="store_true", help="write files only; do not produce")
    cdc.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    return parser


def _configure_cli_logging() -> None:
    """Plain JSON logs on stderr; stdout carries only the counts output."""
    configure_logging(environment="production", force=True)
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    _configure_cli_logging()
    config = GeneratorConfig()
    if args.seed is not None:
        config = config.model_copy(update={"seed": args.seed})
    if not args.dry_run:
        parser.error("producer arrives in W1.D5; pass --dry-run to write files only")
    if not 1 <= args.brands <= len(config.tenants):
        parser.error(f"--brands must be between 1 and {len(config.tenants)} (configured tenants)")
    if args.events < 1:
        parser.error("--events must be >= 1")
    try:
        run = generate_cdc(config, brands=args.brands, events=args.events)
    except InfeasibleDefectPlan as exc:
        print(f"error: infeasible defect plan: {exc}", file=sys.stderr)
        return 1
    events_path, manifest_path = write_run(run, args.out_dir)
    print(f"wrote {events_path} and {manifest_path}", file=sys.stderr)
    for name, value in run.manifest.counts.items():
        print(f"  {name}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
