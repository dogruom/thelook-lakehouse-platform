"""CLI: ``python -m lakehouse.generator {cdc,clickstream} ... --dry-run`` writes files;
``python -m lakehouse.generator produce`` sends CDC and clickstream to Kafka."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from lakehouse.common.logging import configure_logging
from lakehouse.generator.cdc import InfeasibleDefectPlan, generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.io import write_clickstream_run, write_run
from lakehouse.generator.messages import cdc_messages
from lakehouse.generator.producer import DeliveryStats, create_producer, produce_messages


DEFAULT_OUT_DIR = Path("build/generated/cdc")
DEFAULT_CLICKSTREAM_OUT_DIR = Path("build/generated/clickstream")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m lakehouse.generator")
    sub = parser.add_subparsers(dest="command", required=True)
    cdc = sub.add_parser("cdc", help="generate the customer CDC stream and its manifest")
    cdc.add_argument("--brands", type=int, required=True, help="first N tenants of GEN_TENANTS")
    cdc.add_argument("--events", type=int, required=True, help="clean base events (before defects)")
    cdc.add_argument("--seed", type=int, default=None, help="overrides GEN_SEED")
    cdc.add_argument("--dry-run", action="store_true", help="write files only; do not produce")
    cdc.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    click = sub.add_parser("clickstream", help="generate the clickstream and its manifest")
    click.add_argument("--brands", type=int, required=True, help="first N tenants of GEN_TENANTS")
    click.add_argument("--events", type=int, required=True, help="clean base events")
    click.add_argument("--seed", type=int, default=None, help="overrides GEN_SEED")
    click.add_argument("--dry-run", action="store_true", help="write files only; do not produce")
    click.add_argument("--out-dir", type=Path, default=DEFAULT_CLICKSTREAM_OUT_DIR)
    produce = sub.add_parser("produce", help="generate CDC + clickstream and send them to Kafka")
    produce.add_argument("--brands", type=int, default=3, help="first N tenants of GEN_TENANTS")
    produce.add_argument("--events", type=int, default=5000, help="clickstream base events")
    produce.add_argument("--cdc-events", type=int, default=1000, help="CDC base events")
    produce.add_argument("--seed", type=int, default=None, help="overrides GEN_SEED")
    produce.add_argument("--rate", type=float, default=None, help="max messages per second")
    produce.add_argument(
        "--bootstrap-servers", default=None, help="default: GEN_KAFKA_BOOTSTRAP_SERVERS"
    )
    return parser


def _configure_cli_logging() -> None:
    """Plain JSON logs on stderr; stdout carries only the counts output."""
    configure_logging(environment="production", force=True)
    for handler in logging.getLogger().handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stderr)


def _print_counts(counts: dict[str, int], prefix: str = "") -> None:
    for name, value in counts.items():
        print(f"  {prefix}{name}={value}")


def _print_stats(stats: DeliveryStats) -> None:
    print(
        f"  attempted={stats.attempted} delivered={stats.delivered} failed={stats.failed} "
        f"undelivered={stats.undelivered} interrupted={stats.interrupted}"
    )
    for brand_id, delivered in sorted(stats.by_brand.items()):
        print(f"  delivered[{brand_id}]={delivered}")
    for topic, delivered in sorted(stats.by_topic.items()):
        print(f"  delivered[topic={topic}]={delivered}")


def _produce(args: argparse.Namespace, config: GeneratorConfig) -> int:
    try:
        cdc_run = generate_cdc(config, brands=args.brands, events=args.cdc_events)
        click_run = generate_clickstream(config, brands=args.brands, events=args.events)
    except InfeasibleDefectPlan as exc:
        print(f"error: infeasible defect plan: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    messages = cdc_messages(cdc_run, config.topic_customer_cdc) + click_run.messages
    print(f"generated {len(messages)} messages")
    _print_counts(cdc_run.manifest.counts, "cdc.")
    _print_counts(click_run.manifest.counts, "clickstream.")
    servers = args.bootstrap_servers or config.kafka_bootstrap_servers
    try:
        stats = produce_messages(
            messages, producer=create_producer(servers), rate_limit_per_s=args.rate
        )
    except KeyboardInterrupt:
        print("interrupted: pending messages were flushed before exit", file=sys.stderr)
        return 130
    _print_stats(stats)
    return 0 if stats.ok else 1


def _dry_run(
    args: argparse.Namespace, config: GeneratorConfig, parser: argparse.ArgumentParser
) -> int:
    try:
        if args.command == "clickstream":
            click_run = generate_clickstream(config, brands=args.brands, events=args.events)
        else:
            cdc_run = generate_cdc(config, brands=args.brands, events=args.events)
    except InfeasibleDefectPlan as exc:
        print(f"error: infeasible defect plan: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        parser.error(str(exc))
    if args.command == "clickstream":
        events_path, manifest_path = write_clickstream_run(click_run, args.out_dir)
        counts = click_run.manifest.counts
    else:
        events_path, manifest_path = write_run(cdc_run, args.out_dir)
        counts = cdc_run.manifest.counts
    print(f"wrote {events_path} and {manifest_path}", file=sys.stderr)
    _print_counts(counts)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    _configure_cli_logging()
    config = GeneratorConfig()
    if args.seed is not None:
        config = config.model_copy(update={"seed": args.seed})
    if args.command != "produce" and not args.dry_run:
        parser.error(
            "sending to Kafka is done by the `produce` subcommand (W1.D5); "
            "pass --dry-run to write files only"
        )
    if not 1 <= args.brands <= len(config.tenants):
        parser.error(f"--brands must be between 1 and {len(config.tenants)} (configured tenants)")
    if args.events < 1:
        parser.error("--events must be >= 1")
    if args.command == "produce":
        if args.cdc_events < 1:
            parser.error("--cdc-events must be >= 1")
        if args.rate is not None and args.rate <= 0:
            parser.error("--rate must be > 0")
        return _produce(args, config)
    return _dry_run(args, config, parser)


if __name__ == "__main__":
    sys.exit(main())
