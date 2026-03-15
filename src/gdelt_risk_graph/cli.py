from __future__ import annotations

import argparse
import logging
from datetime import datetime

from gdelt_risk_graph.config import AppConfig
from gdelt_risk_graph.feed import BatchFiles
from gdelt_risk_graph.pipeline import Pipeline


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gdelt-risk-graph")
    subparsers = parser.add_subparsers(dest="command", required=True)

    backfill = subparsers.add_parser("backfill", help="Process a trailing backfill window")
    backfill.add_argument("--days", type=int, default=7)
    backfill.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel workers for batch download and parsing during backfill",
    )

    subparsers.add_parser("poll-once", help="Process newly available batches")

    process_batch = subparsers.add_parser("process-batch", help="Process a specific batch timestamp")
    process_batch.add_argument("--timestamp", required=True)
    process_batch.add_argument("--export-url", required=True)
    process_batch.add_argument("--mentions-url", required=True)
    process_batch.add_argument("--gkg-url", required=True)

    rebuild_day = subparsers.add_parser("rebuild-day", help="Recompute a single event day")
    rebuild_day.add_argument("--date", required=True)

    publish = subparsers.add_parser("publish-neo4j", help="Publish daily graph data to Neo4j")
    publish.add_argument("--since")

    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    pipeline = Pipeline(AppConfig.from_env())

    if args.command == "backfill":
        processed = pipeline.backfill(args.days, workers=args.workers)
        logging.info("Processed %s batches", processed)
        return 0

    if args.command == "poll-once":
        processed = pipeline.poll_once()
        logging.info("Processed %s new batches", processed)
        return 0

    if args.command == "process-batch":
        pipeline.process_batch(
            BatchFiles(
                timestamp=args.timestamp,
                export_url=args.export_url,
                mentions_url=args.mentions_url,
                gkg_url=args.gkg_url,
            )
        )
        return 0

    if args.command == "rebuild-day":
        pipeline.rebuild_day(datetime.strptime(args.date, "%Y-%m-%d").date())
        return 0

    if args.command == "publish-neo4j":
        since = datetime.strptime(args.since, "%Y-%m-%d").date() if args.since else None
        published = pipeline.publish_neo4j(since)
        logging.info("Published %s interaction rows", published)
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2
