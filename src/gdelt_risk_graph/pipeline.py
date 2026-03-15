from __future__ import annotations

import json
import logging
import math
import os
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from gdelt_risk_graph.checkpoint import Checkpoint
from gdelt_risk_graph.config import AppConfig
from gdelt_risk_graph.constants import (
    ALLOWED_ROOT_CODES,
    EVENT_COLUMNS,
    GKG_DOCUMENT_COLUMNS,
    GKG_ENTITY_COLUMNS,
    GKG_THEME_COLUMNS,
    INTENSITY_LOG_DENOMINATOR,
    MENTION_COLUMNS,
    RISK_BANDS,
    SEVERITY_WEIGHTS,
)
from gdelt_risk_graph.database import DuckDBStore
from gdelt_risk_graph.feed import (
    BatchFiles,
    LocalBatchFiles,
    dated_path,
    download_to_path,
    fetch_masterfilelist,
    parse_masterfilelist,
    select_batches,
)
from gdelt_risk_graph.neo4j_publish import Neo4jConfig, Neo4jPublisher
from gdelt_risk_graph.parsers import (
    normalize_root_code,
    parse_events_zip,
    parse_gkg_zip,
    parse_mentions_zip,
)


LOGGER = logging.getLogger(__name__)


def risk_band(score: float) -> str:
    for threshold, band in RISK_BANDS:
        if score >= threshold:
            return band
    return "low"


@dataclass(frozen=True)
class EdgeMetrics:
    day: date
    src_country: str
    dst_country: str
    event_root_code: str
    event_count: int
    avg_tone: float
    avg_goldstein: float
    total_mentions: int
    unique_sources: int


@dataclass(frozen=True)
class PreparedBatch:
    batch: BatchFiles
    local_batch: LocalBatchFiles
    events: list[tuple]
    mentions: list[tuple]
    gkg_documents: list[tuple]
    gkg_themes: list[tuple]
    gkg_entities: list[tuple]
    filtered_events: list[tuple]
    affected_event_ids: list[int]


class Pipeline:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.config.ensure_directories()
        self.store = DuckDBStore(self.config.duckdb_path)
        self.checkpoint = Checkpoint.load(self.config.checkpoint_path)

    def backfill(self, days: int, workers: int | None = None) -> int:
        start_day = datetime.now(timezone.utc).date() - timedelta(days=days)
        start_timestamp = f"{start_day.strftime('%Y%m%d')}000000"
        masterfile = fetch_masterfilelist(self.config.masterfile_url, self.config.request_timeout_seconds)
        batches = select_batches(parse_masterfilelist(masterfile), start_timestamp=start_timestamp)
        worker_count = self._resolve_backfill_workers(workers, len(batches))
        LOGGER.info(
            "Discovered %s batches for %s-day backfill using %s worker(s)",
            len(batches),
            days,
            worker_count,
        )
        if not batches:
            return 0

        processed_timestamps: list[str] = []
        affected_days: set[date] = set()

        with self.store.connect() as connection:
            for prepared in self._iter_prepared_batches(batches, worker_count):
                batch_days = self._commit_prepared_batch(connection, prepared, rebuild_days=False)
                affected_days.update(batch_days)
                processed_timestamps.append(prepared.batch.timestamp)

            for target_day in sorted(affected_days):
                self._rebuild_day_with_connection(connection, target_day)

            for batch_timestamp in processed_timestamps:
                self.store.upsert_batch_ledger(
                    connection,
                    batch_timestamp,
                    aggregate_completed=True,
                    score_completed=True,
                )

        self.checkpoint.last_timestamp = processed_timestamps[-1]
        self.checkpoint.save(self.config.checkpoint_path)
        self._enforce_raw_retention()
        return len(processed_timestamps)

    def poll_once(self) -> int:
        masterfile = fetch_masterfilelist(self.config.masterfile_url, self.config.request_timeout_seconds)
        discovered = parse_masterfilelist(masterfile)
        batches = select_batches(discovered, after_timestamp=self.checkpoint.last_timestamp)
        LOGGER.info("Discovered %s new batches", len(batches))
        for batch in batches:
            self.process_batch(batch)
        return len(batches)

    def process_batch(self, batch: BatchFiles) -> None:
        LOGGER.info("Processing batch %s", batch.timestamp)
        prepared = self._prepare_batch(batch)

        with self.store.connect() as connection:
            self._commit_prepared_batch(connection, prepared, rebuild_days=True)

        self.checkpoint.last_timestamp = batch.timestamp
        self.checkpoint.save(self.config.checkpoint_path)
        self._enforce_raw_retention()

    def rebuild_day(self, target_day: date) -> None:
        with self.store.connect() as connection:
            self._rebuild_day_with_connection(connection, target_day)

    def publish_neo4j(self, since: date | None = None) -> int:
        neo4j_config = self._neo4j_config()
        publisher = Neo4jPublisher(neo4j_config)
        since_day = since or (
            datetime.strptime(self.checkpoint.last_published_day, "%Y-%m-%d").date()
            if self.checkpoint.last_published_day
            else None
        )

        with self.store.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    g.day,
                    g.src_country,
                    g.dst_country,
                    g.event_root_code,
                    g.event_count,
                    g.avg_tone,
                    g.avg_goldstein,
                    g.total_mentions,
                    g.unique_sources,
                    g.themes_json,
                    g.top_entities_json,
                    g.top_locations_json,
                    p.pair_score,
                    p.risk_band
                FROM graph_edges_daily g
                JOIN risk_scores_pair_daily p
                  ON g.day = p.day
                 AND g.src_country = p.src_country
                 AND g.dst_country = p.dst_country
                 AND g.event_root_code = p.event_root_code
                WHERE (? IS NULL OR g.day >= ?)
                ORDER BY g.day, g.src_country, g.dst_country, g.event_root_code
                """,
                [since_day, since_day],
            ).fetchall()

            payloads = [
                {
                    "day": row[0].isoformat(),
                    "src_country": row[1],
                    "dst_country": row[2],
                    "event_root_code": row[3],
                    "event_count": row[4],
                    "avg_tone": row[5],
                    "avg_goldstein": row[6],
                    "total_mentions": row[7],
                    "unique_sources": row[8],
                    "themes_json": row[9] or "[]",
                    "top_entities_json": row[10] or "[]",
                    "top_locations_json": row[11] or "[]",
                    "pair_score": row[12],
                    "risk_band": row[13],
                }
                for row in rows
            ]

            publisher.publish_rows(payloads)

            published_days = sorted({row["day"] for row in payloads})
            for day_value in published_days:
                parsed_day = datetime.strptime(day_value, "%Y-%m-%d").date()
                connection.execute(
                    "DELETE FROM neo4j_publish_state WHERE day = ?",
                    [parsed_day],
                )
                connection.execute(
                    """
                    INSERT INTO neo4j_publish_state (day, published_at)
                    VALUES (?, CURRENT_TIMESTAMP)
                    """,
                    [parsed_day],
                )
            if published_days:
                self.checkpoint.last_published_day = published_days[-1]
                self.checkpoint.save(self.config.checkpoint_path)
            return len(payloads)

    def _resolve_backfill_workers(self, requested_workers: int | None, batch_count: int) -> int:
        if batch_count <= 0:
            return 1
        if requested_workers is not None:
            return max(1, min(requested_workers, batch_count))
        return min(4, max(1, os.cpu_count() or 1), batch_count)

    def _iter_prepared_batches(
        self,
        batches: list[BatchFiles],
        worker_count: int,
    ):
        if worker_count <= 1:
            for batch in batches:
                yield self._prepare_batch(batch)
            return

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="gdelt-backfill") as executor:
            pending: dict[int, Future[PreparedBatch]] = {}
            next_submit = 0
            next_yield = 0

            try:
                while next_submit < len(batches) and len(pending) < worker_count:
                    pending[next_submit] = executor.submit(self._prepare_batch, batches[next_submit])
                    next_submit += 1

                while pending:
                    prepared = pending.pop(next_yield).result()
                    yield prepared

                    if next_submit < len(batches):
                        pending[next_submit] = executor.submit(self._prepare_batch, batches[next_submit])
                        next_submit += 1

                    next_yield += 1
            finally:
                for future in pending.values():
                    future.cancel()

    def _prepare_batch(self, batch: BatchFiles) -> PreparedBatch:
        local_batch = self._download_batch(batch)
        events = parse_events_zip(local_batch.export_path, batch.timestamp)
        mentions = parse_mentions_zip(local_batch.mentions_path, batch.timestamp)
        gkg_documents, gkg_themes, gkg_entities = parse_gkg_zip(local_batch.gkg_path, batch.timestamp)

        filtered_events = [row for row in events if self._is_allowed_event(row)]
        affected_event_ids = sorted(
            {
                row[1]
                for row in filtered_events
                if row[1] is not None
            }
            | {
                row[1]
                for row in mentions
                if row[1] is not None
            }
        )

        return PreparedBatch(
            batch=batch,
            local_batch=local_batch,
            events=events,
            mentions=mentions,
            gkg_documents=gkg_documents,
            gkg_themes=gkg_themes,
            gkg_entities=gkg_entities,
            filtered_events=filtered_events,
            affected_event_ids=affected_event_ids,
        )

    def _commit_prepared_batch(self, connection, prepared: PreparedBatch, *, rebuild_days: bool) -> list[date]:
        batch = prepared.batch
        self.store.upsert_batch_ledger(
            connection,
            batch.timestamp,
            download_completed=True,
            raw_export_path=str(prepared.local_batch.export_path),
            raw_mentions_path=str(prepared.local_batch.mentions_path),
            raw_gkg_path=str(prepared.local_batch.gkg_path),
            last_error=None,
        )
        self._replace_batch_tables(
            connection,
            batch.timestamp,
            prepared.events,
            prepared.mentions,
            prepared.gkg_documents,
            prepared.gkg_themes,
            prepared.gkg_entities,
            prepared.filtered_events,
        )
        self.store.upsert_batch_ledger(connection, batch.timestamp, parse_completed=True)

        if prepared.affected_event_ids:
            self._refresh_mentions_agg(connection, prepared.affected_event_ids)
            self._refresh_event_documents(connection, prepared.affected_event_ids)

        affected_days = self._affected_days(connection, batch.timestamp, prepared.affected_event_ids)
        if rebuild_days:
            for target_day in affected_days:
                self._rebuild_day_with_connection(connection, target_day)

            self.store.upsert_batch_ledger(
                connection,
                batch.timestamp,
                aggregate_completed=True,
                score_completed=True,
            )

        return affected_days

    def _download_batch(self, batch: BatchFiles) -> LocalBatchFiles:
        export_path = dated_path(self.config.raw_root, batch.timestamp, "export.CSV.zip")
        mentions_path = dated_path(self.config.raw_root, batch.timestamp, "mentions.CSV.zip")
        gkg_path = dated_path(self.config.raw_root, batch.timestamp, "gkg.csv.zip")

        return LocalBatchFiles(
            timestamp=batch.timestamp,
            export_path=download_to_path(batch.export_url, export_path, self.config.request_timeout_seconds),
            mentions_path=download_to_path(
                batch.mentions_url,
                mentions_path,
                self.config.request_timeout_seconds,
            ),
            gkg_path=download_to_path(batch.gkg_url, gkg_path, self.config.request_timeout_seconds),
        )

    def _replace_batch_tables(
        self,
        connection,
        batch_timestamp: str,
        events: list[tuple],
        mentions: list[tuple],
        gkg_documents: list[tuple],
        gkg_themes: list[tuple],
        gkg_entities: list[tuple],
        filtered_events: list[tuple],
    ) -> None:
        batch_tables = (
            "bronze_events",
            "bronze_mentions",
            "bronze_gkg_documents",
            "silver_events_filtered",
            "silver_gkg_themes",
            "silver_gkg_entities",
        )
        for table in batch_tables:
            self.store.delete_where_batch(connection, table, batch_timestamp)

        self.store.insert_many(
            connection,
            "bronze_events",
            ["batch_timestamp", *EVENT_COLUMNS],
            events,
        )
        self.store.insert_many(
            connection,
            "bronze_mentions",
            ["batch_timestamp", *MENTION_COLUMNS],
            mentions,
        )
        self.store.insert_many(
            connection,
            "bronze_gkg_documents",
            ["batch_timestamp", *GKG_DOCUMENT_COLUMNS],
            gkg_documents,
        )
        self.store.insert_many(
            connection,
            "silver_events_filtered",
            ["batch_timestamp", *EVENT_COLUMNS],
            filtered_events,
        )
        self.store.insert_many(
            connection,
            "silver_gkg_themes",
            ["batch_timestamp", *GKG_THEME_COLUMNS],
            gkg_themes,
        )
        self.store.insert_many(
            connection,
            "silver_gkg_entities",
            ["batch_timestamp", *GKG_ENTITY_COLUMNS],
            gkg_entities,
        )

        self._write_batch_parquet(connection, "bronze_events", batch_timestamp, self.config.bronze_root / "events")
        self._write_batch_parquet(connection, "bronze_mentions", batch_timestamp, self.config.bronze_root / "mentions")
        self._write_batch_parquet(
            connection,
            "bronze_gkg_documents",
            batch_timestamp,
            self.config.bronze_root / "gkg_documents",
        )
        self._write_batch_parquet(
            connection,
            "silver_events_filtered",
            batch_timestamp,
            self.config.silver_root / "events_filtered",
        )
        self._write_batch_parquet(
            connection,
            "silver_gkg_themes",
            batch_timestamp,
            self.config.silver_root / "gkg_themes",
        )
        self._write_batch_parquet(
            connection,
            "silver_gkg_entities",
            batch_timestamp,
            self.config.silver_root / "gkg_entities",
        )

    def _write_batch_parquet(self, connection, table: str, batch_timestamp: str, root: Path) -> None:
        output_path = dated_path(root, batch_timestamp, f"{table}.parquet")
        self.store.copy_batch_to_parquet(connection, table, batch_timestamp, output_path)

    def _refresh_mentions_agg(self, connection, event_ids: list[int]) -> None:
        self.store.delete_rows_for_event_ids(connection, "mentions_agg", event_ids)
        placeholders = ", ".join(["?"] * len(event_ids))
        connection.execute(
            f"""
            INSERT INTO mentions_agg
            SELECT
                global_event_id,
                COUNT(*) AS mention_count,
                COUNT(DISTINCT mention_source_name) AS unique_sources,
                MIN(mention_time) AS first_seen,
                MAX(mention_time) AS last_seen
            FROM bronze_mentions
            WHERE global_event_id IN ({placeholders})
            GROUP BY global_event_id
            """,
            event_ids,
        )

    def _refresh_event_documents(self, connection, event_ids: list[int]) -> None:
        self.store.delete_rows_for_event_ids(connection, "event_documents", event_ids)
        placeholders = ", ".join(["?"] * len(event_ids))
        parameters = list(event_ids) + list(event_ids)
        connection.execute(
            f"""
            INSERT INTO event_documents
            SELECT
                global_event_id,
                document_identifier,
                document_identifier_normalized,
                MIN(origin) AS origin
            FROM (
                SELECT
                    global_event_id,
                    source_url AS document_identifier,
                    source_url_normalized AS document_identifier_normalized,
                    'event' AS origin
                FROM silver_events_filtered
                WHERE global_event_id IN ({placeholders})
                  AND source_url_normalized IS NOT NULL
                UNION ALL
                SELECT
                    global_event_id,
                    mention_identifier AS document_identifier,
                    mention_identifier_normalized AS document_identifier_normalized,
                    'mention' AS origin
                FROM bronze_mentions
                WHERE global_event_id IN ({placeholders})
                  AND mention_identifier_normalized IS NOT NULL
            ) deduped
            GROUP BY 1, 2, 3
            """,
            parameters,
        )

    def _affected_days(self, connection, batch_timestamp: str, event_ids: list[int]) -> list[date]:
        days = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT event_date FROM silver_events_filtered WHERE batch_timestamp = ?",
                [batch_timestamp],
            ).fetchall()
            if row[0] is not None
        }

        if event_ids:
            placeholders = ", ".join(["?"] * len(event_ids))
            rows = connection.execute(
                f"""
                SELECT DISTINCT event_date
                FROM silver_events_filtered
                WHERE global_event_id IN ({placeholders})
                  AND event_date IS NOT NULL
                """,
                event_ids,
            ).fetchall()
            days.update(row[0] for row in rows if row[0] is not None)

        return sorted(days)

    def _rebuild_day_with_connection(self, connection, target_day: date) -> None:
        LOGGER.info("Rebuilding day %s", target_day.isoformat())
        self._refresh_gkg_daily(connection, target_day)
        edge_metrics = self._fetch_edge_metrics(connection, target_day)
        theme_counts = self._fetch_counts_by_key(
            connection,
            """
            SELECT src_country, dst_country, event_root_code, theme, theme_count
            FROM gkg_themes_daily
            WHERE day = ?
            """,
            target_day,
        )
        entity_counts = self._fetch_counts_by_key(
            connection,
            """
            SELECT src_country, dst_country, event_root_code, entity_name, entity_count, entity_type
            FROM gkg_entities_daily
            WHERE day = ?
              AND entity_type IN ('person', 'organization')
            """,
            target_day,
        )
        location_counts = self._fetch_counts_by_key(
            connection,
            """
            SELECT src_country, dst_country, event_root_code, entity_name, entity_count, entity_type
            FROM gkg_entities_daily
            WHERE day = ?
              AND entity_type = 'location'
            """,
            target_day,
        )
        event_locations = self._fetch_event_locations(connection, target_day)

        graph_rows = []
        for metric in edge_metrics:
            key = (metric.src_country, metric.dst_country, metric.event_root_code)
            themes_json = json.dumps(theme_counts.get(key, []))
            top_entities_json = json.dumps(entity_counts.get(key, []))
            merged_locations = self._merge_location_lists(
                location_counts.get(key, []),
                event_locations.get(key, []),
            )
            top_locations_json = json.dumps(merged_locations)
            graph_rows.append(
                (
                    metric.day,
                    metric.src_country,
                    metric.dst_country,
                    metric.event_root_code,
                    metric.event_count,
                    metric.avg_tone,
                    metric.avg_goldstein,
                    metric.total_mentions,
                    metric.unique_sources,
                    themes_json,
                    top_entities_json,
                    top_locations_json,
                    datetime.now(timezone.utc),
                )
            )

        self.store.replace_rows_for_day(
            connection,
            "graph_edges_daily",
            [
                "day",
                "src_country",
                "dst_country",
                "event_root_code",
                "event_count",
                "avg_tone",
                "avg_goldstein",
                "total_mentions",
                "unique_sources",
                "themes_json",
                "top_entities_json",
                "top_locations_json",
                "last_updated",
            ],
            target_day,
            graph_rows,
        )

        pair_scores = self._compute_pair_scores(connection, target_day, edge_metrics)
        self.store.replace_rows_for_day(
            connection,
            "risk_scores_pair_daily",
            [
                "day",
                "src_country",
                "dst_country",
                "event_root_code",
                "pair_score",
                "risk_band",
                "intensity_norm",
                "tone_norm",
                "goldstein_norm",
                "source_norm",
                "mention_spike_norm",
                "severity_weight",
                "total_mentions",
                "last_updated",
            ],
            target_day,
            pair_scores,
        )

        country_scores = self._compute_country_scores(target_day, pair_scores)
        self.store.replace_rows_for_day(
            connection,
            "risk_scores_country_daily",
            [
                "day",
                "country_code",
                "country_score",
                "risk_band",
                "interaction_count",
                "weighted_mentions",
                "last_updated",
            ],
            target_day,
            country_scores,
        )

        self.store.copy_day_to_parquet(
            connection,
            "graph_edges_daily",
            target_day,
            self.config.data_root / "gold" / f"{target_day.isoformat()}.graph_edges_daily.parquet",
        )

    def _refresh_gkg_daily(self, connection, target_day: date) -> None:
        self.store.delete_rows_for_day(connection, "gkg_themes_daily", target_day)
        self.store.delete_rows_for_day(connection, "gkg_entities_daily", target_day)

        connection.execute(
            """
            INSERT INTO gkg_themes_daily
            SELECT
                e.event_date AS day,
                e.actor1_country AS src_country,
                e.actor2_country AS dst_country,
                e.event_root_code,
                t.theme,
                COUNT(*) AS theme_count
            FROM silver_events_filtered e
            JOIN event_documents d
              ON e.global_event_id = d.global_event_id
            JOIN silver_gkg_themes t
              ON d.document_identifier_normalized = t.document_identifier_normalized
            WHERE e.event_date = ?
            GROUP BY 1, 2, 3, 4, 5
            """,
            [target_day],
        )
        connection.execute(
            """
            INSERT INTO gkg_entities_daily
            SELECT
                e.event_date AS day,
                e.actor1_country AS src_country,
                e.actor2_country AS dst_country,
                e.event_root_code,
                ge.entity_type,
                ge.entity_name,
                COUNT(*) AS entity_count
            FROM silver_events_filtered e
            JOIN event_documents d
              ON e.global_event_id = d.global_event_id
            JOIN silver_gkg_entities ge
              ON d.document_identifier_normalized = ge.document_identifier_normalized
            WHERE e.event_date = ?
            GROUP BY 1, 2, 3, 4, 5, 6
            """,
            [target_day],
        )

    def _fetch_edge_metrics(self, connection, target_day: date) -> list[EdgeMetrics]:
        rows = connection.execute(
            """
            WITH event_metrics AS (
                SELECT
                    e.event_date AS day,
                    e.actor1_country AS src_country,
                    e.actor2_country AS dst_country,
                    e.event_root_code,
                    COUNT(*) AS event_count,
                    AVG(COALESCE(e.avg_tone, 0.0)) AS avg_tone,
                    AVG(COALESCE(e.goldstein_scale, 0.0)) AS avg_goldstein,
                    SUM(COALESCE(m.mention_count, e.num_mentions, 0)) AS total_mentions
                FROM silver_events_filtered e
                LEFT JOIN mentions_agg m
                  ON e.global_event_id = m.global_event_id
                WHERE e.event_date = ?
                GROUP BY 1, 2, 3, 4
            ),
            source_counts AS (
                SELECT
                    e.event_date AS day,
                    e.actor1_country AS src_country,
                    e.actor2_country AS dst_country,
                    e.event_root_code,
                    COUNT(DISTINCT bm.mention_source_name) AS unique_sources
                FROM silver_events_filtered e
                LEFT JOIN bronze_mentions bm
                  ON e.global_event_id = bm.global_event_id
                WHERE e.event_date = ?
                GROUP BY 1, 2, 3, 4
            )
            SELECT
                em.day,
                em.src_country,
                em.dst_country,
                em.event_root_code,
                em.event_count,
                em.avg_tone,
                em.avg_goldstein,
                em.total_mentions,
                COALESCE(sc.unique_sources, 0) AS unique_sources
            FROM event_metrics em
            LEFT JOIN source_counts sc
              ON em.day = sc.day
             AND em.src_country = sc.src_country
             AND em.dst_country = sc.dst_country
             AND em.event_root_code = sc.event_root_code
            ORDER BY em.src_country, em.dst_country, em.event_root_code
            """,
            [target_day, target_day],
        ).fetchall()

        return [EdgeMetrics(*row) for row in rows]

    def _fetch_counts_by_key(self, connection, query: str, target_day: date) -> dict[tuple[str, str, str], list[dict]]:
        counts: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
        for row in connection.execute(query, [target_day]).fetchall():
            key = (row[0], row[1], row[2])
            payload = {"name": row[3], "count": int(row[4])}
            if len(row) > 5 and row[5]:
                payload["type"] = row[5]
            counts[key].append(payload)

        for key, items in counts.items():
            items.sort(key=lambda item: (-item["count"], item["name"]))
            counts[key] = items[:10]
        return counts

    def _fetch_event_locations(self, connection, target_day: date) -> dict[tuple[str, str, str], list[dict]]:
        counts: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
        rows = connection.execute(
            """
            SELECT
                actor1_country,
                actor2_country,
                event_root_code,
                action_geo_full_name,
                COUNT(*) AS location_count
            FROM silver_events_filtered
            WHERE event_date = ?
              AND action_geo_full_name IS NOT NULL
              AND TRIM(action_geo_full_name) <> ''
            GROUP BY 1, 2, 3, 4
            """,
            [target_day],
        ).fetchall()
        for row in rows:
            counts[(row[0], row[1], row[2])].append({"name": row[3], "count": int(row[4])})

        for key, items in counts.items():
            items.sort(key=lambda item: (-item["count"], item["name"]))
            counts[key] = items[:10]
        return counts

    def _merge_location_lists(self, primary: list[dict], secondary: list[dict]) -> list[dict]:
        merged: Counter[str] = Counter()
        for item in primary + secondary:
            merged[item["name"]] += int(item["count"])
        ranked = [
            {"name": name, "count": count, "type": "location"}
            for name, count in merged.most_common(10)
        ]
        return ranked

    def _compute_pair_scores(self, connection, target_day: date, edges: list[EdgeMetrics]) -> list[tuple]:
        history_rows = connection.execute(
            """
            SELECT
                day,
                src_country,
                dst_country,
                event_root_code,
                total_mentions
            FROM graph_edges_daily
            WHERE day >= ?
              AND day < ?
            """,
            [target_day - timedelta(days=6), target_day],
        ).fetchall()

        history: dict[tuple[str, str, str], list[int]] = defaultdict(list)
        for row in history_rows:
            history[(row[1], row[2], row[3])].append(int(row[4]))

        scored_rows = []
        for edge in edges:
            key = (edge.src_country, edge.dst_country, edge.event_root_code)
            prior_mentions = history.get(key, [])
            prior_mean = (sum(prior_mentions) / len(prior_mentions)) if prior_mentions else 0.0
            if len(prior_mentions) >= 3:
                mention_spike_norm = min(
                    max((edge.total_mentions - prior_mean) / max(prior_mean, 1.0), 0.0),
                    1.0,
                )
            else:
                mention_spike_norm = min(edge.total_mentions / 50.0, 1.0)

            intensity_norm = min(math.log1p(edge.event_count) / INTENSITY_LOG_DENOMINATOR, 1.0)
            tone_norm = min(max((-edge.avg_tone) / 10.0, 0.0), 1.0)
            goldstein_norm = min(max((-edge.avg_goldstein) / 10.0, 0.0), 1.0)
            source_norm = min(edge.unique_sources / 25.0, 1.0)
            severity_weight = SEVERITY_WEIGHTS.get(edge.event_root_code, 0.0)
            pair_score_raw = (
                0.35 * (severity_weight * intensity_norm)
                + 0.20 * tone_norm
                + 0.20 * goldstein_norm
                + 0.15 * mention_spike_norm
                + 0.10 * source_norm
            )
            pair_score = round(100.0 * min(pair_score_raw, 1.0), 2)
            scored_rows.append(
                (
                    edge.day,
                    edge.src_country,
                    edge.dst_country,
                    edge.event_root_code,
                    pair_score,
                    risk_band(pair_score),
                    intensity_norm,
                    tone_norm,
                    goldstein_norm,
                    source_norm,
                    mention_spike_norm,
                    severity_weight,
                    edge.total_mentions,
                    datetime.now(timezone.utc),
                )
            )
        return scored_rows

    def _compute_country_scores(self, target_day: date, pair_rows: list[tuple]) -> list[tuple]:
        aggregates: dict[str, dict[str, float]] = defaultdict(
            lambda: {"weighted_score": 0.0, "weight_sum": 0.0, "interaction_count": 0.0}
        )
        for row in pair_rows:
            _, src_country, dst_country, _, pair_score, _, _, _, _, _, _, _, total_mentions, _ = row
            weight = max(int(total_mentions), 1)
            for country in (src_country, dst_country):
                aggregates[country]["weighted_score"] += float(pair_score) * weight
                aggregates[country]["weight_sum"] += weight
                aggregates[country]["interaction_count"] += 1

        country_rows = []
        for country, values in sorted(aggregates.items()):
            score = round(values["weighted_score"] / values["weight_sum"], 2)
            band = risk_band(score)
            country_rows.append(
                (
                    target_day,
                    country,
                    score,
                    band,
                    int(values["interaction_count"]),
                    int(values["weight_sum"]),
                    datetime.now(timezone.utc),
                )
            )
        return country_rows

    def _neo4j_config(self) -> Neo4jConfig:
        if not self.config.neo4j_uri or not self.config.neo4j_username or not self.config.neo4j_password:
            raise RuntimeError("Neo4j credentials are not fully configured in the environment")
        return Neo4jConfig(
            uri=self.config.neo4j_uri,
            username=self.config.neo4j_username,
            password=self.config.neo4j_password,
            database=self.config.neo4j_database,
        )

    def _enforce_raw_retention(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.config.raw_retention_days)
        for path in self.config.raw_root.rglob("*.zip"):
            if datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc) < cutoff:
                path.unlink()

    @staticmethod
    def _is_allowed_event(row: tuple) -> bool:
        actor1_country = row[5]
        actor2_country = row[7]
        root_code = normalize_root_code(row[9])
        return bool(actor1_country and actor2_country and root_code in ALLOWED_ROOT_CODES)
