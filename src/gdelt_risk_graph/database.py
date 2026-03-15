from __future__ import annotations

from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import duckdb


class DuckDBStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    @contextmanager
    def connect(self):
        connection = duckdb.connect(str(self.database_path))
        connection.execute("PRAGMA threads=4")
        connection.execute("PRAGMA enable_progress_bar=false")
        self._initialize(connection)
        try:
            yield connection
        finally:
            connection.close()

    def _initialize(self, connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_events (
                batch_timestamp VARCHAR,
                global_event_id BIGINT,
                event_date DATE,
                sql_date VARCHAR,
                actor1_name VARCHAR,
                actor1_country VARCHAR,
                actor2_name VARCHAR,
                actor2_country VARCHAR,
                event_code VARCHAR,
                event_root_code VARCHAR,
                goldstein_scale DOUBLE,
                avg_tone DOUBLE,
                num_mentions BIGINT,
                num_sources BIGINT,
                action_geo_full_name VARCHAR,
                action_geo_country_code VARCHAR,
                action_geo_lat DOUBLE,
                action_geo_lon DOUBLE,
                source_url VARCHAR,
                source_url_normalized VARCHAR,
                date_added VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_mentions (
                batch_timestamp VARCHAR,
                global_event_id BIGINT,
                event_time TIMESTAMP,
                mention_time TIMESTAMP,
                mention_source_name VARCHAR,
                mention_identifier VARCHAR,
                mention_identifier_normalized VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze_gkg_documents (
                batch_timestamp VARCHAR,
                gkg_record_id VARCHAR,
                gkg_time TIMESTAMP,
                document_identifier VARCHAR,
                document_identifier_normalized VARCHAR,
                avg_tone DOUBLE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_events_filtered (
                batch_timestamp VARCHAR,
                global_event_id BIGINT,
                event_date DATE,
                sql_date VARCHAR,
                actor1_name VARCHAR,
                actor1_country VARCHAR,
                actor2_name VARCHAR,
                actor2_country VARCHAR,
                event_code VARCHAR,
                event_root_code VARCHAR,
                goldstein_scale DOUBLE,
                avg_tone DOUBLE,
                num_mentions BIGINT,
                num_sources BIGINT,
                action_geo_full_name VARCHAR,
                action_geo_country_code VARCHAR,
                action_geo_lat DOUBLE,
                action_geo_lon DOUBLE,
                source_url VARCHAR,
                source_url_normalized VARCHAR,
                date_added VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS mentions_agg (
                global_event_id BIGINT,
                mention_count BIGINT,
                unique_sources BIGINT,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS event_documents (
                global_event_id BIGINT,
                document_identifier VARCHAR,
                document_identifier_normalized VARCHAR,
                origin VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_gkg_themes (
                batch_timestamp VARCHAR,
                document_identifier VARCHAR,
                document_identifier_normalized VARCHAR,
                theme VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_gkg_entities (
                batch_timestamp VARCHAR,
                document_identifier VARCHAR,
                document_identifier_normalized VARCHAR,
                entity_type VARCHAR,
                entity_name VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS gkg_themes_daily (
                day DATE,
                src_country VARCHAR,
                dst_country VARCHAR,
                event_root_code VARCHAR,
                theme VARCHAR,
                theme_count BIGINT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS gkg_entities_daily (
                day DATE,
                src_country VARCHAR,
                dst_country VARCHAR,
                event_root_code VARCHAR,
                entity_type VARCHAR,
                entity_name VARCHAR,
                entity_count BIGINT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_edges_daily (
                day DATE,
                src_country VARCHAR,
                dst_country VARCHAR,
                event_root_code VARCHAR,
                event_count BIGINT,
                avg_tone DOUBLE,
                avg_goldstein DOUBLE,
                total_mentions BIGINT,
                unique_sources BIGINT,
                themes_json VARCHAR,
                top_entities_json VARCHAR,
                top_locations_json VARCHAR,
                last_updated TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_scores_pair_daily (
                day DATE,
                src_country VARCHAR,
                dst_country VARCHAR,
                event_root_code VARCHAR,
                pair_score DOUBLE,
                risk_band VARCHAR,
                intensity_norm DOUBLE,
                tone_norm DOUBLE,
                goldstein_norm DOUBLE,
                source_norm DOUBLE,
                mention_spike_norm DOUBLE,
                severity_weight DOUBLE,
                total_mentions BIGINT,
                last_updated TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_scores_country_daily (
                day DATE,
                country_code VARCHAR,
                country_score DOUBLE,
                risk_band VARCHAR,
                interaction_count BIGINT,
                weighted_mentions BIGINT,
                last_updated TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS batch_ledger (
                batch_timestamp VARCHAR PRIMARY KEY,
                download_completed BOOLEAN DEFAULT FALSE,
                parse_completed BOOLEAN DEFAULT FALSE,
                aggregate_completed BOOLEAN DEFAULT FALSE,
                score_completed BOOLEAN DEFAULT FALSE,
                published_completed BOOLEAN DEFAULT FALSE,
                raw_export_path VARCHAR,
                raw_mentions_path VARCHAR,
                raw_gkg_path VARCHAR,
                last_error VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS neo4j_publish_state (
                day DATE PRIMARY KEY,
                published_at TIMESTAMP
            )
            """
        )

    def delete_where_batch(self, connection: duckdb.DuckDBPyConnection, table: str, batch_timestamp: str) -> None:
        connection.execute(f"DELETE FROM {table} WHERE batch_timestamp = ?", [batch_timestamp])

    def insert_many(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        columns: Sequence[str],
        rows: Sequence[tuple],
    ) -> None:
        if not rows:
            return
        placeholders = ", ".join(["?"] * len(columns))
        column_list = ", ".join(columns)
        connection.executemany(
            f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})",
            rows,
        )

    def copy_batch_to_parquet(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        batch_timestamp: str,
        output_path: Path,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        quoted_path = str(output_path).replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT * FROM {table} WHERE batch_timestamp = ?
            ) TO '{quoted_path}' (FORMAT PARQUET)
            """,
            [batch_timestamp],
        )

    def copy_day_to_parquet(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        day: date,
        output_path: Path,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        quoted_path = str(output_path).replace("'", "''")
        connection.execute(
            f"""
            COPY (
                SELECT * FROM {table} WHERE day = ?
            ) TO '{quoted_path}' (FORMAT PARQUET)
            """,
            [day],
        )

    def upsert_batch_ledger(
        self,
        connection: duckdb.DuckDBPyConnection,
        batch_timestamp: str,
        **values,
    ) -> None:
        existing = connection.execute(
            "SELECT COUNT(*) FROM batch_ledger WHERE batch_timestamp = ?",
            [batch_timestamp],
        ).fetchone()[0]
        if not existing:
            connection.execute(
                "INSERT INTO batch_ledger (batch_timestamp) VALUES (?)",
                [batch_timestamp],
            )

        if not values:
            return

        assignments = ", ".join([f"{column} = ?" for column in values]) + ", updated_at = CURRENT_TIMESTAMP"
        parameters = list(values.values()) + [batch_timestamp]
        connection.execute(
            f"UPDATE batch_ledger SET {assignments} WHERE batch_timestamp = ?",
            parameters,
        )

    def delete_rows_for_event_ids(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        event_ids: Sequence[int],
    ) -> None:
        if not event_ids:
            return
        placeholders = ", ".join(["?"] * len(event_ids))
        connection.execute(
            f"DELETE FROM {table} WHERE global_event_id IN ({placeholders})",
            list(event_ids),
        )

    def delete_rows_for_day(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        day: date,
    ) -> None:
        connection.execute(f"DELETE FROM {table} WHERE day = ?", [day])

    def replace_rows_for_day(
        self,
        connection: duckdb.DuckDBPyConnection,
        table: str,
        columns: Sequence[str],
        day: date,
        rows: Sequence[tuple],
    ) -> None:
        self.delete_rows_for_day(connection, table, day)
        self.insert_many(connection, table, columns, rows)

    def fetchall(
        self,
        connection: duckdb.DuckDBPyConnection,
        query: str,
        parameters: Sequence | None = None,
    ) -> list[tuple]:
        return connection.execute(query, parameters or []).fetchall()
