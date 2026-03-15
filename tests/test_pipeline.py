from __future__ import annotations

import json
import os
import tempfile
import unittest
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from gdelt_risk_graph.config import AppConfig
from gdelt_risk_graph.feed import BatchFiles, LocalBatchFiles
from gdelt_risk_graph.pipeline import Pipeline, PreparedBatch


def write_zip(path: Path, member_name: str, lines: list[str]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, "\n".join(lines) + "\n")


def build_event_row(
    global_event_id: int,
    sql_date: str,
    actor1_name: str,
    actor1_country: str,
    actor2_name: str,
    actor2_country: str,
    event_code: str,
    event_root_code: str,
    goldstein: str,
    avg_tone: str,
    num_mentions: str,
    num_sources: str,
    location: str,
    lat: str,
    lon: str,
    source_url: str,
) -> str:
    row = [""] * 58
    row[0] = str(global_event_id)
    row[1] = sql_date
    row[6] = actor1_name
    row[7] = actor1_country
    row[16] = actor2_name
    row[17] = actor2_country
    row[26] = event_code
    row[28] = event_root_code
    row[30] = goldstein
    row[31] = num_mentions
    row[32] = num_sources
    row[34] = avg_tone
    row[50] = location
    row[51] = actor2_country
    row[53] = lat
    row[54] = lon
    row[56] = sql_date + "000000"
    row[57] = source_url
    return "\t".join(row)


def build_mention_row(
    global_event_id: int,
    event_time: str,
    mention_time: str,
    source_name: str,
    mention_identifier: str,
) -> str:
    row = [""] * 16
    row[0] = str(global_event_id)
    row[1] = event_time
    row[2] = mention_time
    row[4] = source_name
    row[5] = mention_identifier
    return "\t".join(row)


def build_gkg_row(document_identifier: str, themes: str, location: str, person: str, organization: str) -> str:
    row = [""] * 16
    row[0] = document_identifier.rsplit("/", 1)[-1]
    row[1] = "20260315010000"
    row[4] = document_identifier
    row[8] = themes
    row[10] = location
    row[12] = person
    row[14] = organization
    row[15] = "-4.0,0.0,0.0,0.0,0.0,0.0,0.0"
    return "\t".join(row)


class PipelineTests(unittest.TestCase):
    def test_backfill_prepares_in_parallel_and_rebuilds_each_day_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            pipeline = Pipeline(AppConfig.from_env(project_root=temp_root))
            batches = [
                BatchFiles(
                    timestamp="20260315000000",
                    export_url="http://example.com/export1.zip",
                    mentions_url="http://example.com/mentions1.zip",
                    gkg_url="http://example.com/gkg1.zip",
                ),
                BatchFiles(
                    timestamp="20260315001500",
                    export_url="http://example.com/export2.zip",
                    mentions_url="http://example.com/mentions2.zip",
                    gkg_url="http://example.com/gkg2.zip",
                ),
            ]

            prepared_timestamps: list[str] = []
            committed_batches: list[tuple[str, bool]] = []
            rebuilt_days: list[date] = []

            def fake_prepare(batch: BatchFiles) -> PreparedBatch:
                prepared_timestamps.append(batch.timestamp)
                return PreparedBatch(
                    batch=batch,
                    local_batch=LocalBatchFiles(
                        timestamp=batch.timestamp,
                        export_path=temp_root / f"{batch.timestamp}.export.CSV.zip",
                        mentions_path=temp_root / f"{batch.timestamp}.mentions.CSV.zip",
                        gkg_path=temp_root / f"{batch.timestamp}.gkg.csv.zip",
                    ),
                    events=[],
                    mentions=[],
                    gkg_documents=[],
                    gkg_themes=[],
                    gkg_entities=[],
                    filtered_events=[],
                    affected_event_ids=[],
                )

            def fake_commit(connection, prepared: PreparedBatch, *, rebuild_days: bool) -> list[date]:
                committed_batches.append((prepared.batch.timestamp, rebuild_days))
                return [date(2026, 3, 15)]

            def fake_rebuild(connection, target_day: date) -> None:
                rebuilt_days.append(target_day)

            with patch("gdelt_risk_graph.pipeline.fetch_masterfilelist", return_value="ignored"):
                with patch("gdelt_risk_graph.pipeline.parse_masterfilelist", return_value=batches):
                    with patch("gdelt_risk_graph.pipeline.select_batches", return_value=batches):
                        with patch.object(pipeline, "_prepare_batch", side_effect=fake_prepare):
                            with patch.object(pipeline, "_commit_prepared_batch", side_effect=fake_commit):
                                with patch.object(
                                    pipeline,
                                    "_rebuild_day_with_connection",
                                    side_effect=fake_rebuild,
                                ):
                                    processed = pipeline.backfill(1, workers=2)

            self.assertEqual(2, processed)
            self.assertCountEqual(["20260315000000", "20260315001500"], prepared_timestamps)
            self.assertEqual(
                [("20260315000000", False), ("20260315001500", False)],
                committed_batches,
            )
            self.assertEqual([date(2026, 3, 15)], rebuilt_days)
            self.assertEqual("20260315001500", pipeline.checkpoint.last_timestamp)

    def test_process_batch_builds_graph_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            export_zip = temp_root / "20260315011500.export.CSV.zip"
            mentions_zip = temp_root / "20260315011500.mentions.CSV.zip"
            gkg_zip = temp_root / "20260315011500.gkg.csv.zip"

            write_zip(
                export_zip,
                "20260315011500.export.CSV",
                [
                    build_event_row(
                        1001,
                        "20260315",
                        "United States",
                        "USA",
                        "Iran",
                        "IRN",
                        "130",
                        "13",
                        "-6.0",
                        "-5.0",
                        "10",
                        "3",
                        "Tehran, Iran",
                        "35.6892",
                        "51.3890",
                        "https://news.example/a",
                    ),
                    build_event_row(
                        1002,
                        "20260315",
                        "United States",
                        "USA",
                        "Iran",
                        "IRN",
                        "131",
                        "13",
                        "-4.0",
                        "-3.0",
                        "5",
                        "2",
                        "Natanz, Iran",
                        "33.7243",
                        "51.7267",
                        "https://news.example/b",
                    ),
                ],
            )
            write_zip(
                mentions_zip,
                "20260315011500.mentions.CSV",
                [
                    build_mention_row(1001, "20260315000000", "20260315001500", "Reuters", "https://news.example/a"),
                    build_mention_row(1001, "20260315000000", "20260315003000", "AP", "https://news.example/c"),
                    build_mention_row(1002, "20260315000000", "20260315004500", "Reuters", "https://news.example/b"),
                ],
            )
            write_zip(
                gkg_zip,
                "20260315011500.gkg.csv",
                [
                    build_gkg_row(
                        "https://news.example/a",
                        "SANCTIONS;NUCLEAR",
                        "1#Tehran, Tehran, Iran#IR#IR07#35.6#51.3#-",
                        "Ali Khamenei,10",
                        "IRGC,20",
                    ),
                    build_gkg_row(
                        "https://news.example/b",
                        "SANCTIONS;URANIUM",
                        "1#Natanz, Isfahan, Iran#IR#IR10#33.7#51.7#-",
                        "Ebrahim Raisi,10",
                        "Atomic Energy Organization of Iran,20",
                    ),
                    build_gkg_row(
                        "https://news.example/c",
                        "SANCTIONS",
                        "1#Tehran, Tehran, Iran#IR#IR07#35.6#51.3#-",
                        "Ali Khamenei,10",
                        "IRGC,20",
                    ),
                ],
            )

            pipeline = Pipeline(AppConfig.from_env(project_root=temp_root))
            batch = BatchFiles(
                timestamp="20260315011500",
                export_url=export_zip.as_uri(),
                mentions_url=mentions_zip.as_uri(),
                gkg_url=gkg_zip.as_uri(),
            )

            pipeline.process_batch(batch)
            pipeline.process_batch(batch)

            with pipeline.store.connect() as connection:
                bronze_events_count = connection.execute("SELECT COUNT(*) FROM bronze_events").fetchone()[0]
                graph_row = connection.execute(
                    """
                    SELECT
                        event_count,
                        avg_tone,
                        avg_goldstein,
                        total_mentions,
                        unique_sources,
                        themes_json,
                        top_entities_json,
                        top_locations_json
                    FROM graph_edges_daily
                    WHERE day = DATE '2026-03-15'
                      AND src_country = 'USA'
                      AND dst_country = 'IRN'
                      AND event_root_code = '13'
                    """
                ).fetchone()
                pair_score_row = connection.execute(
                    """
                    SELECT pair_score, risk_band
                    FROM risk_scores_pair_daily
                    WHERE day = DATE '2026-03-15'
                      AND src_country = 'USA'
                      AND dst_country = 'IRN'
                      AND event_root_code = '13'
                    """
                ).fetchone()
                country_scores = connection.execute(
                    """
                    SELECT country_code, country_score
                    FROM risk_scores_country_daily
                    WHERE day = DATE '2026-03-15'
                    ORDER BY country_code
                    """
                ).fetchall()

            self.assertEqual(2, bronze_events_count)
            self.assertEqual(2, graph_row[0])
            self.assertAlmostEqual(-4.0, graph_row[1], places=3)
            self.assertAlmostEqual(-5.0, graph_row[2], places=3)
            self.assertEqual(3, graph_row[3])
            self.assertEqual(2, graph_row[4])

            themes = json.loads(graph_row[5])
            entities = json.loads(graph_row[6])
            locations = json.loads(graph_row[7])

            self.assertEqual("SANCTIONS", themes[0]["name"])
            self.assertEqual(3, themes[0]["count"])
            self.assertEqual("Ali Khamenei", entities[0]["name"])
            self.assertEqual("Tehran, Tehran, Iran", locations[0]["name"])

            self.assertAlmostEqual(26.92, pair_score_row[0], places=2)
            self.assertEqual("elevated", pair_score_row[1])
            self.assertEqual([("IRN", pair_score_row[0]), ("USA", pair_score_row[0])], country_scores)

    def test_publish_neo4j_marks_published_days(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            with patch.dict(
                os.environ,
                {
                    "NEO4J_URI": "bolt://localhost:7687",
                    "NEO4J_USERNAME": "neo4j",
                    "NEO4J_PASSWORD": "password",
                    "NEO4J_DATABASE": "neo4j",
                },
                clear=False,
            ):
                pipeline = Pipeline(AppConfig.from_env(project_root=temp_root))

            with pipeline.store.connect() as connection:
                connection.execute(
                    """
                    INSERT INTO graph_edges_daily (
                        day,
                        src_country,
                        dst_country,
                        event_root_code,
                        event_count,
                        avg_tone,
                        avg_goldstein,
                        total_mentions,
                        unique_sources,
                        themes_json,
                        top_entities_json,
                        top_locations_json,
                        last_updated
                    )
                    VALUES (
                        DATE '2026-03-15',
                        'USA',
                        'IRN',
                        '13',
                        2,
                        -4.0,
                        -5.0,
                        3,
                        2,
                        '[{"name":"SANCTIONS","count":3}]',
                        '[{"name":"Ali Khamenei","count":2,"type":"person"}]',
                        '[{"name":"Tehran, Tehran, Iran","count":2,"type":"location"}]',
                        ?
                    )
                    """,
                    [datetime.now(UTC)],
                )
                connection.execute(
                    """
                    INSERT INTO risk_scores_pair_daily (
                        day,
                        src_country,
                        dst_country,
                        event_root_code,
                        pair_score,
                        risk_band,
                        intensity_norm,
                        tone_norm,
                        goldstein_norm,
                        source_norm,
                        mention_spike_norm,
                        severity_weight,
                        total_mentions,
                        last_updated
                    )
                    VALUES (
                        DATE '2026-03-15',
                        'USA',
                        'IRN',
                        '13',
                        26.92,
                        'elevated',
                        0.45,
                        0.40,
                        0.50,
                        0.08,
                        0.06,
                        0.45,
                        3,
                        ?
                    )
                    """,
                    [datetime.now(UTC)],
                )

            with patch("gdelt_risk_graph.pipeline.Neo4jPublisher.publish_rows") as publish_rows:
                published_count = pipeline.publish_neo4j()

            self.assertEqual(1, published_count)
            publish_rows.assert_called_once()
            payload = publish_rows.call_args.args[0][0]
            self.assertEqual("2026-03-15", payload["day"])
            self.assertEqual("USA", payload["src_country"])
            self.assertEqual("elevated", payload["risk_band"])
            self.assertEqual("2026-03-15", pipeline.checkpoint.last_published_day)

            with pipeline.store.connect() as connection:
                published_days = connection.execute(
                    "SELECT day FROM neo4j_publish_state ORDER BY day"
                ).fetchall()
            self.assertEqual([(datetime(2026, 3, 15).date(),)], published_days)


if __name__ == "__main__":
    unittest.main()
