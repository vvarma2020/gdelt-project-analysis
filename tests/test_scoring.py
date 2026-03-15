from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta

from gdelt_risk_graph.config import AppConfig
from gdelt_risk_graph.pipeline import EdgeMetrics, Pipeline


class ScoringTests(unittest.TestCase):
    def test_pair_scoring_uses_prior_mean_when_history_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = Pipeline(AppConfig.from_env(project_root=temp_dir))
            target_day = date(2026, 3, 15)

            with pipeline.store.connect() as connection:
                for days_ago in (3, 2, 1):
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
                        VALUES (?, 'USA', 'IRN', '13', 1, -1.0, -1.0, 10, 2, '[]', '[]', '[]', ?)
                        """,
                        [target_day - timedelta(days=days_ago), datetime.now(UTC)],
                    )

                edge = EdgeMetrics(
                    day=target_day,
                    src_country="USA",
                    dst_country="IRN",
                    event_root_code="13",
                    event_count=1,
                    avg_tone=-2.0,
                    avg_goldstein=-4.0,
                    total_mentions=20,
                    unique_sources=5,
                )
                scored = pipeline._compute_pair_scores(connection, target_day, [edge])

            self.assertEqual(1, len(scored))
            self.assertEqual(1.0, scored[0][10])


if __name__ == "__main__":
    unittest.main()
