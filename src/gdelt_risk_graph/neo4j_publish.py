from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    username: str
    password: str
    database: str


class Neo4jPublisher:
    def __init__(self, config: Neo4jConfig) -> None:
        self.config = config

    def publish_rows(self, rows: list[dict]) -> None:
        try:
            from neo4j import GraphDatabase
        except ImportError as exc:
            raise RuntimeError("neo4j driver is not installed") from exc

        driver = GraphDatabase.driver(
            self.config.uri,
            auth=(self.config.username, self.config.password),
        )
        try:
            with driver.session(database=self.config.database) as session:
                for row in rows:
                    session.execute_write(self._upsert_interaction, row)
        finally:
            driver.close()

    @staticmethod
    def _upsert_interaction(tx, row: dict) -> None:
        tx.run(
            """
            MERGE (src:Country {code: $src_country})
            ON CREATE SET src.name = $src_country
            MERGE (dst:Country {code: $dst_country})
            ON CREATE SET dst.name = $dst_country
            MERGE (interaction:InteractionDay {
                src_code: $src_country,
                dst_code: $dst_country,
                relation: $event_root_code,
                day: date($day)
            })
            SET interaction.event_count = $event_count,
                interaction.avg_tone = $avg_tone,
                interaction.avg_goldstein = $avg_goldstein,
                interaction.total_mentions = $total_mentions,
                interaction.unique_sources = $unique_sources,
                interaction.pair_score = $pair_score,
                interaction.risk_band = $risk_band
            MERGE (src)-[:SOURCE_OF]->(interaction)
            MERGE (interaction)-[:TARGETS]->(dst)
            WITH interaction
            OPTIONAL MATCH (interaction)-[old_theme:HAS_THEME]->(:Theme)
            DELETE old_theme
            WITH interaction
            OPTIONAL MATCH (interaction)-[old_entity:MENTIONS_ENTITY]->(:Entity)
            DELETE old_entity
            """,
            row,
        )

        for theme in json.loads(row["themes_json"] or "[]"):
            tx.run(
                """
                MATCH (interaction:InteractionDay {
                    src_code: $src_country,
                    dst_code: $dst_country,
                    relation: $event_root_code,
                    day: date($day)
                })
                MERGE (theme:Theme {name: $theme_name})
                MERGE (interaction)-[rel:HAS_THEME]->(theme)
                SET rel.count = $count
                """,
                {
                    **row,
                    "theme_name": theme["name"],
                    "count": theme["count"],
                },
            )

        entities = json.loads(row["top_entities_json"] or "[]")
        locations = json.loads(row["top_locations_json"] or "[]")
        for entity in entities + locations:
            tx.run(
                """
                MATCH (interaction:InteractionDay {
                    src_code: $src_country,
                    dst_code: $dst_country,
                    relation: $event_root_code,
                    day: date($day)
                })
                MERGE (entity:Entity {name: $entity_name, type: $entity_type})
                MERGE (interaction)-[rel:MENTIONS_ENTITY]->(entity)
                SET rel.count = $count
                """,
                {
                    **row,
                    "entity_name": entity["name"],
                    "entity_type": entity.get("type", "location"),
                    "count": entity["count"],
                },
            )

