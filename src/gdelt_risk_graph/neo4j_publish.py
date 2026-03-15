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

    def reset_projection(self) -> None:
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
                session.execute_write(self._delete_projection)
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

    @staticmethod
    def _delete_projection(tx) -> None:
        interaction_ids = Neo4jPublisher._collect_element_ids(
            tx,
            "MATCH (interaction:InteractionDay) RETURN collect(elementId(interaction)) AS node_ids",
        )
        country_ids = set(
            Neo4jPublisher._collect_element_ids(
                tx,
                "MATCH (country:Country)-[:SOURCE_OF]->(:InteractionDay) RETURN collect(DISTINCT elementId(country)) AS node_ids",
            )
        )
        country_ids.update(
            Neo4jPublisher._collect_element_ids(
                tx,
                "MATCH (:InteractionDay)-[:TARGETS]->(country:Country) RETURN collect(DISTINCT elementId(country)) AS node_ids",
            )
        )
        theme_ids = Neo4jPublisher._collect_element_ids(
            tx,
            "MATCH (:InteractionDay)-[:HAS_THEME]->(theme:Theme) RETURN collect(DISTINCT elementId(theme)) AS node_ids",
        )
        entity_ids = Neo4jPublisher._collect_element_ids(
            tx,
            "MATCH (:InteractionDay)-[:MENTIONS_ENTITY]->(entity:Entity) RETURN collect(DISTINCT elementId(entity)) AS node_ids",
        )

        Neo4jPublisher._delete_nodes_by_id(tx, interaction_ids)
        Neo4jPublisher._delete_isolated_nodes_by_id(tx, "Theme", theme_ids)
        Neo4jPublisher._delete_isolated_nodes_by_id(tx, "Entity", entity_ids)
        Neo4jPublisher._delete_isolated_nodes_by_id(tx, "Country", sorted(country_ids))

    @staticmethod
    def _collect_element_ids(tx, query: str) -> list[str]:
        record = tx.run(query).single()
        if record is None:
            return []
        return [node_id for node_id in record["node_ids"] if node_id is not None]

    @staticmethod
    def _delete_nodes_by_id(tx, node_ids: list[str]) -> None:
        if not node_ids:
            return
        tx.run(
            """
            UNWIND $node_ids AS node_id
            MATCH (node)
            WHERE elementId(node) = node_id
            DETACH DELETE node
            """,
            {"node_ids": node_ids},
        )

    @staticmethod
    def _delete_isolated_nodes_by_id(tx, label: str, node_ids: list[str]) -> None:
        if not node_ids:
            return
        tx.run(
            f"""
            UNWIND $node_ids AS node_id
            MATCH (node:{label})
            WHERE elementId(node) = node_id
              AND NOT (node)--()
            DETACH DELETE node
            """,
            {"node_ids": node_ids},
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
