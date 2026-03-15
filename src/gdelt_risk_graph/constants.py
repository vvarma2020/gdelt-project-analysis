from __future__ import annotations

from math import log

ALLOWED_ROOT_CODES = {"05", "11", "13", "14", "15", "16", "17", "18", "19"}

SEVERITY_WEIGHTS = {
    "05": 0.00,
    "11": 0.20,
    "13": 0.45,
    "14": 0.35,
    "15": 0.50,
    "16": 0.60,
    "17": 0.75,
    "18": 0.90,
    "19": 1.00,
}

INTENSITY_LOG_DENOMINATOR = log(11)

EVENT_COLUMNS = [
    "global_event_id",
    "event_date",
    "sql_date",
    "actor1_name",
    "actor1_country",
    "actor2_name",
    "actor2_country",
    "event_code",
    "event_root_code",
    "goldstein_scale",
    "avg_tone",
    "num_mentions",
    "num_sources",
    "action_geo_full_name",
    "action_geo_country_code",
    "action_geo_lat",
    "action_geo_lon",
    "source_url",
    "source_url_normalized",
    "date_added",
]

MENTION_COLUMNS = [
    "global_event_id",
    "event_time",
    "mention_time",
    "mention_source_name",
    "mention_identifier",
    "mention_identifier_normalized",
]

GKG_DOCUMENT_COLUMNS = [
    "gkg_record_id",
    "gkg_time",
    "document_identifier",
    "document_identifier_normalized",
    "avg_tone",
]

GKG_THEME_COLUMNS = [
    "document_identifier",
    "document_identifier_normalized",
    "theme",
]

GKG_ENTITY_COLUMNS = [
    "document_identifier",
    "document_identifier_normalized",
    "entity_type",
    "entity_name",
]

RISK_BANDS = (
    (75.0, "critical"),
    (50.0, "high"),
    (25.0, "elevated"),
    (0.0, "low"),
)

