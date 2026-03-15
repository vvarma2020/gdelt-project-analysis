from __future__ import annotations

import csv
import io
import sys
import zipfile
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path


def normalize_url(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    return normalized.lower()


def normalize_root_code(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped.zfill(2)


def parse_yyyymmdd(value: str | None) -> date | None:
    if not value:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return datetime.strptime(stripped[0:8], "%Y%m%d").date()


def parse_yyyymmddhhmmss(value: str | None) -> datetime | None:
    if not value:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return datetime.strptime(stripped[0:14], "%Y%m%d%H%M%S")


def to_int(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return int(float(stripped))


def to_float(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return float(stripped)


def _configure_csv_field_limit() -> None:
    field_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(field_limit)
            return
        except OverflowError:
            field_limit //= 10


def iter_tsv_rows(zip_path: Path) -> Iterator[list[str]]:
    _configure_csv_field_limit()
    with zipfile.ZipFile(zip_path, "r") as archive:
        member = archive.namelist()[0]
        with archive.open(member, "r") as handle:
            wrapper = io.TextIOWrapper(handle, encoding="utf-8", newline="")
            reader = csv.reader(wrapper, delimiter="\t")
            for row in reader:
                if row:
                    yield row


def parse_events_zip(zip_path: Path, batch_timestamp: str) -> list[tuple]:
    rows = []
    for raw in iter_tsv_rows(zip_path):
        if len(raw) < 58:
            continue

        if len(raw) >= 61:
            action_geo_full_name = raw[52].strip() or None
            action_geo_country_code = raw[53].strip() or None
            action_geo_lat = to_float(raw[56])
            action_geo_lon = to_float(raw[57])
            date_added = raw[59].strip() or None
            source_url = raw[60].strip() or None
        else:
            action_geo_full_name = raw[50].strip() or None
            action_geo_country_code = raw[51].strip() or None
            action_geo_lat = to_float(raw[53])
            action_geo_lon = to_float(raw[54])
            date_added = raw[56].strip() or None
            source_url = raw[57].strip() or None

        rows.append(
            (
                batch_timestamp,
                to_int(raw[0]),
                parse_yyyymmdd(raw[1]),
                raw[1].strip() or None,
                raw[6].strip() or None,
                raw[7].strip() or None,
                raw[16].strip() or None,
                raw[17].strip() or None,
                raw[26].strip() or None,
                normalize_root_code(raw[28]),
                to_float(raw[30]),
                to_float(raw[34]),
                to_int(raw[31]),
                to_int(raw[32]),
                action_geo_full_name,
                action_geo_country_code,
                action_geo_lat,
                action_geo_lon,
                source_url,
                normalize_url(source_url),
                date_added,
            )
        )
    return rows


def parse_mentions_zip(zip_path: Path, batch_timestamp: str) -> list[tuple]:
    rows = []
    for raw in iter_tsv_rows(zip_path):
        if len(raw) < 6:
            continue
        rows.append(
            (
                batch_timestamp,
                to_int(raw[0]),
                parse_yyyymmddhhmmss(raw[1]),
                parse_yyyymmddhhmmss(raw[2]),
                raw[4].strip() or None,
                raw[5].strip() or None,
                normalize_url(raw[5]),
            )
        )
    return rows


def _split_semicolon_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(";") if item.strip()]


def _extract_theme(value: str) -> str | None:
    theme = value.split(",", 1)[0].strip()
    return theme or None


def _extract_name_token(value: str) -> str | None:
    name = value.split(",", 1)[0].strip()
    return name or None


def _extract_location_name(value: str) -> str | None:
    parts = [part.strip() for part in value.split("#")]
    if len(parts) > 1 and parts[1]:
        return parts[1]
    return parts[0] if parts and parts[0] else None


def parse_gkg_zip(zip_path: Path, batch_timestamp: str) -> tuple[list[tuple], list[tuple], list[tuple]]:
    documents: list[tuple] = []
    themes: list[tuple] = []
    entities: list[tuple] = []

    for raw in iter_tsv_rows(zip_path):
        if len(raw) < 5:
            continue

        document_identifier = raw[4].strip() if len(raw) > 4 else None
        normalized_document_identifier = normalize_url(document_identifier)
        if not normalized_document_identifier:
            continue

        v2_themes = raw[8] if len(raw) > 8 else (raw[7] if len(raw) > 7 else "")
        v2_locations = raw[10] if len(raw) > 10 else ""
        v2_persons = raw[12] if len(raw) > 12 else ""
        v2_organizations = raw[14] if len(raw) > 14 else ""
        v2_tone = raw[15] if len(raw) > 15 else ""

        documents.append(
            (
                batch_timestamp,
                raw[0].strip() or None,
                parse_yyyymmddhhmmss(raw[1]),
                document_identifier,
                normalized_document_identifier,
                to_float(v2_tone.split(",", 1)[0] if v2_tone else None),
            )
        )

        for token in _split_semicolon_values(v2_themes):
            theme = _extract_theme(token)
            if theme:
                themes.append(
                    (
                        batch_timestamp,
                        document_identifier,
                        normalized_document_identifier,
                        theme,
                    )
                )

        for token in _split_semicolon_values(v2_persons):
            entity_name = _extract_name_token(token)
            if entity_name:
                entities.append(
                    (
                        batch_timestamp,
                        document_identifier,
                        normalized_document_identifier,
                        "person",
                        entity_name,
                    )
                )

        for token in _split_semicolon_values(v2_organizations):
            entity_name = _extract_name_token(token)
            if entity_name:
                entities.append(
                    (
                        batch_timestamp,
                        document_identifier,
                        normalized_document_identifier,
                        "organization",
                        entity_name,
                    )
                )

        for token in _split_semicolon_values(v2_locations):
            entity_name = _extract_location_name(token)
            if entity_name:
                entities.append(
                    (
                        batch_timestamp,
                        document_identifier,
                        normalized_document_identifier,
                        "location",
                        entity_name,
                    )
                )

    return documents, themes, entities
