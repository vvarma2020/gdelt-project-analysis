from __future__ import annotations

import os
import re
import ssl
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

import certifi


MASTERFILE_PATTERN = re.compile(
    r"(?P<timestamp>\d{14})\.(?P<kind>export|mentions|gkg)\.(?:CSV|csv)\.zip$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class BatchFiles:
    timestamp: str
    export_url: str
    mentions_url: str
    gkg_url: str


@dataclass(frozen=True)
class LocalBatchFiles:
    timestamp: str
    export_path: Path
    mentions_path: Path
    gkg_path: Path


def build_ssl_context() -> ssl.SSLContext:
    ca_bundle_path = os.environ.get("GDELT_CA_BUNDLE_PATH", certifi.where())
    return ssl.create_default_context(cafile=ca_bundle_path)


def normalize_gdelt_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme == "https" and parsed.netloc == "data.gdeltproject.org":
        return parsed._replace(scheme="http").geturl()
    return url


def fetch_masterfilelist(masterfile_url: str, timeout_seconds: int) -> str:
    resolved_url = normalize_gdelt_url(masterfile_url)
    with urlopen(
        resolved_url,
        timeout=timeout_seconds,
        context=build_ssl_context(),
    ) as response:
        return response.read().decode("utf-8")


def parse_masterfilelist(payload: str) -> list[BatchFiles]:
    grouped: dict[str, dict[str, str]] = {}
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        url = line.split()[-1]
        match = MASTERFILE_PATTERN.search(url)
        if not match:
            continue
        timestamp = match.group("timestamp")
        kind = match.group("kind").lower()
        grouped.setdefault(timestamp, {})[kind] = url

    discovered = []
    for timestamp, files in grouped.items():
        if {"export", "mentions", "gkg"}.issubset(files):
            discovered.append(
                BatchFiles(
                    timestamp=timestamp,
                    export_url=files["export"],
                    mentions_url=files["mentions"],
                    gkg_url=files["gkg"],
                )
            )
    return sorted(discovered, key=lambda item: item.timestamp)


def select_batches(
    batches: list[BatchFiles],
    *,
    after_timestamp: str | None = None,
    start_timestamp: str | None = None,
) -> list[BatchFiles]:
    selected = []
    for batch in batches:
        if after_timestamp and batch.timestamp <= after_timestamp:
            continue
        if start_timestamp and batch.timestamp < start_timestamp:
            continue
        selected.append(batch)
    return selected


def dated_path(root: Path, timestamp: str, suffix: str) -> Path:
    yyyy = timestamp[0:4]
    mm = timestamp[4:6]
    dd = timestamp[6:8]
    return root / yyyy / mm / dd / f"{timestamp}.{suffix}"


def download_to_path(url: str, destination: Path, timeout_seconds: int) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return destination

    temp_path = destination.with_suffix(destination.suffix + ".part")
    resolved_url = normalize_gdelt_url(url)
    with urlopen(
        resolved_url,
        timeout=timeout_seconds,
        context=build_ssl_context(),
    ) as response:
        temp_path.write_bytes(response.read())
    temp_path.replace(destination)
    return destination
