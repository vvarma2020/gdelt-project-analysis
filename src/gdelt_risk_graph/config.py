from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    data_root: Path
    raw_root: Path
    bronze_root: Path
    silver_root: Path
    state_root: Path
    checkpoint_path: Path
    duckdb_path: Path
    masterfile_url: str
    raw_retention_days: int
    request_timeout_seconds: int
    neo4j_uri: str | None
    neo4j_username: str | None
    neo4j_password: str | None
    neo4j_database: str

    @classmethod
    def from_env(cls, project_root: Path | str | None = None) -> "AppConfig":
        bootstrap_root = (
            Path(project_root).expanduser().resolve()
            if project_root is not None
            else Path.cwd().resolve()
        )
        load_dotenv_file(bootstrap_root / ".env")

        root = resolve_path_value(
            os.environ.get("GDELT_PROJECT_ROOT"),
            default=bootstrap_root,
            root=bootstrap_root,
        )
        data_root = resolve_path_value(
            os.environ.get("GDELT_DATA_ROOT"),
            default=root / "data",
            root=root,
        )
        state_root = resolve_path_value(
            os.environ.get("GDELT_STATE_ROOT"),
            default=root / "state",
            root=root,
        )
        duckdb_path = resolve_path_value(
            os.environ.get("GDELT_DUCKDB_PATH"),
            default=data_root / "gdelt.duckdb",
            root=root,
        )
        checkpoint_path = resolve_path_value(
            os.environ.get("GDELT_CHECKPOINT_PATH"),
            default=state_root / "checkpoint.json",
            root=root,
        )
        return cls(
            project_root=root,
            data_root=data_root,
            raw_root=data_root / "raw",
            bronze_root=data_root / "bronze",
            silver_root=data_root / "silver",
            state_root=state_root,
            checkpoint_path=checkpoint_path,
            duckdb_path=duckdb_path,
            masterfile_url=os.environ.get(
                "GDELT_MASTERFILE_URL",
                "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
            ),
            raw_retention_days=int(os.environ.get("GDELT_RAW_RETENTION_DAYS", "7")),
            request_timeout_seconds=int(os.environ.get("GDELT_REQUEST_TIMEOUT_SECONDS", "60")),
            neo4j_uri=os.environ.get("NEO4J_URI"),
            neo4j_username=os.environ.get("NEO4J_USERNAME"),
            neo4j_password=os.environ.get("NEO4J_PASSWORD"),
            neo4j_database=os.environ.get("NEO4J_DATABASE", "neo4j"),
        )

    def ensure_directories(self) -> None:
        for path in (
            self.data_root,
            self.raw_root,
            self.bronze_root,
            self.silver_root,
            self.state_root,
            self.duckdb_path.parent,
            self.checkpoint_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line.removeprefix("export ").strip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue

            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            os.environ.setdefault(key, value)


def resolve_path_value(raw_value: str | None, *, default: Path, root: Path) -> Path:
    if raw_value is None:
        return default

    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (root / candidate).resolve()
