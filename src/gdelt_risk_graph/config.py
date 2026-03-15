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
        root = Path(project_root) if project_root is not None else Path(
            os.environ.get("GDELT_PROJECT_ROOT", Path.cwd())
        )
        data_root = Path(os.environ.get("GDELT_DATA_ROOT", root / "data"))
        state_root = Path(os.environ.get("GDELT_STATE_ROOT", root / "state"))
        duckdb_path = Path(os.environ.get("GDELT_DUCKDB_PATH", data_root / "gdelt.duckdb"))
        checkpoint_path = Path(
            os.environ.get("GDELT_CHECKPOINT_PATH", state_root / "checkpoint.json")
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
