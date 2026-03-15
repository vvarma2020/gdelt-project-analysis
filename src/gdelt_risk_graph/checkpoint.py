from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Checkpoint:
    last_timestamp: str | None = None
    last_published_day: str | None = None

    @classmethod
    def load(cls, path: Path) -> "Checkpoint":
        if not path.exists():
            return cls()
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls(
            last_timestamp=payload.get("last_timestamp"),
            last_published_day=payload.get("last_published_day"),
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(
                {
                    "last_timestamp": self.last_timestamp,
                    "last_published_day": self.last_published_day,
                },
                handle,
                indent=2,
                sort_keys=True,
            )

