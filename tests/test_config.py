from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gdelt_risk_graph.config import AppConfig


class ConfigTests(unittest.TestCase):
    def test_from_env_loads_project_dotenv_and_respects_existing_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            (project_root / ".env").write_text(
                "\n".join(
                    [
                        "# Local pipeline defaults",
                        "GDELT_DATA_ROOT=data-store",
                        "GDELT_STATE_ROOT=state-store",
                        "GDELT_DUCKDB_PATH=data-store/custom.duckdb",
                        "GDELT_CHECKPOINT_PATH=state-store/custom-checkpoint.json",
                        "GDELT_REQUEST_TIMEOUT_SECONDS=90",
                        "NEO4J_URI=bolt://dotenv:7687",
                        "NEO4J_USERNAME=dotenv-user",
                        "NEO4J_PASSWORD=dotenv-password",
                        "export NEO4J_DATABASE=analytics",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"NEO4J_PASSWORD": "shell-password"}, clear=True):
                config = AppConfig.from_env(project_root=project_root)

            resolved_root = project_root.resolve()
            self.assertEqual(resolved_root, config.project_root)
            self.assertEqual(resolved_root / "data-store", config.data_root)
            self.assertEqual(resolved_root / "state-store", config.state_root)
            self.assertEqual(resolved_root / "data-store" / "custom.duckdb", config.duckdb_path)
            self.assertEqual(
                resolved_root / "state-store" / "custom-checkpoint.json",
                config.checkpoint_path,
            )
            self.assertEqual(90, config.request_timeout_seconds)
            self.assertEqual("bolt://dotenv:7687", config.neo4j_uri)
            self.assertEqual("dotenv-user", config.neo4j_username)
            self.assertEqual("shell-password", config.neo4j_password)
            self.assertEqual("analytics", config.neo4j_database)


if __name__ == "__main__":
    unittest.main()
