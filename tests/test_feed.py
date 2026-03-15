from __future__ import annotations

import os
import ssl
import unittest
from unittest.mock import patch

from gdelt_risk_graph.feed import (
    build_ssl_context,
    normalize_gdelt_url,
    parse_masterfilelist,
    select_batches,
)


class FeedTests(unittest.TestCase):
    def test_parse_masterfilelist_groups_complete_batches(self) -> None:
        payload = "\n".join(
            [
                "1 https://data.gdeltproject.org/gdeltv2/20260315000000.export.CSV.zip",
                "2 https://data.gdeltproject.org/gdeltv2/20260315000000.mentions.CSV.zip",
                "3 https://data.gdeltproject.org/gdeltv2/20260315000000.gkg.csv.zip",
                "4 https://data.gdeltproject.org/gdeltv2/20260315001500.export.CSV.zip",
                "5 https://data.gdeltproject.org/gdeltv2/20260315001500.mentions.CSV.zip",
            ]
        )

        batches = parse_masterfilelist(payload)

        self.assertEqual(1, len(batches))
        self.assertEqual("20260315000000", batches[0].timestamp)
        self.assertEqual([batches[0]], select_batches(batches, after_timestamp="20260314235959"))
        self.assertEqual([], select_batches(batches, after_timestamp="20260315000000"))

    def test_build_ssl_context_uses_env_override(self) -> None:
        fake_bundle = "/tmp/test-ca.pem"
        with patch.dict(os.environ, {"GDELT_CA_BUNDLE_PATH": fake_bundle}, clear=False):
            with patch("gdelt_risk_graph.feed.ssl.create_default_context") as create_default_context:
                build_ssl_context()

        create_default_context.assert_called_once_with(cafile=fake_bundle)

    def test_normalize_gdelt_url_prefers_http_for_gdelt_host(self) -> None:
        self.assertEqual(
            "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt",
            normalize_gdelt_url("https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"),
        )
        self.assertEqual(
            "https://example.com/feed.txt",
            normalize_gdelt_url("https://example.com/feed.txt"),
        )


if __name__ == "__main__":
    unittest.main()
