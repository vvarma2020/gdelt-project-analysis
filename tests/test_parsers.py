from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path

from gdelt_risk_graph.parsers import parse_events_zip, parse_gkg_zip


def write_zip(path: Path, member_name: str, lines: list[str]) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, "\n".join(lines) + "\n")


class ParserTests(unittest.TestCase):
    def test_parse_events_zip_supports_61_column_live_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "sample.export.CSV.zip"
            event_row = [""] * 61
            event_row[0] = "1293940307"
            event_row[1] = "20250313"
            event_row[6] = "LIVERPOOL"
            event_row[7] = "GBR"
            event_row[16] = "CORK"
            event_row[17] = "IRL"
            event_row[26] = "042"
            event_row[28] = "04"
            event_row[30] = "1.9"
            event_row[31] = "8"
            event_row[32] = "8"
            event_row[34] = "1.2310606060606"
            event_row[51] = "4"
            event_row[52] = "Dublin, Dublin, Ireland"
            event_row[53] = "EI"
            event_row[54] = "EI07"
            event_row[55] = "18288"
            event_row[56] = "53.3331"
            event_row[57] = "-6.24889"
            event_row[58] = "-1502554"
            event_row[59] = "20260313000000"
            event_row[60] = "http://data.gdeltproject.org/example"
            write_zip(zip_path, "sample.export.CSV", ["\t".join(event_row)])

            rows = parse_events_zip(zip_path, "20260313000000")

            self.assertEqual(1, len(rows))
            parsed = rows[0]
            self.assertEqual("GBR", parsed[5])
            self.assertEqual("IRL", parsed[7])
            self.assertEqual("Dublin, Dublin, Ireland", parsed[14])
            self.assertEqual("EI", parsed[15])
            self.assertAlmostEqual(53.3331, parsed[16], places=4)
            self.assertAlmostEqual(-6.24889, parsed[17], places=5)
            self.assertEqual("http://data.gdeltproject.org/example", parsed[18])
            self.assertEqual("20260313000000", parsed[20])

    def test_parse_gkg_zip_extracts_themes_and_entities(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "sample.gkg.csv.zip"
            gkg_row = [
                "GKG1",
                "20260315010000",
                "1",
                "example.com",
                "https://news.example/a",
                "",
                "",
                "",
                "SANCTIONS;NUCLEAR",
                "",
                "1#Tehran, Tehran, Iran#IR#IR07#35.6892#51.3890#-",
                "",
                "Ali Khamenei,10",
                "",
                "IRGC,20",
                "-5.0,1.0,6.0,0.0,0.0,0.0,0.0",
            ]
            write_zip(zip_path, "sample.gkg.csv", ["\t".join(gkg_row)])

            documents, themes, entities = parse_gkg_zip(zip_path, "20260315011500")

            self.assertEqual(1, len(documents))
            self.assertEqual("https://news.example/a", documents[0][3])
            self.assertCountEqual(["SANCTIONS", "NUCLEAR"], [row[3] for row in themes])
            self.assertCountEqual(
                [("person", "Ali Khamenei"), ("organization", "IRGC"), ("location", "Tehran, Tehran, Iran")],
                [(row[3], row[4]) for row in entities],
            )

    def test_parse_gkg_zip_handles_large_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "large.gkg.csv.zip"
            huge_field = "x" * 150000
            gkg_row = [
                "GKG2",
                "20260315010000",
                "1",
                "example.com",
                "https://news.example/large",
                "",
                "",
                "",
                "SANCTIONS",
                "",
                "",
                "",
                "",
                "",
                "",
                "-2.0,0.0,0.0,0.0,0.0,0.0,0.0",
                "",
                huge_field,
            ]
            write_zip(zip_path, "large.gkg.csv", ["\t".join(gkg_row)])

            documents, themes, entities = parse_gkg_zip(zip_path, "20260315011500")

            self.assertEqual(1, len(documents))
            self.assertEqual(["SANCTIONS"], [row[3] for row in themes])
            self.assertEqual([], entities)


if __name__ == "__main__":
    unittest.main()
