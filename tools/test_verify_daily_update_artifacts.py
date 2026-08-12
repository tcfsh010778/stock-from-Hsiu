from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_daily_update_artifacts import verify_artifacts


class VerifyDailyUpdateArtifactsTest(unittest.TestCase):
    def write_valid_market_flow(self, root: Path, data_date: str) -> None:
        (root / "data" / "daily_market_flow.json").write_text(
            json.dumps({
                "date": data_date,
                "data_quality": {"state": "ok", "warnings": []},
                "markets": {"listed": {"stock_count": 1000}, "otc": {"stock_count": 800}},
            }),
            encoding="utf-8",
        )

    def test_accepts_site_when_latest_report_date_is_rendered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "docs").mkdir()
            (root / "data").mkdir()
            (root / "reports" / "daily_report_2026-05-13.md").write_text("# report", encoding="utf-8")
            (root / "docs" / "index.html").write_text("latest report 2026-05-13", encoding="utf-8")
            (root / "data" / "site_reports.json").write_text(
                json.dumps([{"date": "2026-05-13"}]),
                encoding="utf-8",
            )
            self.write_valid_market_flow(root, "2026-05-14")

            result = verify_artifacts(root)

            self.assertEqual(result.latest_report_date, "2026-05-13")

    def test_rejects_site_when_index_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "docs").mkdir()
            (root / "data").mkdir()
            (root / "reports" / "daily_report_2026-05-13.md").write_text("# report", encoding="utf-8")
            (root / "docs" / "index.html").write_text("latest report 2026-05-12", encoding="utf-8")
            (root / "data" / "site_reports.json").write_text(
                json.dumps([{"date": "2026-05-13"}]),
                encoding="utf-8",
            )
            self.write_valid_market_flow(root, "2026-05-13")

            with self.assertRaises(SystemExit):
                verify_artifacts(root)

    def test_rejects_site_when_any_generated_html_page_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "docs" / "stocks").mkdir(parents=True)
            (root / "data").mkdir()
            (root / "reports" / "daily_report_2026-05-21.md").write_text("# report", encoding="utf-8")
            (root / "docs" / "index.html").write_text("latest report 2026-05-21", encoding="utf-8")
            (root / "docs" / "daily.html").write_text("redirect generated on 2026-05-21", encoding="utf-8")
            (root / "docs" / "stocks" / "2330.html").write_text("stock detail 2026-05-20", encoding="utf-8")
            (root / "data" / "site_reports.json").write_text(
                json.dumps([{"date": "2026-05-21"}]),
                encoding="utf-8",
            )
            self.write_valid_market_flow(root, "2026-05-21")

            with self.assertRaises(SystemExit):
                verify_artifacts(root)

    def test_v2_pages_use_their_own_manifest_freshness_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "docs" / "v2" / "stocks").mkdir(parents=True)
            (root / "data").mkdir()
            (root / "reports" / "daily_report_2026-08-07.md").write_text("# report", encoding="utf-8")
            (root / "docs" / "index.html").write_text("latest report 2026-08-07", encoding="utf-8")
            (root / "docs" / "v2" / "stock.html").write_text("shared V2 shell", encoding="utf-8")
            (root / "docs" / "v2" / "stocks" / "2330.html").write_text(
                "V2 redirect without embedded report date",
                encoding="utf-8",
            )
            (root / "data" / "site_reports.json").write_text(
                json.dumps([{"date": "2026-08-07"}]),
                encoding="utf-8",
            )
            self.write_valid_market_flow(root, "2026-08-07")

            result = verify_artifacts(root)

            self.assertEqual(result.latest_report_date, "2026-08-07")

    def test_rejects_incomplete_market_flow_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "docs").mkdir()
            (root / "data").mkdir()
            (root / "reports" / "daily_report_2026-08-12.md").write_text("# report", encoding="utf-8")
            (root / "docs" / "index.html").write_text("latest report 2026-08-12", encoding="utf-8")
            (root / "data" / "site_reports.json").write_text(
                json.dumps([{"date": "2026-08-12"}]), encoding="utf-8"
            )
            (root / "data" / "daily_market_flow.json").write_text(
                json.dumps({
                    "date": "2026-08-12",
                    "data_quality": {"state": "missing"},
                    "markets": {"listed": {"stock_count": 0}, "otc": {"stock_count": 0}},
                }),
                encoding="utf-8",
            )

            with self.assertRaises(SystemExit):
                verify_artifacts(root)


if __name__ == "__main__":
    unittest.main()
