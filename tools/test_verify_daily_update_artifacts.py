from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_daily_update_artifacts import verify_artifacts


class VerifyDailyUpdateArtifactsTest(unittest.TestCase):
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

            with self.assertRaises(SystemExit):
                verify_artifacts(root)


if __name__ == "__main__":
    unittest.main()
