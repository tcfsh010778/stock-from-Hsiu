"""Regression tests for strict common-session publication checks."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SOURCE = Path(__file__).resolve().parents[2] / "daily-public" / "tools" / "verify_daily_update_artifacts.py"
SPEC = importlib.util.spec_from_file_location("daily_public_verify_daily_update_artifacts", SOURCE)
verifier = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


class DailySessionIntegrityTests(unittest.TestCase):
    SESSION = "2026-09-09"

    def make_root(self, folder: str) -> Path:
        root = Path(folder)
        (root / "reports").mkdir()
        (root / "docs").mkdir()
        (root / "data").mkdir()
        (root / "reports" / f"daily-{self.SESSION}.md").write_text("report", encoding="utf-8")
        (root / "docs" / "index.html").write_text(self.SESSION, encoding="utf-8")
        (root / "data" / "site_reports.json").write_text(
            json.dumps([{"report_date": self.SESSION}]), encoding="utf-8"
        )
        (root / "data" / "daily_market_flow.json").write_text(
            json.dumps({"date": self.SESSION, "data_quality": {"state": "ok"}, "markets": {"listed": {"stock_count": 800}, "otc": {"stock_count": 600}}}),
            encoding="utf-8",
        )
        (root / "data" / "official_adjusted_update_manifest.json").write_text(
            json.dumps({"dataset_id": "official_adjusted_daily_update", "data_as_of": self.SESSION, "status": "complete"}),
            encoding="utf-8",
        )
        (root / "data" / "review_queue.json").write_text(
            json.dumps({"as_of": self.SESSION}), encoding="utf-8"
        )
        return root

    def test_valid_weekday_common_session(self):
        with tempfile.TemporaryDirectory() as folder:
            result = verifier.verify_artifacts(self.make_root(folder), self.SESSION)
            self.assertEqual(result.latest_report_date, self.SESSION)

    def test_stale_and_future_review_queue_sessions_are_rejected(self):
        for queue_date in ("2026-09-08", "2026-09-10"):
            with self.subTest(queue_date=queue_date), tempfile.TemporaryDirectory() as folder:
                root = self.make_root(folder)
                (root / "data" / "review_queue.json").write_text(json.dumps({"as_of": queue_date}), encoding="utf-8")
                with self.assertRaisesRegex(SystemExit, "review queue session mismatch"):
                    verifier.verify_artifacts(root, self.SESSION)

    def test_official_manifest_date_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = self.make_root(folder)
            manifest = {"dataset_id": "official_adjusted_daily_update", "data_as_of": "2026-09-08", "status": "complete"}
            (root / "data" / "official_adjusted_update_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(SystemExit, "manifest does not match"):
                verifier.verify_artifacts(root, self.SESSION)

    def test_unfinished_recovery_directories_are_rejected(self):
        for name in (".v2-staging", ".v2-previous"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as folder:
                root = self.make_root(folder)
                (root / "docs" / name).mkdir()
                with self.assertRaisesRegex(SystemExit, "unfinished V2 recovery directory"):
                    verifier.verify_artifacts(root, self.SESSION)


if __name__ == "__main__":
    unittest.main()
