from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import carybot_signals
import data_contract


class CarybotSignalsTest(unittest.TestCase):
    def test_normalize_signal_row_maps_buy_markers_to_b1_b2_schema(self) -> None:
        ai_buy = carybot_signals.normalize_signal_row(
            {
                "stock": "2330",
                "date": "2026-05-08",
                "signal_type": "AI_Buy",
                "QZ": "1.25",
                "QTYR": "4.5",
                "VAM20": "73.2",
                "carybot_phase": "brown_healthy_pullback",
            },
            source="unit-v50",
            is_current=True,
        )
        prebuy = carybot_signals.normalize_signal_row(
            {"stock": "2454", "date": "2026-05-09", "signal_type": "PreBuy"},
            source="unit-v50",
        )

        self.assertEqual(ai_buy["stock_id"], "2330")
        self.assertEqual(ai_buy["signal_type"], "B1")
        self.assertEqual(ai_buy["raw_signal_type"], "AI_Buy")
        self.assertEqual(ai_buy["score"], 85)
        self.assertEqual(ai_buy["thermometer_score"], 85)
        self.assertTrue(ai_buy["is_current"])
        self.assertEqual(ai_buy["metrics"]["QZ"], 1.25)
        self.assertEqual(prebuy["signal_type"], "B2")
        self.assertEqual(prebuy["score"], 75)

    def test_build_payload_prefers_v51_current_signals_and_keeps_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source_dir = Path(tmp)
            self._write_csv(
                source_dir / "carybot_signal_master_v50.csv",
                [
                    {
                        "stock": "2330",
                        "date": "2026-04-27",
                        "signal_type": "PreBuy",
                        "marker_side": "buy",
                        "QZ": "2",
                    }
                ],
            )
            self._write_csv(
                source_dir / "carybot_daily_ai_buy_v51.csv",
                [
                    {
                        "stock": "2330",
                        "stock_name": "TSMC",
                        "run_date": "2026-05-13",
                        "data_date": "2026-05-12",
                        "signal_type": "AI_Buy_like_v51",
                        "quality_score": "95",
                        "candidate_pass": "True",
                        "recommendation_rank": "1",
                    }
                ],
            )
            self._write_csv(
                source_dir / "carybot_daily_ai_buy_v51_history.csv",
                [
                    {
                        "stock": "2454",
                        "stock_name": "MTK",
                        "run_date": "2026-05-10",
                        "data_date": "2026-05-09",
                        "signal_type": "AI_Buy_like_v51",
                        "quality_score": "91",
                    }
                ],
            )

            payload = carybot_signals.build_payload(source_dir=source_dir)

        self.assertEqual(payload["date"], "2026-05-12")
        self.assertEqual(payload["signals"][0]["stock_id"], "2330")
        self.assertEqual(payload["signals"][0]["signal_type"], "B1")
        self.assertEqual(payload["signals"][0]["score"], 95)
        self.assertTrue(payload["signals"][0]["is_current"])
        self.assertTrue(any(row["stock_id"] == "2454" for row in payload["history"]))
        self.assertIn("carybot_daily_ai_buy_v51.csv", payload["sources"]["current"])

    def test_write_payload_preserves_existing_json_when_source_files_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "carybot_signals.json"
            source_dir = Path(tmp) / "missing"
            output_path.write_text(
                json.dumps(
                    {
                        "date": "2026-05-12",
                        "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 88}],
                    }
                ),
                encoding="utf-8",
            )

            payload = carybot_signals.write_payload(output_path=output_path, source_dir=source_dir)

        self.assertEqual(payload["signals"][0]["stock_id"], "2330")
        self.assertEqual(payload["sources"]["mode"], "preserved_existing_json")

    def test_preserved_payload_exposes_stale_fallback_and_exact_payload_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_path = root / "carybot_signals.json"
            manifest_path = root / "freshness_manifest.json"
            output_path.write_text(
                json.dumps(
                    {
                        "date": "2026-05-12",
                        "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 88}],
                    }
                ),
                encoding="utf-8",
            )
            now = datetime(2026, 8, 4, 20, 0, tzinfo=timezone(timedelta(hours=8)))

            payload = carybot_signals.write_payload(
                output_path=output_path,
                source_dir=root / "missing",
                now=now,
                manifest_path=manifest_path,
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))["artifacts"][
                "carybot_signals:carybot_preserved_json"
            ]
            artifact_digest = data_contract.sha256_bytes(output_path.read_bytes())

        self.assertEqual(payload["freshness"]["status"], "fallback_stale")
        self.assertEqual(payload["freshness"]["source_tier"], "fallback")
        self.assertTrue(payload["freshness"]["fallback"]["used"])
        self.assertGreater(payload["freshness"]["age_calendar_days"], 3)
        self.assertEqual(manifest["sha256"], artifact_digest)

    def test_local_payload_is_primary_and_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_dir = root / "source"
            source_dir.mkdir()
            self._write_csv(
                source_dir / carybot_signals.CURRENT_SOURCE_FILE,
                [
                    {
                        "stock": "2330",
                        "data_date": "2026-08-04",
                        "signal_type": "AI_Buy",
                        "candidate_pass": "True",
                    }
                ],
            )
            now = datetime(2026, 8, 4, 20, 0, tzinfo=timezone(timedelta(hours=8)))
            payload = carybot_signals.write_payload(
                output_path=root / "carybot_signals.json",
                source_dir=source_dir,
                now=now,
                manifest_path=root / "freshness_manifest.json",
            )

        self.assertEqual(payload["freshness"]["status"], "fresh")
        self.assertEqual(payload["freshness"]["source_id"], "carybot_local_csv_derived")
        self.assertFalse(payload["freshness"]["fallback"]["used"])

    @staticmethod
    def _write_csv(path: Path, rows: list[dict]) -> None:
        fieldnames = sorted({key for row in rows for key in row})
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
