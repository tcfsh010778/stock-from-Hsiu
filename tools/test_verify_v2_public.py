from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_v2_public import verify_fixed_stop, verify_price_freshness, verify_technical_evidence


class VerifyV2PublicTest(unittest.TestCase):
    def test_fixed_stop_is_derived_from_current_reference_price(self) -> None:
        stop = verify_fixed_stop(
            {
                "method": "fixed_percent_from_latest_close",
                "reference_date": "2026-08-10",
                "reference_price": 31.0,
                "stop_loss_pct": 15.0,
                "stop_price": 26.35,
            },
            {
                "data_date": "2026-08-10",
                "series": [{"date": "2026-08-10", "close": 31.0}],
            },
            "2026-08-10",
        )

        self.assertEqual(stop, 26.35)

    def test_fixed_stop_rejects_stale_reference_date(self) -> None:
        with self.assertRaisesRegex(AssertionError, "reference date mismatch"):
            verify_fixed_stop(
                {
                    "method": "fixed_percent_from_latest_close",
                    "reference_date": "2026-08-07",
                    "reference_price": 30.25,
                    "stop_loss_pct": 15.0,
                    "stop_price": 25.7125,
                },
                {
                    "data_date": "2026-08-10",
                    "series": [{"date": "2026-08-10", "close": 31.0}],
                },
                "2026-08-10",
            )

    def test_fixed_stop_rejects_value_not_derived_from_reference_price(self) -> None:
        with self.assertRaisesRegex(AssertionError, "15% stop mismatch"):
            verify_fixed_stop(
                {
                    "method": "fixed_percent_from_latest_close",
                    "reference_date": "2026-08-10",
                    "reference_price": 31.0,
                    "stop_loss_pct": 15.0,
                    "stop_price": 25.7125,
                },
                {
                    "data_date": "2026-08-10",
                    "series": [{"date": "2026-08-10", "close": 31.0}],
                },
                "2026-08-10",
            )

    def test_fixed_stop_rejects_stale_but_internally_consistent_price_pair(self) -> None:
        with self.assertRaisesRegex(AssertionError, "reference price mismatch"):
            verify_fixed_stop(
                {
                    "method": "fixed_percent_from_latest_close",
                    "reference_date": "2026-08-10",
                    "reference_price": 30.25,
                    "stop_loss_pct": 15.0,
                    "stop_price": 25.7125,
                },
                {
                    "data_date": "2026-08-10",
                    "series": [{"date": "2026-08-10", "close": 31.0}],
                },
                "2026-08-10",
            )

    def test_accepts_matching_fresh_official_price_date(self) -> None:
        date = verify_price_freshness(
            {"price_refresh_status": "fresh", "price_data_date": "2026-08-07"},
            {"status": "fresh", "latest_data_date": "2026-08-07"},
        )

        self.assertEqual(date, "2026-08-07")

    def test_rejects_manifest_price_date_mismatch(self) -> None:
        with self.assertRaisesRegex(AssertionError, "price date mismatch"):
            verify_price_freshness(
                {"price_refresh_status": "fresh", "price_data_date": "2026-06-26"},
                {"status": "fresh", "latest_data_date": "2026-08-07"},
            )

    def test_accepts_explicit_auxiliary_technical_cards(self) -> None:
        ids = ["rsi_14", "macd_12_26_9", "bollinger_20_2", "volume_vs_avg_3", "volume_vs_avg_5", "volume_vs_avg_10"]
        packet = {"technical_evidence": [{"indicator_id": indicator_id, "calculation_basis": "closed_bar_only", "evidence_role": "auxiliary_evidence_only", "value_status": "available"} for indicator_id in ids]}
        self.assertEqual(verify_technical_evidence(packet), set(ids))


if __name__ == "__main__":
    unittest.main()
