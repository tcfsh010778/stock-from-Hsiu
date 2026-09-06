from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_v2_public import verify_daily_history, verify_fixed_stop, verify_price_freshness, verify_technical_evidence


def daily_packet(count=240, available=None):
    available = count if available is None else available
    rows = []
    for index in range(count):
        close = float(index + 1)
        row = {"date": f"2026-{(index // 28) + 1:02d}-{(index % 28) + 1:02d}", "close": close, "volume": 1000.0, "adjustment_factor": 1.0}
        source_position = available - count + index + 1
        for window in (5, 20, 60, 120, 240):
            row[f"sma{window}"] = close if source_position >= window else None
        rows.append(row)
    # The verifier recomputes current values from the serialized 240 bars.
    for window in (5, 20, 60, 120, 240):
        if count >= window:
            rows[-1][f"sma{window}"] = sum(row["close"] for row in rows[-window:]) / window
    return {"timeframe": "daily", "data_date": rows[-1]["date"], "series": rows, "series_coverage": {"requested_bars": 240, "available_bars": available, "returned_bars": count, "status": "available" if available >= 240 else "insufficient_history"}, "price_adjustment": {"mode": "finmind_raw_reconciled_reference_ratio_back_adjusted_v1", "verified": True, "volume_basis": "finmind_raw_shares"}}


class VerifyV2PublicTest(unittest.TestCase):
    def test_daily_history_accepts_full_warmup_and_current_mas(self) -> None:
        self.assertEqual(verify_daily_history(daily_packet(240, available=520))["returned_bars"], 240)

    def test_daily_history_accepts_240_239_and_1_available_bars(self) -> None:
        for count in (240, 239, 1):
            result = verify_daily_history(daily_packet(count))
            self.assertEqual(result["returned_bars"], count)

    def test_daily_history_rejects_wrong_warmup_nullability(self) -> None:
        packet = daily_packet(240)
        packet["series"][0]["sma240"] = None
        packet["series"][0]["sma5"] = 1.0
        with self.assertRaisesRegex(AssertionError, "must be null"):
            verify_daily_history(packet)
        packet = daily_packet(240, available=520)
        packet["series"][0]["sma240"] = None
        with self.assertRaisesRegex(AssertionError, "missing after"):
            verify_daily_history(packet)

    def test_daily_history_rejects_legacy_basis_and_wrong_current_ma(self) -> None:
        packet = daily_packet(240, available=520)
        packet["price_adjustment"]["mode"] = "legacy_raw_v0"
        with self.assertRaisesRegex(AssertionError, "verified release mode"):
            verify_daily_history(packet)
        packet = daily_packet(240, available=520)
        packet["series"][-1]["sma240"] += 1
        with self.assertRaisesRegex(AssertionError, "current SMA240 mismatch"):
            verify_daily_history(packet)
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
