from __future__ import annotations

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_v2_public import MA_WINDOWS, verify_daily_packet_contract


def packet(available: int, *, stock_id: str = "2330", expected: str = "2026-09-03") -> dict:
    returned = min(available, 240)
    offset = available - returned
    rows = []
    first = date.fromisoformat(expected) - timedelta(days=returned)
    for index in range(returned):
        row = {
            "date": (first + timedelta(days=index)).isoformat() if index < returned - 1 else expected,
            "volume": 1000,
            "adjustment_factor": 1.0,
        }
        for window in MA_WINDOWS:
            row[f"sma{window}"] = 100.0 if offset + index + 1 >= window else None
        rows.append(row)
    return {
        "stock_id": stock_id,
        "timeframe": "daily",
        "data_date": expected,
        "series": rows,
        "series_coverage": {"requested_bars": 240, "available_bars": available, "returned_bars": returned, "status": "available" if available >= 240 else "insufficient_history"},
        "price_adjustment": {
            "mode": "official_reference_ratio_back_adjusted_v1",
            "source": "TWSE TWT49U / TPEx exDailyQ official reference prices",
            "verified": True,
            "adjustment_as_of": expected,
            "volume_basis": "official_raw_shares",
        },
    }


class VerifyDailyArtifactsTest(unittest.TestCase):
    def test_accepts_exact_240_tail_and_warmup_availability(self) -> None:
        value = packet(300)
        verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")

    def test_rejects_wrong_returned_count(self) -> None:
        value = packet(120)
        value["series_coverage"]["returned_bars"] = 119
        with self.assertRaisesRegex(AssertionError, "coverage mismatch"):
            verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")

    def test_rejects_sma_before_and_after_warmup(self) -> None:
        value = packet(120)
        value["series"][4]["sma5"] = None
        with self.assertRaisesRegex(AssertionError, "missing after warmup"):
            verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")
        value = packet(120)
        value["series"][3]["sma240"] = 100
        with self.assertRaisesRegex(AssertionError, "exists before warmup"):
            verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")

    def test_rejects_unverified_basis_nonshare_volume_and_wrong_latest_date(self) -> None:
        value = packet(240)
        value["price_adjustment"]["verified"] = False
        with self.assertRaisesRegex(AssertionError, "price basis metadata mismatch"):
            verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")
        value = packet(240)
        value["price_adjustment"]["volume_basis"] = "lots"
        with self.assertRaisesRegex(AssertionError, "price basis metadata mismatch"):
            verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")
        value = packet(240)
        value["series"][-1]["date"] = "2026-09-02"
        with self.assertRaisesRegex(AssertionError, "official expected date"):
            verify_daily_packet_contract(value, "2026-09-03", stock_id="2330")


if __name__ == "__main__":
    unittest.main()
