from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stock_v2_public.analysis.price_basis import PRICE_BASIS_MODE, price_basis_metadata, project_adjusted_rows


class PriceBasisTest(unittest.TestCase):
    def test_adjusts_only_rows_before_events_known_as_of(self) -> None:
        rows = [
            {"date": "2026-06-09", "stock_id": "2330", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
            {"date": "2026-06-10", "stock_id": "2330", "open": 102, "high": 103, "low": 100, "close": 102, "volume": 1100},
            {"date": "2026-06-11", "stock_id": "2330", "open": 98, "high": 100, "low": 97, "close": 99, "volume": 1200},
        ]
        actions = [
            {"date": "2026-06-11", "stock_id": "2330", "previous_close": 102, "reference_price": 98, "kind": "息"},
            {"date": "2026-09-11", "stock_id": "2330", "previous_close": 120, "reference_price": 115, "kind": "息"},
        ]
        projected = project_adjusted_rows(rows, actions, adjustment_as_of="2026-06-11")
        self.assertAlmostEqual(projected[0]["adjustment_factor"], 98 / 102)
        self.assertAlmostEqual(projected[1]["close"], 98)
        self.assertEqual(projected[2]["adjustment_factor"], 1)
        self.assertEqual(projected[2]["close"], projected[2]["raw_close"])
        self.assertEqual(projected[0]["volume"], 1000)
        self.assertEqual(projected[0]["raw_volume"], 1000)

    def test_metadata_names_exact_basis_and_raw_share_volume(self) -> None:
        metadata = price_basis_metadata(
            stock_id="2330", adjustment_as_of="2026-06-11",
            actions=[{"date": "2026-06-11", "stock_id": "2330", "previous_close": 102, "reference_price": 98}],
        )
        self.assertEqual(metadata["mode"], PRICE_BASIS_MODE)
        self.assertEqual(metadata["volume_basis"], "official_raw_shares")
        self.assertTrue(metadata["verified"])

    def test_rejects_duplicate_actions(self) -> None:
        action = {"date": "2026-06-11", "stock_id": "2330", "previous_close": 102, "reference_price": 98}
        with self.assertRaisesRegex(ValueError, "duplicate corporate action"):
            project_adjusted_rows([], [action, action], adjustment_as_of="2026-06-11")

    def test_rejects_invalid_or_future_raw_rows(self) -> None:
        invalid = {"date": "2026-06-10", "stock_id": "2330", "open": 100, "high": 98, "low": 99, "close": 101, "volume": 1000}
        with self.assertRaisesRegex(ValueError, "geometry"):
            project_adjusted_rows([invalid], [], adjustment_as_of="2026-06-11")
        future = {**invalid, "date": "2026-06-12", "high": 102}
        with self.assertRaisesRegex(ValueError, "later than adjustment_as_of"):
            project_adjusted_rows([future], [], adjustment_as_of="2026-06-11")


if __name__ == "__main__":
    unittest.main()
