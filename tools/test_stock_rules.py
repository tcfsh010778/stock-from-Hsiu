from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_site
import mda_universe_scan
import run_screener
import stock_rules


class StockRulesTest(unittest.TestCase):
    def test_all_callers_share_the_same_overheat_policy(self) -> None:
        stock = {"gain_6w": 206.53, "rsi_14": 84.3, "bband_pct": 93.2, "gain_3d": 8.5}

        self.assertTrue(stock_rules.is_overheated(stock))
        self.assertEqual(run_screener.is_overheated(stock), stock_rules.is_overheated(stock))
        self.assertEqual(generate_site.is_overheated_stock(stock), stock_rules.is_overheated(stock))
        self.assertEqual(
            mda_universe_scan.is_overheated(206.53, 84.3, 93.2, 8.5),
            stock_rules.is_overheated(stock),
        )
        self.assertEqual(run_screener.overheat_reasons(stock), stock_rules.overheat_reasons(stock))
        self.assertEqual(generate_site.overheat_reasons(stock), stock_rules.overheat_reasons(stock))
        self.assertIn("RSI 84.3", stock_rules.overheat_reason_text(stock))

    def test_holding_group_is_identical_for_collector_and_site(self) -> None:
        cases = {
            "1-999": "retail",
            "200,001-400,000": "middle",
            "400,001-600,000": "major",
            "more than 1,000,001": "major",
            "total": "other",
            "差異數調整（說明4）": "other",
        }

        for label, expected in cases.items():
            with self.subTest(label=label):
                self.assertEqual(stock_rules.holding_group(label), expected)
                self.assertEqual(mda_universe_scan.holding_group(label), expected)
                self.assertEqual(generate_site._holding_group(label), expected)

    def test_traffic_light_go_is_an_entry_ready_structured_state(self) -> None:
        state = stock_rules.evaluate_traffic_light(
            {"icon": "🟡", "gain_6w": 10, "score": 90},
            {"trend": "多方", "volume_price": "量增價漲", "close": 120, "ma20": 110, "ma60": 100},
            {"rr": 2.5, "rr_text": "1:2.5"},
            {"wr": -75, "k": 55, "macd_state": "買進", "kd_state": "強"},
            100,
        )

        self.assertEqual(state["state"], "GO")
        self.assertTrue(state["candidate"])
        self.assertTrue(state["armed"])
        self.assertTrue(state["entry"])
        self.assertFalse(state["exit"])

    def test_traffic_light_watch_can_be_armed_without_entry_trigger(self) -> None:
        state = stock_rules.evaluate_traffic_light(
            {"gain_6w": 10, "score": 70},
            {"trend": "多方", "volume_price": "量縮價漲", "close": 120, "ma20": 110, "ma60": 100},
            {"rr": 2.2, "rr_text": "1:2.2"},
            {"wr": -40, "k": 55, "macd_state": "買進", "kd_state": "強"},
            0,
        )

        self.assertEqual(state["state"], "WATCH")
        self.assertTrue(state["candidate"])
        self.assertTrue(state["armed"])
        self.assertFalse(state["entry"])
        self.assertFalse(state["exit"])

    def test_traffic_light_overheat_is_no_go_and_exit(self) -> None:
        state = stock_rules.evaluate_traffic_light(
            {"gain_3d": 21},
            {"trend": "多方", "volume_price": "量增價漲"},
            {"rr": 3, "rr_text": "1:3.0"},
            {"wr": -75, "k": 55, "macd_state": "買進", "kd_state": "強"},
            100,
        )

        self.assertEqual(state["state"], "NO-GO")
        self.assertFalse(state["candidate"])
        self.assertFalse(state["armed"])
        self.assertFalse(state["entry"])
        self.assertTrue(state["exit"])
        self.assertIn("forced_overheat", state["blockers"])


if __name__ == "__main__":
    unittest.main()
