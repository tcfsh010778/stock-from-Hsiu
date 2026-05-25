from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_site
import run_screener


class PR3LogicTest(unittest.TestCase):
    def test_run_screener_forces_overheated_before_original_basket(self) -> None:
        stock = {
            "stock_id": "6173",
            "name": "信昌電",
            "basket": "已發動籃",
            "gain_6w": 206.53,
            "gain_3d": 8.5,
            "rsi": 84.3,
            "bb_pct": 93.2,
        }

        self.assertTrue(run_screener.is_overheated(stock))
        self.assertEqual(run_screener.stock_status(stock), ("🔴", "過熱/風險"))
        reason = run_screener.overheat_reason_text(stock)
        self.assertIn("強制過熱排除", reason)
        self.assertIn("近6週 +206.53%", reason)
        self.assertIn("RSI 84.3", reason)

    def test_site_overheat_guard_includes_three_day_gain(self) -> None:
        stock = {"id": "9999", "gain_6w": "12.0%", "rsi": "62.0", "bband_pct": "86.0%", "gain_3d": "21.4%"}

        self.assertTrue(generate_site.is_overheated_stock(stock))
        reasons = generate_site.overheat_reasons(stock)
        self.assertTrue(any("近3日 +21.40%" in reason for reason in reasons))
        self.assertEqual(generate_site.classify_basket({**stock, "icon": "🟡", "score": 90}), "risk")

    def test_rr_warning_banner_uses_pr3_copy(self) -> None:
        html = generate_site.rr_warning_bar({"rr": 0.9969, "rr_text": "1:1.0"})

        self.assertIn('class="warning-banner"', html)
        self.assertIn("R:R = 1:1.0", html)
        self.assertIn("低於建議門檻 1.5", html)

    def test_legacy_score_values_are_normalized_to_0_100_scale(self) -> None:
        cases = {
            180.0: 90.0,
            150.0: 75.0,
            190.0: 95.0,
            170.0: 85.0,
            130.0: 65.0,
            110.0: 55.0,
            160.0: 80.0,
            120.0: 60.0,
        }

        for old, expected in cases.items():
            with self.subTest(old=old):
                self.assertEqual(generate_site.normalize_score_value(old), expected)


if __name__ == "__main__":
    unittest.main()
