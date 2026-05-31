from __future__ import annotations

import json
import sys
import tempfile
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

    def test_sfz_all_payload_loader_reads_json_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sfz_all.json"
            path.write_text(
                json.dumps({"date": "2026-05-29", "count": 1, "stocks": [{"stock_id": "2330"}]}),
                encoding="utf-8",
            )

            payload = generate_site.load_sfz_all_payload(path)

        self.assertEqual(payload["date"], "2026-05-29")
        self.assertEqual(payload["stocks"][0]["stock_id"], "2330")

    def test_sfz_all_controls_include_paging_filters_and_sort(self) -> None:
        payload = {
            "date": "2026-05-29",
            "count": 1,
            "default_limit": 20,
            "stocks": [{"rank": 1, "stock_id": "2330", "name": "TSMC", "score": 90}],
        }

        html = generate_site.build_sfz_all_controls(payload)

        self.assertIn("data-sfz-table", html)
        self.assertIn('id="sfzPageSize"', html)
        self.assertIn('id="sfzMarketCapFilter"', html)
        self.assertIn('id="sfzVolumeFilter"', html)
        self.assertIn('id="sfzCarybotFilter"', html)
        self.assertIn('id="sfzBullishFilter"', html)
        self.assertIn('id="sfzSort"', html)


if __name__ == "__main__":
    unittest.main()
