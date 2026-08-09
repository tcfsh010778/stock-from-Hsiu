from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from generate_v2 import analyze_stock_task, safe_decision, switch_navigation, trim_packet
from stock_v2_public.site import stock_redirect_html


class PublicV2GenerationTests(unittest.TestCase):
    def test_uncovered_stock_is_not_ai_invented(self):
        decision = safe_decision("9999", None)
        self.assertEqual(decision["action_state"], "UNRATED")
        self.assertIn("不得由 AI 補寫", decision["blockers"][0])

    def test_packet_series_is_bounded(self):
        packet = {"timeframe": "daily", "series": list(range(300)), "patterns": list(range(30)), "trendlines": list(range(10)), "support_resistance": list(range(20))}
        trim_packet(packet)
        self.assertEqual(len(packet["series"]), 120)
        self.assertEqual(len(packet["patterns"]), 24)
        self.assertEqual(len(packet["trendlines"]), 8)
        self.assertEqual(len(packet["support_resistance"]), 12)

    def test_navigation_switch_is_scoped_and_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "index.html"
            path.write_text('<a href="stocks/2353.html">A</a><a href="history.html">H</a>', encoding="utf-8")
            self.assertEqual(switch_navigation(path, {"2353"}), 1)
            self.assertEqual(switch_navigation(path, {"2353"}), 0)
            self.assertIn('href="v2/stocks/2353.html"', path.read_text(encoding="utf-8"))

    def test_redirect_keeps_legacy_page_separate(self):
        page = stock_redirect_html("2353")
        self.assertIn("../stock.html?id=2353", page)
        self.assertNotIn("OPENAI_API_KEY", page)

    def test_stale_price_file_is_excluded_before_analysis(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "2353.csv"
            rows = ["date,open,high,low,close,volume"]
            rows.extend(f"2026-07-{day:02d},10,11,9,10,1000" for day in range(1, 31))
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            _, _, packets, error = analyze_stock_task(
                (
                    "2353",
                    "Acer",
                    str(path),
                    safe_decision("2353", None),
                    "fresh",
                    "2026-08-07",
                    [],
                    False,
                )
            )
            self.assertIsNone(packets)
            self.assertIn("stale OHLCV", error)


if __name__ == "__main__":
    unittest.main()
