from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from generate_v2 import add_public_workbench, load_market_evidence, safe_decision, switch_navigation, trim_packet
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

    def test_fixed_stop_is_exactly_fifteen_percent(self):
        packet = {"timeframe": "daily", "series": [{"date": "2026-08-07", "close": 30.25}]}
        add_public_workbench(packet)
        self.assertEqual(packet["risk_control"]["stop_loss_pct"], 15.0)
        self.assertEqual(packet["risk_control"]["stop_price"], 25.7125)
        self.assertNotIn("target", packet["risk_control"])

    def test_market_evidence_discloses_missing_inputs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name in ("chips", "foreign_shareholding", "margin", "holding_shares"):
                (root / name).mkdir()
            (root / "chips" / "2330.csv").write_text(
                "date,stock_id,buy,name,sell\n2026-08-07,2330,2000,Foreign_Investor,500\n",
                encoding="utf-8",
            )
            result = load_market_evidence(root, "2330")
            self.assertEqual(result["institutional"][0]["foreign"], 1.5)
            self.assertEqual(set(result["gaps"]), {"foreign_ownership", "margin", "holdings"})


if __name__ == "__main__":
    unittest.main()
