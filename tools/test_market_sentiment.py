from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import market_sentiment


class MarketSentimentTest(unittest.TestCase):
    def test_taiex_ma_lights_score_all_green(self) -> None:
        closes = [
            {"date": f"2026-03-{day:02d}", "close": 100 + day}
            for day in range(1, 61)
        ]

        result = market_sentiment.compute_taiex_ma_indicator(closes)

        self.assertEqual(result["lights"], {"ma5": True, "ma20": True, "ma60": True})
        self.assertEqual(result["signal"], "bullish")
        self.assertEqual(result["score"], 30)

    def test_score_missing_inputs_are_neutral_and_bounded(self) -> None:
        indicators = {
            "taiex_ma": {"score": 30, "weight": 30},
            "margin_weekly": {"score": 7.5, "weight": 15, "available": False},
            "short_weekly": {"score": 15, "weight": 15},
            "foreign_5d": {"score": 15, "weight": 15},
            "breadth": {"score": 5, "weight": 10, "available": False},
            "us_vix": {"score": 12, "weight": 15},
        }

        payload = market_sentiment.build_payload(indicators, source_status=[])

        self.assertGreaterEqual(payload["score"], 0)
        self.assertLessEqual(payload["score"], 100)
        self.assertEqual(payload["regime"], "bullish")
        self.assertIn("updated_at", payload)

    def test_write_market_sentiment_json_uses_expected_shape(self) -> None:
        indicators = {
            "taiex_ma": {"score": 0, "weight": 30, "signal": "bearish"},
            "margin_weekly": {"score": 7.5, "weight": 15, "available": False},
            "short_weekly": {"score": 7.5, "weight": 15, "available": False},
            "foreign_5d": {"score": 7.5, "weight": 15, "available": False},
            "breadth": {"score": 5, "weight": 10, "available": False},
            "us_vix": {"score": 0, "weight": 15, "signal": "fear"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "market_sentiment.json"
            payload = market_sentiment.write_payload(indicators, path, source_status=["unit-test"])
            saved = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(saved["score"], payload["score"])
        self.assertIn("taiex_ma", saved["indicators"])
        self.assertIn("us_vix", saved["indicators"])
        self.assertEqual(saved["source_status"], ["unit-test"])


if __name__ == "__main__":
    unittest.main()
