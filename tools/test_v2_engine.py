from __future__ import annotations

import json
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from jsonschema import Draft202012Validator

from stock_v2_public.analysis.core import prepare_ohlcv, with_indicators
from stock_v2_public.analysis.engine import analyze_multi_timeframe, analyze_ohlcv, stable_json
from stock_v2_public.analysis.swings import detect_swings
from stock_v2_public.analysis.trendlines import detect_trendlines


def synthetic_ohlcv() -> pd.DataFrame:
    count = 900
    index = np.arange(count, dtype=float)
    dates = pd.bdate_range("2023-01-02", periods=count)
    trend = 42.0 + index * 0.035
    cycle = np.sin(index * 2 * np.pi / 30.0) * 2.4
    secondary = np.sin(index * 2 * np.pi / 9.0) * 0.35
    close = trend + cycle + secondary
    open_ = close + np.sin(index * 2 * np.pi / 7.0) * 0.25
    high = np.maximum(open_, close) + 0.8 + np.abs(np.sin(index / 5.0)) * 0.2
    low = np.minimum(open_, close) - 0.8 - np.abs(np.cos(index / 6.0)) * 0.2
    volume = 1_000_000 + (np.sin(index * 2 * np.pi / 20.0) + 1.2) * 180_000
    return pd.DataFrame({"date": dates, "open": open_.round(2), "high": high.round(2), "low": low.round(2), "close": close.round(2), "volume": volume.round(0)})


class V2EngineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frame = synthetic_ohlcv()
        cls.schema = json.loads((Path(__file__).resolve().parents[1] / "schemas" / "technical_pattern_packet.schema.json").read_text(encoding="utf-8"))

    def test_deterministic_and_preserves_action_state(self):
        kwargs = {"stock_id": "2353", "decision": {"action_state": "SETUP"}, "freshness": {"status": "fresh"}}
        first = analyze_ohlcv(self.frame, **kwargs)
        second = analyze_ohlcv(self.frame, **kwargs)
        self.assertEqual(stable_json(first), stable_json(second))
        self.assertEqual(first["decision"]["action_state"], "SETUP")
        Draft202012Validator(self.schema).validate(first)

    def test_multitimeframe_and_no_future_swing(self):
        packets = analyze_multi_timeframe(self.frame, stock_id="2353", price_adjustment="none")
        self.assertEqual([p["timeframe"] for p in packets], ["daily", "weekly", "monthly"])
        frame = with_indicators(prepare_ohlcv(self.frame))
        swings = detect_swings(frame, "daily")
        self.assertTrue(swings)
        self.assertTrue(all(swing["confirmed_index"] > swing["index"] for swing in swings))
        self.assertTrue(all(pd.Timestamp(swing["confirmed_at"]) > pd.Timestamp(swing["date"]) for swing in swings))

    def test_confirmed_trendlines_need_third_touch_and_use_price_coordinates(self):
        frame = with_indicators(prepare_ohlcv(self.frame))
        lines = detect_trendlines(frame, detect_swings(frame, "daily"), "daily")
        self.assertTrue(lines)
        self.assertTrue(all(line["touch_count"] >= 3 for line in lines if line["status"] == "confirmed"))
        self.assertTrue(all(set(point) == {"date", "price"} for line in lines for point in line["anchors"]))
        self.assertNotIn('"x"', stable_json(lines))
        self.assertNotIn('"y"', stable_json(lines))


if __name__ == "__main__":
    unittest.main()
