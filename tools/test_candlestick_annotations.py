from __future__ import annotations

import json
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import talib
from jsonschema import Draft202012Validator

from stock_v2_public.analysis.candlesticks import build_candlestick_event_envelope, talib_pattern_functions
from stock_v2_public.analysis.core import prepare_ohlcv, with_indicators
from stock_v2_public.analysis.talib_registry import compute_talib_features, talib_function_catalog
from stock_v2_public.site import V2_JS


ROOT = Path(__file__).resolve().parents[1]


def synthetic_ohlcv() -> pd.DataFrame:
    count = 220
    index = np.arange(count, dtype=float)
    close = 40 + index * 0.02 + np.sin(index / 7) * 2
    open_ = close + np.sin(index / 5) * 0.35
    high = np.maximum(open_, close) + 0.8
    low = np.minimum(open_, close) - 0.8
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-02", periods=count),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": 1_000_000 + index * 100,
        }
    )


class CandlestickAnnotationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.frame = synthetic_ohlcv()
        cls.schema = json.loads((ROOT / "schemas" / "candlestick_pattern_event.schema.json").read_text(encoding="utf-8"))

    def envelope(self, frame: pd.DataFrame | None = None, *, public_only: bool = True) -> dict:
        return build_candlestick_event_envelope(
            with_indicators(prepare_ohlcv(frame if frame is not None else self.frame)),
            symbol="2353",
            market="listed",
            price_basis="raw",
            public_only=public_only,
        )

    def test_all_talib_functions_are_discoverable_and_cdl_catalog_is_complete(self):
        self.assertEqual({item["function"] for item in talib_function_catalog()}, set(talib.get_functions()))
        self.assertEqual(len(talib_pattern_functions()), 61)
        result = compute_talib_features(self.frame, ["RSI", "MACD", "CDLHAMMER"])
        self.assertEqual(list(result), ["RSI", "MACD", "CDLHAMMER"])

    def test_public_event_contract_is_daily_closed_bar_only(self):
        envelope = self.envelope()
        Draft202012Validator(self.schema).validate(envelope)
        self.assertEqual(envelope["timeframe"], "1d")
        self.assertEqual(envelope["talib"]["pattern_function_count"], 61)
        self.assertTrue(all(event["bar_status"] == "closed" for event in envelope["events"]))

    def test_past_events_do_not_change_when_future_bars_are_appended(self):
        prefix = self.frame.iloc[:180].copy()
        cutoff = str(prefix.iloc[-1]["date"].date())
        expected = self.envelope(prefix)["events"]
        actual = [event for event in self.envelope()["events"] if event["bar_date"] <= cutoff]
        self.assertEqual(expected, actual)

    def test_ui_uses_neutral_grouped_markers(self):
        self.assertIn("createSeriesMarkers", V2_JS)
        self.assertIn('shape:"circle"', V2_JS)
        self.assertNotIn("arrowUp", V2_JS)
        self.assertNotIn("arrowDown", V2_JS)
        self.assertIn("groupedAnnotations", V2_JS)
        self.assertIn("display_priority||0)>=55", V2_JS)

    def test_annotation_copy_has_no_trade_instruction_language(self):
        start = V2_JS.index("function annotationSummary")
        end = V2_JS.index("function markerColor", start)
        copy = V2_JS[start:end]
        for phrase in ("買進", "賣出", "進場", "出場", "加碼", "減碼", "停損", "停利", "目標價", "勝率", "成功機率"):
            self.assertNotIn(phrase, copy)


if __name__ == "__main__":
    unittest.main()
