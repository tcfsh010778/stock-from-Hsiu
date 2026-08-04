from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_site
import run_screener
import stock_rules


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

    def test_site_traffic_light_html_consumes_structured_rule_result(self) -> None:
        original_indicator = generate_site.indicator_snapshot
        original_sum_recent = generate_site._sum_recent
        original_evaluator = generate_site.evaluate_traffic_light
        captured = {}
        expected = {
            "state": "WATCH",
            "css_class": "watch",
            "light_class": "light-yellow",
            "icon_entity": "&#128993;",
            "headline": "WATCH · 等確認",
            "label": "黃燈",
            "reason": "結構化結果",
        }
        try:
            generate_site.indicator_snapshot = lambda _daily: {"wr": -40}
            generate_site._sum_recent = lambda *_args: 0

            def fake_evaluator(stock, tech, decision, indicator, chip_total):
                captured.update(
                    stock=stock,
                    tech=tech,
                    decision=decision,
                    indicator=indicator,
                    chip_total=chip_total,
                )
                return expected

            generate_site.evaluate_traffic_light = fake_evaluator
            html = generate_site.stock_traffic_light("2330", {"id": "2330"}, {}, {}, [], [])
        finally:
            generate_site.indicator_snapshot = original_indicator
            generate_site._sum_recent = original_sum_recent
            generate_site.evaluate_traffic_light = original_evaluator

        self.assertEqual(captured["indicator"], {"wr": -40})
        self.assertEqual(captured["chip_total"], 0)
        self.assertIn("WATCH · 等確認", html)
        self.assertIn("結構化結果", html)
        self.assertIn("light-yellow", html)
        self.assertIs(stock_rules.evaluate_traffic_light, original_evaluator)

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

    def test_market_sentiment_panel_renders_score_and_vix(self) -> None:
        payload = {
            "score": 68,
            "regime": "bullish",
            "updated_at": "2026-05-31T18:00:00+08:00",
            "indicators": {
                "taiex_ma": {"label": "TAIEX MA", "signal": "bullish", "display": "3/3"},
                "us_vix": {"label": "US VIX", "signal": "neutral", "display": "16.2"},
            },
        }

        html = generate_site.build_market_sentiment_panel(payload)

        self.assertIn("data-market-sentiment", html)
        self.assertIn("68", html)
        self.assertIn("US VIX", html)

    def test_publish_data_assets_copies_public_json_into_docs_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "data" / "carybot_signals.json"
            output = root / "docs"
            source.parent.mkdir()
            source.write_text('{"date":"2026-05-12"}\n', encoding="utf-8")

            original_output = generate_site.OUTPUT_DIR
            try:
                generate_site.OUTPUT_DIR = output
                published = generate_site.publish_data_assets([source])
            finally:
                generate_site.OUTPUT_DIR = original_output

            self.assertEqual(published, ["carybot_signals.json"])
            self.assertEqual((output / "data" / "carybot_signals.json").read_text(encoding="utf-8"), source.read_text(encoding="utf-8"))

    def test_freshness_manifest_is_a_public_data_asset(self) -> None:
        self.assertIn(generate_site.FRESHNESS_MANIFEST_PATH, generate_site.PUBLIC_DATA_FILES)

    def test_sfz_controls_enable_bullish_filter_when_market_score_is_bullish(self) -> None:
        payload = {
            "date": "2026-05-29",
            "count": 1,
            "default_limit": 20,
            "stocks": [{"rank": 1, "stock_id": "2330", "name": "TSMC", "score": 90}],
        }
        market = {"score": 68, "regime": "bullish"}

        html = generate_site.build_sfz_all_controls(payload, market)

        self.assertIn('data-market-bullish="1"', html)
        self.assertIn('data-bullish="1"', html)
        self.assertIn('<option value="yes">大盤偏多訊號</option>', html)
        self.assertNotIn("Task 1", html)

    def test_carybot_signal_loader_and_latest_priority(self) -> None:
        payload = {
            "date": "2026-05-12",
            "signals": [
                {"stock_id": "2330", "signal_type": "B2", "score": 75, "date": "2026-05-09"},
                {"stock_id": "2330", "signal_type": "B1", "score": 92, "date": "2026-05-12"},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "carybot_signals.json"
            path.write_text(json.dumps(payload), encoding="utf-8")

            loaded = generate_site.load_carybot_signals_payload(path)
            latest = generate_site.latest_carybot_signals_by_stock(loaded)

        self.assertEqual(loaded["date"], "2026-05-12")
        self.assertEqual(latest["2330"]["signal_type"], "B1")
        self.assertEqual(latest["2330"]["score"], 92)

    def test_sfz_controls_promote_and_label_carybot_confirmed_stocks(self) -> None:
        payload = {
            "date": "2026-05-29",
            "count": 2,
            "default_limit": 20,
            "stocks": [
                {"rank": 1, "stock_id": "2454", "name": "MTK", "score": 91},
                {"rank": 2, "stock_id": "2330", "name": "TSMC", "score": 90},
            ],
        }
        carybot = {
            "date": "2026-05-12",
            "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 95, "date": "2026-05-12"}],
        }

        html = generate_site.build_sfz_all_controls(payload, carybot_payload=carybot)

        self.assertIn('data-carybot="1"', html)
        self.assertIn("SFZ + CaryBot", html)
        self.assertIn("B1", html)
        self.assertLess(html.find("2330 TSMC"), html.find("2454 MTK"))

    def test_sfz_default_frontend_sort_keeps_carybot_confirmations_first(self) -> None:
        payload = {
            "date": "2026-05-29",
            "count": 2,
            "default_limit": 20,
            "stocks": [
                {"rank": 1, "stock_id": "2454", "name": "MTK", "score": 91},
                {"rank": 2, "stock_id": "2330", "name": "TSMC", "score": 90},
            ],
        }
        carybot = {
            "date": "2026-05-12",
            "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 95, "date": "2026-05-12"}],
        }

        html = generate_site.build_sfz_all_controls(payload, carybot_payload=carybot)

        self.assertIn("function carybotFirst", html)
        self.assertIn("return carybotFirst(a,b)||num(a.dataset.rank)-num(b.dataset.rank);", html)

    def test_carybot_history_panel_renders_recent_stock_history(self) -> None:
        carybot = {
            "date": "2026-05-12",
            "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 95, "date": "2026-05-12"}],
            "history": [
                {"stock_id": "2330", "signal_type": "B2", "score": 76, "date": "2026-05-09"},
                {"stock_id": "2454", "signal_type": "B1", "score": 91, "date": "2026-05-09"},
            ],
        }

        html = generate_site.build_carybot_signal_history_panel("2330", carybot)

        self.assertIn("data-carybot-history", html)
        self.assertIn("2026-05-12", html)
        self.assertIn("B1", html)
        self.assertIn("B2", html)
        self.assertNotIn("2454", html)

    def test_carybot_history_panel_dedupes_current_and_history_same_day(self) -> None:
        carybot = {
            "date": "2026-05-12",
            "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 95, "date": "2026-05-12", "source": "current"}],
            "history": [{"stock_id": "2330", "signal_type": "B1", "score": 95, "date": "2026-05-12", "source": "history"}],
        }

        rows = generate_site.carybot_signals_for_stock("2330", carybot)

        self.assertEqual(len(rows), 1)

    def test_carybot_history_panel_warns_when_preserved_json_is_stale(self) -> None:
        carybot = {
            "date": "2026-05-12",
            "signals": [{"stock_id": "2330", "signal_type": "B1", "score": 95, "date": "2026-05-12"}],
            "freshness": {
                "status": "fallback_stale",
                "data_date": "2026-05-12",
                "expected_data_date": "2026-08-04",
            },
        }

        html = generate_site.build_carybot_signal_history_panel("2330", carybot)

        self.assertIn('data-artifact-freshness="fallback_stale"', html)
        self.assertIn("請勿視為最新結果", html)


if __name__ == "__main__":
    unittest.main()
