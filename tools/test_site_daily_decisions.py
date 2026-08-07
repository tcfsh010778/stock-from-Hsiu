from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_site


class SiteDailyDecisionsTest(unittest.TestCase):
    def test_missing_payload_is_safe_and_explains_the_gap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            payload = generate_site.load_daily_decisions_payload(Path(tmp) / "missing.json")

        html = generate_site.build_daily_decisions_panel(payload)
        self.assertIn('data-daily-decisions', html)
        self.assertIn("daily_decisions.json 尚未產生", html)
        self.assertIn("本區只呈現合約結果", html)
        self.assertIn("selection.html#sfz-baskets", html)

    def test_panel_prioritizes_entry_and_setup_without_recomputing_rules(self) -> None:
        payload = {
            "date": "2026-08-07",
            "updated_at": "2026-08-07T08:00:00+08:00",
            "action_counts": {"ENTRY_CANDIDATE": 1, "SETUP": 1, "WATCH": 1, "NO-GO": 1},
            "data_quality": {"state": "warning", "warnings": ["carybot_signals freshness is fallback_stale"]},
            "decisions": [
                {
                    "stock_id": "2330",
                    "name": "台積電",
                    "rank": 1,
                    "action_state": "ENTRY_CANDIDATE",
                    "traffic_light": {"reason": "交通燈與 B1 對齊"},
                    "evidence": {
                        "mda": {"basket": "已發動籃"},
                        "carybot": {"signal_type": "B1"},
                    },
                },
                {
                    "stock_id": "2454",
                    "name": "聯發科",
                    "rank": 2,
                    "action_state": "SETUP",
                    "traffic_light": {"reason": "等待 CaryBot 確認"},
                    "evidence": {"mda": {"basket": "盤整籃"}, "carybot": {"signal_type": "B2"}},
                },
                {
                    "stock_id": "6173",
                    "name": "信昌電",
                    "rank": 3,
                    "action_state": "NO-GO",
                    "traffic_light": {"reason": "過熱，先不做"},
                    "evidence": {"mda": {"basket": "過熱/風險"}},
                },
            ],
        }

        html = generate_site.build_daily_decisions_panel(payload)
        self.assertIn("2330 台積電", html)
        self.assertIn("2454 聯發科", html)
        self.assertIn("可進一步確認", html)
        self.assertIn("CaryBot B1", html)
        self.assertIn("資料品質提醒", html)
        self.assertIn("目前使用舊版 fallback", html)
        self.assertNotIn("6173 信昌電", html)

    def test_loader_discards_malformed_rows_and_keeps_contract_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "daily_decisions.json"
            path.write_text(
                json.dumps(
                    {
                        "date": "2026-08-07",
                        "decisions": [{"stock_id": "2330", "action_state": "WATCH"}, "bad-row", 3],
                        "action_counts": {"WATCH": 1},
                        "data_quality": {"state": "ok"},
                    }
                ),
                encoding="utf-8",
            )
            payload = generate_site.load_daily_decisions_payload(path)

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["decisions"][0]["stock_id"], "2330")
        self.assertEqual(payload["data_quality"]["state"], "ok")

    def test_official_risk_is_visible_in_home_queue_and_detail_badge(self) -> None:
        payload = {
            "date": "2026-08-07",
            "decisions": [{
                "stock_id": "2330",
                "action_state": "NO-GO",
                "traffic_light": {"reason": "官方處置覆蓋策略進場"},
                "evidence": {"market_risk": {"risk_level": "disposition", "source_state": "complete", "reasons": ["處置期間"]}},
            }],
            "action_counts": {"NO-GO": 1},
            "data_quality": {"state": "ok", "warnings": []},
        }
        html = generate_site.build_daily_decisions_panel(payload)
        self.assertIn("官方處置", html)
        self.assertIn('data-market-risk="disposition"', html)
        badge = generate_site.build_daily_decision_badge("2330", payload)
        self.assertIn("決策：先不做", badge)
        self.assertIn("官方處置", badge)

    def test_nav_and_history_have_single_analysis_entry(self) -> None:
        nav = generate_site.nav_html("backtest")
        self.assertNotIn('href="backtest_dashboard.html" class="tab"', nav)
        self.assertIn('href="history.html" class="tab active"', nav)
        history = generate_site.build_history_combined_page([])
        self.assertIn('data-tab="backtest"', history)
        self.assertIn('data-tab="reports"', history)
        self.assertIn('data-backtest-dashboard', history)


if __name__ == "__main__":
    unittest.main()
