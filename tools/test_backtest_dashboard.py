from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_site


def _load_backtest_module():
    try:
        return importlib.import_module("backtest_dashboard")
    except ModuleNotFoundError as exc:
        raise AssertionError("backtest_dashboard.py module is missing") from exc


def _require_attr(obj, name: str):
    if not hasattr(obj, name):
        raise AssertionError(f"{obj!r} is missing required attribute {name}")
    return getattr(obj, name)


class BacktestDashboardTest(unittest.TestCase):
    def test_taiwan_cost_model_round_trip_uses_task3_rates(self) -> None:
        mod = _load_backtest_module()

        cost = mod.DEFAULT_COST_MODEL

        self.assertEqual(cost["buy_fee_rate"], 0.0006)
        self.assertEqual(cost["sell_fee_rate"], 0.0006)
        self.assertEqual(cost["sell_tax_rate"], 0.003)
        self.assertEqual(cost["slippage_rate"], 0.0002)
        self.assertAlmostEqual(mod.round_trip_cost_rate(cost), 0.0044, places=8)
        self.assertAlmostEqual(mod.apply_trade_cost(0.10, cost), 0.0956, places=8)

    def test_build_payload_standardizes_trade_rows_and_metrics(self) -> None:
        mod = _load_backtest_module()
        with tempfile.TemporaryDirectory() as tmp:
            source_dir = Path(tmp)
            (source_dir / "sfz_ma_trailing_after_activation_detail.csv").write_text(
                "\n".join(
                    [
                        "year,stock,signal_date,basket_type,ma_line,activation_pct,exit_date,ret",
                        "2024,2330,2024-01-03,ALL,MA20,10,2024-01-31,10",
                        "2024,2454,2024-02-05,ALL,MA20,10,2024-02-29,-5",
                        "2024,2317,2024-03-04,ALL,MA20,10,2024-03-29,8",
                    ]
                ),
                encoding="utf-8",
            )

            payload = mod.build_payload(
                source_dir=source_dir,
                now=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            )

        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["cost_model"]["round_trip_rate"], 0.0044)
        self.assertGreaterEqual(len(payload["strategies"]), 1)
        strategy = payload["strategies"][0]
        self.assertIn("strategy_name", strategy)
        self.assertEqual(set(strategy["period"].keys()), {"start", "end"})
        self.assertEqual(
            set(strategy["metrics"].keys()),
            {
                "annual_return",
                "sharpe_ratio",
                "max_drawdown",
                "win_rate",
                "total_trades",
                "profit_factor",
            },
        )
        self.assertIn("2024-01", strategy["monthly_returns"])
        self.assertGreaterEqual(len(strategy["equity_curve"]), 4)
        self.assertIn("slippage_rate", strategy["parameters"])

    def test_event_study_uses_monthly_average_returns_not_event_compounding(self) -> None:
        mod = _load_backtest_module()
        trades = [
            {"date": f"2024-01-{day:02d}", "return": 0.10, "stock_id": str(day)}
            for day in range(1, 11)
        ] + [
            {"date": f"2024-02-{day:02d}", "return": -0.05, "stock_id": str(day)}
            for day in range(1, 11)
        ]

        strategy = mod.build_strategy_from_trades(
            "CaryBot_event_test",
            trades,
            cost_model=mod.DEFAULT_COST_MODEL,
            category="event_study",
        )

        self.assertIsNotNone(strategy)
        self.assertAlmostEqual(strategy["monthly_returns"]["2024-01"], 0.0956, places=4)
        self.assertAlmostEqual(strategy["monthly_returns"]["2024-02"], -0.0544, places=4)
        self.assertLess(strategy["metrics"]["annual_return"], 5)

    def test_signal_level_dashboard_curves_use_monthly_average_returns(self) -> None:
        mod = _load_backtest_module()
        trades = [
            {"date": f"2024-01-{day:02d}", "return": 0.10, "stock_id": str(day)}
            for day in range(1, 11)
        ]

        strategy = mod.build_strategy_from_trades(
            "SFZ_signal_group",
            trades,
            cost_model=mod.DEFAULT_COST_MODEL,
            category="trade_simulation",
        )

        self.assertIsNotNone(strategy)
        self.assertAlmostEqual(strategy["monthly_returns"]["2024-01"], 0.0956, places=4)

    def test_generate_site_renders_dashboard_ui_from_standard_json(self) -> None:
        build_page = _require_attr(generate_site, "build_backtest_dashboard_page")
        payload = {
            "schema_version": 1,
            "updated_at": "2026-06-01T20:00:00+08:00",
            "cost_model": {
                "buy_fee_rate": 0.0006,
                "sell_fee_rate": 0.0006,
                "sell_tax_rate": 0.003,
                "slippage_rate": 0.0002,
                "round_trip_rate": 0.0044,
            },
            "strategies": [
                {
                    "strategy_name": "SFZ_Top20",
                    "period": {"start": "2024-01-03", "end": "2024-03-29"},
                    "metrics": {
                        "annual_return": 0.18,
                        "sharpe_ratio": 1.25,
                        "max_drawdown": -0.08,
                        "win_rate": 0.58,
                        "total_trades": 3,
                        "profit_factor": 1.8,
                    },
                    "monthly_returns": {"2024-01": 0.03, "2024-02": -0.02},
                    "equity_curve": [["2024-01-03", 1.0], ["2024-03-29", 1.18]],
                    "parameters": {"entry_rule": "SFZ", "slippage_rate": 0.0002},
                    "source": {"file": "unit-test.csv"},
                }
            ],
        }

        html = build_page(payload=payload)

        self.assertIn("data-backtest-dashboard", html)
        self.assertIn("backtestDashboardData", html)
        self.assertIn("Chart.js", html)
        self.assertIn("new Chart", html)
        self.assertIn("data-sort-key=\"sharpe_ratio\"", html)
        self.assertIn("data-monthly-heatmap", html)
        self.assertIn("0.44%", html)
        self.assertIn("SFZ_Top20", html)

    def test_generate_site_publishes_backtest_json_and_nav_page(self) -> None:
        self.assertTrue(hasattr(generate_site, "BACKTEST_DASHBOARD_PATH"))
        self.assertIn(generate_site.BACKTEST_DASHBOARD_PATH, generate_site.PUBLIC_DATA_FILES)

        html = generate_site.nav_html("backtest")

        self.assertIn("backtest_dashboard.html", html)

    def test_workflow_runs_backtest_json_before_static_site_generation(self) -> None:
        workflow = Path(".github/workflows/daily_update.yml").read_text(encoding="utf-8")

        self.assertIn("python backtest_dashboard.py", workflow)
        self.assertLess(workflow.find("python backtest_dashboard.py"), workflow.find("python generate_site.py"))


if __name__ == "__main__":
    unittest.main()
