from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generate_site
import mda_full_market_refresh as refresh
import refresh_prices


class Phase4APipelineTest(unittest.TestCase):
    def test_fetch_finmind_bulk_retries_transient_http_error(self) -> None:
        class FakeResponse:
            def __init__(self, payload: dict | None = None, error: Exception | None = None) -> None:
                self._payload = payload or {}
                self._error = error
                self.status_code = 200

            def raise_for_status(self) -> None:
                if self._error:
                    raise self._error

            def json(self) -> dict:
                return self._payload

        responses = [
            FakeResponse(error=RuntimeError("HTTP 400")),
            FakeResponse(error=RuntimeError("HTTP 400")),
            FakeResponse({"status": "200", "msg": "success", "data": [{"date": "2026-01-09"}]}),
        ]

        with patch.object(refresh, "load_finmind_token", return_value="token"), patch.object(
            refresh.requests, "get", side_effect=responses
        ) as get_mock, patch.object(refresh, "_sleep", create=True):
            rows = refresh.fetch_finmind_bulk("TaiwanStockHoldingSharesPer", "2026-01-09")

        self.assertEqual(rows, [{"date": "2026-01-09"}])
        self.assertEqual(get_mock.call_count, 3)

    def test_weekly_holding_failure_is_logged_and_cached_data_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            holding_dir = tmp_path / "holding_shares"
            log_dir = tmp_path / "logs"
            holding_dir.mkdir()
            with (holding_dir / "2330.csv").open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=["date", "stock_id", "HoldingSharesLevel", "people", "percent", "unit"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "date": "2026-01-02",
                        "stock_id": "2330",
                        "HoldingSharesLevel": "1-999",
                        "people": "1",
                        "percent": "0.1",
                        "unit": "1",
                    }
                )

            good_row = {
                "date": "2026-01-16",
                "stock_id": "2330",
                "HoldingSharesLevel": "1-999",
                "people": "2",
                "percent": "0.2",
                "unit": "2",
            }

            with patch.object(refresh, "HOLDING_DIR", holding_dir), patch.object(
                refresh, "LOG_DIR", log_dir, create=True
            ), patch.object(refresh, "friday_dates", return_value=["2026-01-09", "2026-01-16"]), patch.object(
                refresh,
                "fetch_finmind_bulk",
                side_effect=[RuntimeError("400 Bad Request"), [good_row]],
            ):
                result = refresh.refresh_weekly_holdings({"2330": {}}, "2026-01-09")

            self.assertEqual(result["fallback_count"], 1)
            self.assertEqual(result["written_files"], 1)
            with (holding_dir / "2330.csv").open("r", encoding="utf-8-sig", newline="") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual({row["date"] for row in rows}, {"2026-01-02", "2026-01-16"})
            failure_logs = list(log_dir.glob("finmind_failures_*.json"))
            self.assertEqual(len(failure_logs), 1)
            failures = json.loads(failure_logs[0].read_text(encoding="utf-8"))
            self.assertEqual(failures[0]["dataset"], "TaiwanStockHoldingSharesPer")

    def test_one_day_price_failure_uses_existing_cache_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            price_dir = tmp_path / "prices"
            log_dir = tmp_path / "logs"
            price_dir.mkdir()
            (price_dir / "2330.csv").write_text(
                "date,open,high,low,close,volume\n2026-05-22,100,101,99,100,1000\n",
                encoding="utf-8",
            )

            with patch.object(refresh, "PRICE_DIR", price_dir), patch.object(
                refresh, "LOG_DIR", log_dir, create=True
            ), patch.object(refresh, "fetch_finmind_bulk", side_effect=RuntimeError("400 Bad Request")):
                result = refresh.refresh_one_day_prices({"2330": {}}, "2026-05-27")

            self.assertTrue(result["fallback"])
            self.assertEqual(result["cached_files"], 1)
            self.assertEqual(result["written_files"], 0)
            self.assertTrue((price_dir / "2330.csv").exists())

    def test_one_day_price_default_start_uses_today_not_long_history_start(self) -> None:
        self.assertEqual(refresh.resolve_price_start(True, None), date.today().isoformat())
        self.assertEqual(
            refresh.resolve_price_start(False, None),
            (date.today() - timedelta(days=430)).strftime("%Y-%m-%d"),
        )
        self.assertEqual(refresh.resolve_price_start(True, "2026-05-22"), "2026-05-22")

    def test_build_stock_pages_includes_query_only_cached_stocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            stock_map = {
                "2317": {"id": "2317", "name": "Hon Hai", "query_only": False},
                "2330": {"id": "2330", "name": "TSMC", "query_only": True},
            }

            with patch.object(generate_site, "OUTPUT_DIR", tmp_path), patch.object(
                generate_site, "build_stock_query_map", return_value=stock_map
            ), patch.object(generate_site, "build_signal_ledger", return_value={}), patch.object(
                generate_site, "build_stock_detail_page", side_effect=lambda code, *_: f"<html>{code}</html>"
            ):
                count = generate_site.build_stock_pages([])

            self.assertEqual(count, 2)
            self.assertTrue((tmp_path / "stocks" / "2317.html").exists())
            self.assertTrue((tmp_path / "stocks" / "2330.html").exists())

    def test_refresh_prices_all_scope_includes_market_cache_and_cached_price_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            price_dir = tmp_path / "prices"
            chip_dir = tmp_path / "chips"
            holding_dir = tmp_path / "holding"
            foreign_dir = tmp_path / "foreign"
            margin_dir = tmp_path / "margin"
            price_dir.mkdir()
            (price_dir / "9955.csv").write_text("date,open,high,low,close,volume\n", encoding="utf-8")
            market_path = tmp_path / "stock_markets.json"
            market_path.write_text(
                json.dumps({"stocks": {"2330": {"name": "TSMC"}, "1101": {"name": "TCC"}}}),
                encoding="utf-8",
            )
            industry_path = tmp_path / "stock_industries.json"
            industry_path.write_text(json.dumps({"2454": {"name": "MTK"}}), encoding="utf-8")

            with patch.dict(os.environ, {"V44_REFRESH_SCOPE": "all"}, clear=False), patch.object(
                refresh_prices, "MARKET_CACHE_PATH", market_path, create=True
            ), patch.object(refresh_prices, "INDUSTRY_CACHE_PATH", industry_path, create=True), patch.object(
                refresh_prices, "LOCAL_PRICE_DIR", price_dir
            ), patch.object(refresh_prices, "LOCAL_CHIP_DIR", chip_dir), patch.object(
                refresh_prices, "LOCAL_HOLDING_DIR", holding_dir
            ), patch.object(
                refresh_prices, "LOCAL_FOREIGN_SHAREHOLDING_DIR", foreign_dir
            ), patch.object(
                refresh_prices, "LOCAL_MARGIN_DIR", margin_dir
            ), patch.object(
                refresh_prices, "find_all_reports", return_value=[]
            ), patch.object(
                refresh_prices,
                "load_reports",
                return_value=[{"date": "2026-05-27", "stocks": [{"id": "2317"}]}],
            ):
                ids = refresh_prices.collect_stock_ids()

            self.assertEqual(ids, ["1101", "2317", "2330", "9955"])

    def test_bulk_market_price_refresh_merges_existing_price_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            price_dir = tmp_path / "prices"
            price_dir.mkdir()
            (price_dir / "2330.csv").write_text(
                "date,open,high,low,close,volume\n2026-05-22,100,101,99,100,1000\n",
                encoding="utf-8",
            )
            rows = [
                {
                    "date": "2026-05-27",
                    "stock_id": "2330",
                    "open": 102,
                    "max": 103,
                    "min": 101,
                    "close": 102.5,
                    "Trading_Volume": 2000,
                },
                {
                    "date": "2026-05-27",
                    "stock_id": "2317",
                    "open": 150,
                    "max": 151,
                    "min": 149,
                    "close": 150.5,
                    "Trading_Volume": 3000,
                },
            ]

            with patch.object(refresh_prices, "LOCAL_PRICE_DIR", price_dir):
                written = refresh_prices.write_bulk_price_rows({"2330", "2317"}, rows)

            self.assertEqual(written, 2)
            with (price_dir / "2330.csv").open("r", encoding="utf-8-sig", newline="") as fh:
                merged = list(csv.DictReader(fh))
            self.assertEqual([row["date"] for row in merged], ["2026-05-22", "2026-05-27"])
            self.assertTrue((price_dir / "2317.csv").exists())

    def test_write_summary_preserves_previous_holding_when_price_snapshot_runs_second(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "summary.json"
            previous = {
                "date": date.today().isoformat(),
                "holding": {"query_dates": 20, "fallback_count": 0},
                "price": {"written_files": 531},
                "candidate_count": 531,
            }
            summary_path.write_text(json.dumps(previous), encoding="utf-8")
            current = {
                "date": date.today().isoformat(),
                "holding": None,
                "price": {"written_files": 1966},
                "candidate_count": None,
            }

            with patch.object(refresh, "SUMMARY_PATH", summary_path):
                refresh.write_summary(current)

            merged = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(merged["holding"], {"query_dates": 20, "fallback_count": 0})
            self.assertEqual(merged["candidate_count"], 531)
            self.assertEqual(merged["price"], {"written_files": 1966})


if __name__ == "__main__":
    unittest.main()
