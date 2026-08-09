from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import official_price_refresh as prices


class OfficialPriceRefreshTest(unittest.TestCase):
    def test_latest_openapi_normalizers_align_roc_dates(self) -> None:
        twse_date, twse_rows = prices.normalize_twse_latest(
            [
                {
                    "Date": "1150807",
                    "Code": "2330",
                    "OpeningPrice": "2,360.00",
                    "HighestPrice": "2,370.00",
                    "LowestPrice": "2,325.00",
                    "ClosingPrice": "2,340.00",
                    "TradeVolume": "53,800,344",
                },
                {"Date": "1150807", "Code": "0050", "OpeningPrice": "--"},
            ]
        )
        tpex_date, tpex_rows = prices.normalize_tpex_latest(
            [
                {
                    "Date": "1150807",
                    "SecuritiesCompanyCode": "8069",
                    "Open": "100",
                    "High": "103",
                    "Low": "99",
                    "Close": "102",
                    "TradingShares": "1,234,567",
                }
            ]
        )

        self.assertEqual(twse_date, "2026-08-07")
        self.assertEqual(tpex_date, twse_date)
        self.assertEqual(twse_rows[0]["stock_id"], "2330")
        self.assertEqual(twse_rows[0]["volume"], 53_800_344)
        self.assertEqual(tpex_rows[0]["stock_id"], "8069")

    def test_historical_normalizers_require_exact_requested_date(self) -> None:
        twse = {
            "date": "20260807",
            "tables": [
                {
                    "fields": ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
                    "data": [["2330", "1,000", "100", "103", "99", "102"]],
                }
            ],
        }
        tpex = {
            "date": "20260807",
            "tables": [
                {
                    "fields": ["代號", "收盤", "開盤", "最高", "最低", "成交股數"],
                    "data": [["8069", "51", "50", "52", "49", "2,000"]],
                }
            ],
        }

        self.assertEqual(len(prices.normalize_twse_history(twse, date(2026, 8, 7))), 1)
        self.assertEqual(len(prices.normalize_tpex_history(tpex, date(2026, 8, 7))), 1)
        self.assertEqual(prices.normalize_twse_history(twse, date(2026, 8, 6)), [])
        self.assertEqual(prices.normalize_tpex_history(tpex, date(2026, 8, 6)), [])

    def test_refresh_backfills_and_preserves_existing_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            price_dir = root / "prices"
            price_dir.mkdir()
            summary_path = root / "price_refresh_summary.json"
            (price_dir / "2330.csv").write_text(
                "date,open,high,low,close,volume\n2026-06-26,90,91,89,90,900\n",
                encoding="utf-8",
            )

            def latest():
                rows = [
                    {"date": "2026-08-07", "stock_id": "2330", "open": 102, "high": 104, "low": 101, "close": 103, "volume": 3000},
                    {"date": "2026-08-07", "stock_id": "8069", "open": 50, "high": 52, "low": 49, "close": 51, "volume": 4000},
                ]
                return "2026-08-07", rows, {"twse": 1, "tpex": 1}

            def history(day: date):
                if day == date(2026, 8, 6):
                    return [
                        {"date": "2026-08-06", "stock_id": "2330", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 2000},
                        {"date": "2026-08-06", "stock_id": "8069", "open": 49, "high": 51, "low": 48, "close": 50, "volume": 2500},
                    ]
                return []

            summary = prices.refresh_official_prices(
                stock_ids={"2330", "8069"},
                price_dir=price_dir,
                summary_path=summary_path,
                initial_days=2,
                fetch_latest=latest,
                fetch_history=history,
            )

            with (price_dir / "2330.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual([row["date"] for row in rows], ["2026-06-26", "2026-08-06", "2026-08-07"])
            self.assertEqual(summary["latest_data_date"], "2026-08-07")
            self.assertEqual(summary["latest_matched_stocks"], 2)
            self.assertEqual(json.loads(summary_path.read_text(encoding="utf-8"))["status"], "fresh")

    def test_refresh_fails_closed_when_latest_snapshot_matches_zero_stocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def latest():
                rows = [{"date": "2026-08-07", "stock_id": "2330", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
                return "2026-08-07", rows, {"twse": 1, "tpex": 1}

            with self.assertRaisesRegex(RuntimeError, "matched zero"):
                prices.refresh_official_prices(
                    stock_ids={"8069"},
                    price_dir=root / "prices",
                    summary_path=root / "summary.json",
                    initial_days=1,
                    fetch_latest=latest,
                    fetch_history=lambda _: [],
                )

    def test_refresh_fails_closed_when_a_backfill_date_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def latest():
                rows = [{"date": "2026-08-07", "stock_id": "2330", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
                return "2026-08-07", rows, {"twse": 1, "tpex": 1}

            with self.assertRaisesRegex(RuntimeError, "backfill was incomplete"):
                prices.refresh_official_prices(
                    stock_ids={"2330"},
                    price_dir=root / "prices",
                    summary_path=root / "summary.json",
                    initial_days=2,
                    fetch_latest=latest,
                    fetch_history=lambda _: (_ for _ in ()).throw(RuntimeError("temporary official error")),
                )
            self.assertFalse((root / "summary.json").exists())

    def test_previous_official_summary_uses_small_overlap(self) -> None:
        start = prices.choose_start_date(
            date(2026, 8, 7),
            {"source": "official_twse_tpex", "latest_data_date": "2026-08-06"},
            initial_days=75,
            overlap_days=7,
        )
        self.assertEqual(start, date(2026, 7, 30))


if __name__ == "__main__":
    unittest.main()
