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
    def test_latest_snapshot_keeps_aligned_openapi_as_primary_path(self) -> None:
        def fetch_json(url: str, params: dict[str, str] | None = None):
            self.assertIsNone(params)
            if url == prices.TWSE_LATEST_URL:
                return [
                    {
                        "Date": "1150810",
                        "Code": "2330",
                        "OpeningPrice": "104",
                        "HighestPrice": "106",
                        "LowestPrice": "103",
                        "ClosingPrice": "105",
                        "TradeVolume": "3,000",
                    }
                ]
            if url == prices.TPEX_LATEST_URL:
                return [
                    {
                        "Date": "1150810",
                        "SecuritiesCompanyCode": "8069",
                        "Open": "51",
                        "High": "54",
                        "Low": "50",
                        "Close": "53",
                        "TradingShares": "4,000",
                    }
                ]
            self.fail(f"unexpected recovery URL for aligned snapshots: {url}")

        trading_date, rows, counts, metadata = prices.fetch_latest_snapshot(fetch_json)

        self.assertEqual(trading_date, "2026-08-10")
        self.assertEqual({row["stock_id"] for row in rows}, {"2330", "8069"})
        self.assertEqual(counts, {"twse": 1, "tpex": 1})
        self.assertEqual(metadata["mode"], "latest_openapi")
        self.assertFalse(metadata["date_skew_recovered"])

    def test_latest_snapshot_recovers_date_skew_from_complete_exact_date_history(self) -> None:
        def fetch_json(url: str, params: dict[str, str] | None = None):
            if url == prices.TWSE_LATEST_URL:
                return [
                    {
                        "Date": "1150807",
                        "Code": "2330",
                        "OpeningPrice": "100",
                        "HighestPrice": "103",
                        "LowestPrice": "99",
                        "ClosingPrice": "102",
                        "TradeVolume": "1,000",
                    }
                ]
            if url == prices.TPEX_LATEST_URL:
                return [
                    {
                        "Date": "1150810",
                        "SecuritiesCompanyCode": "8069",
                        "Open": "50",
                        "High": "52",
                        "Low": "49",
                        "Close": "51",
                        "TradingShares": "2,000",
                    }
                ]
            if url == prices.TWSE_HISTORY_URL:
                self.assertEqual(params, {"date": "20260810", "type": "ALLBUT0999", "response": "json"})
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
                            "data": [["2330", "3,000", "104", "106", "103", "105"]],
                        }
                    ],
                }
            if url == prices.TPEX_HISTORY_URL:
                self.assertEqual(params, {"date": "2026/08/10", "id": "", "response": "json"})
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["代號", "收盤", "開盤", "最高", "最低", "成交股數"],
                            "data": [["8069", "53", "51", "54", "50", "4,000"]],
                        }
                    ],
                }
            self.fail(f"unexpected URL: {url}")

        trading_date, rows, counts, metadata = prices.fetch_latest_snapshot(
            fetch_json,
            recovery_min_unique_ids={"twse": 1, "tpex": 1},
        )

        self.assertEqual(trading_date, "2026-08-10")
        self.assertEqual({row["stock_id"] for row in rows}, {"2330", "8069"})
        self.assertEqual(counts, {"twse": 1, "tpex": 1})
        self.assertEqual(metadata["mode"], "historical_exact_date_recovery")
        self.assertTrue(metadata["date_skew_recovered"])
        self.assertEqual(metadata["twse_latest_date"], "2026-08-07")
        self.assertEqual(metadata["tpex_latest_date"], "2026-08-10")
        self.assertEqual(metadata["target_date"], "2026-08-10")
        self.assertEqual(metadata["recovery_coverage"]["count_basis"], "unique_security_ids")
        self.assertEqual(metadata["recovery_coverage"]["recovered_unique_ids"], counts)

    def test_latest_snapshot_date_skew_recovery_fails_closed_when_a_partition_is_missing(self) -> None:
        def fetch_json(url: str, params: dict[str, str] | None = None):
            if url == prices.TWSE_LATEST_URL:
                return [
                    {
                        "Date": "1150807",
                        "Code": "2330",
                        "OpeningPrice": "100",
                        "HighestPrice": "103",
                        "LowestPrice": "99",
                        "ClosingPrice": "102",
                        "TradeVolume": "1,000",
                    }
                ]
            if url == prices.TPEX_LATEST_URL:
                return [
                    {
                        "Date": "1150810",
                        "SecuritiesCompanyCode": "8069",
                        "Open": "50",
                        "High": "52",
                        "Low": "49",
                        "Close": "51",
                        "TradingShares": "2,000",
                    }
                ]
            if url == prices.TWSE_HISTORY_URL:
                return {"date": "20260810", "tables": []}
            if url == prices.TPEX_HISTORY_URL:
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["代號", "收盤", "開盤", "最高", "最低", "成交股數"],
                            "data": [["8069", "53", "51", "54", "50", "4,000"]],
                        }
                    ],
                }
            self.fail(f"unexpected URL: {url}")

        with self.assertRaisesRegex(RuntimeError, "date-skew recovery failed"):
            prices.fetch_latest_snapshot(fetch_json)

    def test_date_skew_recovery_rejects_nonempty_partial_partitions_before_summary_write(self) -> None:
        def fetch_json(url: str, params: dict[str, str] | None = None):
            if url == prices.TWSE_LATEST_URL:
                return [
                    {
                        "Date": "1150807",
                        "Code": "2330",
                        "OpeningPrice": "100",
                        "HighestPrice": "103",
                        "LowestPrice": "99",
                        "ClosingPrice": "102",
                        "TradeVolume": "1,000",
                    }
                ]
            if url == prices.TPEX_LATEST_URL:
                return [
                    {
                        "Date": "1150810",
                        "SecuritiesCompanyCode": "8069",
                        "Open": "50",
                        "High": "52",
                        "Low": "49",
                        "Close": "51",
                        "TradingShares": "2,000",
                    }
                ]
            if url == prices.TWSE_HISTORY_URL:
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
                            "data": [["2330", "3,000", "104", "106", "103", "105"]],
                        }
                    ],
                }
            if url == prices.TPEX_HISTORY_URL:
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["代號", "收盤", "開盤", "最高", "最低", "成交股數"],
                            "data": [["8069", "53", "51", "54", "50", "4,000"]],
                        }
                    ],
                }
            self.fail(f"unexpected URL: {url}")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summary_path = root / "price_refresh_summary.json"
            with self.assertRaisesRegex(RuntimeError, "recovery coverage is incomplete"):
                prices.refresh_official_prices(
                    stock_ids={"2330", "8069", "2317", "6488"},
                    price_dir=root / "prices",
                    summary_path=summary_path,
                    initial_days=1,
                    fetch_latest=lambda: prices.fetch_latest_snapshot(fetch_json),
                    fetch_history=lambda _: [],
                )
            self.assertFalse(summary_path.exists())
            self.assertFalse((root / "prices").exists())

    def test_date_skew_recovery_rejects_duplicate_inflated_partition_counts(self) -> None:
        def fetch_json(url: str, params: dict[str, str] | None = None):
            if url == prices.TWSE_LATEST_URL:
                return [
                    {
                        "Date": "1150807",
                        "Code": "2330",
                        "OpeningPrice": "100",
                        "HighestPrice": "103",
                        "LowestPrice": "99",
                        "ClosingPrice": "102",
                        "TradeVolume": "1,000",
                    }
                ]
            if url == prices.TPEX_LATEST_URL:
                return [
                    {
                        "Date": "1150810",
                        "SecuritiesCompanyCode": "8069",
                        "Open": "50",
                        "High": "52",
                        "Low": "49",
                        "Close": "51",
                        "TradingShares": "2,000",
                    }
                ]
            if url == prices.TWSE_HISTORY_URL:
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
                            "data": [["2330", "3,000", "104", "106", "103", "105"] for _ in range(800)],
                        }
                    ],
                }
            if url == prices.TPEX_HISTORY_URL:
                return {
                    "date": "20260810",
                    "tables": [
                        {
                            "fields": ["代號", "收盤", "開盤", "最高", "最低", "成交股數"],
                            "data": [["8069", "53", "51", "54", "50", "4,000"] for _ in range(600)],
                        }
                    ],
                }
            self.fail(f"unexpected URL: {url}")

        with self.assertRaisesRegex(RuntimeError, "duplicate security IDs"):
            prices.fetch_latest_snapshot(fetch_json)

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
                metadata = {
                    "mode": "latest_openapi",
                    "date_skew_recovered": False,
                    "twse_latest_date": "2026-08-07",
                    "tpex_latest_date": "2026-08-07",
                    "target_date": "2026-08-07",
                }
                return "2026-08-07", rows, {"twse": 1, "tpex": 1}, metadata

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
            self.assertEqual(summary["schema_version"], "1.1.0")
            self.assertEqual(summary["latest_snapshot"]["mode"], "latest_openapi")
            self.assertFalse(summary["latest_snapshot"]["date_skew_recovered"])
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
