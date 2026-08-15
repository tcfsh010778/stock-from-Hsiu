import unittest
import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import market_flow


class MarketFlowTests(unittest.TestCase):
    def test_fetch_json_retries_truncated_official_response(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"ok": true}'
        with patch.object(market_flow, "urlopen", side_effect=[OSError("truncated"), response]) as mocked, patch.object(
            market_flow, "sleep"
        ) as mocked_sleep:
            payload = market_flow._fetch_json("https://example.test/data")

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(mocked.call_count, 2)
        mocked_sleep.assert_called_once_with(1)

    def test_normalize_twse_and_tpex_rows(self):
        twse = market_flow.normalize_twse_payload(
            {
                "date": "1150806",
                "fields": [
                    "證券代號", "證券名稱", "外陸資買進股數(不含外資自營商)", "外陸資賣出股數(不含外資自營商)",
                    "外陸資買賣超股數(不含外資自營商)", "投信買進股數", "投信賣出股數", "投信買賣超股數",
                    "自營商買賣超股數", "三大法人買賣超股數",
                ],
                "data": [["2330", "台積電", "1,200", "800", "400", "100", "50", "50", "-20", "430"]],
            }
        )
        tpex = market_flow.normalize_tpex_payload(
            [{
                "Date": "1150806",
                "SecuritiesCompanyCode": "6488",
                "CompanyName": "環球晶",
                "ForeignInvestorsIncludeMainlandAreaInvestors-TotalBuy": "900",
                "ForeignInvestorsIncludeMainlandAreaInvestors-TotalSell": "1,100",
                "ForeignInvestorsInclude MainlandAreaInvestors-Difference": "-200",
                "SecuritiesInvestmentTrustCompanies-TotalBuy": "300",
                "SecuritiesInvestmentTrustCompanies-TotalSell": "100",
                "SecuritiesInvestmentTrustCompanies-Difference": "200",
                "Dealers-Difference": "10",
                "TotalDifference": "10",
            }]
        )
        self.assertEqual(twse[0]["trading_date"], "2026-08-06")
        self.assertEqual(twse[0]["foreign_net"], 400)
        self.assertEqual(twse[0]["investment_trust_net"], 50)
        self.assertEqual(tpex[0]["market"], "otc")
        self.assertEqual(tpex[0]["foreign_net"], -200)
        self.assertEqual(tpex[0]["investment_trust_net"], 200)

    def test_normalize_tpex_dated_history_table(self):
        values = ["6488", "環球晶", "1", "2", "-1", "0", "0", "0", "900", "1,100", "-200", "300", "100", "200", "0", "0", "0", "0", "0", "0", "10", "0", "10", "10"]
        rows = market_flow.normalize_tpex_history_payload({
            "date": "20260806",
            "tables": [{"data": [values]}],
        })
        self.assertEqual(rows[0]["trading_date"], "2026-08-06")
        self.assertEqual(rows[0]["foreign_net"], -200)
        self.assertEqual(rows[0]["investment_trust_net"], 200)

    def test_normalizers_prefer_official_date_over_requested_date(self):
        twse = market_flow.normalize_twse_payload(
            {
                "date": "20260807",
                "fields": ["證券代號", "證券名稱"],
                "data": [["2330", "台積電"]],
            },
            "20260808",
        )
        tpex = market_flow.normalize_tpex_payload(
            [{"Date": "20260807", "SecuritiesCompanyCode": "6488", "CompanyName": "環球晶"}],
            "20260808",
        )
        self.assertEqual(twse[0]["trading_date"], "2026-08-07")
        self.assertEqual(tpex[0]["trading_date"], "2026-08-07")

    def test_aggregate_market_rows_keeps_top_buy_sell(self):
        rows = [
            {"security_id": "2330", "name": "甲", "foreign_net": 300, "investment_trust_net": -50},
            {"security_id": "6488", "name": "乙", "foreign_net": -100, "investment_trust_net": 80},
            {"security_id": "0050", "name": "元大台灣50", "foreign_net": 999, "investment_trust_net": 999},
        ]
        summary = market_flow.aggregate_market_rows(rows)
        self.assertEqual(summary["foreign_net"], 1199)
        self.assertEqual(summary["investment_trust_net"], 1029)
        self.assertEqual(summary["foreign_top_buy"][0]["security_id"], "2330")
        self.assertEqual(summary["foreign_top_sell"][0]["security_id"], "6488")
        self.assertEqual(summary["ranking_excluded_count"], 1)

    def test_normalize_exact_official_amount_summaries(self):
        listed = market_flow.normalize_amount_payload(
            {
                "date": "20260807",
                "fields": ["單位名稱", "買進金額", "賣出金額", "買賣差額"],
                "data": [
                    ["自營商(自行買賣)", "100", "80", "20"],
                    ["自營商(避險)", "50", "70", "-20"],
                    ["投信", "300", "200", "100"],
                    ["外資及陸資(不含外資自營商)", "800", "900", "-100"],
                    ["合計", "1,250", "1,250", "0"],
                ],
            },
            "listed",
        )
        otc = market_flow.normalize_amount_payload(
            {
                "date": "20260807",
                "tables": [{
                    "fields": ["單位名稱", "買進金額(元)", "賣出金額(元)", "買賣超(元)"],
                    "data": [
                        ["外資及陸資(不含自營商)", "500", "300", "200"],
                        ["投信", "100", "120", "-20"],
                        ["自營商合計", "80", "50", "30"],
                        ["三大法人合計*", "680", "470", "210"],
                    ],
                }],
            },
            "otc",
        )
        self.assertEqual(listed["dealer_net_amount"], 0)
        self.assertEqual(listed["foreign_net_amount"], -100)
        self.assertEqual(otc["investment_trust_net_amount"], -20)
        self.assertEqual(otc["institutional_total_net_amount"], 210)

    def test_normalize_margin_rows_and_calculate_ratio(self):
        listed = market_flow.normalize_twse_margin_payload({
            "date": "20260814",
            "tables": [{
                "fields": ["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "限額", "買進", "賣出", "現券償還", "前日餘額", "今日餘額"],
                "data": [["2330", "台積電", "1", "2", "0", "1,000", "1,120", "0", "1", "2", "0", "20", "28"]],
            }],
        })
        otc = market_flow.normalize_tpex_margin_payload([{
            "Date": "1150814",
            "SecuritiesCompanyCode": "6488",
            "CompanyName": "環球晶",
            "MarginPurchaseBalancePreviousDay": "500",
            "MarginPurchaseBalance": "400",
            "ShortSaleBalancePreviousDay": "8",
            "ShortSaleBalance": "20",
        }])
        metrics = market_flow.margin_metrics([*listed, *otc])
        self.assertEqual(listed[0]["trading_date"], "2026-08-14")
        self.assertEqual(metrics["2330"], {"margin_balance_delta": 120, "short_margin_ratio_pct": 2.5})
        self.assertEqual(metrics["6488"], {"margin_balance_delta": -100, "short_margin_ratio_pct": 5.0})

    def test_load_retail_weekly_metrics_uses_200_lot_or_less_ratio_reduction(self):
        payload = {
            "snapshots": [
                {"date": "2026-08-01", "rows": [{"security_id": "2330", "retail_200_percent": 12.4}]},
                {"date": "2026-08-08", "rows": [{"security_id": "2330", "retail_200_percent": 11.9}]},
            ]
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "holders.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            metrics, reference = market_flow.load_retail_weekly_metrics(path)
        self.assertEqual(metrics["2330"]["retail_sell_pctpt"], 0.5)
        self.assertEqual(reference["date"], "2026-08-08")
        self.assertEqual(reference["previous_date"], "2026-08-01")

    def test_full_rankings_exclude_non_ordinary_instruments(self):
        rows = [
            {"security_id": "2330", "name": "台積電", "market": "listed", "foreign_net": 100, "investment_trust_net": -10},
            {"security_id": "0050", "name": "元大台灣50", "market": "listed", "foreign_net": 900, "investment_trust_net": 50},
            {"security_id": "02001L", "name": "富邦蘋果正二N", "market": "listed", "foreign_net": 800, "investment_trust_net": 40},
            {"security_id": "9103", "name": "美德醫-DR", "market": "listed", "foreign_net": 700, "investment_trust_net": 30},
            {"security_id": "6488", "name": "環球晶", "market": "otc", "foreign_net": -50, "investment_trust_net": 20},
        ]
        rankings = market_flow.build_rankings(rows, {"2330": {"foreign": {"net_5d": 500, "net_10d": 800, "net_20d": 1000, "concentration_ratio_pct": 1.25}}})
        self.assertEqual(rankings["eligible_count"], 2)
        self.assertEqual(rankings["excluded_count"], 3)
        self.assertEqual([row["security_id"] for row in rankings["foreign_buy"]], ["2330"])
        self.assertEqual([row["security_id"] for row in rankings["foreign_sell"]], ["6488"])
        self.assertEqual(rankings["foreign_buy"][0]["net_5d"], 500)
        self.assertEqual(rankings["foreign_buy"][0]["concentration_ratio_pct"], 1.25)

    def test_supplemental_fields_are_limited_to_the_rendered_top_fifty(self):
        rows = [
            {"security_id": str(1100 + index), "name": f"Stock {index}", "market": "listed", "foreign_net": 10_000 - index}
            for index in range(52)
        ]
        supplemental = {row["security_id"]: {"foreign": {"net_5d": 1}} for row in rows}
        ranked = market_flow.build_rankings(rows, supplemental)["foreign_buy"]
        self.assertIn("net_5d", ranked[49])
        self.assertNotIn("net_5d", ranked[50])

    def test_rolling_metrics_sum_5_10_20_sessions_and_use_20_day_volume(self):
        dates = [f"2026-08-{day:02d}" for day in range(1, 21)]
        history = {
            "snapshots": [
                {"date": data_date, "rows": [{"security_id": "2330", "foreign_net": 1_000, "investment_trust_net": -500}]}
                for data_date in dates
            ]
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            price_dir = Path(temp_dir)
            (price_dir / "2330.csv").write_text(
                "date,open,high,low,close,volume\n" + "".join(f"{data_date},1,1,1,1,10000\n" for data_date in dates),
                encoding="utf-8",
            )
            metrics = market_flow.rolling_institutional_metrics(history, {"2330"}, price_dir=price_dir)
        self.assertEqual(metrics["2330"]["foreign"]["net_5d"], 5_000)
        self.assertEqual(metrics["2330"]["foreign"]["net_10d"], 10_000)
        self.assertEqual(metrics["2330"]["foreign"]["net_20d"], 20_000)
        self.assertEqual(metrics["2330"]["foreign"]["concentration_ratio_pct"], 10.0)

    def test_build_payload_exposes_missing_market_partition(self):
        payload = market_flow.build_payload([{"security_id": "2330", "foreign_net": 1}], [], data_date="2026-08-06", fetched_at="2026-08-06T20:00:00+08:00", source_errors={"otc": "timeout"})
        self.assertEqual(payload["data_quality"]["state"], "warning")
        otc = next(item for item in payload["source_artifacts"] if item["market"] == "otc")
        self.assertEqual(otc["status"], "missing")

    def test_collect_rolls_weekend_back_to_complete_common_trading_day(self):
        detail_fields = [
            "證券代號", "證券名稱", "外陸資買進股數(不含外資自營商)",
            "外陸資賣出股數(不含外資自營商)", "外陸資買賣超股數(不含外資自營商)",
            "投信買進股數", "投信賣出股數", "投信買賣超股數",
            "自營商買賣超股數", "三大法人買賣超股數",
        ]
        listed_amount_rows = [
            ["外資及陸資(不含外資自營商)", "800", "900", "-100"],
            ["投信", "300", "200", "100"],
            ["自營商合計", "100", "80", "20"],
            ["合計", "1,200", "1,180", "20"],
        ]
        otc_amount_rows = [
            ["外資及陸資(不含自營商)", "500", "300", "200"],
            ["投信", "100", "120", "-20"],
            ["自營商合計", "80", "50", "30"],
            ["三大法人合計*", "680", "470", "210"],
        ]
        requested_twse_dates = []

        def fake_fetch(url, params=None, timeout=45):
            del timeout
            if url == market_flow.TWSE_URL:
                requested = params["date"]
                requested_twse_dates.append(requested)
                if requested == "20260808":
                    return {"date": "", "fields": detail_fields, "data": []}
                return {
                    "date": "20260807",
                    "fields": detail_fields,
                    "data": [["2330", "台積電", "1200", "800", "400", "100", "50", "50", "-20", "430"]],
                }
            if url == market_flow.TPEX_URL:
                return [{
                    "Date": "20260807",
                    "SecuritiesCompanyCode": "6488",
                    "CompanyName": "環球晶",
                    "ForeignInvestorsIncludeMainlandAreaInvestors-TotalBuy": "900",
                    "ForeignInvestorsIncludeMainlandAreaInvestors-TotalSell": "1100",
                    "ForeignInvestorsIncludeMainlandAreaInvestors-Difference": "-200",
                    "SecuritiesInvestmentTrustCompanies-TotalBuy": "300",
                    "SecuritiesInvestmentTrustCompanies-TotalSell": "100",
                    "SecuritiesInvestmentTrustCompanies-Difference": "200",
                    "Dealers-Difference": "10",
                    "TotalDifference": "10",
                }]
            if url == market_flow.TWSE_AMOUNT_URL:
                if params["dayDate"] == "20260808":
                    return {"date": "", "fields": [], "data": []}
                return {
                    "date": "20260807",
                    "fields": ["單位名稱", "買進金額", "賣出金額", "買賣差額"],
                    "data": listed_amount_rows,
                }
            if url == market_flow.TWSE_MARGIN_URL:
                requested = params["date"]
                if requested == "20260808":
                    return {"date": "", "tables": []}
                return {
                    "date": "20260807",
                    "tables": [{
                        "fields": ["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "限額", "買進", "賣出", "現券償還", "前日餘額", "今日餘額"],
                        "data": [["2330", "台積電", "1", "2", "0", "1000", "1120", "0", "1", "2", "0", "20", "28"]],
                    }],
                }
            if url == market_flow.TPEX_MARGIN_URL:
                return [{
                    "Date": "20260807",
                    "SecuritiesCompanyCode": "6488",
                    "CompanyName": "環球晶",
                    "MarginPurchaseBalancePreviousDay": "500",
                    "MarginPurchaseBalance": "400",
                    "ShortSaleBalancePreviousDay": "8",
                    "ShortSaleBalance": "20",
                }]
            raise AssertionError(f"unexpected URL {url}")

        def fake_post(url, params, timeout=45):
            del timeout
            self.assertEqual(url, market_flow.TPEX_AMOUNT_URL)
            return {
                "date": "20260807",
                "tables": [{
                    "fields": ["單位名稱", "買進金額(元)", "賣出金額(元)", "買賣超(元)"],
                    "data": otc_amount_rows,
                }],
            }

        history = {
            "session_count": 20,
            "snapshots": [
                {"date": f"2026-07-{day:02d}", "rows": [{"security_id": "2330", "foreign_net": 1}]}
                for day in range(1, 20)
            ] + [{"date": "2026-08-07", "rows": [{"security_id": "2330", "foreign_net": 1}]}],
        }
        with patch.object(market_flow, "_fetch_json", side_effect=fake_fetch), patch.object(
            market_flow, "_post_json", side_effect=fake_post
        ), patch.object(market_flow, "load_retail_weekly_metrics", return_value=(
            {"2330": {"retail_sell_pctpt": 0.5}, "6488": {"retail_sell_pctpt": -0.2}},
            {"date": "2026-08-07", "previous_date": "2026-07-31", "coverage_count": 2},
        )), patch.object(market_flow, "build_institutional_history", return_value=(history, None)):
            payload = market_flow.collect(now=datetime(2026, 8, 8, 10, 0, tzinfo=market_flow.TAIPEI_TZ))

        self.assertEqual(requested_twse_dates, ["20260808", "20260807"])
        self.assertEqual(payload["date"], "2026-08-07")
        self.assertEqual(payload["markets"]["listed"]["stock_count"], 1)
        self.assertEqual(payload["markets"]["otc"]["stock_count"], 1)
        self.assertEqual(payload["data_quality"], {"state": "ok", "warnings": []})
        self.assertTrue(market_flow._is_complete_snapshot(payload))

    def test_main_keeps_existing_artifact_when_official_partitions_are_incomplete(self):
        incomplete = market_flow.build_payload(
            [],
            [],
            data_date="2026-08-12",
            fetched_at="2026-08-12T18:00:00+08:00",
            source_errors={"listed": "not ready", "otc": "not ready"},
            amount_source_errors={"listed": "not ready", "otc": "not ready"},
        )
        with patch.object(market_flow, "collect", return_value=incomplete), patch.object(
            market_flow, "write_payload"
        ) as mocked_write, patch("sys.argv", ["market_flow.py"]):
            result = market_flow.main()

        self.assertEqual(result, 0)
        mocked_write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
