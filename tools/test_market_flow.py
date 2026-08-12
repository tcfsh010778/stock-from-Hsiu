import unittest
from datetime import datetime
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

    def test_full_rankings_exclude_non_ordinary_instruments(self):
        rows = [
            {"security_id": "2330", "name": "台積電", "market": "listed", "foreign_net": 100, "investment_trust_net": -10},
            {"security_id": "0050", "name": "元大台灣50", "market": "listed", "foreign_net": 900, "investment_trust_net": 50},
            {"security_id": "02001L", "name": "富邦蘋果正二N", "market": "listed", "foreign_net": 800, "investment_trust_net": 40},
            {"security_id": "9103", "name": "美德醫-DR", "market": "listed", "foreign_net": 700, "investment_trust_net": 30},
            {"security_id": "6488", "name": "環球晶", "market": "otc", "foreign_net": -50, "investment_trust_net": 20},
        ]
        rankings = market_flow.build_rankings(rows)
        self.assertEqual(rankings["eligible_count"], 2)
        self.assertEqual(rankings["excluded_count"], 3)
        self.assertEqual([row["security_id"] for row in rankings["foreign_buy"]], ["2330"])
        self.assertEqual([row["security_id"] for row in rankings["foreign_sell"]], ["6488"])

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

        with patch.object(market_flow, "_fetch_json", side_effect=fake_fetch), patch.object(
            market_flow, "_post_json", side_effect=fake_post
        ):
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
