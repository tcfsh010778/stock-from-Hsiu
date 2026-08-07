import unittest

import market_flow


class MarketFlowTests(unittest.TestCase):
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

    def test_aggregate_market_rows_keeps_top_buy_sell(self):
        rows = [
            {"security_id": "A", "name": "甲", "foreign_net": 300, "investment_trust_net": -50},
            {"security_id": "B", "name": "乙", "foreign_net": -100, "investment_trust_net": 80},
        ]
        summary = market_flow.aggregate_market_rows(rows)
        self.assertEqual(summary["foreign_net"], 200)
        self.assertEqual(summary["investment_trust_net"], 30)
        self.assertEqual(summary["foreign_top_buy"][0]["security_id"], "A")
        self.assertEqual(summary["foreign_top_sell"][0]["security_id"], "B")

    def test_build_payload_exposes_missing_market_partition(self):
        payload = market_flow.build_payload([{"security_id": "2330", "foreign_net": 1}], [], data_date="2026-08-06", fetched_at="2026-08-06T20:00:00+08:00", source_errors={"otc": "timeout"})
        self.assertEqual(payload["data_quality"]["state"], "warning")
        otc = next(item for item in payload["source_artifacts"] if item["market"] == "otc")
        self.assertEqual(otc["status"], "missing")


if __name__ == "__main__":
    unittest.main()
