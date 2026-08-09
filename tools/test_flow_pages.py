import unittest

import generate_site


class FlowPageTests(unittest.TestCase):
    def market_payload(self):
        return {
            "date": "2026-08-07",
            "updated_at": "2026-08-07T20:00:00+08:00",
            "markets": {
                "listed": {
                    "stock_count": 2,
                    "amounts": {
                        "foreign_net_amount": -40_715_743_790,
                        "investment_trust_net_amount": -1_201_721_402,
                        "institutional_total_net_amount": -42_885_537_066,
                    },
                },
                "otc": {
                    "stock_count": 1,
                    "amounts": {
                        "foreign_net_amount": -11_154_747_169,
                        "investment_trust_net_amount": -205_688_809,
                        "institutional_total_net_amount": -12_160_553_242,
                    },
                },
            },
            "rankings": {
                "eligibility_policy": "ordinary_equity_v1",
                "eligible_count": 2,
                "excluded_count": 1,
                "foreign_buy": [{"security_id": "2330", "name": "台積電", "market": "listed", "net_shares": 100_000}],
                "foreign_sell": [{"security_id": "6488", "name": "環球晶", "market": "otc", "net_shares": -50_000}],
                "investment_trust_buy": [{"security_id": "6488", "name": "環球晶", "market": "otc", "net_shares": 20_000}],
                "investment_trust_sell": [{"security_id": "2330", "name": "台積電", "market": "listed", "net_shares": -10_000}],
            },
            "data_quality": {"state": "ok", "warnings": []},
            "freshness": {"status": "fresh"},
        }

    def test_home_panel_uses_official_amounts_and_links_to_full_ranking(self):
        html = generate_site.build_daily_market_flow_panel(self.market_payload())
        self.assertIn("-407.16 億元", html)
        self.assertIn("institutional-flow.html", html)
        self.assertNotIn("外資淨買排行", html)

    def test_institutional_page_contains_all_four_filtered_rankings(self):
        html = generate_site.build_institutional_flow_page(self.market_payload())
        self.assertIn("外資／投信買賣超排行", html)
        self.assertIn("ETF、ETN、權證、TDR", html)
        self.assertIn("2330 台積電", html)
        self.assertIn("6488 環球晶", html)
        self.assertEqual(html.count("<tr data-flow-rank-row"), 4)
        for section_id, label in (
            ("foreign-buy", "外資買超"),
            ("foreign-sell", "外資賣超"),
            ("trust-buy", "投信買超"),
            ("trust-sell", "投信賣超"),
        ):
            self.assertIn(f'id="{section_id}"', html)
            self.assertIn(label, html)
        self.assertNotIn('class="tab-panel', html)

    def test_institutional_page_uses_two_column_top_50_grid(self):
        payload = self.market_payload()
        rows = [
            {
                "security_id": str(1000 + index),
                "name": f"Stock {index}",
                "market": "listed",
                "net_shares": 100_000 - index,
            }
            for index in range(60)
        ]
        payload["rankings"].update(
            {
                "foreign_buy": rows,
                "foreign_sell": rows,
                "investment_trust_buy": rows,
                "investment_trust_sell": rows,
            }
        )

        html = generate_site.build_institutional_flow_page(payload)

        self.assertIn('class="ranking-grid"', html)
        self.assertEqual(html.count("<tr data-flow-rank-row"), 200)
        self.assertEqual(html.count("Top 50"), 6)
        self.assertNotIn("1050 Stock 50", html)
        self.assertIn('class="stock-table flow-ranking-table"', html)
        self.assertIn("max-height:720px", html)
        self.assertIn("background:#ffe0e5", html)
        self.assertIn("background:#dcfae6", html)

    def test_institutional_page_is_a_primary_navigation_destination(self):
        html = generate_site.nav_html("flow")
        self.assertIn('href="institutional-flow.html" class="tab active">法人排行</a>', html)

    def test_holder_page_renders_every_positive_row(self):
        rows = [
            {
                "security_id": str(2000 + index),
                "name": f"測試{index}",
                "market": "上市",
                "previous_date": "2026-07-24",
                "data_date": "2026-07-31",
                "previous_major_percent": 20.0,
                "major_percent": 20.1,
                "major_delta_pctpt": 0.1,
                "major_people": 10,
            }
            for index in range(60)
        ]
        payload = {
            "date": "2026-07-31",
            "previous_date": "2026-07-24",
            "rows": rows,
            "data_quality": {"state": "ok", "warnings": []},
            "freshness": {"status": "fresh"},
        }
        page = generate_site.build_weekly_holder_risers_page(payload)
        panel = generate_site.build_weekly_holder_risers_panel(payload)
        self.assertEqual(page.count("<tr data-holder-riser-row"), 60)
        self.assertIn("2059 測試59", page)
        self.assertIn("60 檔", panel)
        self.assertIn("holder-risers.html", panel)


if __name__ == "__main__":
    unittest.main()
