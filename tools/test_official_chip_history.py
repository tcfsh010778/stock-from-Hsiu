import unittest

import official_chip_history as sidecars


class OfficialChipHistoryTests(unittest.TestCase):
    def test_build_sidecars_preserves_net_and_balance_semantics(self):
        flow = {
            "date": "2026-09-04",
            "data_quality": {"state": "ok"},
            "institutional_history": {"snapshots": [
                {
                    "date": f"2026-08-{day:02d}" if day < 20 else "2026-09-04",
                    "rows_csv": "2330,l,1200,-300,50\n6488,o,-900,200,-10",
                }
                for day in range(1, 21)
            ]},
            "official_security_data": {"date": "2026-09-04", "margin_balance": [{
                "security_id": "2330", "market": "listed",
                "margin_balance_previous_lots": 100, "margin_balance_lots": 120,
                "short_balance_previous_lots": 5, "short_balance_lots": 4,
            }, {
                "security_id": "6488", "market": "otc",
                "margin_balance_previous_lots": 80, "margin_balance_lots": 75,
                "short_balance_previous_lots": 2, "short_balance_lots": 3,
            }]},
        }
        chip, margin = sidecars.build_sidecars(flow, min_partition_counts={"listed": 1, "otc": 1})
        self.assertEqual(chip["rows"][0]["dealer_net_shares"], 50)
        self.assertNotIn("buy", chip["rows"][0])
        self.assertEqual(next(row for row in margin["rows"] if row["security_id"] == "2330")["margin_balance_lots"], 120)
        self.assertNotIn("margin_buy", margin["rows"][0])

    def test_rejects_unverified_flow(self):
        with self.assertRaisesRegex(ValueError, "not a verified"):
            sidecars.build_sidecars({"date": "2026-09-04", "data_quality": {"state": "warning"}})


if __name__ == "__main__":
    unittest.main()
