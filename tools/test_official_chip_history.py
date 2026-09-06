import unittest

import official_chip_history as sidecars


class OfficialChipHistoryTests(unittest.TestCase):
    def test_build_sidecars_preserves_net_and_balance_semantics(self):
        flow = {
            "date": "2026-09-04",
            "data_quality": {"state": "ok"},
            "institutional_history": {"snapshots": [{
                "date": "2026-09-04",
                "rows_csv": "2330,l,1200,-300,50\n6488,o,-900,200,-10",
            }]},
            "official_security_data": {"margin_balance": [{
                "security_id": "2330", "market": "listed",
                "margin_balance_previous_lots": 100, "margin_balance_lots": 120,
                "short_balance_previous_lots": 5, "short_balance_lots": 4,
            }]},
        }
        chip, margin = sidecars.build_sidecars(flow)
        self.assertEqual(chip["rows"][0]["dealer_net_shares"], 50)
        self.assertNotIn("buy", chip["rows"][0])
        self.assertEqual(margin["rows"][0]["margin_balance_lots"], 120)
        self.assertNotIn("margin_buy", margin["rows"][0])

    def test_rejects_unverified_flow(self):
        with self.assertRaisesRegex(ValueError, "not a verified"):
            sidecars.build_sidecars({"date": "2026-09-04", "data_quality": {"state": "warning"}})


if __name__ == "__main__":
    unittest.main()
