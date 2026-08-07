import json
import tempfile
import unittest
from pathlib import Path

import weekly_holder_risers


class WeeklyHolderRisersTests(unittest.TestCase):
    def test_build_rows_returns_only_positive_major_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            holding = root / "holding_shares"
            holding.mkdir()
            (holding / "2330.csv").write_text(
                "date,stock_id,HoldingSharesLevel,people,percent,unit\n"
                "2026-07-24,2330,400,001-600,000,10,20.0,%\n"
                "2026-07-24,2330,more than 1,000,001,2,30.0,%\n"
                "2026-07-31,2330,400,001-600,000,11,21.0,%\n"
                "2026-07-31,2330,more than 1,000,001,3,31.0,%\n",
                encoding="utf-8",
            )
            # CSV commas in labels are quoted in the real cache; replace with a
            # clean fixture that still exercises both major tiers.
            (holding / "2330.csv").write_text(
                'date,stock_id,HoldingSharesLevel,people,percent,unit\n'
                '2026-07-24,2330,"400,001-600,000",10,20.0,%\n'
                '2026-07-24,2330,"more than 1,000,001",2,30.0,%\n'
                '2026-07-31,2330,"400,001-600,000",11,21.0,%\n'
                '2026-07-31,2330,"more than 1,000,001",3,31.0,%\n',
                encoding="utf-8",
            )
            (holding / "2331.csv").write_text(
                'date,stock_id,HoldingSharesLevel,people,percent,unit\n'
                '2026-07-24,2331,"400,001-600,000",10,21.0,%\n'
                '2026-07-31,2331,"400,001-600,000",10,20.0,%\n',
                encoding="utf-8",
            )
            rows = weekly_holder_risers.build_rows(
                holding_dir=holding,
                market_map={"2330": {"name": "台積電", "market": "上市"}, "2331": {"name": "假想股", "market": "上櫃"}},
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["security_id"], "2330")
        self.assertEqual(rows[0]["major_delta_pctpt"], 2.0)
        self.assertEqual(rows[0]["market"], "上市")


if __name__ == "__main__":
    unittest.main()
