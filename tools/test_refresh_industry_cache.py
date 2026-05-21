from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from refresh_industry_cache import normalize_stock_info


class RefreshIndustryCacheTest(unittest.TestCase):
    def test_normalizes_finmind_stock_info_by_stock_id(self) -> None:
        rows = [
            {"stock_id": "2330", "stock_name": "台積電", "industry_category": "半導體業", "type": "twse", "date": "2026-01-01"},
            {"stock_id": "0050", "stock_name": "元大台灣50", "industry_category": "ETF", "type": "twse", "date": "2026-01-01"},
            {"stock_id": "", "stock_name": "missing", "industry_category": "其他", "type": "twse"},
        ]

        out = normalize_stock_info(rows)

        self.assertEqual(out["2330"]["industry_category"], "半導體業")
        self.assertEqual(out["2330"]["stock_name"], "台積電")
        self.assertEqual(out["0050"]["industry_category"], "ETF")
        self.assertNotIn("", out)


if __name__ == "__main__":
    unittest.main()
