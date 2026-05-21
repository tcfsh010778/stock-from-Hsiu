from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_screener


class SectorFilterTest(unittest.TestCase):
    def test_hot_sector_candidates_rank_before_equal_score_cold_sector(self) -> None:
        rows = [
            {"stock_id": "1111", "date": "2026-05-21", "close": 50, "score": 100, "sector": "Cold"},
            {"stock_id": "2222", "date": "2026-05-21", "close": 50, "score": 90, "sector": "Hot"},
            {"stock_id": "3333", "date": "2026-05-21", "close": 50, "score": 80, "sector": "Hot"},
        ]
        sector_scores = {
            "Hot": {"rank": 1, "score": 120.0},
            "Cold": {"rank": 9, "score": 10.0},
        }

        ranked = run_screener.rank_candidates_with_sector_flow(
            rows,
            sector_scores,
            top_n=3,
            sector_top_n=5,
            max_per_sector=5,
        )

        self.assertEqual([row["stock_id"] for row in ranked], ["2222", "3333", "1111"])

    def test_sector_cap_keeps_one_hot_group_from_crowding_out_the_list(self) -> None:
        rows = [
            {"stock_id": "1111", "date": "2026-05-21", "close": 50, "score": 100, "sector": "Hot"},
            {"stock_id": "2222", "date": "2026-05-21", "close": 50, "score": 99, "sector": "Hot"},
            {"stock_id": "3333", "date": "2026-05-21", "close": 50, "score": 98, "sector": "Hot"},
            {"stock_id": "4444", "date": "2026-05-21", "close": 50, "score": 70, "sector": "Warm"},
        ]
        sector_scores = {
            "Hot": {"rank": 1, "score": 120.0},
            "Warm": {"rank": 2, "score": 80.0},
        }

        ranked = run_screener.rank_candidates_with_sector_flow(
            rows,
            sector_scores,
            top_n=3,
            sector_top_n=5,
            max_per_sector=2,
        )

        self.assertEqual([row["stock_id"] for row in ranked], ["1111", "2222", "4444"])


if __name__ == "__main__":
    unittest.main()
