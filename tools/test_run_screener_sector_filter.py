from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run_screener


class SectorFilterTest(unittest.TestCase):
    def test_sfz_all_payload_keeps_all_latest_candidates_and_ranking_metadata(self) -> None:
        rows = [
            {"stock_id": "1111", "date": "2026-05-29", "close": 50, "score": 100, "sector": "Hot"},
            {"stock_id": "2222", "date": "2026-05-29", "close": 80, "score": 90, "sector": "Warm"},
            {"stock_id": "3333", "date": "2026-05-28", "close": 60, "score": 80, "sector": "Old"},
        ]
        sector_scores = {
            "Hot": {"rank": 1, "score": 120.0, "turnover_billion": 30.0},
            "Warm": {"rank": 2, "score": 80.0, "turnover_billion": 10.0},
        }

        payload = run_screener.build_sfz_all_payload(rows, {}, sector_scores)

        self.assertEqual(payload["date"], "2026-05-29")
        self.assertEqual(payload["count"], 2)
        self.assertEqual([s["stock_id"] for s in payload["stocks"]], ["1111", "2222"])
        self.assertEqual(payload["stocks"][0]["rank"], 1)
        self.assertEqual(payload["stocks"][0]["sector_rank"], 1)

    def test_select_top20_still_keeps_daily_report_capped(self) -> None:
        rows = [
            {
                "stock_id": f"{1000 + idx}",
                "date": "2026-05-29",
                "close": 50,
                "score": 100 - idx,
                "sector": "Hot",
            }
            for idx in range(25)
        ]

        ranked = run_screener.select_top20(rows, {}, {})

        self.assertEqual(len(ranked), run_screener.TOP_N)
        self.assertEqual(ranked[0]["stock_id"], "1000")

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
