from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from verify_v2_public import verify_price_freshness


class VerifyV2PublicTest(unittest.TestCase):
    def test_accepts_matching_fresh_official_price_date(self) -> None:
        date = verify_price_freshness(
            {"price_refresh_status": "fresh", "price_data_date": "2026-08-07"},
            {"status": "fresh", "latest_data_date": "2026-08-07"},
        )

        self.assertEqual(date, "2026-08-07")

    def test_rejects_manifest_price_date_mismatch(self) -> None:
        with self.assertRaisesRegex(AssertionError, "price date mismatch"):
            verify_price_freshness(
                {"price_refresh_status": "fresh", "price_data_date": "2026-06-26"},
                {"status": "fresh", "latest_data_date": "2026-08-07"},
            )


if __name__ == "__main__":
    unittest.main()
