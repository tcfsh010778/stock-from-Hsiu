import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import refresh_prices
from official_price_refresh import CSV_FIELDS
from stock_v2_public.analysis.price_basis import PRICE_BASIS_MODE


class RefreshPricesMigrationPreflightTests(unittest.TestCase):
    def test_legacy_cache_fails_before_network_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            price_dir = root / "prices"
            price_dir.mkdir()
            (price_dir / "2330.csv").write_text(
                "date,open,high,low,close,volume\n2026-09-03,1,2,1,2,1000\n",
                encoding="utf-8",
            )
            with patch.object(refresh_prices, "LOCAL_PRICE_DIR", price_dir), patch.object(
                refresh_prices, "PRICE_BASIS_DIR", root / "price_basis"
            ), patch.object(refresh_prices, "collect_stock_ids", return_value=["2330"]), patch.object(
                refresh_prices, "refresh_official_prices"
            ) as network_refresh, patch.object(sys, "argv", ["refresh_prices.py"]):
                with self.assertRaisesRegex(RuntimeError, "requires --rebuild-history before network refresh"):
                    refresh_prices.main()
            network_refresh.assert_not_called()

    def test_verified_adjusted_cache_passes_preflight(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            price_dir = root / "prices"
            basis_dir = root / "price_basis"
            price_dir.mkdir()
            basis_dir.mkdir()
            (price_dir / "2330.csv").write_text(
                ",".join(CSV_FIELDS) + "\n" + ",".join(["2026-09-03"] + ["1"] * (len(CSV_FIELDS) - 1)) + "\n",
                encoding="utf-8",
            )
            (basis_dir / "2330.json").write_text(
                json.dumps({"mode": PRICE_BASIS_MODE, "verified": True}), encoding="utf-8"
            )
            refresh_prices.preflight_incremental_price_cache(
                {"2330"}, price_dir=price_dir, basis_dir=basis_dir
            )


if __name__ == "__main__":
    unittest.main()
