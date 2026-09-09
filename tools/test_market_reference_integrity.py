"""Regression tests for the two-market reference cache in daily-public."""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "generate_site.py"
SPEC = importlib.util.spec_from_file_location("daily_public_generate_site", SOURCE)
site = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(site)


def stock_map(listed=800, otc=600):
    result = {}
    for index in range(listed):
        result[f"{1000 + index:04d}"] = {"market": "上市", "name": f"L{index}"}
    for index in range(otc):
        result[f"{5000 + index:04d}"] = {"market": "上櫃", "name": f"O{index}"}
    return result


def endpoint_rows(market, count):
    if market == "上市":
        return [{"Code": f"{1000 + i:04d}", "Name": f"L{i}"} for i in range(count)]
    return [{"SecuritiesCompanyCode": f"{5000 + i:04d}", "CompanyName": f"O{i}"} for i in range(count)]


class MarketReferenceIntegrityTests(unittest.TestCase):
    def test_complete_reference_requires_both_market_minimums(self):
        self.assertTrue(site.complete_market_reference(stock_map(800, 600)))
        self.assertFalse(site.complete_market_reference(stock_map(799, 600)))
        self.assertFalse(site.complete_market_reference(stock_map(800, 599)))

    def test_partial_fetch_preserves_old_complete_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = Path(folder) / "stock_markets.json"
            old = {"updated_at": (datetime.now() - timedelta(days=2)).isoformat(), "stocks": stock_map()}
            original = json.dumps(old, ensure_ascii=False)
            cache.write_text(original, encoding="utf-8")
            with patch.object(site, "MARKET_CACHE_PATH", cache), patch.object(site, "LOCAL_DATA_DIR", cache.parent), patch.object(
                site, "_fetch_json", side_effect=[endpoint_rows("上市", 800), RuntimeError("TPEx unavailable")]
            ):
                result = site.load_stock_reference_map()
            self.assertEqual(result, old["stocks"])
            self.assertEqual(cache.read_text(encoding="utf-8"), original)
            self.assertFalse(cache.with_suffix(".json.tmp").exists())

    def test_incomplete_cache_and_one_endpoint_failure_raises(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = Path(folder) / "stock_markets.json"
            cache.write_text(json.dumps({"updated_at": "2026-01-01T00:00:00", "stocks": stock_map(800, 100)}), encoding="utf-8")
            with patch.object(site, "MARKET_CACHE_PATH", cache), patch.object(site, "LOCAL_DATA_DIR", cache.parent), patch.object(
                site, "_fetch_json", side_effect=[endpoint_rows("上市", 800), RuntimeError("TPEx unavailable")]
            ):
                with self.assertRaisesRegex(RuntimeError, "both-market"):
                    site.load_stock_reference_map()

    def test_full_fetch_atomically_replaces_cache(self):
        with tempfile.TemporaryDirectory() as folder:
            cache = Path(folder) / "stock_markets.json"
            cache.write_text(json.dumps({"updated_at": "2026-01-01T00:00:00", "stocks": {}}), encoding="utf-8")
            with patch.object(site, "MARKET_CACHE_PATH", cache), patch.object(site, "LOCAL_DATA_DIR", cache.parent), patch.object(
                site, "_fetch_json", side_effect=[endpoint_rows("上市", 800), endpoint_rows("上櫃", 600)]
            ):
                result = site.load_stock_reference_map()
            self.assertTrue(site.complete_market_reference(result))
            payload = json.loads(cache.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["stocks"]), 1400)
            self.assertEqual(payload["markets"]["1000"], "上市")
            self.assertEqual(payload["markets"]["5000"], "上櫃")
            self.assertFalse(cache.with_suffix(".json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
