from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import collect_volume_profile as collector


def stamp(text: str) -> int:
    return int(datetime.fromisoformat(text).timestamp())


def payload(timestamps: list[int], *, symbol: str = "2330.TW") -> dict:
    n = len(timestamps)
    return {
        "chart": {"result": [{
            "meta": {"exchangeTimezoneName": "Asia/Taipei", "symbol": symbol},
            "timestamp": timestamps,
            "indicators": {"quote": [{
                "open": [100.0] * n, "high": [102.0] * n, "low": [99.0] * n,
                "close": [101.0] * n, "volume": [1000.0] * n,
            }]},
        }]},
    }


class FakeResponse:
    def __init__(self, raw: bytes, length: str | None = None):
        self.raw = raw
        self.headers = {} if length is None else {"Content-Length": length}

    def iter_content(self, chunk_size: int):
        yield self.raw


class CollectVolumeProfileTest(unittest.TestCase):
    def test_partial_coverage_metadata_and_exact_session(self) -> None:
        timestamps = [
            stamp("2026-09-03T09:00:00+08:00"),
            stamp("2026-09-03T09:10:00+08:00"),
            stamp("2026-09-03T13:30:00+08:00"),
            stamp("2026-09-03T13:35:00+08:00"),
        ]
        result = collector.summarize(payload(timestamps), "2330", symbol="2330.TW", now=datetime(2026, 9, 4, tzinfo=timezone.utc))
        self.assertTrue(result["partial"])
        self.assertEqual(result["observations_by_date"]["2026-09-03"], 3)
        self.assertEqual(result["observed_volume_by_date"]["2026-09-03"], 3000)
        self.assertEqual(result["observed_daily_ohlc"]["2026-09-03"], {"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 3000.0, "basis": "provider_available_bars_only"})
        self.assertEqual(result["skipped_bars"]["off_session"], 1)
        self.assertIn("09:05", result["missing_slots_by_date"]["2026-09-03"])
        self.assertEqual(result["last_timestamp_by_date"]["2026-09-03"], "13:30")
        self.assertAlmostEqual(sum(row["volume"] for row in result["rows"]), 3000)
        self.assertTrue(all(row["date"] <= result["data_date"] for row in result["rows"]))

    def test_null_and_unfinished_bars_are_counted(self) -> None:
        timestamps = [stamp("2026-09-03T09:00:00+08:00"), stamp("2026-09-03T09:05:00+08:00"), stamp("2026-09-03T09:10:00+08:00")]
        source = payload(timestamps)
        source["chart"]["result"][0]["indicators"]["quote"][0]["close"][0] = None
        result = collector.summarize(source, "2330", symbol="2330.TW", now=datetime.fromtimestamp(timestamps[2] + 100, timezone.utc))
        self.assertEqual(result["skipped_bars"], {"null": 1, "off_session": 0, "unfinished": 1})

    def test_rejects_symbol_and_array_mismatch(self) -> None:
        source = payload([stamp("2026-09-03T09:00:00+08:00")], symbol="2317.TW")
        with self.assertRaisesRegex(ValueError, "symbol mismatch"):
            collector.summarize(source, "2330", symbol="2330.TW")
        source = payload([stamp("2026-09-03T09:00:00+08:00")])
        source["chart"]["result"][0]["indicators"]["quote"][0]["volume"] = []
        with self.assertRaisesRegex(ValueError, "do not align"):
            collector.summarize(source, "2330", symbol="2330.TW")

    def test_bounded_json_enforces_header_and_stream_limits(self) -> None:
        with self.assertRaisesRegex(ValueError, "byte limit"):
            collector.bounded_json(FakeResponse(b"{}", "999"), max_bytes=10)
        with self.assertRaisesRegex(ValueError, "byte limit"):
            collector.bounded_json(FakeResponse(b"01234567890"), max_bytes=10)
        self.assertEqual(collector.bounded_json(FakeResponse(json.dumps({"ok": True}).encode()), max_bytes=100), {"ok": True})


if __name__ == "__main__":
    unittest.main()
