import unittest
import json
import tempfile
from pathlib import Path

import tdcc_holder_snapshot


class TdccHolderSnapshotTests(unittest.TestCase):
    def test_security_map_excludes_etfs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "stock_markets.json"
            path.write_text(
                json.dumps({
                    "stocks": {
                        "0050": {"market": "上市", "name": "元大台灣50"},
                        "2330": {"market": "上市", "name": "台積電"},
                    },
                    "markets": {},
                }, ensure_ascii=False),
                encoding="utf-8",
            )
            security_map = tdcc_holder_snapshot.load_security_map(path)
        self.assertNotIn("0050", security_map)
        self.assertIn("2330", security_map)

    def test_aggregate_keeps_only_400_lot_tiers_and_known_markets(self):
        raw = [
            {"\ufeff資料日期": "20260807", "證券代號": "2330", "持股分級": "11", "人數": "9", "占集保庫存數比例%": "1.20"},
            {"\ufeff資料日期": "20260807", "證券代號": "2330", "持股分級": "12", "人數": "10", "占集保庫存數比例%": "2.10"},
            {"\ufeff資料日期": "20260807", "證券代號": "2330", "持股分級": "15", "人數": "2", "占集保庫存數比例%": "30.40"},
            {"\ufeff資料日期": "20260807", "證券代號": "0050", "持股分級": "15", "人數": "8", "占集保庫存數比例%": "40.00"},
        ]
        snapshot = tdcc_holder_snapshot.aggregate_snapshot(
            raw,
            {"2330": {"name": "台積電", "market": "listed"}},
        )
        self.assertEqual(snapshot["date"], "2026-08-07")
        self.assertEqual(len(snapshot["rows"]), 1)
        self.assertEqual(snapshot["rows"][0]["major_percent"], 32.5)
        self.assertEqual(snapshot["rows"][0]["major_people"], 12)

    def test_archive_replaces_same_date_without_duplication(self):
        first = {"date": "2026-08-07", "rows": [{"security_id": "2330", "major_percent": 30.0}]}
        second = {"date": "2026-08-07", "rows": [{"security_id": "2330", "major_percent": 31.0}]}
        payload = tdcc_holder_snapshot.merge_archive(first)
        payload = tdcc_holder_snapshot.merge_archive(second, payload)
        self.assertEqual(payload["snapshot_count"], 1)
        self.assertEqual(payload["snapshots"][0]["rows"][0]["major_percent"], 31.0)


if __name__ == "__main__":
    unittest.main()
