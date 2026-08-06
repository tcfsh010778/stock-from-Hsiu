from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import daily_decisions
import data_contract


TAIPEI = timezone(timedelta(hours=8))


def _fresh(dataset_id: str) -> dict:
    return {
        "dataset_id": dataset_id,
        "source_id": f"{dataset_id}_unit",
        "source_tier": "primary",
        "status": "fresh",
        "data_date": "2026-08-04",
        "expected_data_date": "2026-08-04",
        "row_count": 1,
    }


class DailyDecisionsTest(unittest.TestCase):
    def test_build_payload_maps_existing_evidence_to_action_states(self) -> None:
        mda_payload = {
            "date": "2026-08-04",
            "freshness": _fresh("mda_candidate_pool"),
            "stocks": [
                {
                    "rank": 1,
                    "stock_id": "2330",
                    "name": "TSMC",
                    "date": "2026-08-04",
                    "basket": "已發動籃",
                    "mda_basket": "已發動籃",
                    "status": "強勢追蹤",
                    "score": 92,
                    "entry": 100,
                    "target": 130,
                    "stop": 90,
                    "gain_6w": 12,
                    "pit_eligible": True,
                },
                {
                    "rank": 2,
                    "stock_id": "2454",
                    "name": "MTK",
                    "date": "2026-08-04",
                    "basket": "未發動觀察籃",
                    "status": "健康整理",
                    "score": 75,
                    "gain_6w": 6,
                },
                {
                    "rank": 3,
                    "stock_id": "6173",
                    "name": "Hot",
                    "date": "2026-08-04",
                    "basket": "已發動籃",
                    "status": "強勢追蹤",
                    "score": 90,
                    "gain_3d": 25,
                },
            ],
        }
        carybot_payload = {
            "date": "2026-08-04",
            "freshness": _fresh("carybot_signals"),
            "signals": [
                {"stock_id": "2330", "date": "2026-08-04", "signal_type": "B1", "score": 95, "is_current": True},
                {"stock_id": "2454", "date": "2026-08-04", "signal_type": "B2", "score": 78, "is_current": True},
            ],
        }
        traffic_inputs = {
            "2330": {
                "tech": {"trend": "多方", "volume_price": "量增價漲", "close": 120, "ma20": 110, "ma60": 100},
                "decision": {"rr": 2.5, "rr_text": "1:2.5"},
                "indicator": {"wr": -75, "k": 55, "macd_state": "買進", "kd_state": "強"},
                "chip_total_5d": 10,
            },
            "2454": {
                "tech": {"trend": "多方", "volume_price": "量縮價漲", "close": 90, "ma20": 85, "ma60": 80},
                "decision": {"rr": 2.2, "rr_text": "1:2.2"},
                "indicator": {"wr": -40, "k": 50, "macd_state": "買進", "kd_state": "強"},
                "chip_total_5d": 0,
            },
        }

        payload = daily_decisions.build_payload(
            mda_payload=mda_payload,
            carybot_payload=carybot_payload,
            now=datetime(2026, 8, 4, 20, 0, tzinfo=TAIPEI),
            traffic_inputs_by_stock=traffic_inputs,
        )
        by_stock = {row["stock_id"]: row for row in payload["decisions"]}

        self.assertEqual(by_stock["2330"]["action_state"], "ENTRY_CANDIDATE")
        self.assertTrue(by_stock["2330"]["entry"])
        self.assertEqual(by_stock["2330"]["traffic_light"]["state"], "GO")
        self.assertEqual(by_stock["2454"]["action_state"], "SETUP")
        self.assertEqual(by_stock["2454"]["traffic_light"]["state"], "WATCH")
        self.assertEqual(by_stock["6173"]["action_state"], "NO-GO")
        self.assertIn("forced_overheat", by_stock["6173"]["traffic_light"]["blockers"])
        self.assertEqual(payload["action_counts"]["ENTRY_CANDIDATE"], 1)
        self.assertEqual(payload["action_counts"]["SETUP"], 1)
        self.assertEqual(payload["action_counts"]["NO-GO"], 1)
        self.assertEqual(by_stock["2330"]["evidence"]["mda"]["pit_eligible"], True)

    def test_stale_source_freshness_becomes_visible_warning(self) -> None:
        payload = daily_decisions.build_payload(
            mda_payload={
                "date": "2026-08-04",
                "freshness": _fresh("mda_candidate_pool"),
                "stocks": [{"stock_id": "2330", "date": "2026-08-04", "score": 80}],
            },
            carybot_payload={
                "date": "2026-05-12",
                "freshness": {
                    "dataset_id": "carybot_signals",
                    "status": "fallback_stale",
                    "data_date": "2026-05-12",
                    "expected_data_date": "2026-08-04",
                    "row_count": 1,
                },
                "signals": [],
            },
        )

        self.assertEqual(payload["data_quality"]["state"], "warning")
        self.assertIn("carybot_signals freshness is fallback_stale", payload["data_quality"]["warnings"])
        self.assertIn("carybot_signals freshness is fallback_stale", payload["decisions"][0]["warnings"])

    def test_write_payload_attaches_freshness_and_hashes_exact_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            mda_path = root / "mda_candidates.json"
            carybot_path = root / "carybot_signals.json"
            output_path = root / "daily_decisions.json"
            manifest_path = root / "freshness_manifest.json"
            mda_path.write_text(
                json.dumps(
                    {
                        "date": "2026-08-04",
                        "stocks": [{"stock_id": "2330", "name": "TSMC", "date": "2026-08-04", "score": 80}],
                    }
                ),
                encoding="utf-8",
            )
            carybot_path.write_text(
                json.dumps(
                    {
                        "date": "2026-08-04",
                        "freshness": _fresh("carybot_signals"),
                        "signals": [{"stock_id": "2330", "date": "2026-08-04", "signal_type": "B2", "score": 76}],
                    }
                ),
                encoding="utf-8",
            )

            payload = daily_decisions.write_payload(
                output_path=output_path,
                mda_path=mda_path,
                carybot_path=carybot_path,
                freshness_manifest_path=root / "missing_manifest.json",
                manifest_path=manifest_path,
                now=datetime(2026, 8, 4, 20, 0, tzinfo=TAIPEI),
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))["artifacts"][
                "daily_decisions:daily_decisions_derived"
            ]
            output_digest = data_contract.sha256_bytes(output_path.read_bytes())

        self.assertEqual(payload["freshness"]["status"], "fresh")
        self.assertEqual(payload["freshness"]["source_id"], "daily_decisions_derived")
        self.assertEqual(payload["freshness"]["row_count"], 1)
        self.assertEqual(manifest["sha256"], output_digest)
        self.assertEqual(manifest["dataset_id"], "daily_decisions")


if __name__ == "__main__":
    unittest.main()
