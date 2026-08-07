from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import attention_disposition as risk
import data_contract


TAIPEI = timezone(timedelta(hours=8))


def _table(fields: list[str], rows: list[list[object]]) -> dict:
    return {"stat": "OK", "fields": fields, "data": rows}


def _empty_payloads() -> dict[str, dict]:
    return {
        "twse_attention": _table(["證券代號", "證券名稱", "注意交易資訊", "日期"], []),
        "twse_disposition": _table(["公布日期", "證券代號", "證券名稱", "累計", "處置條件", "處置起迄時間", "處置內容"], []),
        "twse_near_disposition": _table(["證券代號", "證券名稱", "近期達本公司「公布注意交易資訊」標準之情形"], []),
        "tpex_attention": _table(["證券代號", "證券名稱", "注意交易資訊", "公告日期"], []),
        "tpex_disposition": _table(["公布日期", "證券代號", "證券名稱", "累計", "處置起訖時間", "處置原因", "處置內容"], []),
        "tpex_near_disposition": _table(["證券代號", "證券名稱", "近期達本公司「公布注意交易資訊」標準之情形"], []),
    }


class AttentionDispositionTest(unittest.TestCase):
    def test_twse_transition_uses_revised_end_and_new_two_minute_interval(self) -> None:
        payloads = _empty_payloads()
        payloads["twse_disposition"] = _table(
            ["公布日期", "證券代號", "證券名稱", "累計", "處置條件", "處置起迄時間", "處置內容"],
            [[
                "115/08/05",
                "053859",
                "測試權證",
                "1",
                "連續三次",
                "115/08/06～115/08/19",
                "約每五分鐘撮合一次。修正其處置至一百十五年八月十二日止，並自一百十五年八月十日起改以約每二分鐘撮合一次。",
            ]],
        )

        snapshot = risk.collect_snapshot(
            payloads,
            target_date=date(2026, 8, 10),
            fetched_at=datetime(2026, 8, 10, 18, 0, tzinfo=TAIPEI),
        )
        row = snapshot["disposition"][0]

        self.assertEqual(snapshot["rule_version"], risk.CURRENT_RULE_VERSION)
        self.assertEqual(row["effective_end_date"], "2026-08-12")
        self.assertEqual(row["matching_interval_minutes"], 2)
        self.assertTrue(row["transition_revised"])
        self.assertTrue(row["active_on_data_date"])
        self.assertEqual(snapshot["risk_summary"][0]["risk_level"], "disposition")

    def test_tpex_transition_and_day_trade_extension_are_normalized(self) -> None:
        payloads = _empty_payloads()
        payloads["tpex_disposition"] = _table(
            ["公布日期", "證券代號", "證券名稱", "累計", "處置起訖時間", "處置原因", "處置內容"],
            [[
                "115/08/04",
                "3362",
                "先進光(../../mainboard/listed/company-detail.html?code=3362)",
                "3",
                "115/08/05~115/08/20",
                "連續5個營業日及沖銷標準",
                "最近5個營業日曾達第十三款，原12個營業日。爰修正其處置至115年8月13日止，並自115年8月10日起改以約每2分鐘撮合一次。",
            ], ["", "", "", "", "", "", "本日無處置資料"]],
        )

        snapshot = risk.collect_snapshot(payloads, target_date=date(2026, 8, 10))
        row = snapshot["disposition"][0]

        self.assertEqual(len(snapshot["disposition"]), 1)
        self.assertEqual(row["security_name"], "先進光")
        self.assertEqual(row["effective_end_date"], "2026-08-13")
        self.assertTrue(row["day_trade_trigger"])
        self.assertEqual(row["matching_interval_minutes"], 2)

    def test_attention_and_official_near_warning_have_explicit_priority(self) -> None:
        payloads = _empty_payloads()
        payloads["twse_attention"] = _table(
            ["證券代號", "證券名稱", "注意交易資訊", "日期"],
            [["2330", "台積電", "達注意標準", "115/08/06"]],
        )
        payloads["twse_near_disposition"] = _table(
            ["證券代號", "證券名稱", "近期達本公司「公布注意交易資訊」標準之情形"],
            [["2330", "台積電", "最近二個營業日連續達標準"]],
        )

        snapshot = risk.collect_snapshot(payloads, target_date=date(2026, 8, 6))
        summary = snapshot["risk_summary"][0]

        self.assertEqual(summary["risk_level"], "near_disposition")
        self.assertTrue(summary["attention"])
        self.assertTrue(summary["near_disposition"])
        self.assertFalse(summary["disposition"])

    def test_schema_loss_and_missing_partition_are_not_reported_as_safe(self) -> None:
        payloads = _empty_payloads()
        payloads["tpex_near_disposition"] = _table(["證券代號"], [])
        snapshot = risk.collect_snapshot(payloads, target_date=date(2026, 8, 6))

        artifact = next(row for row in snapshot["source_artifacts"] if row["source_id"] == "tpex_near_disposition")
        self.assertEqual(artifact["status"], "schema_error")
        self.assertEqual(snapshot["data_quality"]["state"], "partial")
        self.assertIn("tpex_near_disposition", snapshot["data_quality"]["missing_partitions"])

    def test_near_disposition_snapshot_date_mismatch_is_stale_not_relabelled(self) -> None:
        payloads = _empty_payloads()
        payloads["tpex_near_disposition"]["date"] = "20260806"
        payloads["tpex_near_disposition"]["data"] = [["6274", "台燿", "連續二個營業日"]]

        snapshot = risk.collect_snapshot(payloads, target_date=date(2026, 8, 5))
        artifact = next(row for row in snapshot["source_artifacts"] if row["source_id"] == "tpex_near_disposition")

        self.assertEqual(artifact["status"], "stale")
        self.assertEqual(snapshot["near_disposition"], [])
        self.assertIn("tpex_near_disposition", snapshot["data_quality"]["missing_partitions"])

    def test_write_snapshot_hashes_exact_bytes_and_marks_partial_manifest(self) -> None:
        payloads = _empty_payloads()
        payloads["twse_near_disposition"] = None
        snapshot = risk.collect_snapshot(
            payloads,
            target_date=date(2026, 8, 6),
            fetched_at=datetime(2026, 8, 6, 18, 0, tzinfo=TAIPEI),
            source_errors={"twse_near_disposition": "unit test outage"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "attention_disposition.json"
            manifest_path = Path(tmp) / "freshness_manifest.json"
            prepared = risk.write_snapshot(
                snapshot,
                output_path=output,
                manifest_path=manifest_path,
                trading_sessions=["2026-08-06"],
            )
            artifacts = json.loads(manifest_path.read_text(encoding="utf-8"))["artifacts"]
            manifest = artifacts[
                "attention_disposition_risk:attention_disposition_derived"
            ]

            self.assertEqual(manifest["sha256"], data_contract.sha256_bytes(output.read_bytes()))
            self.assertEqual(manifest["missing"]["status"], "partial")
            self.assertEqual(prepared["freshness"]["status"], "fresh")
            self.assertEqual(prepared["freshness"]["calendar_basis"], "official_trading_sessions")
            self.assertEqual(len(artifacts), 7)
            self.assertEqual(artifacts["near_disposition_risk:twse_near_disposition"]["freshness"]["status"], "missing")
            self.assertEqual(artifacts["attention_securities:twse_attention"]["freshness"]["status"], "fresh")

    def test_workflow_collects_market_risk_before_daily_decisions(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "daily_update.yml").read_text(encoding="utf-8")

        self.assertIn("python attention_disposition.py", workflow)
        self.assertLess(workflow.index("python attention_disposition.py"), workflow.index("python daily_decisions.py"))


if __name__ == "__main__":
    unittest.main()
