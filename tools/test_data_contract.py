from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import data_contract


FIXTURE_PATH = ROOT / "tools" / "fixtures" / "data_contract" / "daily_price_rows.json"


class DataContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.registry = data_contract.load_registry()
        cls.rows = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def build_daily(self, **overrides):
        params = {
            "dataset_id": "daily_price",
            "source_id": "twse_daily_price",
            "rows": self.rows,
            "data_date": "2026-08-03",
            "trading_date": "2026-08-03",
            "expected_data_date": "2026-08-03",
            "fetched_at": "2026-08-03T18:00:00+08:00",
            "trading_sessions": ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"],
            "calendar_source_ids": ["twse_trading_calendar"],
            "registry": self.registry,
        }
        params.update(overrides)
        return data_contract.build_manifest(**params)

    def test_registry_contains_official_primary_and_visible_fallbacks(self) -> None:
        daily = self.registry["datasets"]["daily_price"]
        primary = {item["source_id"] for item in daily["primary_sources"]}
        fallback = {item["source_id"] for item in daily["fallback_sources"]}

        self.assertEqual(primary, {"twse_daily_price", "tpex_daily_price"})
        self.assertEqual(fallback, {"finmind_normalized_fallback"})
        self.assertTrue(self.registry["sources"]["twse_daily_price"]["official"])
        self.assertFalse(self.registry["sources"]["finmind_normalized_fallback"]["official"])

    def test_fresh_primary_manifest_has_required_metadata_hash_and_row_count(self) -> None:
        manifest = self.build_daily()

        self.assertEqual(manifest["freshness"]["status"], "fresh")
        self.assertEqual(manifest["row_count"], 2)
        self.assertEqual(manifest["sha256"], data_contract.sha256_rows(self.rows))
        self.assertEqual(manifest["missing"]["status"], "complete")
        self.assertFalse(manifest["fallback"]["used"])
        self.assertEqual(manifest["data_date"], "2026-08-03")
        self.assertEqual(manifest["trading_date"], "2026-08-03")
        self.assertEqual(manifest["fetched_at"], "2026-08-03T18:00:00+08:00")
        self.assertEqual(manifest["freshness"]["calendar_basis"], "official_trading_sessions")
        self.assertEqual(manifest["freshness"]["calendar_source_ids"], ["twse_trading_calendar"])

    def test_trading_day_lag_uses_explicit_holidays(self) -> None:
        without_holiday = data_contract.business_day_lag("2026-08-03", "2026-08-05")
        with_holiday = data_contract.business_day_lag("2026-08-03", "2026-08-05", holidays=["2026-08-04"])

        self.assertEqual(without_holiday, 2)
        self.assertEqual(with_holiday, 1)

    def test_one_trading_day_primary_lag_is_expected_lag(self) -> None:
        manifest = self.build_daily(expected_data_date="2026-08-04")

        self.assertEqual(manifest["freshness"]["status"], "expected_lag")
        self.assertEqual(manifest["freshness"]["age_trading_days"], 1)

    def test_primary_data_beyond_sla_is_stale(self) -> None:
        manifest = self.build_daily(expected_data_date="2026-08-06")

        self.assertEqual(manifest["freshness"]["status"], "stale")
        self.assertEqual(manifest["freshness"]["age_trading_days"], 3)

    def test_fallback_source_is_never_hidden(self) -> None:
        manifest = self.build_daily(
            source_id="finmind_normalized_fallback",
            fallback_from_source_id="twse_daily_price",
            fallback_reason="official endpoint temporarily unavailable",
        )

        self.assertEqual(manifest["source_tier"], "fallback")
        self.assertTrue(manifest["fallback"]["used"])
        self.assertEqual(manifest["fallback"]["from_source_id"], "twse_daily_price")
        self.assertEqual(manifest["freshness"]["status"], "fallback_fresh")

    def test_stale_fallback_is_explicit(self) -> None:
        manifest = self.build_daily(
            source_id="finmind_normalized_fallback",
            expected_data_date="2026-08-06",
            fallback_from_source_id="twse_daily_price",
            fallback_reason="official endpoint temporarily unavailable",
        )

        self.assertEqual(manifest["freshness"]["status"], "fallback_stale")

    def test_zero_rows_are_missing_not_fresh(self) -> None:
        manifest = self.build_daily(rows=[], data_date="", trading_date=None)

        self.assertEqual(manifest["row_count"], 0)
        self.assertEqual(manifest["missing"]["status"], "missing")
        self.assertEqual(manifest["freshness"]["status"], "missing")

    def test_required_field_loss_is_schema_error(self) -> None:
        rows = [dict(self.rows[0])]
        rows[0].pop("close")

        manifest = self.build_daily(rows=rows)

        self.assertEqual(manifest["freshness"]["status"], "schema_error")
        self.assertEqual(manifest["schema_validation"]["missing_required_fields"], ["close"])
        self.assertEqual(manifest["missing"]["status"], "partial")

    def test_required_field_loss_in_one_of_multiple_rows_is_schema_error(self) -> None:
        rows = copy.deepcopy(self.rows)
        rows[1].pop("volume")

        manifest = self.build_daily(rows=rows)

        self.assertEqual(manifest["freshness"]["status"], "schema_error")
        self.assertEqual(manifest["schema_validation"]["missing_required_field_rows"], {"volume": [1]})

    def test_trading_day_manifest_requires_official_calendar_provenance(self) -> None:
        with self.assertRaisesRegex(data_contract.ContractError, "official trading_sessions"):
            self.build_daily(trading_sessions=None, calendar_source_ids=[])

        with self.assertRaisesRegex(data_contract.ContractError, "official trading-calendar route"):
            self.build_daily(calendar_source_ids=["twse_daily_price"])

    def test_fallback_requires_failed_primary_and_reason(self) -> None:
        with self.assertRaisesRegex(data_contract.ContractError, "requires fallback_from_source_id"):
            self.build_daily(source_id="finmind_normalized_fallback")

    def test_manifest_validation_rejects_row_count_and_hash_mismatch(self) -> None:
        manifest = self.build_daily()
        wrong_count = copy.deepcopy(manifest)
        wrong_count["row_count"] = 99
        wrong_hash = copy.deepcopy(manifest)
        wrong_hash["sha256"] = "0" * 64

        with self.assertRaisesRegex(data_contract.ContractError, "row_count mismatch"):
            data_contract.validate_manifest(wrong_count, registry=self.registry, rows=self.rows)
        with self.assertRaisesRegex(data_contract.ContractError, "sha256 does not match"):
            data_contract.validate_manifest(
                wrong_hash,
                registry=self.registry,
                payload=data_contract.canonical_json_bytes(self.rows),
            )

    def test_weekly_tdcc_data_uses_calendar_lag(self) -> None:
        rows = [{
            "data_date": "2026-07-31",
            "security_id": "2330",
            "holding_level": "15",
            "holder_count": 123,
            "share_count": 456789,
            "custody_percent": 1.23,
        }]
        manifest = data_contract.build_manifest(
            "shareholder_distribution",
            "tdcc_shareholder_distribution",
            rows,
            data_date="2026-07-31",
            expected_data_date="2026-08-07",
            fetched_at="2026-08-08T10:00:00+08:00",
            registry=self.registry,
        )

        self.assertEqual(manifest["freshness"]["status"], "expected_lag")
        self.assertEqual(manifest["freshness"]["age_calendar_days"], 7)

    def test_manifest_file_upsert_is_structured_and_stable(self) -> None:
        manifest = self.build_daily()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "freshness_manifest.json"
            saved = data_contract.update_manifest_file(manifest, path, registry=self.registry)
            disk = json.loads(path.read_text(encoding="utf-8"))

        key = "daily_price:twse_daily_price"
        self.assertEqual(saved, disk)
        self.assertEqual(disk["manifest_schema_version"], "1.0.0")
        self.assertEqual(disk["artifacts"][key]["sha256"], manifest["sha256"])


if __name__ == "__main__":
    unittest.main()
