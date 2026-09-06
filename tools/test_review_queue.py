import unittest

from review_queue import build_review_queue


def payload(route, rows, *, date="2026-09-04", quality="ok", version="v1"):
    return {"dataset_id": route, "data_date": date, "quality": {"state": quality}, "rule_version": version, "stocks": rows}


class ReviewQueueTests(unittest.TestCase):
    def test_union_deduplicates_stock_and_preserves_route_conflict(self):
        sfz = payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout", "reason": "突破"}])
        mda = payload("mda", [{"stock_id": "2330", "candidate": False, "basket": "等待", "reason": "長期趨勢未完成"}])
        result = build_review_queue(sfz, mda, as_of="2026-09-04")
        self.assertEqual(len(result["stocks"]), 1)
        self.assertEqual(result["stocks"][0]["route_ids"], ["mda", "sfz"])
        self.assertEqual(result["stocks"][0]["conflicts"], [])
        self.assertEqual(result["stocks"][0]["route_differences"], ["candidate"])
        self.assertNotIn("score", result["stocks"][0])

    def test_mda_route_has_no_sfz_dependency(self):
        result = build_review_queue(payload("sfz", []), payload("mda", [{"stock_id": "6488", "candidate": True, "basket": "行進", "reason": "長多"}]), as_of="2026-09-04")
        self.assertEqual(result["stocks"][0]["route_ids"], ["mda"])

    def test_first_run_and_same_date_rerun_are_baselines(self):
        sfz = payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout"}])
        first = build_review_queue(sfz, payload("mda", []), as_of="2026-09-04")
        rerun = build_review_queue(sfz, payload("mda", []), first, as_of="2026-09-04")
        self.assertEqual(first["alerts"], [])
        self.assertEqual(rerun["alerts"], [])

    def test_meaningful_events_after_baseline_ignore_daily_float_noise(self):
        first = build_review_queue(
            payload("sfz", [{"stock_id": "2330", "candidate": False, "stage": "watch", "metrics": {"close": 100.1}}]),
            payload("mda", [{"stock_id": "6488", "candidate": True, "basket": "等待", "evidence": [{"id": "chip", "data_date": "2026-09-04", "metrics": {"foreign_5d": 10, "close": 50}}]}]),
            as_of="2026-09-04",
        )
        second = build_review_queue(
            payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout", "metrics": {"close": 101.7}}], date="2026-09-05"),
            payload("mda", [{"stock_id": "6488", "candidate": True, "basket": "等待", "evidence": [{"id": "chip", "data_date": "2026-09-05", "metrics": {"foreign_5d": 20, "close": 55}}]}], date="2026-09-05"),
            first, as_of="2026-09-05",
        )
        self.assertEqual({item["event_type"] for item in second["alerts"]}, {"first_qualified", "mda_chip_changed"})

    def test_missing_source_does_not_invalidate_previous_route(self):
        first = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout"}]), payload("mda", []), as_of="2026-09-04")
        second = build_review_queue(payload("sfz", [], date="2026-09-05", quality="missing"), payload("mda", [], date="2026-09-05"), first, as_of="2026-09-05")
        card = second["stocks"][0]
        self.assertTrue(card["candidate"])
        self.assertIn("current_source_unavailable", card["missing"])
        self.assertEqual(second["alerts"], [])

    def test_rule_change_rebaselines_and_future_previous_is_rejected(self):
        first = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": False}]), payload("mda", []), as_of="2026-09-05")
        changed = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True}], date="2026-09-06", version="v2"), payload("mda", [], date="2026-09-06"), first, as_of="2026-09-06")
        self.assertEqual(changed["alerts"], [])
        with self.assertRaisesRegex(ValueError, "later than as_of"):
            build_review_queue(payload("sfz", []), payload("mda", []), first, as_of="2026-09-04")

    def test_blocked_legacy_pool_is_not_an_alert_baseline(self):
        blocked = build_review_queue(
            payload("sfz", [], quality="blocked"),
            payload("mda", [{"stock_id": "2330", "candidate": True, "basket": "行進"}], quality="blocked"),
            as_of="2026-09-04",
        )
        fresh = build_review_queue(
            payload("sfz", [], date="2026-09-05"),
            payload("mda", [{"stock_id": "2330", "candidate": True, "basket": "行進"}], date="2026-09-05"),
            blocked,
            as_of="2026-09-05",
        )
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(fresh["alerts"], [])

    def test_mda_reason_and_undated_metric_changes_do_not_alert(self):
        first = build_review_queue(
            payload("sfz", []),
            payload("mda", [{"stock_id": "2330", "candidate": True, "reason": "文字 A", "evidence": [{"id": "chip", "metrics": {"foreign_5d": 1}}]}]),
            as_of="2026-09-04",
        )
        second = build_review_queue(
            payload("sfz", [], date="2026-09-05"),
            payload("mda", [{"stock_id": "2330", "candidate": True, "reason": "文字 B", "evidence": [{"id": "chip", "metrics": {"foreign_5d": 999}}]}], date="2026-09-05"),
            first,
            as_of="2026-09-05",
        )
        self.assertEqual(second["alerts"], [])

    def test_unknown_quality_is_blocked_and_future_dates_rejected(self):
        unknown = {"dataset_id": "sfz", "data_date": "2026-09-04", "stocks": [{"stock_id": "2330", "candidate": True}]}
        result = build_review_queue(unknown, payload("mda", []), as_of="2026-09-04")
        self.assertFalse(result["quality"]["sfz"]["alert_eligible"])
        stale = build_review_queue(payload("sfz", [], date="2026-09-03"), payload("mda", []), as_of="2026-09-04")
        self.assertFalse(stale["quality"]["sfz"]["alert_eligible"])
        with self.assertRaisesRegex(ValueError, "later than as_of"):
            build_review_queue(payload("sfz", [], date="2026-09-05"), payload("mda", []), as_of="2026-09-04")

    def test_display_preserves_name_conflicts_and_price_metrics(self):
        sfz = payload("sfz", [{
            "stock_id": "2330", "name": "台積電", "candidate": True,
            "conflicts": ["source_row_conflict"],
            "evidence": [{"id": "setup", "data_date": "2026-09-04", "metrics": {"close": 100.5, "score": 7}}],
        }])
        card = build_review_queue(sfz, payload("mda", []), as_of="2026-09-04")["stocks"][0]
        self.assertEqual(card["name"], "台積電")
        self.assertEqual(card["conflicts"], ["source_row_conflict"])
        self.assertEqual(card["evidence"][0]["metrics"]["close"], 100.5)

    def test_sfz_actual_stage_codes_emit_events_and_same_day_retains_them(self):
        first = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "box_forming"}]), payload("mda", []), as_of="2026-09-04")
        second = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout_wait_retest"}], date="2026-09-05"), payload("mda", [], date="2026-09-05"), first, as_of="2026-09-05")
        self.assertEqual(second["alerts"][0]["event_type"], "sfz_breakout")
        rerun = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout_wait_retest"}], date="2026-09-05"), payload("mda", [], date="2026-09-05"), second, as_of="2026-09-05")
        self.assertEqual(rerun["alerts"], second["alerts"])

    def test_changed_input_same_day_compares_with_start_of_day_baseline(self):
        start = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "box_forming"}]), payload("mda", []), as_of="2026-09-04")
        changed = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "retest_confirmed"}]), payload("mda", []), start, as_of="2026-09-04")
        self.assertEqual(changed["alerts"][0]["event_type"], "sfz_retest")

    def test_new_stock_after_valid_route_baseline_alerts(self):
        first = build_review_queue(payload("sfz", []), payload("mda", []), as_of="2026-09-04")
        second = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout_wait_retest"}], date="2026-09-05"), payload("mda", [], date="2026-09-05"), first, as_of="2026-09-05")
        self.assertEqual(second["alerts"][0]["event_type"], "first_qualified")

    def test_no_setup_expiry_and_row_missing_do_not_invalidate(self):
        first = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": True, "stage": "breakout_wait_retest"}]), payload("mda", []), as_of="2026-09-04")
        expired = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": False, "stage": "no_setup"}], date="2026-09-05"), payload("mda", [], date="2026-09-05"), first, as_of="2026-09-05")
        self.assertEqual(expired["alerts"], [])
        missing = build_review_queue(payload("sfz", [{"stock_id": "2330", "candidate": False, "stage": "invalidated", "missing": ["price_history"]}], date="2026-09-06"), payload("mda", [], date="2026-09-06"), expired, as_of="2026-09-06")
        self.assertEqual(missing["alerts"], [])
        self.assertEqual(missing["stocks"][0]["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
