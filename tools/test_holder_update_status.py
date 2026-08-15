import unittest

from holder_update_status import append_check, finalize_run, official_source_date


class HolderUpdateStatusTests(unittest.TestCase):
    def test_official_source_date_requires_one_aligned_date(self):
        rows = [{"資料日期": "20260814"}, {"資料日期": "20260814"}]
        self.assertEqual(official_source_date(rows), "2026-08-14")
        with self.assertRaises(RuntimeError):
            official_source_date([{"資料日期": "20260807"}, {"資料日期": "20260814"}])

    def test_waiting_check_is_recorded_without_requesting_update(self):
        status, update_required = append_check(
            {},
            source_date="2026-08-07",
            published_date="2026-08-07",
            timestamp="2026-08-15T10:40:25+08:00",
            run_id="123",
            trigger="schedule",
            schedule="30 1 * * 6",
            url="https://example.test/run/123",
        )
        self.assertFalse(update_required)
        self.assertEqual(status["state"], "waiting_for_tdcc")
        self.assertEqual(status["attempts"][0]["official_date"], "2026-08-07")

    def test_new_official_date_is_published_and_finalized(self):
        status, update_required = append_check(
            {},
            source_date="2026-08-14",
            published_date="2026-08-07",
            timestamp="2026-08-16T09:30:00+08:00",
            run_id="456",
            trigger="schedule",
            schedule="30 1 * * 0",
            url="https://example.test/run/456",
        )
        self.assertTrue(update_required)
        finalized = finalize_run(status, "2026-08-14", "456")
        self.assertEqual(finalized["state"], "published")
        self.assertEqual(finalized["attempts"][0]["published_date_after"], "2026-08-14")


if __name__ == "__main__":
    unittest.main()
