from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import patch

from generate_v2 import add_public_workbench, analyze_stock_task, build_v2, load_market_evidence, load_verified_universe, safe_decision, switch_navigation, trim_packet
from stock_v2_public.site import stock_redirect_html


class PublicV2GenerationTests(unittest.TestCase):
    def _base(self, root):
        docs, data = root / "docs", root / "data"
        (docs / "v2" / "data").mkdir(parents=True)
        (docs / "v2" / "data" / "index.json").write_text('{"old":true}', encoding="utf-8")
        (data / "prices").mkdir(parents=True)
        (data / "price_basis").mkdir()
        (data / "price_refresh_summary.json").write_text(json.dumps({"status": "fresh", "latest_data_date": "2026-09-10"}), encoding="utf-8")
        return docs, data

    def _basis(self, data, sid="9999", **changes):
        meta = {"stock_id": sid, "mode": "reference_ratio_back_adjusted_mixed_sources_v1", "verified": True, "volume_basis": "raw_shares", "adjustment_as_of": "2026-09-10"}
        meta.update(changes)
        (data / "price_basis" / f"{sid}.json").write_text(json.dumps(meta), encoding="utf-8")
        (data / "prices" / f"{sid}.csv").write_text("date,open,high,low,close,volume\n2026-09-10,10,11,9,10,1000\n", encoding="utf-8")

    def test_verified_universe_accepts_new_mixed_mode_without_legacy_name(self):
        with tempfile.TemporaryDirectory() as folder:
            _, data = self._base(Path(folder)); self._basis(data)
            ids, failures = load_verified_universe(data)
            self.assertEqual(ids, {"9999"}); self.assertEqual(failures, [])

    def test_missing_basis_is_expected_exclusion_and_corrupt_claim_is_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            docs, data = self._base(Path(folder))
            (data / "prices" / "1111.csv").write_text("raw", encoding="utf-8")
            with patch("build_review_data.expected_session", return_value="2026-09-10"):
                result = build_v2(docs_dir=docs, data_dir=data)
            self.assertEqual(result["failure_count"], 0)
            status = json.loads((data / "v2_build_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["excluded_count"], 1)
        with tempfile.TemporaryDirectory() as folder:
            docs, data = self._base(Path(folder)); self._basis(data, volume_basis="wrong")
            result = build_v2(docs_dir=docs, data_dir=data)
            self.assertEqual(result["failure_count"], 1)

    def test_worker_failure_preserves_old_published_v2(self):
        with tempfile.TemporaryDirectory() as folder:
            docs, data = self._base(Path(folder)); self._basis(data)
            class InlineExecutor:
                def __init__(self, **kwargs): pass
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def map(self, function, tasks, chunksize=1):
                    for task in tasks: yield (task[0], task[1], None, "metadata corruption")
            with patch("generate_v2.concurrent.futures.ProcessPoolExecutor", InlineExecutor):
                result = build_v2(docs_dir=docs, data_dir=data)
            self.assertEqual(result["failure_count"], 1)
            self.assertEqual((docs / "v2" / "data" / "index.json").read_text(encoding="utf-8"), '{"old":true}')

    def test_wrong_refresh_summary_date_does_not_replace_published_v2(self):
        with tempfile.TemporaryDirectory() as folder:
            docs, data = self._base(Path(folder))
            (data / "price_refresh_summary.json").write_text(json.dumps({"status": "fresh", "latest_data_date": "2026-09-09"}), encoding="utf-8")
            with patch("build_review_data.expected_session", return_value="2026-09-10"):
                result = build_v2(docs_dir=docs, data_dir=data)
            self.assertFalse(result["release_ready"])
            status = json.loads((data / "v2_build_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["release_blocker"], "price_refresh_mismatch")
            self.assertEqual((docs / "v2" / "data" / "index.json").read_text(encoding="utf-8"), '{"old":true}')
    def test_uncovered_stock_is_not_ai_invented(self):
        decision = safe_decision("9999", None)
        self.assertEqual(decision["action_state"], "UNRATED")
        self.assertIn("不得由 AI 補寫", decision["blockers"][0])

    def test_packet_series_is_bounded(self):
        series = [{"date": f"2026-01-{day:02d}"} for day in range(1, 31)]
        packet = {
            "timeframe": "daily",
            "series": series,
            "candlestick_annotations": {"events": [{"bar_date": "2026-01-01"}, {"bar_date": "2026-01-30"}]},
            "patterns": list(range(30)),
            "trendlines": list(range(10)),
            "support_resistance": list(range(20)),
        }
        trim_packet(packet)
        self.assertEqual(len(packet["series"]), 30)
        self.assertEqual(len(packet["candlestick_annotations"]["events"]), 2)
        self.assertEqual(len(packet["patterns"]), 24)
        self.assertEqual(len(packet["trendlines"]), 8)
        self.assertEqual(len(packet["support_resistance"]), 12)

    def test_navigation_switch_is_scoped_and_idempotent(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "index.html"
            path.write_text('<a href="stocks/2353.html">A</a><a href="history.html">H</a>', encoding="utf-8")
            self.assertEqual(switch_navigation(path, {"2353"}), 1)
            self.assertEqual(switch_navigation(path, {"2353"}), 0)
            self.assertIn('href="v2/stocks/2353.html"', path.read_text(encoding="utf-8"))

    def test_redirect_keeps_legacy_page_separate(self):
        page = stock_redirect_html("2353")
        self.assertIn("../stock.html?id=2353", page)
        self.assertNotIn("OPENAI_API_KEY", page)

    def test_fixed_stop_is_exactly_fifteen_percent(self):
        packet = {"timeframe": "daily", "series": [{"date": "2026-08-07", "close": 30.25}]}
        add_public_workbench(packet)
        self.assertEqual(packet["risk_control"]["stop_loss_pct"], 15.0)
        self.assertEqual(packet["risk_control"]["stop_price"], 25.7125)
        self.assertNotIn("target", packet["risk_control"])

    def test_market_evidence_discloses_missing_inputs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name in ("chips", "foreign_shareholding", "margin", "holding_shares"):
                (root / name).mkdir()
            (root / "chips" / "2330.csv").write_text(
                "date,stock_id,buy,name,sell\n2026-08-07,2330,2000,Foreign_Investor,500\n",
                encoding="utf-8",
            )
            result = load_market_evidence(root, "2330")
            self.assertEqual(result["institutional"][0]["foreign"], 1.5)
            self.assertEqual(set(result["gaps"]), {"foreign_ownership", "margin", "holdings"})

    def test_stale_price_file_is_excluded_before_analysis(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "2353.csv"
            rows = ["date,open,high,low,close,volume"]
            rows.extend(f"2026-07-{day:02d},10,11,9,10,1000" for day in range(1, 31))
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            _, _, packets, error = analyze_stock_task(
                (
                    "2353",
                    "Acer",
                    str(path),
                    str(Path(folder)),
                    safe_decision("2353", None),
                    "fresh",
                    "2026-08-07",
                    [],
                    False,
                )
            )
            self.assertIsNone(packets)
            self.assertIn("stale OHLCV", error)


if __name__ == "__main__":
    unittest.main()
