import json
import tempfile
import unittest
from pathlib import Path

import weekly_holder_risers


class WeeklyHolderRisersTests(unittest.TestCase):
    def test_build_rows_returns_only_positive_major_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            holding = root / "holding_shares"
            holding.mkdir()
            dates = ["2026-06-19", "2026-06-26", "2026-07-03", "2026-07-10", "2026-07-17", "2026-07-24", "2026-07-31"]
            lines_2330 = ["date,stock_id,HoldingSharesLevel,people,percent,unit"]
            lines_2331 = ["date,stock_id,HoldingSharesLevel,people,percent,unit"]
            for index, data_date in enumerate(dates):
                lines_2330.append(f'{data_date},2330,"400,001-600,000",10,{52.0 if index == 6 else 50.0},%')
                lines_2331.append(f'{data_date},2331,"400,001-600,000",10,{20.0 if index == 6 else 21.0},%')
            (holding / "2330.csv").write_text("\n".join(lines_2330) + "\n", encoding="utf-8")
            (holding / "2331.csv").write_text("\n".join(lines_2331) + "\n", encoding="utf-8")
            rows = weekly_holder_risers.build_rows(
                holding_dir=holding,
                market_map={"2330": {"name": "台積電", "market": "上市"}, "2331": {"name": "假想股", "market": "上櫃"}},
                snapshot_path=root / "missing.json",
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["security_id"], "2330")
        self.assertEqual(rows[0]["major_delta_pctpt"], 2.0)
        self.assertEqual(rows[0]["market"], "上市")

    def test_default_output_is_not_capped_at_fifty(self):
        with tempfile.TemporaryDirectory() as tmp:
            holding = Path(tmp) / "holding_shares"
            holding.mkdir()
            market_map = {}
            dates = ["2026-06-19", "2026-06-26", "2026-07-03", "2026-07-10", "2026-07-17", "2026-07-24", "2026-07-31"]
            for index in range(55):
                stock_id = str(1000 + index)
                lines = ["date,stock_id,HoldingSharesLevel,people,percent,unit"]
                for date_index, data_date in enumerate(dates):
                    lines.append(f'{data_date},{stock_id},"400,001-600,000",10,{20.1 if date_index == 6 else 20.0},%')
                (holding / f"{stock_id}.csv").write_text(
                    "\n".join(lines) + "\n",
                    encoding="utf-8",
                )
                market_map[stock_id] = {"name": f"測試{index}", "market": "上市"}
            rows = weekly_holder_risers.build_rows(holding_dir=holding, market_map=market_map, snapshot_path=Path(tmp) / "missing.json")
        self.assertEqual(len(rows), 55)

    def test_six_week_changes_are_aligned_and_summed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            holding = root / "holding_shares"
            holding.mkdir()
            dates = ["2026-06-26", "2026-07-03", "2026-07-10", "2026-07-17", "2026-07-24", "2026-07-31", "2026-08-07"]
            majors = [20.0, 20.5, 20.2, 21.0, 21.4, 21.3, 22.0]
            lines = ["date,stock_id,HoldingSharesLevel,people,percent,unit"]
            for data_date, percent in zip(dates, majors):
                lines.append(f'{data_date},2330,"400,001-600,000",10,{percent},%')
            (holding / "2330.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
            rows = weekly_holder_risers.build_rows(
                holding_dir=holding,
                market_map={"2330": {"name": "台積電", "market": "listed"}},
                snapshot_path=root / "missing.json",
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual([item["delta_pctpt"] for item in rows[0]["weekly_changes"]], [0.5, -0.3, 0.8, 0.4, -0.1, 0.7])
        self.assertEqual(rows[0]["six_week_delta_pctpt"], 2.0)
        self.assertEqual(rows[0]["positive_week_count"], 4)
        self.assertTrue(rows[0]["six_week_complete"])

    def test_multi_week_gap_is_not_reported_as_one_week_change(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            holding_dir = Path(temp_dir)
            (holding_dir / "2330.csv").write_text(
                'date,stock_id,HoldingSharesLevel,people,percent,unit\n'
                '2026-06-18,2330,"400,001-600,000",10,20.0,%\n'
                '2026-08-07,2330,"400,001-600,000",10,25.0,%\n',
                encoding="utf-8",
            )
            rows = weekly_holder_risers.build_rows(
                holding_dir=holding_dir,
                market_map={"2330": {"name": "台積電", "market": "listed"}},
                snapshot_path=holding_dir / "missing.json",
            )
        self.assertEqual(rows, [])

    def test_newer_partial_run_does_not_hide_latest_complete_six_weeks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            holding_dir = Path(temp_dir)
            dates = ["2026-05-08", "2026-05-15", "2026-05-22", "2026-05-29", "2026-06-05", "2026-06-12", "2026-06-18"]
            lines = ["date,stock_id,HoldingSharesLevel,people,percent,unit"]
            for index, data_date in enumerate(dates):
                lines.append(f'{data_date},2330,"400,001-600,000",10,{20 + index * 0.1},%')
            (holding_dir / "2330.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
            snapshot_path = holding_dir / "snapshots.json"
            snapshot_path.write_text(json.dumps({
                "snapshots": [{
                    "date": "2026-08-07",
                    "rows": [{"security_id": "2330", "name": "台積電", "market": "listed", "major_percent": 30.0, "major_people": 12}],
                }],
            }, ensure_ascii=False), encoding="utf-8")
            rows = weekly_holder_risers.build_rows(
                holding_dir=holding_dir,
                market_map={"2330": {"name": "台積電", "market": "listed"}},
                snapshot_path=snapshot_path,
            )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["data_date"], "2026-06-18")
        self.assertTrue(rows[0]["six_week_complete"])


if __name__ == "__main__":
    unittest.main()
