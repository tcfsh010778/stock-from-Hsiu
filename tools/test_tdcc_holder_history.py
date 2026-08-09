import unittest

import tdcc_holder_history


FORM_HTML = """
<form>
  <input name="SYNCHRONIZER_TOKEN" value="token-1">
  <input name="firDate" value="20260807">
  <select name="scaDate">
    <option value="20260807">2026/08/07</option>
    <option value="20260731">2026/07/31</option>
    <option value="20260724">2026/07/24</option>
    <option value="20260717">2026/07/17</option>
    <option value="20260709">2026/07/09</option>
    <option value="20260703">2026/07/03</option>
    <option value="20260626">2026/06/26</option>
  </select>
  <table>
    <tr><th>序</th><th>持股/單位數分級</th><th>人數</th><th>股數/單位數</th><th>占集保庫存數比例 (%)</th></tr>
    <tr><td>11</td><td>200,001-400,000</td><td>9</td><td>1</td><td>1.20</td></tr>
    <tr><td>12</td><td>400,001-600,000</td><td>10</td><td>1</td><td>2.10</td></tr>
    <tr><td>13</td><td>600,001-800,000</td><td>8</td><td>1</td><td>3.20</td></tr>
    <tr><td>14</td><td>800,001-1,000,000</td><td>6</td><td>1</td><td>4.30</td></tr>
    <tr><td>15</td><td>1,000,001以上</td><td>2</td><td>1</td><td>30.40</td></tr>
  </table>
</form>
"""


class FakeFetcher:
    def __init__(self, series):
        self.series = series
        self.calls = []

    def available_dates(self):
        return ["2026-08-07", "2026-07-31", "2026-07-24", "2026-07-17", "2026-07-09", "2026-07-03", "2026-06-26"]

    def fetch_many(self, security_ids, data_date, security_map, progress=None):
        codes = list(security_ids)
        self.calls.append((data_date, codes))
        rows = []
        for code in codes:
            item = self.series.get((data_date, code))
            if item is not None:
                rows.append({"security_id": code, **security_map[code], **item})
        return rows, len(codes) - len(rows)


class TdccHolderHistoryTests(unittest.TestCase):
    def test_parser_extracts_dates_token_and_400_plus_aggregate(self):
        parser = tdcc_holder_history.parse_query_page(FORM_HTML)
        aggregate = tdcc_holder_history.extract_major_aggregate(parser)
        self.assertEqual(parser.inputs["SYNCHRONIZER_TOKEN"], "token-1")
        self.assertEqual(parser.available_dates[:2], ["2026-08-07", "2026-07-31"])
        self.assertEqual(aggregate, {"major_percent": 40.0, "major_people": 26})

    def test_builder_ranks_latest_week_then_backfills_only_leading_candidates(self):
        codes = [f"{1100 + index}" for index in range(12)]
        security_map = {code: {"name": f"Stock {code}", "market": "listed"} for code in codes}
        latest_rows = [
            {"security_id": code, **security_map[code], "major_percent": 50.0 + (12 - index), "major_people": 20}
            for index, code in enumerate(codes)
        ]
        series = {}
        for index, code in enumerate(codes):
            series[("2026-07-31", code)] = {"major_percent": 50.0, "major_people": 19}
            for date in ["2026-07-24", "2026-07-17", "2026-07-09", "2026-07-03", "2026-06-26"]:
                series[(date, code)] = {"major_percent": 49.0, "major_people": 18}
        fetcher = FakeFetcher(series)
        archive = {
            "latest_date": "2026-08-07",
            "snapshots": [{"date": "2026-08-07", "rows": latest_rows}],
        }
        payload = tdcc_holder_history.build_latest_history(
            archive,
            security_map=security_map,
            fetcher=fetcher,
            ranking_limit=5,
        )
        meta = payload["history_backfill"]
        self.assertEqual(meta["selected_security_ids"], codes[:5])
        self.assertEqual(meta["ranking_limit"], 5)
        self.assertEqual(meta["universe_count"], 12)
        self.assertEqual(payload["latest_date"], "2026-08-07")
        self.assertEqual(len(payload["snapshots"]), 7)
        self.assertEqual(fetcher.calls[0], ("2026-07-31", codes))
        self.assertEqual({date for date, _ in fetcher.calls[1:]}, {"2026-07-24", "2026-07-17", "2026-07-09", "2026-07-03", "2026-06-26"})


if __name__ == "__main__":
    unittest.main()
