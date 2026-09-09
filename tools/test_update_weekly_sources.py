from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from tools.update_weekly_mda_pool import ARCHIVE_ID
from tools.update_weekly_sources import prepare, run


def rosters(report="1150909"):
    result = {}
    for market, count, start, date_key in (("listed", 800, 1000, "上市日期"), ("otc", 600, 5000, "上櫃日期")):
        rows = [{"出表日期": report, "公司代號": f"{start+i:04d}", "公司簡稱": f"短{i}", date_key: "2020/01/01"}
                for i in range(count)]
        result[market] = json.dumps(rows, ensure_ascii=False).encode()
    return result


def tdcc_rows(day="20260904"):
    rows = []
    for sid in [f"{1000+i:04d}" for i in range(800)] + [f"{5000+i:04d}" for i in range(600)]:
        for level in range(1, 18):
            pct = 100 if level == 17 else (2 if level in {12, 13, 14, 15} else 0)
            rows.append({"資料日期": day, "證券代號": sid, "持股分級": str(level),
                         "人數": "10", "股數": "1000", "占集保庫存數比例%": str(pct)})
    return rows


def prior():
    rows = ([{"security_id": f"{1000+i:04d}", "name": f"短{i}", "market": "listed", "major_percent": 7, "major_people": 40} for i in range(800)]
            + [{"security_id": f"{5000+i:04d}", "name": f"短{i}", "market": "otc", "major_percent": 7, "major_people": 40} for i in range(600)])
    return {"date": "2026-08-28", "row_count": 1400, "market_counts": {"listed": 800, "otc": 600}, "rows": rows}


class Public:
    def __init__(self): self.fetch_calls = 0
    def fetch_rows(self): self.fetch_calls += 1; return tdcc_rows()
    def aggregate_snapshot(self, raw, security_map):
        rows = []
        for sid, ref in sorted(security_map.items()):
            rows.append({"security_id": sid, "name": ref["name"], "market": ref["market"],
                         "major_percent": 8.0, "major_people": 40, "retail_200_percent": 0.0})
        return {"date": "2026-09-04", "rows": rows}
    def merge_archive(self, snapshot, existing):
        old = [x for x in existing.get("snapshots", []) if x.get("date") != snapshot["date"]]
        snaps = old + [snapshot]
        return {"dataset_id": "holder_weekly_snapshots", "latest_date": snapshot["date"], "snapshot_count": len(snaps), "snapshots": snaps}


def setup_data(path):
    path.mkdir()
    (path / "tdcc_compact_weekly_snapshots.json").write_text(json.dumps({"dataset_id": ARCHIVE_ID, "snapshots": [prior()]}), encoding="utf-8")


def test_one_command_fetches_once_and_builds_all_outputs(tmp_path):
    data = tmp_path / "data"; setup_data(data); raw_rosters = rosters(); calls = []
    def roster_fetch(url):
        calls.append(url); return raw_rosters["listed" if "twse" in url else "otc"]
    public = Public()
    outputs = run(data_dir=data, official_root=tmp_path, as_of="2026-09-10", public_module=public,
                  roster_fetch=roster_fetch, now=lambda: datetime(2026, 9, 10, tzinfo=timezone.utc))
    assert public.fetch_calls == 1 and len(calls) == 2
    assert set(outputs) == {"official_stock_universe.json", "tdcc_compact_weekly_snapshots.json",
                            "mda_weekly_top50.json", "holder_weekly_snapshots.json", "stock_markets.json"}
    assert all((data / name).exists() for name in outputs)
    markets = outputs["stock_markets.json"]
    assert markets["market_counts"] == {"listed": 800, "otc": 600}
    assert markets["stocks"]["5000"] == {"name": "短0", "market": "上櫃", "listing_date": "2020-01-01"}
    assert outputs["mda_weekly_top50.json"]["rows"][0]["delta_percentage_points"] == 1.0
    assert (outputs["tdcc_compact_weekly_snapshots.json"]["snapshots"][-1]["universe_source_sha256"]
            == hashlib.sha256((data / "official_stock_universe.json").read_bytes()).hexdigest())


@pytest.mark.parametrize("report,day,as_of,error", [
    ("1150901", "20260904", "2026-09-10", "not fresh"),
    ("1150909", "20260910", "2026-09-09", "newer than as-of"),
    ("1150903", "20260904", "2026-09-10", "predates TDCC"),
])
def test_date_failures_happen_before_any_write(tmp_path, report, day, as_of, error):
    data = tmp_path / "data"; setup_data(data); before = {p.name: p.read_bytes() for p in data.iterdir()}
    raw_rosters = rosters(report)
    with pytest.raises(ValueError, match=error):
        run(data_dir=data, official_root=tmp_path, as_of=as_of, public_module=Public(),
            tdcc_fetch=lambda: tdcc_rows(day),
            roster_fetch=lambda url: raw_rosters["listed" if "twse" in url else "otc"])
    assert {p.name: p.read_bytes() for p in data.iterdir()} == before


def test_legacy_coverage_mismatch_and_roster_date_mismatch_fail_closed(tmp_path):
    data = tmp_path / "data"; setup_data(data); raw_rosters = rosters()
    public = Public()
    original_aggregate = public.aggregate_snapshot
    def bad_aggregate(raw, security_map):
        value = original_aggregate(raw, security_map); value["rows"].pop(); return value
    public.aggregate_snapshot = bad_aggregate
    with pytest.raises(ValueError, match="coverage differs"):
        prepare(data_dir=data, official_root=tmp_path, as_of="2026-09-10", public_module=public,
                roster_fetch=lambda url: raw_rosters["listed" if "twse" in url else "otc"])
    mismatched = rosters(); otc = json.loads(mismatched["otc"])
    for row in otc: row["出表日期"] = "1150908"
    mismatched["otc"] = json.dumps(otc).encode()
    with pytest.raises(ValueError, match="report dates differ"):
        prepare(data_dir=data, official_root=tmp_path, as_of="2026-09-10", public_module=Public(),
                roster_fetch=lambda url: mismatched["listed" if "twse" in url else "otc"])


def test_mixed_tdcc_dates_are_rejected_before_writes(tmp_path):
    data = tmp_path / "data"; setup_data(data); before = {p.name: p.read_bytes() for p in data.iterdir()}
    mixed = tdcc_rows(); mixed[-1]["資料日期"] = "20260828"; raw_rosters = rosters()
    with pytest.raises(ValueError, match="one distinct date"):
        run(data_dir=data, official_root=tmp_path, as_of="2026-09-10", public_module=Public(),
            tdcc_fetch=lambda: mixed,
            roster_fetch=lambda url: raw_rosters["listed" if "twse" in url else "otc"])
    assert {p.name: p.read_bytes() for p in data.iterdir()} == before
