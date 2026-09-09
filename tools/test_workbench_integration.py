import json

from generate_v2 import _prepared_market_evidence, load_market_evidence
from tools.test_official_workbench import flow, weekly


def test_generator_reads_official_sources_and_invalidates_shared_cache(tmp_path):
    flow_path = tmp_path / "daily_market_flow.json"
    flow_path.write_text(json.dumps(flow()), encoding="utf-8")
    (tmp_path / "tdcc_compact_weekly_snapshots.json").write_text(json.dumps(weekly()), encoding="utf-8")
    _prepared_market_evidence.cache_clear()
    listed = load_market_evidence(tmp_path, "2330", as_of="2026-09-09")
    otc = load_market_evidence(tmp_path, "6488", as_of="2026-09-09")
    assert listed["source_dates"] == {"institutional": "2026-09-09", "margin": "2026-09-09", "holdings": "2026-09-04"}
    assert listed["institutional"][-1]["foreign"] == 1
    assert otc["institutional"][-1]["foreign"] == .001
    assert _prepared_market_evidence.cache_info().hits == 1
    changed = flow()
    changed["workbench_details"]["institutional"][0]["foreign_net"] = 123000
    flow_path.write_text(json.dumps(changed), encoding="utf-8")
    assert load_market_evidence(tmp_path, "2330", as_of="2026-09-09")["institutional"][-1]["foreign"] == 123
