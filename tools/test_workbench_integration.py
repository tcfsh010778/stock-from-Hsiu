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


def test_legacy_missing_categories_are_unknown_and_numeric_holder_bands_are_not_shares(tmp_path):
    (tmp_path / "chips").mkdir()
    (tmp_path / "chips/2330.csv").write_text("date,name,buy,sell\n2026-09-01,Foreign_Investor,1000,0\n", encoding="utf-8")
    (tmp_path / "holding_shares").mkdir()
    (tmp_path / "holding_shares/2330.csv").write_text("date,HoldingSharesLevel,people,percent\n2026-09-04,12,2,8\n2026-09-04,3,100,1\n", encoding="utf-8")
    evidence = load_market_evidence(tmp_path, "2330", as_of="2026-09-09")
    assert evidence["institutional"][0] == {"date": "2026-09-01", "foreign": 1.0, "trust": None, "dealer": None, "total": None}
    assert evidence["holdings"][0]["major"] == 8
    assert evidence["holdings"][0]["retail"] == 1


def test_market_flow_entrypoint_preserves_prior_verified_margin_sessions(tmp_path, monkeypatch):
    import market_flow
    from copy import deepcopy
    destination = tmp_path / "flow.json"
    prior = flow()
    prior["date"] = prior["workbench_details"]["date"] = "2026-09-08"
    destination.write_text(json.dumps(prior), encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["market_flow.py", "--strict", "--output", str(destination)])
    monkeypatch.setattr(market_flow, "collect", lambda _: deepcopy(flow()))
    monkeypatch.setattr(market_flow, "_is_complete_snapshot", lambda _: True)
    monkeypatch.setattr(market_flow, "write_payload", lambda payload, path, manifest: path.write_text(json.dumps(payload), encoding="utf-8"))
    assert market_flow.main() == 0
    first = json.loads(destination.read_text(encoding="utf-8"))
    assert [s["date"] for s in first["workbench_history"]] == ["2026-09-08", "2026-09-09"]
    assert market_flow.main() == 0
    assert json.loads(destination.read_text(encoding="utf-8"))["workbench_history"] == first["workbench_history"]
