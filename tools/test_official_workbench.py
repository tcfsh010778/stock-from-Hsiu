import copy
import pytest

from tools.official_workbench import merge_official_evidence, prepare_official_evidence


def flow(state="ok"):
    return {"date": "2026-09-09", "data_quality": {"state": state},
        "institutional_history": {"snapshots": [{"date": "2026-09-08", "row_count": 2, "rows_csv": "2330,l,1000,-2000\n6488,o,1,2"}]},
        "workbench_details": {"date": "2026-09-09", "institutional_unit": "shares", "margin_unit": "official_report_balance",
            "institutional": [{"security_id": "2330", "market": "listed", "foreign_net": 1000, "investment_trust_net": 2000, "dealer_net": None, "institutional_total_net": 3000}, {"security_id": "6488", "market": "otc", "foreign_net": 1, "investment_trust_net": 2, "dealer_net": 3, "institutional_total_net": 6}],
            "margin": [{"security_id": "2330", "market": "listed", "margin_balance": 100, "short_balance": 5}, {"security_id": "6488", "market": "otc", "margin_balance": 20, "short_balance": 1}]}}


def weekly():
    rows = [{"security_id": "2330", "market": "listed", "major_percent": 12, "major_people": 2}, {"security_id": "6488", "market": "otc", "major_percent": 8, "major_people": 1}]
    return {"dataset_id": "tdcc_compact_weekly_snapshots", "snapshots": [{"date": "2026-09-04", "row_count": 2, "market_counts": {"listed": 1, "otc": 1}, "rows": rows}]}


def test_shares_convert_to_lots_and_unknown_dealer_stays_none():
    out = merge_official_evidence({}, "2330", "2026-09-09", flow(), {})
    assert out["institutional"][0]["foreign"] == 1
    assert out["institutional"][0]["trust"] == -2
    assert out["institutional"][0]["dealer"] is None
    assert out["institutional"][-1]["total"] == 3
    assert out["margin"][-1]["margin_balance"] == 100


def test_bad_source_preserves_old_and_foreign_ownership_gap_remains():
    old = {"institutional": [{"date": "2026-09-01", "foreign": 1}], "foreign_ownership": [], "margin": [], "holdings": []}
    out = merge_official_evidence(old, "2330", "2026-09-09", flow("warning"), {})
    assert out["institutional"] == old["institutional"]
    assert "foreign_ownership" in out["gaps"]


def test_both_markets_required_and_duplicates_or_invalid_dates_fail():
    bad = flow(); bad["workbench_details"]["institutional"] = bad["workbench_details"]["institutional"][:1]
    with pytest.raises(ValueError, match="both markets"):
        merge_official_evidence({}, "2330", "2026-09-09", bad, {})
    bad = flow(); bad["workbench_details"]["margin"].append(copy.deepcopy(bad["workbench_details"]["margin"][0]))
    with pytest.raises(ValueError, match="duplicated"):
        merge_official_evidence({}, "2330", "2026-09-09", bad, {})
    bad = flow(); bad["institutional_history"]["snapshots"][0]["date"] = "bad"
    with pytest.raises(ValueError, match="invalid source date"):
        merge_official_evidence({}, "2330", "2026-09-09", bad, {})


def test_weekly_holder_is_not_promoted_to_daily_or_fabricated():
    out = merge_official_evidence({}, "2330", "2026-09-09", flow("warning"), weekly())
    assert out["holdings"] == [{"date": "2026-09-04", "major": 12.0, "middle": None, "retail": None, "total_people": None}]
    assert out["source_dates"]["holdings"] == "2026-09-04"


def test_asof_filters_old_and_new_series_independently():
    old = {"institutional": [{"date": "2026-09-01", "foreign": 1}, {"date": "2026-09-10", "foreign": 9}],
           "foreign_ownership": [{"date": "2026-09-10", "foreign_ratio": 10}], "margin": [], "holdings": []}
    out = merge_official_evidence(old, "2330", "2026-09-08", flow(), weekly())
    assert [row["date"] for row in out["institutional"]] == ["2026-09-01"]
    assert out["foreign_ownership"] == []
    assert out["holdings"][-1]["date"] == "2026-09-04"


def test_prepared_merge_matches_direct_merge_for_multiple_stocks():
    prepared = prepare_official_evidence("2026-09-09", flow(), weekly())
    for sid in ("2330", "6488"):
        direct = merge_official_evidence({}, sid, "2026-09-09", flow(), weekly())
        cached = merge_official_evidence({}, sid, "2026-09-09", {}, {}, prepared=prepared)
        assert cached == direct


def test_duplicate_snapshot_dates_and_missing_history_market_fail():
    bad = flow(); bad["institutional_history"]["snapshots"] *= 2
    with pytest.raises(ValueError, match="dates are duplicated"):
        prepare_official_evidence("2026-09-09", bad, {})
    bad = flow(); bad["institutional_history"]["snapshots"][0].update(row_count=1, rows_csv="2330,l,1,2")
    with pytest.raises(ValueError, match="both markets"):
        prepare_official_evidence("2026-09-09", bad, {})
