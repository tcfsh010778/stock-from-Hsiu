import copy
import pytest

from tools.official_workbench import accumulate_workbench_history, merge_official_evidence, prepare_official_evidence


def flow(state="ok"):
    return {"dataset_id": "daily_market_flow", "date": "2026-09-09", "data_quality": {"state": state},
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
    assert out["source_status"]["institutional"]["state"] == "current"
    assert "原單位" in out["unit_notes"]["margin"]


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
    assert out["source_status"]["holdings"]["state"] == "current"
    assert out["source_status"]["foreign_ownership"]["state"] == "missing"


def test_official_margin_does_not_join_unproven_legacy_unit():
    old = {"institutional": [], "foreign_ownership": [], "holdings": [], "margin": [
        {"date": "2026-09-07", "margin_balance": 999},
        {"date": "2026-09-08", "margin_balance": 90, "short_balance": 4, "unit": "official_report_balance"},
    ]}
    out = merge_official_evidence(old, "2330", "2026-09-09", flow(), {})
    assert [row["date"] for row in out["margin"]] == ["2026-09-08", "2026-09-09"]


def test_daily_sources_stale_after_one_day_but_weekly_has_seven_day_window():
    old = {"institutional": [{"date": "2026-09-08", "foreign": 1}], "margin": [
        {"date": "2026-09-08", "margin_balance": 1, "unit": "official_report_balance"}],
        "holdings": [{"date": "2026-09-03", "major": 10}], "foreign_ownership": []}
    out = merge_official_evidence(old, "2330", "2026-09-09", flow("warning"), {})
    assert out["source_status"]["institutional"]["state"] == "stale"
    assert out["source_status"]["margin"]["state"] == "stale"
    assert out["source_status"]["holdings"]["state"] == "current"


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


def test_accumulate_preserves_margin_sessions_and_same_date_is_idempotent():
    prior = flow(); prior["date"] = prior["workbench_details"]["date"] = "2026-09-08"
    prior["workbench_details"]["margin"][0]["margin_balance"] = 80
    combined = accumulate_workbench_history(flow(), prior)
    assert [item["date"] for item in combined["workbench_history"]] == ["2026-09-08", "2026-09-09"]
    prepared = prepare_official_evidence("2026-09-09", combined, {})
    assert [row["margin_balance"] for row in prepared["margin"]["2330"]] == [80.0, 100.0]
    again = accumulate_workbench_history(flow(), combined)
    assert again["workbench_history"] == combined["workbench_history"]


def test_accumulate_rejects_future_bad_units_and_duplicate_old_dates():
    prior = flow(); prior["date"] = prior["workbench_details"]["date"] = "2026-09-10"
    with pytest.raises(ValueError, match="future"):
        accumulate_workbench_history(flow(), prior)
    bad = flow(); bad["workbench_details"]["margin_unit"] = "lots"
    with pytest.raises(ValueError, match="units"):
        accumulate_workbench_history(bad, {})
    prior = accumulate_workbench_history(flow(), {})
    snap = copy.deepcopy(prior["workbench_history"][0]); snap["date"] = "2026-09-08"
    prior["workbench_history"] = [snap, copy.deepcopy(snap)]
    with pytest.raises(ValueError, match="duplicate"):
        accumulate_workbench_history(flow(), prior)
    with pytest.raises(ValueError, match="schema"):
        accumulate_workbench_history(flow(), {"dataset_id": "wrong"})


def test_accumulate_requires_current_details_date_to_equal_flow_date():
    current = flow(); current["workbench_details"]["date"] = "2026-09-08"
    with pytest.raises(ValueError, match="date or units"):
        accumulate_workbench_history(current, {})


def test_accumulate_stores_compact_margin_only_history():
    saved = accumulate_workbench_history(flow(), {})["workbench_history"][0]
    assert set(saved) == {"date", "margin_unit", "row_count", "rows_csv"}
    assert saved["row_count"] == 2
    assert "2330,l,100.0,5.0" in saved["rows_csv"]
    assert "foreign_net" not in saved["rows_csv"]


@pytest.mark.parametrize("history", [{}, [{"date": "2026-09-08"}, "bad"]])
def test_accumulate_rejects_nonlist_or_nonobject_old_history(history):
    previous = {"dataset_id": "daily_market_flow", "workbench_history": history}
    with pytest.raises(ValueError, match="history"):
        accumulate_workbench_history(flow(), previous)


def test_accumulate_rejects_two_old_history_rows_for_current_date():
    saved = accumulate_workbench_history(flow(), {})["workbench_history"][0]
    previous = {"dataset_id": "daily_market_flow", "workbench_history": [saved, copy.deepcopy(saved)]}
    with pytest.raises(ValueError, match="duplicate"):
        accumulate_workbench_history(flow(), previous)


def test_prepare_rejects_history_later_than_flow_date_even_if_within_asof():
    payload = flow(); future = accumulate_workbench_history(payload, {})["workbench_history"][0]; future["date"] = "2026-09-10"
    payload["workbench_history"] = [future]
    with pytest.raises(ValueError, match="later than flow date"):
        prepare_official_evidence("2026-09-10", payload, {})
    bad = flow(); bad["institutional_history"]["snapshots"][0].update(row_count=1, rows_csv="2330,l,1,2")
    with pytest.raises(ValueError, match="both markets"):
        prepare_official_evidence("2026-09-09", bad, {})
