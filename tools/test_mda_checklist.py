from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from stock_v2_public.analysis.core import AnalysisInputError
from stock_v2_public.analysis.mda_checklist import analyze_mda_checklist


def _inputs(periods: int = 270):
    dates = pd.bdate_range("2025-01-02", periods=periods)
    base = np.linspace(50, 80, periods)
    daily = pd.DataFrame({"date": dates, "open": base, "high": base + 1, "low": base - 1, "close": base + .2, "volume": np.linspace(1500, 800, periods)})
    holder_dates = list(dates[-121::5])
    weekly = pd.DataFrame({"date": holder_dates, "major_percent_400_plus": np.linspace(32.0, 36.0, len(holder_dates))})
    margin = pd.DataFrame({"date": dates, "margin_balance": np.linspace(10000, 9000, periods)})
    return daily, weekly, margin


def _run(daily, weekly, margin, **kwargs):
    return analyze_mda_checklist(daily, weekly, margin, stock_id="9999", as_of=kwargs.pop("as_of", daily.iloc[-1]["date"].date().isoformat()), pool_provenance={"selection_rule": "caller-owned-top50"}, **kwargs)


def test_a_and_x_are_separate_checks():
    daily, weekly, margin = _inputs()
    result = _run(daily, weekly, margin)
    assert result["checks"]["long_bull_A"]["status"] == "pass"
    assert result["checks"]["reversal_X"]["status"] == "unknown"
    assert result["checks"]["familiar_pattern_A_or_X"]["metrics"]["operator"] == "OR"
    assert result["checks"]["familiar_pattern_A_or_X"]["status"] == "pass"
    a2 = next(row for row in result["criteria_registry"] if row["id"] == "A2")
    assert a2["status"] == "unknown"


def test_x_candidate_can_satisfy_observation_gate_when_a_fails():
    daily, weekly, margin = _inputs()
    falling = np.linspace(80, 50, len(daily))
    daily[["open", "close"]] = np.column_stack([falling, falling - .1])
    daily["high"] = falling + 1
    daily["low"] = falling - 1
    daily.loc[245, "low"] = 35
    daily.loc[258, "low"] = 38
    daily.loc[[245, 258], "volume"] = 3000
    result = _run(daily, weekly, margin)
    assert result["checks"]["long_bull_A"]["status"] == "fail"
    assert result["checks"]["reversal_X"]["status"] == "candidate"
    assert result["checks"]["familiar_pattern_A_or_X"]["status"] == "candidate"


def test_short_a_history_is_unknown_and_never_uses_partial_ma240():
    daily, weekly, margin = _inputs(200)
    result = _run(daily, weekly, margin)
    assert result["checks"]["long_bull_A"]["status"] == "unknown"
    assert "241" in result["missing"][0]


def test_two_holder_periods_do_not_claim_long_b():
    daily, weekly, margin = _inputs()
    result = _run(daily, weekly.tail(2), margin)
    assert result["checks"]["long_term_B1"]["status"] == "unknown"
    assert result["checks"]["selling_pressure_B2"]["status"] == "unknown"


def test_gapped_holder_history_does_not_claim_long_b():
    daily, weekly, margin = _inputs()
    gapped = weekly.drop(weekly.index[-5]).reset_index(drop=True)
    result = _run(daily, gapped, margin)
    window = result["checks"]["long_term_B1"]["metrics"]["holder_windows"]["8w"]
    assert result["checks"]["long_term_B1"]["status"] == "unknown"
    assert window["continuous_4_to_10_day_gaps"] is False


def test_future_rows_do_not_change_asof_result():
    daily, weekly, margin = _inputs()
    cutoff = daily.iloc[-2]["date"].date().isoformat()
    baseline = _run(daily.iloc[:-1], weekly, margin.iloc[:-1], as_of=cutoff)
    poisoned = daily.copy()
    poisoned.loc[poisoned.index[-1], ["open", "high", "low", "close", "volume"]] = [np.inf, -1, 9, np.nan, -1]
    assert _run(poisoned, weekly, margin, as_of=cutoff) == baseline


def test_missing_margin_is_unknown_not_zero_filled():
    daily, weekly, margin = _inputs()
    result = _run(daily, weekly, margin.iloc[:0])
    relation = result["checks"]["chip_price_relation"]
    assert relation["status"] == "unknown"
    assert relation["metrics"]["margin_balance_delta"] is None


def test_middle_only_margin_rows_do_not_count_as_complete_endpoints():
    daily, weekly, margin = _inputs()
    start, end = weekly.iloc[-2]["date"], weekly.iloc[-1]["date"]
    middle = margin.loc[(margin["date"] > start) & (margin["date"] < end)].copy()
    result = _run(daily, weekly, middle)
    assert result["checks"]["chip_price_relation"]["status"] == "unknown"


def test_cross_series_conflict_is_explicit():
    daily, weekly, margin = _inputs()
    result = _run(daily, weekly, margin)
    assert result["checks"]["chip_price_relation"]["status"] == "unknown"
    assert result["checks"]["chip_price_relation"]["metrics"]["short_interval_only"] is True
    assert result["conflicts"]


def test_margin_increase_without_price_lift_is_not_positive_evidence():
    daily, weekly, margin = _inputs()
    interval_start = weekly.iloc[-2]["date"]
    daily.loc[daily["date"] >= interval_start, ["open", "high", "low", "close"]] -= np.linspace(0, 3, int((daily["date"] >= interval_start).sum()))[:, None]
    margin["margin_balance"] = np.linspace(9000, 10000, len(margin))
    result = _run(daily, weekly, margin)
    relation = result["checks"]["chip_price_relation"]
    assert relation["status"] == "unknown"
    assert relation["metrics"]["capital_efficiency_observation"] == "margin_increase_without_price_lift"
    assert any("仍有賣壓" in item for item in result["conflicts"])


def test_confirmed_pivot_needs_right_side_bars():
    daily, weekly, margin = _inputs()
    # A low at the final row cannot be used because its three right bars do not exist.
    daily.loc[daily.index[-1], "low"] = daily.loc[daily.index[-1], "close"] - 20
    result = _run(daily, weekly, margin)
    assert all(p["date"] != result["data_date"] for p in result["checks"]["higher_lows_daily"]["metrics"]["pivots"])


def test_intraday_review_exists_only_after_activation_and_stays_missing_without_minutes():
    daily, weekly, margin = _inputs()
    idle = _run(daily, weekly, margin, activated=False)
    active = _run(daily, weekly, margin, activated=True)
    assert idle["short_term_review"] is None
    assert not any("minute" in item for item in idle["missing"])
    assert active["short_term_review"]["status"] == "unknown"
    assert any("minute" in item for item in active["missing"])


def test_original_table_rows_are_registered_in_sections():
    daily, weekly, margin = _inputs()
    result = _run(daily, weekly, margin)
    assert set(result["table_sections"]) == {"A甲", "A乙", "B1", "B2", "C"}
    ids = {row["id"] for row in result["criteria_registry"]}
    assert {"A1", "A7", "X1", "X4", "B1_1", "B1_6", "B2_1", "B2_11", "C1", "C12"} <= ids
    assert any(row["status"] == "manual" for row in result["criteria_registry"])
    b14 = next(row for row in result["criteria_registry"] if row["id"] == "B1_4")
    assert b14["status"] == "unknown"
    assert b14["missing_components"] == ["retail_percent_20_minus"]
    assert result["checks"]["ma240_deduction"]["status"] in {"pass", "fail"}
    assert result["checks"]["volume_timing"]["status"] in {"pass", "fail"}


def test_b1_4_uses_exact_20_minus_retail_series():
    daily, weekly, margin = _inputs()
    weekly["retail_percent_20_minus"] = np.linspace(18.0, 15.0, len(weekly))
    weekly["retail_percent_200_minus"] = np.linspace(40.0, 45.0, len(weekly))
    result = _run(daily, weekly, margin)
    b14 = next(row for row in result["criteria_registry"] if row["id"] == "B1_4")
    assert b14["status"] == "candidate"
    assert b14["missing_components"] == []
    assert result["checks"]["long_term_B1"]["metrics"]["holder_windows"]["8w"]["retail_percent_20_minus_delta_pctpt"] < 0


def test_200_minus_retail_never_substitutes_for_20_minus():
    daily, weekly, margin = _inputs()
    weekly["retail_percent_200_minus"] = np.linspace(18.0, 15.0, len(weekly))
    result = _run(daily, weekly, margin)
    b14 = next(row for row in result["criteria_registry"] if row["id"] == "B1_4")
    assert b14["status"] == "unknown"
    assert b14["missing_components"] == ["retail_percent_20_minus"]


def test_long_term_comovement_has_weekly_endpoints_for_each_window():
    daily, weekly, margin = _inputs()
    result = _run(daily, weekly, margin)
    windows = result["checks"]["long_term_comovement"]["metrics"]["windows"]
    assert [windows[key]["status"] for key in ("8w", "12w", "24w")] == ["observed"] * 3
    assert len(windows["8w"]["weekly_intervals"]) >= 8
    assert all(row["complete"] for row in windows["8w"]["weekly_intervals"])


def test_long_term_comovement_is_unknown_when_margin_endpoint_missing():
    daily, weekly, margin = _inputs()
    missing_date = weekly.iloc[-4]["date"]
    margin = margin.loc[~margin["date"].between(missing_date - pd.Timedelta(days=7), missing_date)].copy()
    result = _run(daily, weekly, margin)
    assert result["checks"]["long_term_comovement"]["metrics"]["windows"]["8w"]["status"] == "unknown"


def test_in_scope_missing_value_is_rejected():
    daily, weekly, margin = _inputs()
    margin.loc[margin.index[-2], "margin_balance"] = np.nan
    with pytest.raises(AnalysisInputError, match="missing or non-finite"):
        _run(daily, weekly, margin)


def test_b2_rejects_a_break_below_the_preexisting_floor():
    daily, weekly, margin = _inputs()
    baseline_floor = float(daily["low"].iloc[-25:-5].min())
    daily.loc[daily.index[-3], ["open", "high", "low", "close"]] = [baseline_floor, baseline_floor + 1, baseline_floor * .9, baseline_floor * .95]
    result = _run(daily, weekly, margin)
    assert result["checks"]["selling_pressure_B2"]["status"] == "fail"
    assert result["checks"]["selling_pressure_B2"]["metrics"]["recent5_holds_prior_floor"] is False


def test_b2_rejects_latest_holder_reduction_despite_positive_eight_week_net():
    daily, weekly, margin = _inputs()
    weekly.loc[weekly.index[-1], "major_percent_400_plus"] = weekly.iloc[-2]["major_percent_400_plus"] - .2
    result = _run(daily, weekly, margin)
    metrics = result["checks"]["selling_pressure_B2"]["metrics"]
    assert metrics["holder_delta_8w_pctpt"] > 0
    assert metrics["latest_holder_delta_pctpt"] < 0
    assert metrics["b1_holder_not_reduced_8w_and_latest"] is False
    assert result["checks"]["selling_pressure_B2"]["status"] == "fail"


@pytest.mark.parametrize(("dataset", "value", "message"), [("holder", 101, "between 0 and 100"), ("margin", -1, "non-negative")])
def test_domain_bounds_are_rejected(dataset, value, message):
    daily, weekly, margin = _inputs()
    if dataset == "holder":
        weekly.loc[weekly.index[-1], "major_percent_400_plus"] = value
    else:
        margin.loc[margin.index[-1], "margin_balance"] = value
    with pytest.raises(AnalysisInputError, match=message):
        _run(daily, weekly, margin)
