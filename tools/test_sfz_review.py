from __future__ import annotations

import pandas as pd
import pytest

from stock_v2_public.analysis.core import AnalysisInputError
from stock_v2_public.analysis.sfz_review import analyze_sfz, review_sfz_technical


def _qualifying_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=86)
    rows = []
    for index, day in enumerate(dates):
        if index < 63:
            close = 45.0 + index * 0.22
            high, low, volume = close + 0.5, close - 0.5, 1500.0
        elif index < 83:
            close = 59.3 + (index % 4) * 0.25
            high, low, volume = 61.0, 58.0, 1200.0 - (index - 63) * 10
        elif index == 83:
            close, high, low, volume = 62.2, 62.8, 60.9, 1800.0
        elif index == 84:
            close, high, low, volume = 61.2, 62.0, 60.5, 1250.0
        else:
            close, high, low, volume = 62.0, 62.4, 61.0, 1350.0
        rows.append({"date": day, "open": close - 0.1, "high": high, "low": low, "close": close, "volume": volume})
    return pd.DataFrame(rows)


def test_symbol_outside_mda_can_qualify_from_ohlcv_alone():
    frame = _qualifying_frame()
    result = analyze_sfz(frame, "9999", frame.iloc[-1]["date"].date().isoformat())
    assert result["candidate"] is True
    assert result["stage"] == "retest_confirmed"
    assert {item["id"] for item in result["evidence"]} >= {"trend_context", "box", "breakout", "retest"}
    assert "mda" not in str(result).lower()


def test_as_of_is_causal_and_future_rows_cannot_change_result():
    frame = _qualifying_frame()
    cutoff = frame.iloc[84]["date"].date().isoformat()
    prefix = review_sfz_technical(frame.iloc[:85], stock_id="9999", as_of=cutoff)
    poisoned = frame.copy()
    poisoned.loc[85, ["open", "high", "low", "close", "volume"]] = [1.0, 2.0, 0.5, 1.0, 99_000_000]
    full = review_sfz_technical(poisoned, stock_id="9999", as_of=cutoff)
    assert full == prefix


def test_breakout_without_retest_is_observation_not_buy_action():
    frame = _qualifying_frame().iloc[:84]
    result = review_sfz_technical(frame, stock_id="9999", as_of=frame.iloc[-1]["date"].date().isoformat())
    assert result["candidate"] is False
    assert result["stage"] == "breakout_wait_retest"
    assert "買進" in result["next_observation"]
    assert "action" not in result


def test_short_history_reports_missing_instead_of_guessing():
    frame = _qualifying_frame().iloc[:30]
    result = review_sfz_technical(frame, stock_id="9999", as_of=frame.iloc[-1]["date"].date().isoformat())
    assert result["candidate"] is False
    assert result["missing"]
    assert not any(item["id"] == "trend_context" for item in result["evidence"])


def test_failed_breakout_is_explicitly_invalidated():
    frame = _qualifying_frame()
    frame.loc[85, ["open", "high", "low", "close"]] = [59.0, 59.5, 58.0, 59.0]
    result = review_sfz_technical(frame, stock_id="9999", as_of=frame.iloc[-1]["date"].date().isoformat())
    assert result["candidate"] is False
    assert result["stage"] == "invalidated"
    assert result["conflicts"]


def test_malformed_future_numeric_row_is_ignored_before_validation():
    frame = _qualifying_frame()
    cutoff = frame.iloc[-1]["date"].date().isoformat()
    baseline = analyze_sfz(frame, "9999", cutoff)
    future = frame.iloc[-1].copy()
    future["date"] = pd.Timestamp(cutoff) + pd.Timedelta(days=1)
    future[["open", "high", "low", "close", "volume"]] = [float("inf"), -2, 10, "bad", -1]
    poisoned = pd.concat([frame, future.to_frame().T], ignore_index=True)
    assert analyze_sfz(poisoned, "9999", cutoff) == baseline


def test_duplicate_date_inside_as_of_is_rejected():
    frame = _qualifying_frame()
    duplicate = pd.concat([frame, frame.iloc[[-1]]], ignore_index=True)
    with pytest.raises(AnalysisInputError, match="duplicate dates"):
        analyze_sfz(duplicate, "9999", frame.iloc[-1]["date"].date().isoformat())


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [("close", float("inf"), "non-finite"), ("volume", -1, "high/low/volume"), ("low", -1, "high/low/volume")],
)
def test_invalid_in_scope_numeric_values_are_rejected(column, value, message):
    frame = _qualifying_frame()
    frame.loc[10, column] = value
    with pytest.raises(AnalysisInputError, match=message):
        analyze_sfz(frame, "9999", frame.iloc[-1]["date"].date().isoformat())


def test_valid_noncandidate_has_no_missing_data_flag():
    frame = _qualifying_frame().iloc[:63].copy()
    frame.loc[frame.index[-20:], "high"] *= 1.2
    frame.loc[frame.index[-20:], "low"] *= 0.8
    result = analyze_sfz(frame, "9999", frame.iloc[-1]["date"].date().isoformat())
    assert result["candidate"] is False
    assert result["stage"] == "no_setup"
    assert result["missing"] == []
    assert any(item["id"] == "no_box_setup" for item in result["evidence"])
