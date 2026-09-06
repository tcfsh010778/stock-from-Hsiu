import numpy as np
import pandas as pd
import pytest

from stock_v2_public.analysis.sfz_universe import SFZUniverseConfig, SFZUniverseInputError, analyze_sfz_universe


def bars(n=700, end="2026-08-28", start=30.0, slope=0.08):
    dates = pd.bdate_range(end=end, periods=n)
    close = start * np.power(1.012, np.arange(n))
    return pd.DataFrame({"date": dates, "open": close - .1, "high": close + .5, "low": close - .5, "close": close, "volume": 2_500_000.0})


def by_id(packet, check_id):
    return next(item for item in packet["checks"] if item["id"] == check_id)


def test_full_89_completed_weeks_required_and_no_ma34_substitute():
    packet = analyze_sfz_universe(bars(300), "2330", "2026-08-28")
    check = by_id(packet, "weekly_ma_5_21_89")
    assert check["status"] == "unknown"
    assert check["metrics"]["required_weeks"] == 89
    assert packet["stage"] == "insufficient_data"


def test_future_and_partial_week_do_not_change_old_result():
    base = bars()
    old = analyze_sfz_universe(base, "2330", "2026-08-28")
    future = pd.concat([base, pd.DataFrame([{"date": "2026-08-31", "open": 999, "high": 1000, "low": 998, "close": 999, "volume": 999999}])], ignore_index=True)
    newer = analyze_sfz_universe(future, "2330", "2026-08-28")
    assert newer == old
    assert old["weekly_context"]["last_completed_week"] == "2026-08-28"


def test_asof_midweek_excludes_that_unfinished_week():
    frame = bars(end="2026-09-02")
    packet = analyze_sfz_universe(frame, "2330", "2026-09-02")
    assert packet["data_date"] == "2026-09-02"
    assert packet["weekly_context"]["last_completed_week"] == "2026-08-28"


def test_universe_candidate_never_enables_entry():
    packet = analyze_sfz_universe(bars(), "2330", "2026-08-28")
    assert packet["candidate"] is True
    assert packet["stage"] == "universe_candidate"
    assert packet["entry_observations"]["enabled"] is False
    assert packet["entry_observations"]["status"] == "manual"


def test_eight_week_gain_uses_eight_weekly_returns():
    packet = analyze_sfz_universe(bars(), "2330", "2026-08-28")
    check = by_id(packet, "eight_week_average_gain")
    assert check["status"] == "pass"
    assert check["metrics"]["threshold_pct"] == 5.0
    assert check["metrics"]["formula_provenance"] == "legacy_screener_engineering_interpretation"


@pytest.mark.parametrize(
    ("lots", "expected"),
    [(9999, "fail"), (10000, "fail"), (10001, "pass")],
)
def test_five_day_volume_converts_raw_shares_to_lots_and_uses_strict_boundary(lots, expected):
    frame = bars()
    frame.loc[frame.index[-5:], "volume"] = lots * 1000 / 5
    packet = analyze_sfz_universe(frame, "2330", "2026-08-28")
    check = by_id(packet, "five_day_volume")
    assert check["status"] == expected
    assert check["metrics"]["value_lots"] == lots


def test_ten_raw_shares_cannot_pass_ten_thousand_lot_threshold():
    frame = bars()
    frame.loc[frame.index[-5:], "volume"] = 2.0
    check = by_id(analyze_sfz_universe(frame, "2330", "2026-08-28"), "five_day_volume")
    assert check["status"] == "fail"
    assert check["metrics"]["value_lots"] == 0.01


@pytest.mark.parametrize("column,value", [("close", np.inf), ("volume", -1), ("low", -2)])
def test_invalid_values_at_or_before_asof_are_rejected(column, value):
    frame = bars()
    frame.loc[frame.index[-1], column] = value
    with pytest.raises(SFZUniverseInputError):
        analyze_sfz_universe(frame, "2330", "2026-08-28")


def test_duplicate_dates_rejected_but_malformed_future_numeric_ignored():
    frame = bars()
    duplicate = pd.concat([frame, frame.tail(1)], ignore_index=True)
    with pytest.raises(SFZUniverseInputError, match="duplicate"):
        analyze_sfz_universe(duplicate, "2330", "2026-08-28")
    future = pd.concat([frame, pd.DataFrame([{"date": "2026-09-01", "open": 1, "high": 1, "low": 1, "close": "bad", "volume": -2}])], ignore_index=True)
    assert analyze_sfz_universe(future, "2330", "2026-08-28")["candidate"] is True


def test_top50_route_is_provenance_only():
    packet = analyze_sfz_universe(bars(), "2330", "2026-08-28", pool_provenance={"route": "market_top50_momentum", "rank": 7, "universe_date": "2026-08-28"})
    assert packet["pool_provenance"]["rank"] == 7
    assert "caller" in packet["notes"][0]


def test_optional_three_week_filter_is_configurable():
    cfg = SFZUniverseConfig(require_three_week_closing_high=True)
    packet = analyze_sfz_universe(bars(), "2330", "2026-08-28", config=cfg)
    assert by_id(packet, "optional_three_week_closing_high")["status"] == "pass"


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf])
def test_nonfinite_configuration_is_rejected(value):
    with pytest.raises(SFZUniverseInputError, match="finite"):
        analyze_sfz_universe(bars(), "2330", "2026-08-28", config=SFZUniverseConfig(min_five_day_volume_lots=value))


def test_empty_frame_returns_auditable_insufficient_data_packet():
    empty = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
    packet = analyze_sfz_universe(empty, "2330", "2026-08-28")
    assert packet["stage"] == "insufficient_data"
    assert packet["weekly_context"]["completed_weeks"] == 0
    assert packet["data_date"] is None
