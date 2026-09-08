"""Pure, as-of-safe MDA checklist metrics from already validated market data.

The checklist keeps source-explicit observations separate from versioned
engineering candidates.  It does not select the universe or create an order.
"""

from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from .core import AnalysisInputError, iso_date

RULE_VERSION = "mda-checklist-candidate-v1.0.0"
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
B2_FLOOR_TOLERANCE_PCT = 2.0
X_PRIOR_DECLINE_PCT = 15.0
X_VOLUME_RATIO = 1.2


def _cutoff(as_of: str) -> pd.Timestamp:
    try:
        value = pd.Timestamp(as_of)
    except (TypeError, ValueError) as exc:
        raise ValueError("as_of must be a valid date") from exc
    if pd.isna(value):
        raise ValueError("as_of must be a valid date")
    return value


def _prepare(
    data: pd.DataFrame | list[dict[str, Any]],
    *,
    cutoff: pd.Timestamp,
    required: tuple[str, ...],
    label: str,
) -> pd.DataFrame:
    frame = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    absent = [name for name in required if name not in frame.columns]
    if absent:
        raise AnalysisInputError(f"missing {label} columns: {', '.join(absent)}")
    dates = pd.to_datetime(frame["date"], errors="coerce")
    if bool(dates.isna().any()):
        raise AnalysisInputError(f"{label} rows contain an invalid date")
    frame = frame.copy()
    frame["date"] = dates
    frame = frame.loc[frame["date"] <= cutoff].copy()
    if bool(frame["date"].duplicated(keep=False).any()):
        raise AnalysisInputError(f"{label} rows contain duplicate dates on or before as_of")
    for name in required[1:]:
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    if not frame.empty:
        numeric = frame.loc[:, required[1:]].to_numpy(dtype=float)
        if not bool(np.isfinite(numeric).all()):
            raise AnalysisInputError(f"{label} rows contain missing or non-finite values")
    return frame.sort_values("date").reset_index(drop=True)


def _daily(data: pd.DataFrame | list[dict[str, Any]], cutoff: pd.Timestamp) -> pd.DataFrame:
    frame = _prepare(
        data,
        cutoff=cutoff,
        required=("date", "open", "high", "low", "close", "volume"),
        label="daily OHLCV",
    )
    if frame.empty:
        raise AnalysisInputError("no daily OHLCV rows are available on or before as_of")
    bad = (
        (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
        | (frame["volume"] < 0)
        | (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
    )
    if bool(bad.any()):
        raise AnalysisInputError("daily OHLCV rows contain invalid price or volume values")
    return frame


def _check(status: str, *, basis: str, summary: str, metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {"status": status, "threshold_basis": basis, "summary": summary, "metrics": dict(metrics)}


def _confirmed_lows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    lows = frame["low"].to_numpy(dtype=float)
    found: list[dict[str, Any]] = []
    for index in range(PIVOT_LEFT, len(frame) - PIVOT_RIGHT):
        value = lows[index]
        left = lows[index - PIVOT_LEFT : index]
        right = lows[index + 1 : index + 1 + PIVOT_RIGHT]
        if value < float(left.min()) and value <= float(right.min()):
            found.append(
                {
                    "date": iso_date(frame.iloc[index]["date"]),
                    "price": round(float(value), 6),
                    "confirmed_at": iso_date(frame.iloc[index + PIVOT_RIGHT]["date"]),
                }
            )
    return found


def _holder_windows(weekly: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if weekly.empty:
        return result
    end = weekly.iloc[-1]
    for weeks in (8, 12, 24):
        eligible = weekly.loc[weekly["date"] <= end["date"] - pd.Timedelta(weeks=weeks)]
        if eligible.empty:
            result[f"{weeks}w"] = {"status": "unknown", "required_weeks": weeks, "actual_days": int((end["date"] - weekly.iloc[0]["date"]).days)}
            continue
        start = eligible.iloc[-1]
        covered = weekly.loc[weekly["date"] >= start["date"]]
        gaps = covered["date"].diff().dt.days.dropna()
        continuous = len(covered) >= weeks + 1 and bool(gaps.between(4, 10).all())
        result[f"{weeks}w"] = {
            "status": "observed" if continuous else "unknown",
            "start_date": iso_date(start["date"]),
            "end_date": iso_date(end["date"]),
            "actual_points": len(covered),
            "continuous_4_to_10_day_gaps": continuous,
            "delta_pctpt": round(float(end["major_percent_400_plus"] - start["major_percent_400_plus"]), 6),
        }
        if "retail_percent_20_minus" in weekly.columns and bool(weekly["retail_percent_20_minus"].notna().all()):
            result[f"{weeks}w"]["retail_percent_20_minus_delta_pctpt"] = round(
                float(end["retail_percent_20_minus"] - start["retail_percent_20_minus"]), 6
            )
    return result


def _weekly_comovement(prices: pd.DataFrame, weekly: pd.DataFrame, financing: pd.DataFrame, holder_windows: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for window in ("8w", "12w", "24w"):
        meta = holder_windows.get(window, {})
        if meta.get("status") != "observed":
            output[window] = {"status": "unknown", "reason": "weekly holder coverage is incomplete", "weekly_intervals": []}
            continue
        covered = weekly.loc[weekly["date"] >= pd.Timestamp(meta["start_date"])]
        endpoints: list[dict[str, Any]] = []
        for _, holder in covered.iterrows():
            price = prices.loc[prices["date"] <= holder["date"]].tail(1)
            margin = financing.loc[financing["date"] <= holder["date"]].tail(1)
            price_ok = not price.empty and price.iloc[0]["date"] >= holder["date"] - pd.Timedelta(days=7)
            margin_ok = not margin.empty and margin.iloc[0]["date"] >= holder["date"] - pd.Timedelta(days=7)
            endpoints.append({
                "holder_date": iso_date(holder["date"]),
                "major_percent_400_plus": round(float(holder["major_percent_400_plus"]), 6),
                "price_date": iso_date(price.iloc[0]["date"]) if price_ok else None,
                "close": round(float(price.iloc[0]["close"]), 6) if price_ok else None,
                "margin_date": iso_date(margin.iloc[0]["date"]) if margin_ok else None,
                "margin_balance": round(float(margin.iloc[0]["margin_balance"]), 6) if margin_ok else None,
            })
        intervals: list[dict[str, Any]] = []
        complete = True
        for prior, current in zip(endpoints, endpoints[1:]):
            available = all(prior[key] is not None and current[key] is not None for key in ("close", "margin_balance"))
            complete = complete and available
            intervals.append({
                "start_holder_date": prior["holder_date"], "end_holder_date": current["holder_date"],
                "price_start_date": prior["price_date"], "price_end_date": current["price_date"],
                "margin_start_date": prior["margin_date"], "margin_end_date": current["margin_date"],
                "holder_delta_pctpt": round(current["major_percent_400_plus"] - prior["major_percent_400_plus"], 6),
                "price_delta_pct": round((current["close"] / prior["close"] - 1) * 100, 6) if available else None,
                "margin_delta": round(current["margin_balance"] - prior["margin_balance"], 6) if available else None,
                "complete": available,
            })
        output[window] = {"status": "observed" if complete and intervals else "unknown", "weekly_intervals": intervals}
    return output


def _criterion(criterion_id: str, section: str, label: str, status: str, metric_refs: list[str], missing_components: list[str] | None = None) -> dict[str, Any]:
    absent = list(missing_components or [])
    if status == "manual" and not absent:
        absent = ["manual_chart_or_context_review"]
    return {"id": criterion_id, "section": section, "label": label, "status": status, "metric_refs": metric_refs, "missing_components": absent}


def analyze_mda_checklist(
    daily: pd.DataFrame | list[dict[str, Any]],
    holders: pd.DataFrame | list[dict[str, Any]],
    margin: pd.DataFrame | list[dict[str, Any]],
    *,
    stock_id: str,
    as_of: str,
    pool_provenance: Mapping[str, Any],
    activated: bool = False,
) -> dict[str, Any]:
    """Return auditable checklist measurements for one preselected symbol."""

    cutoff = _cutoff(as_of)
    prices = _daily(daily, cutoff)
    weekly = _prepare(
        holders,
        cutoff=cutoff,
        required=("date", "major_percent_400_plus"),
        label="weekly holder",
    )
    financing = _prepare(
        margin,
        cutoff=cutoff,
        required=("date", "margin_balance"),
        label="daily margin",
    )
    if not weekly.empty and bool(((weekly["major_percent_400_plus"] < 0) | (weekly["major_percent_400_plus"] > 100)).any()):
        raise AnalysisInputError("weekly holder major_percent_400_plus must be between 0 and 100")
    if "retail_percent_20_minus" in weekly.columns:
        weekly["retail_percent_20_minus"] = pd.to_numeric(weekly["retail_percent_20_minus"], errors="coerce")
        present_retail = weekly["retail_percent_20_minus"].dropna()
        if bool(((present_retail < 0) | (present_retail > 100)).any()):
            raise AnalysisInputError("weekly holder retail_percent_20_minus must be between 0 and 100 when present")
    if not financing.empty and bool((financing["margin_balance"] < 0).any()):
        raise AnalysisInputError("daily margin balance must be non-negative")
    missing: list[str] = []
    conflicts: list[str] = []
    checks: dict[str, Any] = {}

    close = prices["close"]
    sma240 = close.rolling(240, min_periods=240).mean()
    if len(prices) < 241:
        checks["long_bull_A"] = _check(
            "unknown", basis="source_explicit", summary="A 需要完整 MA240 與前一根 MA240 才能核對方向。",
            metrics={"required_bars": 241, "actual_bars": len(prices)},
        )
        missing.append(f"long_bull_A requires 241 daily bars; actual {len(prices)}")
    else:
        delta = float(sma240.iloc[-1] - sma240.iloc[-2])
        passed = delta > 0
        checks["long_bull_A"] = _check(
            "pass" if passed else "fail", basis="source_explicit",
            summary="原書以 MA240 趨勢觀察 A；收盤相對 MA240 另列為位置證據。",
            metrics={"close": round(float(close.iloc[-1]), 6), "ma240": round(float(sma240.iloc[-1]), 6), "ma240_delta_1bar": round(delta, 6), "close_above_ma240": bool(float(close.iloc[-1]) > float(sma240.iloc[-1]))},
        )

    if len(prices) < 480:
        checks["one_year_high_history"] = _check(
            "unknown", basis="engineering_candidate_v1", summary="檢查近240根內是否曾在當時創一年新高，需要至少480根歷史。",
            metrics={"required_bars": 480, "actual_bars": len(prices)},
        )
    else:
        prior_high = close.shift(1).rolling(240, min_periods=240).max()
        events = (close >= prior_high) & prior_high.notna()
        recent_events = events.iloc[-240:]
        dates = [iso_date(value) for value in prices.loc[recent_events.index[recent_events], "date"]]
        checks["one_year_high_history"] = _check(
            "pass" if dates else "fail", basis="engineering_candidate_v1",
            summary="以每一交易日當時可見的前240根收盤高點，檢查近240根是否曾創一年新高。",
            metrics={"lookback_bars": 240, "event_dates": dates},
        )

    if len(prices) < 241:
        checks["ma240_deduction"] = _check("unknown", basis="source_explicit_observation", summary="240日扣抵價需要241根日線。", metrics={"required_bars": 241, "actual_bars": len(prices)})
    else:
        deduction = float(close.iloc[-241])
        checks["ma240_deduction"] = _check(
            "pass" if deduction < float(close.iloc[-1]) else "fail", basis="source_explicit_observation",
            summary="核對目前價格是否高於240日扣抵價。", metrics={"close": round(float(close.iloc[-1]), 6), "deduction_close_240": round(deduction, 6)},
        )
    pivots = _confirmed_lows(prices)
    recent = pivots[-2:]
    higher = len(recent) == 2 and recent[-1]["price"] > recent[-2]["price"]
    x_prior_decline = False
    x_bottom_volume = False
    if len(recent) == 2:
        first_index = int(prices.index[prices["date"] == pd.Timestamp(recent[0]["date"])][0])
        history = prices.iloc[: first_index + 1]
        prior_peak = float(history["high"].max())
        decline_pct = (float(recent[0]["price"]) / prior_peak - 1) * 100 if prior_peak > 0 else 0.0
        x_prior_decline = decline_pct <= -X_PRIOR_DECLINE_PCT
        pivot_ratios = []
        for pivot in recent:
            index = int(prices.index[prices["date"] == pd.Timestamp(pivot["date"])][0])
            prior_mean = float(prices["volume"].iloc[max(0, index - 20) : index].mean())
            pivot_ratios.append(float(prices.iloc[index]["volume"]) / prior_mean if prior_mean > 0 else 0.0)
        x_bottom_volume = max(pivot_ratios, default=0.0) >= X_VOLUME_RATIO
    else:
        decline_pct = None
        pivot_ratios = []
    checks["higher_lows_daily"] = _check(
        "candidate" if higher else ("unknown" if len(recent) < 2 else "fail"),
        basis="engineering_candidate_v1", summary="日線低點只在右側三根完成後確認；教材未指定 pivot 寬度。",
        metrics={"left_bars": PIVOT_LEFT, "right_bars": PIVOT_RIGHT, "pivots": recent},
    )
    checks["reversal_X"] = _check(
        "candidate" if higher and x_prior_decline and x_bottom_volume else "unknown", basis="engineering_candidate_v1",
        summary="原表的 X 是跌勢改變、不再破底及底部墊高；數值窗與量門檻為工程設定。",
        metrics={"confirmed_higher_lows": higher, "confirmed_pivots": recent, "prior_decline_pct": None if decline_pct is None else round(decline_pct, 6), "required_prior_decline_pct": X_PRIOR_DECLINE_PCT, "bottom_volume_ratios": [round(value, 6) for value in pivot_ratios], "required_bottom_volume_ratio": X_VOLUME_RATIO},
    )
    a_status = checks["long_bull_A"]["status"]
    x_status = checks["reversal_X"]["status"]
    checks["familiar_pattern_A_or_X"] = _check(
        "pass" if a_status == "pass" else ("candidate" if x_status == "candidate" else "unknown"),
        basis="source_explicit",
        summary="原書第二欄先確認熟悉樣態：已長多/即將長多的 A，或跌勢明顯改變的 X。",
        metrics={"A_status": a_status, "X_status": x_status, "operator": "OR"},
    )

    relation_metrics: dict[str, Any] = {}
    if len(weekly) < 2:
        checks["chip_price_relation"] = _check("unknown", basis="source_explicit_observation", summary="至少需要兩期週持股資料。", metrics={"holder_periods": len(weekly)})
        missing.append("chip_price_relation requires at least two weekly holder observations")
    else:
        start, end = weekly.iloc[-2], weekly.iloc[-1]
        price_start = prices.loc[prices["date"] <= start["date"]].tail(1)
        price_end = prices.loc[prices["date"] <= end["date"]].tail(1)
        margin_start = financing.loc[financing["date"] <= start["date"]].tail(1)
        margin_end = financing.loc[financing["date"] <= end["date"]].tail(1)
        relation_metrics = {
            "start_date": iso_date(start["date"]), "end_date": iso_date(end["date"]),
            "major_percent_400_plus_delta_pctpt": round(float(end["major_percent_400_plus"] - start["major_percent_400_plus"]), 6),
            "price_delta_pct": None, "margin_balance_delta": None,
        }
        price_complete = not price_start.empty and not price_end.empty and price_start.iloc[0]["date"] >= start["date"] - pd.Timedelta(days=7) and price_end.iloc[0]["date"] >= end["date"] - pd.Timedelta(days=7) and price_start.iloc[0]["date"] < price_end.iloc[0]["date"]
        margin_complete = not margin_start.empty and not margin_end.empty and margin_start.iloc[0]["date"] >= start["date"] - pd.Timedelta(days=7) and margin_end.iloc[0]["date"] >= end["date"] - pd.Timedelta(days=7) and margin_start.iloc[0]["date"] < margin_end.iloc[0]["date"]
        if price_complete:
            relation_metrics["price_start_date"] = iso_date(price_start.iloc[0]["date"])
            relation_metrics["price_end_date"] = iso_date(price_end.iloc[0]["date"])
            relation_metrics["price_delta_pct"] = round((float(price_end.iloc[0]["close"]) / float(price_start.iloc[0]["close"]) - 1) * 100, 6)
        else:
            missing.append("chip_price_relation lacks two daily closes inside the holder interval")
        if margin_complete:
            relation_metrics["margin_start_date"] = iso_date(margin_start.iloc[0]["date"])
            relation_metrics["margin_end_date"] = iso_date(margin_end.iloc[0]["date"])
            relation_metrics["margin_balance_delta"] = round(float(margin_end.iloc[0]["margin_balance"] - margin_start.iloc[0]["margin_balance"]), 6)
        else:
            missing.append("chip_price_relation lacks two margin observations inside the holder interval")
        complete = relation_metrics["price_delta_pct"] is not None and relation_metrics["margin_balance_delta"] is not None
        holder_up = relation_metrics["major_percent_400_plus_delta_pctpt"] > 0
        price_up = bool(complete and relation_metrics["price_delta_pct"] > 0)
        relation_metrics["alignment"] = "price_and_holder_up" if price_up and holder_up else "divergent_or_not_rising"
        margin_up = bool(complete and relation_metrics["margin_balance_delta"] > 0)
        relation_metrics["capital_efficiency_observation"] = (
            "holder_increase_with_price_lift" if price_up and holder_up
            else "margin_increase_without_price_lift" if margin_up and not price_up
            else "inconclusive"
        )
        if complete and holder_up and relation_metrics["margin_balance_delta"] < 0:
            conflicts.append("大戶持股增加但同期間融資減少；需依型態判讀籌碼交換，不能以任一序列增加直接通過。")
        if margin_up and not price_up:
            conflicts.append("融資增加但同期間價格未被維持或推升；原書將此列為仍有賣壓、資金運用較辛苦。")
        relation_metrics["short_interval_only"] = True
        checks["chip_price_relation"] = _check(
            "unknown",
            basis="source_explicit_observation", summary="單一週期間只列觀察，不能支撐原書要求的長期關係；相關不代表因果。", metrics=relation_metrics,
        )

    holder_windows = _holder_windows(weekly)
    co_movement_windows = _weekly_comovement(prices, weekly, financing, holder_windows)
    long_b_ready = holder_windows.get("8w", {}).get("status") == "observed"
    checks["long_term_B1"] = _check(
        "candidate" if long_b_ready and holder_windows["8w"]["delta_pctpt"] > 0 else ("unknown" if not long_b_ready else "fail"),
        basis="engineering_candidate_v1", summary="長 B 需長時間延續；兩期週資料不能代表長期。",
        metrics={"holder_windows": holder_windows},
    )
    checks["long_term_comovement"] = _check(
        "candidate" if co_movement_windows.get("8w", {}).get("status") == "observed" and holder_windows["8w"]["delta_pctpt"] > 0 else "unknown",
        basis="engineering_candidate_v1", summary="以8/12/24週逐週端點並列價格、大戶與融資變化；只作長期共同變化觀察。",
        metrics={"windows": co_movement_windows},
    )
    if not long_b_ready:
        missing.append("long_term_B1 requires at least 8 weeks of weekly holder coverage")

    if len(prices) < 25:
        checks["selling_pressure_B2"] = _check("unknown", basis="engineering_candidate_v1", summary="B2 指量縮價穩且 B1 未離開；資料不足。", metrics={"daily_bars": len(prices), "holder_periods": len(weekly)})
    else:
        prior_volume = float(prices["volume"].iloc[-25:-5].mean())
        recent_volume = float(prices["volume"].iloc[-5:].mean())
        volume_ratio = recent_volume / prior_volume if prior_volume > 0 else None
        prior_floor = float(prices["low"].iloc[-25:-5].min())
        tolerated_floor = prior_floor * (1.0 - B2_FLOOR_TOLERANCE_PCT / 100.0)
        observation = prices.iloc[-5:]
        price_hold = bool((observation["low"] >= tolerated_floor).all() and (observation["close"] >= tolerated_floor).all())
        recent_holder_delta = float(weekly.iloc[-1]["major_percent_400_plus"] - weekly.iloc[-2]["major_percent_400_plus"]) if len(weekly) >= 2 else None
        b1_retained = bool(long_b_ready and holder_windows["8w"]["delta_pctpt"] >= 0 and recent_holder_delta is not None and recent_holder_delta >= 0)
        candidate = volume_ratio is not None and volume_ratio < 1 and price_hold and b1_retained
        checks["selling_pressure_B2"] = _check(
            "candidate" if candidate else ("unknown" if not long_b_ready else "fail"), basis="engineering_candidate_v1",
            summary="量縮價穩及 B1 未離開為原意；五日對前二十日的數值窗為工程設定。",
            metrics={"recent5_vs_prior20_volume_ratio": None if volume_ratio is None else round(volume_ratio, 6), "prior20_floor_excluding_recent5": round(prior_floor, 6), "floor_tolerance_pct": B2_FLOOR_TOLERANCE_PCT, "recent5_holds_prior_floor": price_hold, "holder_delta_8w_pctpt": holder_windows.get("8w", {}).get("delta_pctpt"), "latest_holder_delta_pctpt": None if recent_holder_delta is None else round(recent_holder_delta, 6), "b1_holder_not_reduced_8w_and_latest": b1_retained},
        )

    if len(prices) < 20:
        checks["volume_timing"] = _check("unknown", basis="source_explicit_observation", summary="量能時機需要20根日線。", metrics={"actual_bars": len(prices)})
    else:
        volume_ma5 = float(prices["volume"].iloc[-5:].mean())
        volume_ma20 = float(prices["volume"].iloc[-20:].mean())
        checks["volume_timing"] = _check(
            "pass" if volume_ma5 > volume_ma20 else "fail", basis="source_explicit_observation",
            summary="原表列示成交量漸增（5MA大於20MA）。", metrics={"volume_ma5": round(volume_ma5, 6), "volume_ma20": round(volume_ma20, 6)},
        )
        deduction_volume = float(prices["volume"].iloc[-20])
        history = prices["volume"].iloc[-120:-20]
        percentile = float((history <= deduction_volume).mean() * 100) if not history.empty else None
        checks["volume20_deduction"] = _check(
            "candidate" if percentile is not None and percentile <= 25 else ("unknown" if percentile is None else "fail"),
            basis="engineering_candidate_v1", summary="原表要求20日扣抵量偏低；以先前100根的25百分位作工程量測。",
            metrics={"deduction_volume_20": round(deduction_volume, 6), "historical_percentile": None if percentile is None else round(percentile, 6), "candidate_max_percentile": 25},
        )

    manual = "manual"
    a2_status = "pass" if checks["long_bull_A"]["status"] == "pass" and checks["one_year_high_history"]["status"] == "pass" else ("unknown" if "unknown" in {checks["long_bull_A"]["status"], checks["one_year_high_history"]["status"]} else "fail")
    b2_metrics = checks["selling_pressure_B2"].get("metrics", {})
    contraction_hold = b2_metrics.get("recent5_vs_prior20_volume_ratio") is not None and b2_metrics["recent5_vs_prior20_volume_ratio"] < 1 and b2_metrics.get("recent5_holds_prior_floor") is True
    a4_status = "candidate" if checks["higher_lows_daily"]["status"] == "candidate" or contraction_hold else "unknown"
    b21_status = "candidate" if contraction_hold else ("unknown" if b2_metrics.get("recent5_vs_prior20_volume_ratio") is None else "fail")
    retail_available = "retail_percent_20_minus" in weekly.columns and bool(weekly["retail_percent_20_minus"].notna().all())
    b14_status = "candidate" if retail_available and long_b_ready and holder_windows["8w"]["delta_pctpt"] > 0 and holder_windows["8w"].get("retail_percent_20_minus_delta_pctpt", 0) < 0 else ("fail" if retail_available and long_b_ready else "unknown")
    criterion_rows = [
        ("A1", "A甲", "低基期窄幅整理", manual, [], ["base_range_definition"]), ("A2", "A甲", "一年新高與240日趨勢", a2_status, ["long_bull_A", "one_year_high_history"]),
        ("A3", "A甲", "上升過程量能堆疊", manual, [], ["volume_cluster_definition"]), ("A4", "A甲", "底部墊高或量縮價穩", a4_status, ["higher_lows_daily", "selling_pressure_B2"]),
        ("A5", "A甲", "下跌後快速站回全部均線", manual, []), ("A6", "A甲", "支撐或慣性均線反彈", manual, []), ("A7", "A甲", "下跌爆量後籌碼交換", manual, []),
        ("X1", "A乙", "下跌中爆量並見籌碼交換", checks["reversal_X"]["status"], ["reversal_X"]), ("X2", "A乙", "240日扣抵價低於現價", checks["ma240_deduction"]["status"], ["ma240_deduction"]),
        ("X3", "A乙", "特定籌碼開始穩定吸收", checks["long_term_B1"]["status"], ["long_term_B1"]), ("X4", "A乙", "下跌趨勢明顯轉變", checks["reversal_X"]["status"], ["reversal_X"]),
        ("B1_1", "B1", "外資或投信長期增加", manual, []), ("B1_2", "B1", "獲利仍持有的資金或融資", manual, []),
        ("B1_3", "B1", "獲利後仍持有的新增股東", manual, [], ["shareholder_count_by_cost"]), ("B1_4", "B1", "大戶增加且20張以下小股東持比減少", b14_status, ["long_term_B1"], [] if retail_available else ["retail_percent_20_minus"]),
        ("B1_5", "B1", "不同大戶級距增加", manual, []), ("B1_6", "B1", "其他持續投入跡象", manual, []),
        ("B2_1", "B2", "量縮且不再下跌", b21_status, ["selling_pressure_B2"]), ("B2_2", "B2", "價格上移且底部墊高", checks["higher_lows_daily"]["status"], ["higher_lows_daily"]),
        ("B2_3", "B2", "下跌時主力或大戶承接", manual, []), ("B2_4", "B2", "主要賣方僅調節未離開", manual, []),
        ("B2_5", "B2", "爆量後量縮且有人承接", manual, []), ("B2_6", "B2", "距離修正幅度合理", manual, []),
        ("B2_7", "B2", "回到先前量縮平台", manual, []), ("B2_8", "B2", "下影線或明顯支撐", manual, []),
        ("B2_9", "B2", "成交量回溫", checks["volume_timing"]["status"], ["volume_timing"]), ("B2_10", "B2", "20日扣抵量偏低", checks["volume20_deduction"]["status"], ["volume20_deduction"]), ("B2_11", "B2", "異常事件後成功換手", manual, [], ["event_context", "turnover_evidence"]),
        ("C1", "C", "產業或概念", manual, []), ("C2", "C", "產業前景", manual, []), ("C3", "C", "產業邏輯改變", manual, []),
        ("C4", "C", "研究新技術", manual, []), ("C5", "C", "供應鏈題材", manual, []), ("C6", "C", "景氣循環", manual, []),
        ("C7", "C", "具長期持有能力的股東", manual, []), ("C8", "C", "政府政策支持", manual, []), ("C9", "C", "擴廠或併購等事件", manual, []),
        ("C10", "C", "委託專業判斷", manual, []), ("C11", "C", "主升後三破檢查", manual, []), ("C12", "C", "其他長期理由", manual, []),
    ]
    criteria_registry = [_criterion(*row) for row in criterion_rows]
    table_sections = {section: [row["id"] for row in criteria_registry if row["section"] == section] for section in ("A甲", "A乙", "B1", "B2", "C")}

    activation = {"status": "activated" if activated else "not_started", "source": "caller_supplied"}
    if activated:
        short_term_review = {"status": "unknown", "reason": "minute OHLCV was not supplied; no intraday entry/exit evidence was fabricated"}
        missing.append("activated review requires separately validated minute OHLCV for short-term levels")
    else:
        short_term_review = None

    return {
        "stock_id": str(stock_id), "as_of": iso_date(cutoff), "data_date": iso_date(prices.iloc[-1]["date"]),
        "rule_version": RULE_VERSION, "pool_provenance": dict(pool_provenance), "checks": checks,
        "table_sections": table_sections, "criteria_registry": criteria_registry,
        "activation": activation, "short_term_review": short_term_review, "missing": missing, "conflicts": conflicts,
    }
