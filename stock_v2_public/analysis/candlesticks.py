from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .core import iso_date


SCHEMA_VERSION = "candlestick-pattern-event/1.0.0"
PATTERN_CONFIG_VERSION = "context-v1.0.0"

PUBLIC_TALIB_PATTERNS: dict[str, dict[str, Any]] = {
    "CDLDOJI": {"pattern_id": "doji", "label_zh": "十字", "family": "single_bar", "priority": 30},
    "CDLHAMMER": {"pattern_id": "hammer_shape", "label_zh": "錘形線", "family": "single_bar", "priority": 55},
    "CDLINVERTEDHAMMER": {
        "pattern_id": "inverted_hammer_shape",
        "label_zh": "倒錘形線",
        "family": "single_bar",
        "priority": 55,
    },
    "CDLENGULFING": {"pattern_id": "engulfing_body", "label_zh": "吞噬", "family": "two_bar", "priority": 70},
    "CDLMORNINGSTAR": {
        "pattern_id": "morning_star_shape",
        "label_zh": "晨星型態",
        "family": "three_bar",
        "priority": 80,
    },
    "CDLEVENINGSTAR": {
        "pattern_id": "evening_star_shape",
        "label_zh": "夜星型態",
        "family": "three_bar",
        "priority": 80,
    },
    "CDL3WHITESOLDIERS": {
        "pattern_id": "three_white_soldiers_shape",
        "label_zh": "紅三兵型態",
        "family": "three_bar",
        "priority": 85,
    },
    "CDL3BLACKCROWS": {
        "pattern_id": "three_black_crows_shape",
        "label_zh": "黑三鴉型態",
        "family": "three_bar",
        "priority": 85,
    },
}

COMPONENT_BARS = {
    "CDLENGULFING": 2,
    "CDLMORNINGSTAR": 3,
    "CDLEVENINGSTAR": 3,
    "CDL3WHITESOLDIERS": 3,
    "CDL3BLACKCROWS": 3,
}


def _talib():
    try:
        import talib
        from talib import abstract
    except ImportError as exc:  # pragma: no cover - CI installs the locked wheel
        raise RuntimeError("TA-Lib is required; candlestick calculation failed closed") from exc
    return talib, abstract


def talib_pattern_functions() -> tuple[str, ...]:
    talib, _ = _talib()
    return tuple(sorted(talib.get_function_groups()["Pattern Recognition"]))


def _rounded(value: Any, digits: int = 6) -> float | None:
    if value is None or pd.isna(value) or not np.isfinite(float(value)):
        return None
    return round(float(value), digits)


def _context_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    previous_close = out["close"].shift(1)
    true_range = pd.concat(
        [out["high"] - out["low"], (out["high"] - previous_close).abs(), (out["low"] - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    out["body"] = (out["close"] - out["open"]).abs()
    out["range"] = out["high"] - out["low"]
    out["body_ratio"] = out["body"] / out["range"].replace(0, np.nan)
    out["upper_shadow_ratio"] = (out["high"] - out[["open", "close"]].max(axis=1)) / out["range"].replace(0, np.nan)
    out["lower_shadow_ratio"] = (out[["open", "close"]].min(axis=1) - out["low"]) / out["range"].replace(0, np.nan)
    out["atr14_prev"] = true_range.rolling(14, min_periods=14).mean().shift(1)
    out["prior_high60"] = out["high"].shift(1).rolling(60, min_periods=60).max()
    out["prior_low60"] = out["low"].shift(1).rolling(60, min_periods=60).min()
    out["position60"] = (out["close"] - out["prior_low60"]) / (out["prior_high60"] - out["prior_low60"]).replace(0, np.nan)
    out["volume_mean20_prev"] = out["volume"].shift(1).rolling(20, min_periods=20).mean()
    out["relative_volume_prev20"] = out["volume"] / out["volume_mean20_prev"].replace(0, np.nan)
    return out


def _body_percentile(frame: pd.DataFrame, index: int) -> float | None:
    if index < 60:
        return None
    current = float(frame.iloc[index]["body"])
    prior = frame.iloc[index - 60 : index]["body"]
    return float((prior <= current).mean())


def _position_label(position: float | None) -> str:
    if position is None:
        return "unknown"
    if position >= 0.8:
        return "high_zone"
    if position <= 0.2:
        return "low_zone"
    return "mid_zone"


def _volume_label(relative_volume: float | None) -> str:
    if relative_volume is None:
        return "unknown"
    if relative_volume >= 1.2:
        return "expanded"
    if relative_volume <= 0.8:
        return "contracted"
    return "normal"


def _event(
    frame: pd.DataFrame,
    index: int,
    *,
    symbol: str,
    pattern_id: str,
    label_zh: str,
    family: str,
    orientation: str,
    engine: str,
    priority: int,
    talib_function: str | None = None,
    raw_value: int | None = None,
    minimum_lookback: int | None = None,
    component_bars: int = 1,
) -> dict[str, Any]:
    row = frame.iloc[index]
    date = iso_date(row["date"])
    atr = _rounded(row.get("atr14_prev"))
    body = _rounded(row.get("body"))
    body_atr = _rounded(body / atr if body is not None and atr not in (None, 0) else None)
    body_pct60 = _rounded(_body_percentile(frame, index))
    position60 = _rounded(row.get("position60"))
    relative_volume = _rounded(row.get("relative_volume_prev20"))
    notes = ["descriptive_annotation_only"]
    if talib_function:
        notes.append("talib_geometry_does_not_imply_required_trend_context")
    return {
        "event_id": f"{symbol}:1d:{date}:{pattern_id}",
        "bar_date": date,
        "bar_status": "closed",
        "pattern_id": pattern_id,
        "label_zh": label_zh,
        "family": family,
        "orientation": orientation,
        "engine": engine,
        "raw": {
            "function": talib_function,
            "value": raw_value,
            "minimum_lookback": minimum_lookback,
        },
        "geometry": {
            "component_bars": component_bars,
            "body_ratio": _rounded(row.get("body_ratio")),
            "body_atr_prev": body_atr,
            "body_percentile60": body_pct60,
            "upper_shadow_ratio": _rounded(row.get("upper_shadow_ratio")),
            "lower_shadow_ratio": _rounded(row.get("lower_shadow_ratio")),
            "position60": position60,
        },
        "context": {
            "position": _position_label(position60),
            "trend": "not_required",
            "volume": _volume_label(relative_volume),
            "relative_volume_prev20": relative_volume,
            "corporate_action_overlap": None,
        },
        "display": {
            "display_priority": int(priority),
            "marker_group": orientation,
            "short_label": label_zh,
        },
        "notes": notes,
    }


def _talib_events(frame: pd.DataFrame, *, symbol: str, public_only: bool) -> tuple[list[dict[str, Any]], str, int]:
    talib, abstract = _talib()
    functions = talib_pattern_functions()
    selected = tuple(name for name in functions if not public_only or name in PUBLIC_TALIB_PATTERNS)
    output: list[dict[str, Any]] = []
    prices = frame[["open", "high", "low", "close"]]
    for function_name in selected:
        function = abstract.Function(function_name)
        result = function(prices)
        metadata = PUBLIC_TALIB_PATTERNS.get(function_name)
        for index in np.flatnonzero(result.to_numpy(dtype=int)):
            raw_value = int(result.iloc[index])
            if function_name == "CDLENGULFING":
                up = raw_value > 0
                pattern_id = "engulfing_up_body" if up else "engulfing_down_body"
                label_zh = "陽吞噬" if up else "陰吞噬"
                orientation = "up_body" if up else "down_body"
            elif metadata:
                pattern_id = str(metadata["pattern_id"])
                label_zh = str(metadata["label_zh"])
                orientation = "positive_raw" if raw_value > 0 else "negative_raw" if raw_value < 0 else "neutral"
            else:
                pattern_id = f"talib_{function_name[3:].lower()}"
                label_zh = str(function.info["display_name"])
                orientation = "positive_raw" if raw_value > 0 else "negative_raw"
            output.append(
                _event(
                    frame,
                    int(index),
                    symbol=symbol,
                    pattern_id=pattern_id,
                    label_zh=label_zh,
                    family=str(metadata["family"]) if metadata else "talib_pattern",
                    orientation=orientation,
                    engine="ta-lib",
                    priority=int(metadata["priority"]) if metadata else 10,
                    talib_function=function_name,
                    raw_value=raw_value,
                    minimum_lookback=int(function.lookback),
                    component_bars=COMPONENT_BARS.get(function_name, 1),
                )
            )
    return output, str(talib.__version__), len(functions)


def _custom_events(frame: pd.DataFrame, *, symbol: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        if not np.isfinite(float(row["range"])) or float(row["range"]) <= 0:
            continue
        body_ratio = float(row["body_ratio"])
        upper_ratio = float(row["upper_shadow_ratio"])
        lower_ratio = float(row["lower_shadow_ratio"])
        body_pct60 = _body_percentile(frame, int(index))
        atr = _rounded(row.get("atr14_prev"))
        body = float(row["body"])
        long_body = body_ratio >= 0.6 and (
            (atr is not None and body >= 1.2 * atr) or (body_pct60 is not None and body_pct60 >= 0.9)
        )
        up_body = float(row["close"]) > float(row["open"])
        if long_body:
            base_id = "long_bullish_body" if up_body else "long_bearish_body"
            base_label = "長紅 K" if up_body else "長黑 K"
            orientation = "up_body" if up_body else "down_body"
            output.append(
                _event(
                    frame,
                    int(index),
                    symbol=symbol,
                    pattern_id=base_id,
                    label_zh=base_label,
                    family="long_body",
                    orientation=orientation,
                    engine="custom",
                    priority=40,
                )
            )
            position = _rounded(row.get("position60"))
            if not up_body and position is not None and position >= 0.8:
                output.append(
                    _event(
                        frame,
                        int(index),
                        symbol=symbol,
                        pattern_id="high_zone_long_bearish",
                        label_zh="高檔長黑",
                        family="body_with_position",
                        orientation="down_body",
                        engine="custom",
                        priority=100,
                    )
                )
            if up_body and position is not None and position <= 0.2:
                output.append(
                    _event(
                        frame,
                        int(index),
                        symbol=symbol,
                        pattern_id="low_zone_long_bullish",
                        label_zh="低檔長紅",
                        family="body_with_position",
                        orientation="up_body",
                        engine="custom",
                        priority=100,
                    )
                )
        if upper_ratio >= 0.55:
            output.append(
                _event(
                    frame,
                    int(index),
                    symbol=symbol,
                    pattern_id="long_upper_shadow",
                    label_zh="長上影",
                    family="shadow",
                    orientation="upper_shadow",
                    engine="custom",
                    priority=50,
                )
            )
        if lower_ratio >= 0.55:
            output.append(
                _event(
                    frame,
                    int(index),
                    symbol=symbol,
                    pattern_id="long_lower_shadow",
                    label_zh="長下影",
                    family="shadow",
                    orientation="lower_shadow",
                    engine="custom",
                    priority=50,
                )
            )
    return output


def build_candlestick_event_envelope(
    frame: pd.DataFrame,
    *,
    symbol: str,
    market: str = "listed",
    price_basis: str = "raw",
    source_id: str = "daily_price",
    public_only: bool = True,
) -> dict[str, Any]:
    if frame.empty:
        raise ValueError("candlestick input is empty")
    enriched = _context_columns(frame.reset_index(drop=True))
    talib_events, talib_version, catalog_size = _talib_events(enriched, symbol=str(symbol), public_only=public_only)
    events = talib_events + _custom_events(enriched, symbol=str(symbol))
    events.sort(key=lambda item: (item["bar_date"], -item["display"]["display_priority"], item["pattern_id"]))
    data_date = iso_date(enriched.iloc[-1]["date"])
    return {
        "schema_version": SCHEMA_VERSION,
        "feature_version": f"candlestick-v1.0.0-talib-{talib_version}",
        "pattern_config_version": PATTERN_CONFIG_VERSION,
        "symbol": str(symbol),
        "market": market,
        "timeframe": "1d",
        "as_of": data_date,
        "generated_at": f"{data_date}T00:00:00+08:00",
        "source": {
            "dataset_id": source_id,
            "data_date": data_date,
            "price_basis": price_basis,
            "fallback": False,
            "gaps": ["corporate_action_flags_unavailable"] if price_basis == "raw" else [],
        },
        "talib": {
            "version": talib_version,
            "pattern_function_count": catalog_size,
            "calculation_state": "ok",
            "selection": "public_whitelist" if public_only else "all_pattern_recognition_functions",
        },
        "events": events,
    }


def detect_candlestick_patterns(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], list[str]]:
    """Backward-compatible adapter for callers not yet migrated to envelopes."""

    envelope = build_candlestick_event_envelope(frame, symbol="UNKNOWN", public_only=True)
    return envelope["events"], []
