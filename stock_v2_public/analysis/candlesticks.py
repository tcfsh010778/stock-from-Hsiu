from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .core import clamp_score, finite_float, iso_date

_TALIB_PATTERNS = {
    "CDLDOJI": ("doji", "十字線", "neutral"),
    "CDLHAMMER": ("hammer", "鎚頭", "bullish"),
    "CDLINVERTEDHAMMER": ("inverted_hammer", "倒鎚頭", "bullish"),
    "CDLSHOOTINGSTAR": ("shooting_star", "流星", "bearish"),
    "CDLENGULFING": ("engulfing", "吞噬", "signed"),
    "CDLMORNINGSTAR": ("morning_star", "晨星", "bullish"),
    "CDLEVENINGSTAR": ("evening_star", "夜星", "bearish"),
}


def _context(frame: pd.DataFrame, index: int, direction: str) -> tuple[list[str], list[str], list[str], float]:
    row = frame.iloc[index]
    start = max(0, index - 59)
    closes = frame.iloc[start : index + 1]["close"]
    percentile = float((closes <= row["close"]).mean())
    sma20 = row.get("sma20")
    sma20_previous = frame.iloc[max(0, index - 5)].get("sma20")
    trend = "unknown"
    if pd.notna(sma20) and pd.notna(sma20_previous):
        trend = "up" if sma20 > sma20_previous else "down" if sma20 < sma20_previous else "flat"
    relative_volume = float(row.get("relative_volume") or 0.0)
    evidence = [f"60-bar price percentile={percentile:.2f}", f"SMA20 trend={trend}"]
    missing: list[str] = []
    counter: list[str] = []
    score = 48.0
    if relative_volume > 1.2:
        evidence.append(f"relative volume={relative_volume:.2f}")
        score += 8
    else:
        missing.append("volume confirmation")
    if direction == "bullish":
        if percentile <= 0.45 or trend == "up":
            score += 12
        else:
            counter.append("bullish candle is high in its recent range")
    elif direction == "bearish":
        if percentile >= 0.55 or trend == "down":
            score += 12
        else:
            counter.append("bearish candle is low in its recent range")
    else:
        score += 4
    return evidence, missing, counter, clamp_score(score)


def _pattern(
    frame: pd.DataFrame,
    index: int,
    pattern_id: str,
    name: str,
    direction: str,
    detector: str,
    lookback: int = 1,
) -> dict[str, Any]:
    evidence, missing, counter, score = _context(frame, index, direction)
    row = frame.iloc[index]
    invalidation = row["low"] if direction == "bullish" else row["high"] if direction == "bearish" else None
    return {
        "pattern_id": f"candle-{pattern_id}-{iso_date(row['date'])}",
        "name": name,
        "category": "candlestick",
        "direction": direction,
        "status": "confirmed",
        "start_date": iso_date(frame.iloc[max(0, index - lookback + 1)]["date"]),
        "end_date": iso_date(row["date"]),
        "confirmed_at": iso_date(row["date"]),
        "invalidation_price": finite_float(invalidation),
        "evidence": evidence,
        "missing_conditions": missing,
        "counterevidence": counter,
        "detector": detector,
        "parameters_version": "candles-v1",
        "quality_score": score,
    }


def _custom_patterns(frame: pd.DataFrame) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for index in range(max(0, len(frame) - 3), len(frame)):
        row = frame.iloc[index]
        candle_range = max(float(row["high"] - row["low"]), 1e-9)
        body = abs(float(row["close"] - row["open"]))
        upper = float(row["high"] - max(row["open"], row["close"]))
        lower = float(min(row["open"], row["close"]) - row["low"])
        atr = max(float(row.get("atr14") or candle_range), 1e-9)
        if body <= candle_range * 0.1:
            output.append(_pattern(frame, index, "doji", "十字線", "neutral", "custom"))
        if lower >= max(body * 2, candle_range * 0.55) and upper <= candle_range * 0.25:
            output.append(_pattern(frame, index, "long-lower-shadow", "長下影", "bullish", "custom"))
        if upper >= max(body * 2, candle_range * 0.55) and lower <= candle_range * 0.25:
            output.append(_pattern(frame, index, "long-upper-shadow", "長上影", "bearish", "custom"))
        if body >= atr * 1.1:
            direction = "bullish" if row["close"] > row["open"] else "bearish"
            name = "長紅 K" if direction == "bullish" else "長黑 K"
            output.append(_pattern(frame, index, "long-body", name, direction, "custom"))
        if index:
            previous = frame.iloc[index - 1]
            bullish = row["close"] > row["open"] and previous["close"] < previous["open"]
            bearish = row["close"] < row["open"] and previous["close"] > previous["open"]
            engulf = min(row["open"], row["close"]) <= min(previous["open"], previous["close"]) and max(
                row["open"], row["close"]
            ) >= max(previous["open"], previous["close"])
            if engulf and (bullish or bearish):
                direction = "bullish" if bullish else "bearish"
                output.append(_pattern(frame, index, "engulfing", "吞噬 K", direction, "custom", lookback=2))
    return output


def detect_candlestick_patterns(frame: pd.DataFrame) -> tuple[list[dict[str, Any]], list[str]]:
    patterns: list[dict[str, Any]] = []
    warnings: list[str] = []
    try:
        import talib
    except ImportError:
        warnings.append("TA-Lib is unavailable; custom candlestick detectors were used")
        return _custom_patterns(frame), warnings

    opens = frame["open"].to_numpy(float)
    highs = frame["high"].to_numpy(float)
    lows = frame["low"].to_numpy(float)
    closes = frame["close"].to_numpy(float)
    recent_start = max(0, len(frame) - 3)
    for function_name, (pattern_id, name, configured_direction) in _TALIB_PATTERNS.items():
        function = getattr(talib, function_name)
        result = function(opens, highs, lows, closes)
        for index in np.flatnonzero(result[recent_start:]) + recent_start:
            signed = int(result[index])
            direction = configured_direction
            if direction == "signed":
                direction = "bullish" if signed > 0 else "bearish"
            patterns.append(_pattern(frame, int(index), pattern_id, name, direction, "ta-lib", lookback=3))

    custom = _custom_patterns(frame)
    seen = {(item["pattern_id"].split("-")[1], item["end_date"]) for item in patterns}
    patterns.extend(
        item for item in custom if (item["pattern_id"].split("-")[1], item["end_date"]) not in seen
    )
    patterns.sort(key=lambda item: (item["end_date"], item["quality_score"], item["pattern_id"]), reverse=True)
    return patterns, warnings
