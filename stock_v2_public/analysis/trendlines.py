from __future__ import annotations

from itertools import combinations
from typing import Any

import numpy as np
import pandas as pd

from .core import clamp_score, finite_float, iso_date


def _line_value(first: dict[str, Any], second: dict[str, Any], index: int) -> float:
    span = max(1, int(second["index"]) - int(first["index"]))
    slope = (float(second["price"]) - float(first["price"])) / span
    return float(first["price"]) + slope * (index - int(first["index"]))


def _evaluate_line(
    frame: pd.DataFrame,
    pivots: list[dict[str, Any]],
    first: dict[str, Any],
    second: dict[str, Any],
    kind: str,
    timeframe: str,
) -> dict[str, Any] | None:
    if second["index"] - first["index"] < 4:
        return None
    candidate_pivots = [pivot for pivot in pivots if pivot["index"] >= first["index"]]
    touches: list[dict[str, Any]] = []
    residuals: list[float] = []
    for pivot in candidate_pivots:
        expected = _line_value(first, second, int(pivot["index"]))
        atr = max(float(frame.iloc[pivot["index"]]["atr14"]), abs(float(pivot["price"])) * 0.005)
        residual = abs(float(pivot["price"]) - expected) / atr
        if residual <= 0.65:
            touches.append(pivot)
            residuals.append(residual)
    if len(touches) < 2:
        return None

    last_index = len(frame) - 1
    start_index = int(first["index"])
    violations = 0
    for index in range(start_index, last_index + 1):
        expected = _line_value(first, second, index)
        atr = max(float(frame.iloc[index]["atr14"]), abs(expected) * 0.005)
        close = float(frame.iloc[index]["close"])
        if kind == "support" and close < expected - atr * 0.7:
            violations += 1
        elif kind == "resistance" and close > expected + atr * 0.7:
            violations += 1

    span = max(1, last_index - start_index)
    recency = last_index - int(touches[-1]["index"])
    average_residual = float(np.mean(residuals)) if residuals else 1.0
    score = 25 + len(touches) * 16 + min(span, 120) * 0.12 - violations * 7 - average_residual * 10 - recency * 0.15
    status = "confirmed" if len(touches) >= 3 and violations <= 1 else "forming"
    slope = (_line_value(first, second, last_index) - float(first["price"])) / max(1, last_index - start_index)
    tolerance = max(float(frame.iloc[-1]["atr14"]) * 0.5, float(frame.iloc[-1]["close"]) * 0.005)
    latest_values = [_line_value(first, second, index) for index in (max(0, last_index - 1), last_index)]
    latest_closes = frame.iloc[-2:]["close"].to_numpy(float)
    if kind == "resistance":
        broken = bool(np.all(latest_closes > np.asarray(latest_values) + tolerance))
    else:
        broken = bool(np.all(latest_closes < np.asarray(latest_values) - tolerance))
    return {
        "line_id": f"{timeframe}-{kind}-{first['index']}-{second['index']}",
        "kind": kind,
        "timeframe": timeframe,
        "status": status,
        "start": {"date": first["date"], "price": finite_float(first["price"])},
        "end": {"date": iso_date(frame.iloc[-1]["date"]), "price": finite_float(_line_value(first, second, last_index))},
        "anchors": [{"date": touch["date"], "price": finite_float(touch["price"])} for touch in touches],
        "touch_count": len(touches),
        "violation_count": int(violations),
        "slope_per_bar": finite_float(slope, 8),
        "quality_score": clamp_score(score),
        "breakout": {
            "detected": broken,
            "confirmed_at": iso_date(frame.iloc[-1]["date"]) if broken else None,
            "retest_detected": False,
        },
        "parameters_version": "trendlines-v1",
    }


def detect_trendlines(
    frame: pd.DataFrame, swings: list[dict[str, Any]], timeframe: str, max_lines: int = 3
) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    for pivot_kind, line_kind in (("low", "support"), ("high", "resistance")):
        pivots = [item for item in swings if item["kind"] == pivot_kind][-14:]
        for first, second in combinations(pivots, 2):
            line = _evaluate_line(frame, pivots, first, second, line_kind, timeframe)
            if line:
                lines.append(line)

    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    for line in lines:
        key = (line["kind"], line["start"]["date"], f"{line['end']['price']:.3f}")
        current = unique.get(key)
        if current is None or line["quality_score"] > current["quality_score"]:
            unique[key] = line
    ranked = sorted(
        unique.values(),
        key=lambda item: (item["status"] == "confirmed", item["quality_score"], item["touch_count"]),
        reverse=True,
    )
    # Keep the chart readable: max_lines is the total per timeframe, not per
    # side. Preserve support/resistance diversity before filling by score.
    selected: list[dict[str, Any]] = []
    for kind in ("support", "resistance"):
        best = next((line for line in ranked if line["kind"] == kind), None)
        if best is not None:
            selected.append(best)
    for line in ranked:
        if len(selected) >= max_lines:
            break
        if line["line_id"] not in {item["line_id"] for item in selected}:
            selected.append(line)

    supports = [line for line in selected if line["kind"] == "support" and line["status"] == "confirmed"]
    resistances = [line for line in selected if line["kind"] == "resistance" and line["status"] == "confirmed"]
    if supports and resistances:
        support, resistance = supports[0], resistances[0]
        denominator = max(abs(float(support["slope_per_bar"])), abs(float(resistance["slope_per_bar"])), 1e-9)
        parallel_error = abs(float(support["slope_per_bar"]) - float(resistance["slope_per_bar"])) / denominator
        if parallel_error <= 0.35 and resistance["end"]["price"] > support["end"]["price"]:
            selected.append(
                {
                    "line_id": f"{timeframe}-channel-{support['line_id']}-{resistance['line_id']}",
                    "kind": "channel",
                    "timeframe": timeframe,
                    "status": "confirmed",
                    "start": support["start"],
                    "end": support["end"],
                    "anchors": support["anchors"] + resistance["anchors"],
                    "touch_count": support["touch_count"] + resistance["touch_count"],
                    "violation_count": support["violation_count"] + resistance["violation_count"],
                    "slope_per_bar": support["slope_per_bar"],
                    "quality_score": clamp_score((support["quality_score"] + resistance["quality_score"]) / 2),
                    "breakout": {"detected": False, "confirmed_at": None, "retest_detected": False},
                    "channel": {"lower_line_id": support["line_id"], "upper_line_id": resistance["line_id"]},
                    "parameters_version": "trendlines-v1",
                }
            )
    return sorted(
        selected,
        key=lambda item: (item["status"] == "confirmed", item["quality_score"]),
        reverse=True,
    )[:max_lines]
