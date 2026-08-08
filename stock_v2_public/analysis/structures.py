from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .core import clamp_score, finite_float, iso_date


def _structure_pattern(
    frame: pd.DataFrame,
    pattern_id: str,
    name: str,
    direction: str,
    status: str,
    start_index: int,
    evidence: list[str],
    missing: list[str] | None = None,
    counter: list[str] | None = None,
    score: float = 60,
    invalidation_price: float | None = None,
) -> dict[str, Any]:
    return {
        "pattern_id": f"structure-{pattern_id}-{iso_date(frame.iloc[-1]['date'])}",
        "name": name,
        "category": "price_structure",
        "direction": direction,
        "status": status,
        "start_date": iso_date(frame.iloc[max(0, start_index)]["date"]),
        "end_date": iso_date(frame.iloc[-1]["date"]),
        "confirmed_at": iso_date(frame.iloc[-1]["date"]) if status == "confirmed" else None,
        "invalidation_price": finite_float(invalidation_price),
        "evidence": evidence,
        "missing_conditions": missing or [],
        "counterevidence": counter or [],
        "detector": "custom",
        "parameters_version": "structures-v1",
        "quality_score": clamp_score(score),
    }


def detect_price_structures(frame: pd.DataFrame, swings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    patterns: list[dict[str, Any]] = []
    highs = [item for item in swings if item["kind"] == "high"]
    lows = [item for item in swings if item["kind"] == "low"]
    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1]["price"] > highs[-2]["price"]
        hl = lows[-1]["price"] > lows[-2]["price"]
        if hh and hl:
            patterns.append(
                _structure_pattern(
                    frame,
                    "higher-high-higher-low",
                    "高點與低點同步墊高",
                    "bullish",
                    "confirmed",
                    min(highs[-2]["index"], lows[-2]["index"]),
                    ["latest swing high is higher", "latest swing low is higher"],
                    score=76,
                    invalidation_price=lows[-1]["price"],
                )
            )
        elif not hh and not hl:
            patterns.append(
                _structure_pattern(
                    frame,
                    "lower-high-lower-low",
                    "高點與低點同步下移",
                    "bearish",
                    "confirmed",
                    min(highs[-2]["index"], lows[-2]["index"]),
                    ["latest swing high is lower", "latest swing low is lower"],
                    score=76,
                    invalidation_price=highs[-1]["price"],
                )
            )

    for pivots, label, direction in ((highs, "雙頭", "bearish"), (lows, "雙底", "bullish")):
        if len(pivots) < 2:
            continue
        first, second = pivots[-2], pivots[-1]
        distance = abs(second["price"] - first["price"]) / max(abs(first["price"]), 1e-9)
        separated = second["index"] - first["index"] >= 5
        if distance <= 0.03 and separated:
            patterns.append(
                _structure_pattern(
                    frame,
                    "double-top" if direction == "bearish" else "double-bottom",
                    label,
                    direction,
                    "forming",
                    first["index"],
                    [f"two pivots differ by {distance:.2%}", f"pivot separation={second['index'] - first['index']} bars"],
                    ["neckline confirmation"],
                    score=67,
                    invalidation_price=second["price"],
                )
            )

    # Head-and-shoulders geometry is derived only from already-confirmed swings.
    # The neckline is evidence; it never changes the rule engine action_state.
    if len(highs) >= 3 and len(lows) >= 2:
        left, head, right = highs[-3:]
        between_left = [item for item in lows if left["index"] < item["index"] < head["index"]]
        between_right = [item for item in lows if head["index"] < item["index"] < right["index"]]
        if between_left and between_right:
            neckline = (min(between_left, key=lambda item: item["price"])["price"] + min(
                between_right, key=lambda item: item["price"]
            )["price"]) / 2
            shoulders_close = abs(left["price"] - right["price"]) / max(left["price"], right["price"], 1e-9)
            head_clear = head["price"] > max(left["price"], right["price"]) * 1.015
            depth_ok = (head["price"] - neckline) / max(neckline, 1e-9) >= 0.04
            if shoulders_close <= 0.06 and head_clear and depth_ok:
                broke = float(frame.iloc[-1]["close"]) < neckline * 0.995
                patterns.append(
                    _structure_pattern(
                        frame,
                        "head-shoulders-top",
                        "頭肩頂",
                        "bearish",
                        "confirmed" if broke else "forming",
                        left["index"],
                        [
                            f"shoulder difference={shoulders_close:.2%}",
                            f"head is above both shoulders; neckline={neckline:.2f}",
                        ],
                        [] if broke else ["close below neckline"],
                        score=84 if broke else 70,
                        invalidation_price=head["price"],
                    )
                )

    if len(lows) >= 3 and len(highs) >= 2:
        left, head, right = lows[-3:]
        between_left = [item for item in highs if left["index"] < item["index"] < head["index"]]
        between_right = [item for item in highs if head["index"] < item["index"] < right["index"]]
        if between_left and between_right:
            neckline = (max(between_left, key=lambda item: item["price"])["price"] + max(
                between_right, key=lambda item: item["price"]
            )["price"]) / 2
            shoulders_close = abs(left["price"] - right["price"]) / max(left["price"], right["price"], 1e-9)
            head_clear = head["price"] < min(left["price"], right["price"]) * 0.985
            depth_ok = (neckline - head["price"]) / max(head["price"], 1e-9) >= 0.04
            if shoulders_close <= 0.06 and head_clear and depth_ok:
                broke = float(frame.iloc[-1]["close"]) > neckline * 1.005
                patterns.append(
                    _structure_pattern(
                        frame,
                        "inverse-head-shoulders",
                        "頭肩底",
                        "bullish",
                        "confirmed" if broke else "forming",
                        left["index"],
                        [
                            f"shoulder difference={shoulders_close:.2%}",
                            f"head is below both shoulders; neckline={neckline:.2f}",
                        ],
                        [] if broke else ["close above neckline"],
                        score=84 if broke else 70,
                        invalidation_price=head["price"],
                    )
                )

    lookback = min(20, len(frame) - 1)
    previous = frame.iloc[-lookback - 1 : -1]
    current = frame.iloc[-1]
    box_high = float(previous["high"].max())
    box_low = float(previous["low"].min())
    width = (box_high - box_low) / max(float(previous["close"].median()), 1e-9)
    relative_volume = float(current.get("relative_volume") or 0.0)
    if width <= 0.16:
        patterns.append(
            _structure_pattern(
                frame,
                "box-consolidation",
                "箱型整理",
                "neutral",
                "forming",
                len(frame) - lookback - 1,
                [f"20-bar range width={width:.2%}", f"box={box_low:.2f}–{box_high:.2f}"],
                ["close outside the box"],
                score=72 - width * 100,
            )
        )
    if float(current["close"]) > box_high:
        patterns.append(
            _structure_pattern(
                frame,
                "box-breakout",
                "箱頂突破",
                "bullish",
                "confirmed" if relative_volume >= 1.2 else "forming",
                len(frame) - lookback - 1,
                [f"close {current['close']:.2f} above prior box {box_high:.2f}", f"relative volume={relative_volume:.2f}"],
                [] if relative_volume >= 1.2 else ["volume confirmation"],
                score=82 if relative_volume >= 1.2 else 62,
                invalidation_price=box_high,
            )
        )
    elif float(current["close"]) < box_low:
        patterns.append(
            _structure_pattern(
                frame,
                "box-breakdown",
                "箱底跌破",
                "bearish",
                "confirmed",
                len(frame) - lookback - 1,
                [f"close {current['close']:.2f} below prior box {box_low:.2f}"],
                score=78,
                invalidation_price=box_low,
            )
        )

    recent = frame.iloc[-6:]
    if len(recent) >= 3 and float(recent.iloc[:-1]["high"].max()) > box_high and float(current["close"]) < box_high:
        patterns.append(
            _structure_pattern(
                frame,
                "false-breakout",
                "箱頂假突破回落",
                "bearish",
                "confirmed",
                len(frame) - lookback - 1,
                ["recent high exceeded the box", "latest close returned below box resistance"],
                score=80,
                invalidation_price=float(recent["high"].max()),
            )
        )

    # Break-bottom reversal: a new intraday low is reclaimed on a closing basis.
    if len(frame) >= 25:
        reference = float(frame.iloc[-22:-2]["low"].min())
        latest, previous_bar = frame.iloc[-1], frame.iloc[-2]
        reclaimed = (
            (float(latest["low"]) < reference * 0.995 and float(latest["close"]) > reference)
            or (float(previous_bar["low"]) < reference * 0.995 and float(latest["close"]) > reference)
        )
        if reclaimed:
            trap_low = min(float(latest["low"]), float(previous_bar["low"]))
            patterns.append(
                _structure_pattern(
                    frame,
                    "break-bottom-reversal",
                    "破底翻",
                    "bullish",
                    "confirmed",
                    len(frame) - 22,
                    [f"prior support={reference:.2f}", "intraday break was reclaimed by the close"],
                    score=82,
                    invalidation_price=trap_low,
                )
            )

    # Stock From Zero's four trend/flag combinations are retained as explicit,
    # measurable candidates. The names describe the preceding trend and the
    # consolidation slope; confirmation still requires the rule-side trigger.
    if len(frame) >= 30:
        consolidation = frame.iloc[-8:]
        preceding = frame.iloc[-20:-8]
        start_close, end_close = float(preceding.iloc[0]["close"]), float(preceding.iloc[-1]["close"])
        trend_return = end_close / max(start_close, 1e-9) - 1
        range_pct = (float(consolidation["high"].max()) - float(consolidation["low"].min())) / max(
            float(consolidation.iloc[-1]["close"]), 1e-9
        )
        mid_first = (float(consolidation.iloc[0]["high"]) + float(consolidation.iloc[0]["low"])) / 2
        mid_last = (float(consolidation.iloc[-1]["high"]) + float(consolidation.iloc[-1]["low"])) / 2
        flag_return = mid_last / max(mid_first, 1e-9) - 1
        volume_contracts = float(consolidation.iloc[-3:]["volume"].mean()) <= float(
            consolidation.iloc[:3]["volume"].mean()
        ) * 1.1
        if abs(trend_return) >= 0.08 and range_pct <= 0.10 and abs(flag_return) >= 0.003 and volume_contracts:
            trend_name = "多頭" if trend_return > 0 else "空頭"
            flag_name = "向上" if flag_return > 0 else "向下"
            expected = "等待突破後回測" if (trend_return > 0) != (flag_return > 0) else "等待跌破後反彈"
            patterns.append(
                _structure_pattern(
                    frame,
                    f"sfz-flag-{'up' if trend_return > 0 else 'down'}-{'up' if flag_return > 0 else 'down'}",
                    f"SFZ {trend_name}／{flag_name}整理",
                    "bullish" if expected == "等待突破後回測" else "bearish",
                    "forming",
                    len(frame) - 20,
                    [
                        f"preceding trend return={trend_return:.2%}",
                        f"consolidation range={range_pct:.2%}",
                        "volume contracted during consolidation",
                    ],
                    [expected],
                    score=71,
                    invalidation_price=(
                        float(consolidation["low"].min())
                        if expected == "等待突破後回測"
                        else float(consolidation["high"].max())
                    ),
                )
            )

    # A quantitative M-style X candidate: the long decline has flattened,
    # the latest confirmed low is higher, and price has reclaimed SMA60.
    if len(frame) >= 80 and len(lows) >= 2:
        sma60_now = frame.iloc[-1].get("sma60")
        sma60_then = frame.iloc[-20].get("sma60")
        prior_decline = float(frame.iloc[-80]["close"]) > float(frame.iloc[-35]["close"]) * 1.08
        higher_low = lows[-1]["price"] > lows[-2]["price"]
        reclaimed = pd.notna(sma60_now) and float(frame.iloc[-1]["close"]) > float(sma60_now)
        flattening = pd.notna(sma60_then) and float(sma60_now) >= float(sma60_then) * 0.99
        if prior_decline and higher_low and reclaimed and flattening:
            patterns.append(
                _structure_pattern(
                    frame,
                    "m-x-candidate",
                    "M 哥 X 型態候選",
                    "bullish",
                    "forming",
                    max(0, lows[-2]["index"] - 20),
                    ["prior decline exceeded 8%", "confirmed low is higher", "close reclaimed SMA60"],
                    ["right-side breakout and volume/chip confirmation"],
                    ["candidate is not a confirmed breakout"],
                    score=66,
                    invalidation_price=lows[-1]["price"],
                )
            )
    if len(frame) >= 45:
        sample = frame.iloc[-45:]
        x = np.linspace(-1.0, 1.0, len(sample))
        coefficients = np.polyfit(x, sample["close"].to_numpy(float), 2)
        fitted = np.polyval(coefficients, x)
        residual = float(np.mean(np.abs(fitted - sample["close"].to_numpy(float))))
        scale = max(float(sample["close"].mean()), 1e-9)
        if coefficients[0] > scale * 0.02 and coefficients[1] > 0 and residual / scale < 0.08:
            patterns.append(
                _structure_pattern(
                    frame,
                    "smile-curve",
                    "微笑曲線",
                    "bullish",
                    "forming",
                    len(frame) - 45,
                    ["positive quadratic curvature", "right-side slope is positive"],
                    ["breakout and volume confirmation"],
                    score=68,
                    invalidation_price=float(sample["low"].min()),
                )
            )
    return sorted(patterns, key=lambda item: (item["quality_score"], item["pattern_id"]), reverse=True)


def detect_support_resistance(frame: pd.DataFrame, swings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    zones: list[dict[str, Any]] = []
    atr = max(float(frame.iloc[-1]["atr14"]), float(frame.iloc[-1]["close"]) * 0.005)
    for swing in swings[-8:]:
        zones.append(
            {
                "zone_id": f"swing-{swing['swing_id']}",
                "kind": "resistance" if swing["kind"] == "high" else "support",
                "price_low": finite_float(swing["price"] - atr * 0.25),
                "price_high": finite_float(swing["price"] + atr * 0.25),
                "basis": "confirmed swing",
                "quality_score": clamp_score(55 + min(float(swing.get("prominence") or 0) / atr * 8, 25)),
            }
        )
    recent = frame.iloc[-20:]
    zones.extend(
        [
            {
                "zone_id": "box-upper-20",
                "kind": "resistance",
                "price_low": finite_float(recent["high"].max() - atr * 0.35),
                "price_high": finite_float(recent["high"].max()),
                "basis": "20-bar upper range",
                "quality_score": 65.0,
            },
            {
                "zone_id": "box-lower-20",
                "kind": "support",
                "price_low": finite_float(recent["low"].min()),
                "price_high": finite_float(recent["low"].min() + atr * 0.35),
                "basis": "20-bar lower range",
                "quality_score": 65.0,
            },
        ]
    )
    spike = frame.iloc[int(frame.iloc[-60:]["volume"].to_numpy().argmax()) + max(0, len(frame) - 60)]
    zones.append(
        {
            "zone_id": f"volume-spike-{iso_date(spike['date'])}",
            "kind": "reference",
            "price_low": finite_float(spike["low"]),
            "price_high": finite_float(spike["high"]),
            "basis": "highest-volume candle in last 60 bars",
            "quality_score": 70.0,
        }
    )
    zones.sort(key=lambda item: (item["quality_score"], item["zone_id"]), reverse=True)
    return zones[:10]
