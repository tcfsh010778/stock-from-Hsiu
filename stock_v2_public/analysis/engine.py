from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pandas as pd

from .candlesticks import detect_candlestick_patterns
from .core import finite_float, iso_date, prepare_ohlcv, with_indicators
from .indicators import build_technical_evidence
from .structures import detect_price_structures, detect_support_resistance
from .swings import detect_swings
from .trendlines import detect_trendlines

ENGINE_VERSION = "2.0.0"
SCHEMA_VERSION = "1.0.0"


def _price_adjustment(value: str | dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(value, dict):
        mode = str(value.get("mode") or "none")
        return {"mode": mode, "source": value.get("source"), "verified": bool(value.get("verified", False))}
    return {"mode": str(value or "none"), "source": None, "verified": value not in (None, "none")}


def _series(frame: pd.DataFrame, limit: int = 520) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for _, row in frame.iloc[-limit:].iterrows():
        output.append(
            {
                "date": iso_date(row["date"]),
                "open": finite_float(row["open"]),
                "high": finite_float(row["high"]),
                "low": finite_float(row["low"]),
                "close": finite_float(row["close"]),
                "volume": finite_float(row["volume"], 2),
            }
        )
    return output


def analyze_ohlcv(
    data: pd.DataFrame | list[dict[str, Any]],
    *,
    stock_id: str,
    timeframe: str = "daily",
    price_adjustment: str | dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    freshness: dict[str, Any] | None = None,
    market: str = "listed",
) -> dict[str, Any]:
    if timeframe not in {"daily", "weekly", "monthly"}:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    frame = with_indicators(prepare_ohlcv(data))
    adjustment = _price_adjustment(price_adjustment)
    warnings: list[str] = []
    candles, candle_warnings = detect_candlestick_patterns(frame)
    warnings.extend(candle_warnings)
    swings = detect_swings(frame, timeframe)
    patterns = candles + detect_price_structures(frame, swings)
    trendlines = detect_trendlines(frame, swings, timeframe)
    zones = detect_support_resistance(frame, swings)

    if timeframe in {"weekly", "monthly"} and not adjustment["verified"]:
        warnings.append("adjusted-price metadata is missing; long-horizon geometry confidence was reduced")
        for item in patterns:
            item["quality_score"] = round(float(item["quality_score"]) * 0.8, 2)
        for item in trendlines:
            item["quality_score"] = round(float(item["quality_score"]) * 0.8, 2)

    data_date = iso_date(frame.iloc[-1]["date"])
    technical_evidence = (
        build_technical_evidence(frame, stock_id=str(stock_id), market=market, freshness=freshness)
        if timeframe == "daily"
        else []
    )
    safe_decision = deepcopy(decision or {})
    safe_decision.setdefault("action_state", "UNKNOWN")
    packet = {
        "schema_version": SCHEMA_VERSION,
        "engine_version": ENGINE_VERSION,
        "stock_id": str(stock_id),
        "timeframe": timeframe,
        "data_date": data_date,
        "generated_at": f"{data_date}T00:00:00+08:00",
        "price_adjustment": adjustment,
        "decision": safe_decision,
        "freshness": deepcopy(freshness or {"status": "unknown"}),
        "technical_evidence": technical_evidence,
        "series": _series(frame),
        "swings": swings,
        "patterns": sorted(patterns, key=lambda item: (item["quality_score"], item["pattern_id"]), reverse=True),
        "trendlines": trendlines,
        "support_resistance": zones,
        "warnings": sorted(set(warnings)),
    }
    return packet


def _resample(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    prepared = prepare_ohlcv(frame).set_index("date")
    if timeframe == "daily":
        return prepared.reset_index()
    rule = "W-FRI" if timeframe == "weekly" else "ME"
    return (
        prepared.resample(rule)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )


def analyze_multi_timeframe(
    data: pd.DataFrame | list[dict[str, Any]],
    *,
    stock_id: str,
    price_adjustment: str | dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    freshness: dict[str, Any] | None = None,
    market: str = "listed",
) -> list[dict[str, Any]]:
    frame = prepare_ohlcv(data)
    packets: list[dict[str, Any]] = []
    for timeframe in ("daily", "weekly", "monthly"):
        resampled = _resample(frame, timeframe)
        if len(resampled) < 30:
            continue
        packets.append(
            analyze_ohlcv(
                resampled,
                stock_id=stock_id,
                timeframe=timeframe,
                price_adjustment=price_adjustment,
                decision=decision,
                freshness=freshness,
                market=market,
            )
        )
    return packets


def stable_json(packet: dict[str, Any] | list[dict[str, Any]]) -> str:
    return json.dumps(packet, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
