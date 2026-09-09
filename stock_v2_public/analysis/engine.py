from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pandas as pd

from .candlesticks import build_candlestick_event_envelope
from .core import finite_float, iso_date, prepare_ohlcv, with_indicators
from .indicators import build_technical_evidence
from .structures import detect_price_structures, detect_support_resistance
from .swings import detect_swings
from .trendlines import detect_trendlines

ENGINE_VERSION = "2.2.0"
SCHEMA_VERSION = "1.2.0"
DAILY_CHART_BARS = 240
MA_WINDOWS = (5, 20, 60, 120, 240)


def _price_adjustment(value: str | dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(value, dict):
        mode = str(value.get("mode") or "none")
        return {"mode": mode, "source": value.get("source"), "verified": bool(value.get("verified", False)), **{key: value[key] for key in ("adjustment_as_of", "event_count", "volume_basis") if key in value}}
    return {"mode": str(value or "none"), "source": None, "verified": value not in (None, "none")}


def _series(frame: pd.DataFrame, limit: int = 520) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for _, row in frame.iloc[-limit:].iterrows():
        item = {
                "date": iso_date(row["date"]),
                "open": finite_float(row["open"]),
                "high": finite_float(row["high"]),
                "low": finite_float(row["low"]),
                "close": finite_float(row["close"]),
                "volume": finite_float(row["volume"], 2),
            }
        for window in MA_WINDOWS:
            item[f"sma{window}"] = finite_float(row.get(f"sma{window}"))
        if "adjustment_factor" in frame.columns:
            item["adjustment_factor"] = finite_float(row.get("adjustment_factor"), 12)
        output.append(item)
    return output


def _series_coverage(frame: pd.DataFrame, *, timeframe: str, limit: int) -> dict[str, Any]:
    available = len(frame)
    returned = min(available, limit)
    sufficient = timeframe != "daily" or available >= DAILY_CHART_BARS
    return {
        "requested_bars": limit,
        "available_bars": available,
        "returned_bars": returned,
        "status": "available" if sufficient else "insufficient_history",
        "message": (
            f"顯示最近 {returned} 根日 K；SMA240 資料不足（需要 {DAILY_CHART_BARS} 根，現有 {available} 根）"
            if timeframe == "daily" and not sufficient
            else f"顯示最近 {returned} 根{('日 K' if timeframe == 'daily' else 'K 線')}"
        ),
    }


def analyze_ohlcv(
    data: pd.DataFrame | list[dict[str, Any]],
    *,
    stock_id: str,
    timeframe: str = "daily",
    price_adjustment: str | dict[str, Any] | None = None,
    decision: dict[str, Any] | None = None,
    freshness: dict[str, Any] | None = None,
    market: str = "listed",
    source_data_date: str | None = None,
) -> dict[str, Any]:
    if timeframe not in {"daily", "weekly", "monthly"}:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    frame = with_indicators(prepare_ohlcv(data, min_rows=1 if timeframe == "daily" else 30))
    adjustment = _price_adjustment(price_adjustment)
    warnings: list[str] = []
    enough_for_geometry = len(frame) >= 30
    swings = detect_swings(frame, timeframe) if enough_for_geometry else []
    patterns = detect_price_structures(frame, swings) if enough_for_geometry else []
    trendlines = detect_trendlines(frame, swings, timeframe) if enough_for_geometry else []
    zones = detect_support_resistance(frame, swings) if enough_for_geometry else []

    if timeframe in {"weekly", "monthly"} and not adjustment["verified"]:
        warnings.append("adjusted-price metadata is missing; long-horizon geometry confidence was reduced")
        for item in patterns:
            item["quality_score"] = round(float(item["quality_score"]) * 0.8, 2)
        for item in trendlines:
            item["quality_score"] = round(float(item["quality_score"]) * 0.8, 2)

    data_date = iso_date(source_data_date) if source_data_date is not None else iso_date(frame.iloc[-1]["date"])
    candlestick_annotations = (
        build_candlestick_event_envelope(
            frame,
            symbol=str(stock_id),
            market=market,
            price_basis="raw" if adjustment["mode"] == "none" else adjustment["mode"],
            public_only=True,
        )
        if timeframe == "daily" and enough_for_geometry
        else None
    )
    technical_evidence = (
        build_technical_evidence(frame, stock_id=str(stock_id), market=market, freshness=freshness)
        if timeframe == "daily"
        else []
    )
    safe_decision = deepcopy(decision or {})
    safe_decision.setdefault("action_state", "UNKNOWN")
    series_limit = DAILY_CHART_BARS if timeframe == "daily" else 520
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
        "candlestick_annotations": candlestick_annotations,
        "series": _series(frame, series_limit),
        "series_coverage": _series_coverage(frame, timeframe=timeframe, limit=series_limit),
        "swings": swings,
        "patterns": sorted(patterns, key=lambda item: (item["quality_score"], item["pattern_id"]), reverse=True),
        "trendlines": trendlines,
        "support_resistance": zones,
        "warnings": sorted(set(warnings)),
    }
    return packet


def _resample(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    prepared = prepare_ohlcv(frame, min_rows=1).set_index("date")
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
    frame = prepare_ohlcv(data, min_rows=1)
    source_data_date = iso_date(frame.iloc[-1]["date"])
    packets: list[dict[str, Any]] = []
    for timeframe in ("daily", "weekly", "monthly"):
        resampled = _resample(frame, timeframe)
        if timeframe != "daily" and len(resampled) < 30:
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
                source_data_date=source_data_date,
            )
        )
    return packets


def stable_json(packet: dict[str, Any] | list[dict[str, Any]]) -> str:
    return json.dumps(packet, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
