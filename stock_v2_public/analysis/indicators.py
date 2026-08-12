"""Closed-bar technical evidence for the public V2 analysis surface.

The functions in this module return evidence packets rather than trading
signals. They are deterministic, use only completed rows supplied by the
caller, and keep calculation metadata beside every value.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from .core import finite_float, iso_date

INDICATOR_DEFINITION_VERSION = "technical-indicators-v1"
_FRESHNESS_STATUSES = {
    "fresh",
    "expected_lag",
    "stale",
    "missing",
    "fallback_fresh",
    "fallback_stale",
    "schema_error",
}


def _number(value: Any, digits: int = 6) -> float | None:
    number = finite_float(value, digits)
    return number if number is not None and math.isfinite(number) else None


def _date(value: Any, fallback: str) -> str:
    try:
        return iso_date(value)
    except (TypeError, ValueError, OverflowError):
        return fallback


def _freshness_metadata(
    freshness: dict[str, Any] | None,
    *,
    data_date: str,
) -> tuple[dict[str, str | None], str, list[str]]:
    metadata = freshness or {}
    raw_status = str(metadata.get("status") or "")
    status = raw_status if raw_status in _FRESHNESS_STATUSES else "missing"
    known_gaps: list[str] = []
    if status == "missing" and raw_status not in {"", "missing"}:
        known_gaps.append(f"input freshness status '{raw_status}' is not classified")
    if not raw_status:
        known_gaps.append("freshness status metadata was not provided")
    as_of = _date(metadata.get("as_of") or metadata.get("data_date"), data_date)
    sla = str(metadata.get("sla") or "max 1 official trading-day lag after close")
    fetched_at = str(metadata.get("fetched_at") or f"{data_date}T00:00:00+08:00")
    if not metadata.get("fetched_at"):
        known_gaps.append("fetched_at falls back to the closed-bar data date for deterministic input")
    return {"status": status, "as_of": as_of, "sla": sla}, fetched_at, known_gaps


def _packet(
    *,
    stock_id: str,
    market: str,
    data_date: str,
    indicator_id: str,
    name: str,
    parameters: dict[str, Any],
    unit: str,
    comparison_basis: str,
    value: float | None,
    value_status: str,
    comparison_values: dict[str, Any] | None,
    freshness: dict[str, Any] | None,
    confirmed: bool,
    known_gaps: list[str] | None = None,
) -> dict[str, Any]:
    freshness_packet, fetched_at, freshness_gaps = _freshness_metadata(freshness, data_date=data_date)
    normalized_market = market if market in {"listed", "otc", "emerging"} else "listed"
    gaps = list(dict.fromkeys([*(known_gaps or []), *freshness_gaps]))
    if normalized_market != market:
        gaps.append(f"market '{market}' is not recognized; normalized to listed")
    return {
        "schema_version": "1.0.0",
        "dataset_id": "technical_indicator_evidence",
        "security_id": str(stock_id),
        "market": normalized_market,
        "data_date": data_date,
        "timeframe": "daily",
        "indicator_id": indicator_id,
        "name": name,
        "definition_version": INDICATOR_DEFINITION_VERSION,
        "parameters": parameters,
        "unit": unit,
        "comparison_basis": comparison_basis,
        "missing_value_policy": "null with value_status when history or input is insufficient",
        "extreme_value_policy": "retain finite extreme values and show threshold context; never clip",
        "value": value,
        "value_status": value_status,
        "calculation_basis": "closed_bar_only",
        "confirmed_at": data_date if confirmed else None,
        "source_id": "technical_indicator_evidence_derived",
        "upstream_dataset_id": "daily_price",
        "fetched_at": fetched_at,
        "frequency": "daily_after_close",
        "timezone": "Asia/Taipei",
        "adjustment_policy": "use the caller's OHLCV adjustment policy; no adjustment is inferred here",
        "comparison_values": comparison_values or {},
        "test_status": "unit_tested",
        "evidence_role": "auxiliary_evidence_only",
        "freshness": freshness_packet,
        "known_gaps": gaps,
    }


def _available(
    *,
    stock_id: str,
    market: str,
    data_date: str,
    indicator_id: str,
    name: str,
    parameters: dict[str, Any],
    unit: str,
    comparison_basis: str,
    value: Any,
    comparison_values: dict[str, Any],
    freshness: dict[str, Any] | None,
    known_gaps: list[str] | None = None,
) -> dict[str, Any]:
    finite_value = _number(value)
    status = "available" if finite_value is not None else "non_finite"
    normalized_comparisons = {
        key: (_number(item) if isinstance(item, (int, float)) else item)
        for key, item in comparison_values.items()
    }
    return _packet(
        stock_id=stock_id,
        market=market,
        data_date=data_date,
        indicator_id=indicator_id,
        name=name,
        parameters=parameters,
        unit=unit,
        comparison_basis=comparison_basis,
        value=finite_value,
        value_status=status,
        comparison_values=normalized_comparisons,
        freshness=freshness,
        confirmed=finite_value is not None,
        known_gaps=known_gaps,
    )


def _insufficient(
    *,
    stock_id: str,
    market: str,
    data_date: str,
    indicator_id: str,
    name: str,
    parameters: dict[str, Any],
    unit: str,
    comparison_basis: str,
    required_rows: int,
    actual_rows: int,
    freshness: dict[str, Any] | None,
) -> dict[str, Any]:
    return _packet(
        stock_id=stock_id,
        market=market,
        data_date=data_date,
        indicator_id=indicator_id,
        name=name,
        parameters=parameters,
        unit=unit,
        comparison_basis=comparison_basis,
        value=None,
        value_status="insufficient_history",
        comparison_values={"required_rows": required_rows, "actual_rows": actual_rows},
        freshness=freshness,
        confirmed=False,
        known_gaps=[f"requires {required_rows} closed bars; received {actual_rows}"],
    )


def _rsi_zone(value: float) -> str:
    if value >= 70:
        return "overbought_context"
    if value <= 30:
        return "oversold_context"
    return "middle_range"


def _indicator_rsi(close: pd.Series, *, stock_id: str, market: str, data_date: str, freshness: dict[str, Any] | None) -> dict[str, Any]:
    period = 14
    if len(close) < period + 1:
        return _insufficient(
            stock_id=stock_id, market=market, data_date=data_date, indicator_id="rsi_14", name="RSI(14)",
            parameters={"period": period, "smoothing": "Wilder RMA"}, unit="index_0_100",
            comparison_basis="30 oversold-context / 70 overbought-context reference levels",
            required_rows=period + 1, actual_rows=len(close), freshness=freshness,
        )
    delta = close.diff().iloc[1:]
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    average_gain = float(gains.iloc[:period].mean())
    average_loss = float(losses.iloc[:period].mean())
    for gain, loss in zip(gains.iloc[period:], losses.iloc[period:]):
        average_gain = ((average_gain * (period - 1)) + float(gain)) / period
        average_loss = ((average_loss * (period - 1)) + float(loss)) / period
    if average_loss == 0:
        value = 50.0 if average_gain == 0 else 100.0
    else:
        value = 100.0 - (100.0 / (1.0 + float(average_gain) / float(average_loss)))
    return _available(
        stock_id=stock_id, market=market, data_date=data_date, indicator_id="rsi_14", name="RSI(14)",
        parameters={"period": period, "smoothing": "Wilder RMA"}, unit="index_0_100",
        comparison_basis="30 oversold-context / 70 overbought-context reference levels", value=value,
        comparison_values={"oversold_reference": 30.0, "overbought_reference": 70.0, "zone": _rsi_zone(value)}, freshness=freshness,
    )


def _indicator_macd(close: pd.Series, *, stock_id: str, market: str, data_date: str, freshness: dict[str, Any] | None) -> dict[str, Any]:
    fast, slow, signal_period = 12, 26, 9
    required_rows = slow + signal_period - 1
    if len(close) < required_rows:
        return _insufficient(
            stock_id=stock_id, market=market, data_date=data_date, indicator_id="macd_12_26_9", name="MACD(12,26,9)",
            parameters={"fast": fast, "slow": slow, "signal": signal_period, "ema_adjust": False}, unit="price_units",
            comparison_basis="MACD line versus signal line; histogram is MACD minus signal",
            required_rows=required_rows, actual_rows=len(close), freshness=freshness,
        )
    fast_ema = close.ewm(span=fast, adjust=False, min_periods=fast).mean()
    slow_ema = close.ewm(span=slow, adjust=False, min_periods=slow).mean()
    macd = fast_ema - slow_ema
    signal = macd.ewm(span=signal_period, adjust=False, min_periods=signal_period).mean()
    macd_value = _number(macd.iloc[-1])
    signal_value = _number(signal.iloc[-1])
    histogram = _number(macd.iloc[-1] - signal.iloc[-1]) if macd_value is not None and signal_value is not None else None
    return _available(
        stock_id=stock_id, market=market, data_date=data_date, indicator_id="macd_12_26_9", name="MACD(12,26,9)",
        parameters={"fast": fast, "slow": slow, "signal": signal_period, "ema_adjust": False}, unit="price_units",
        comparison_basis="MACD line versus signal line; histogram is MACD minus signal", value=macd_value,
        comparison_values={"macd": macd_value, "signal": signal_value, "histogram": histogram, "bias": "above_signal" if histogram is not None and histogram > 0 else "below_signal" if histogram is not None else "unavailable"}, freshness=freshness,
    )


def _indicator_bollinger(close: pd.Series, *, stock_id: str, market: str, data_date: str, freshness: dict[str, Any] | None) -> dict[str, Any]:
    period, deviations = 20, 2.0
    if len(close) < period:
        return _insufficient(
            stock_id=stock_id, market=market, data_date=data_date, indicator_id="bollinger_20_2", name="布林軌道(20,2)",
            parameters={"period": period, "deviations": deviations, "ddof": 0}, unit="percent_b",
            comparison_basis="close position between lower and upper band; 0 lower / 0.5 middle / 1 upper reference",
            required_rows=period, actual_rows=len(close), freshness=freshness,
        )
    middle = close.rolling(period, min_periods=period).mean().iloc[-1]
    standard_deviation = close.rolling(period, min_periods=period).std(ddof=0).iloc[-1]
    upper = middle + deviations * standard_deviation
    lower = middle - deviations * standard_deviation
    if pd.isna(middle) or pd.isna(standard_deviation):
        percent_b = None
    elif upper == lower:
        percent_b = 0.5
    else:
        percent_b = (close.iloc[-1] - lower) / (upper - lower)
    bandwidth = _number((upper - lower) / middle if middle else None)
    return _available(
        stock_id=stock_id, market=market, data_date=data_date, indicator_id="bollinger_20_2", name="布林軌道(20,2)",
        parameters={"period": period, "deviations": deviations, "ddof": 0}, unit="percent_b",
        comparison_basis="close position between lower and upper band; 0 lower / 0.5 middle / 1 upper reference", value=percent_b,
        comparison_values={"close": _number(close.iloc[-1]), "upper": _number(upper), "middle": _number(middle), "lower": _number(lower), "bandwidth": bandwidth}, freshness=freshness,
    )


def _indicator_volume(volume: pd.Series, *, window: int, stock_id: str, market: str, data_date: str, freshness: dict[str, Any] | None) -> dict[str, Any]:
    if len(volume) < window + 1:
        return _insufficient(
            stock_id=stock_id, market=market, data_date=data_date, indicator_id=f"volume_vs_avg_{window}", name=f"量能 vs 前{window}根均量",
            parameters={"window": window, "baseline": "previous_closed_bars", "average": "simple_mean"}, unit="ratio_current_to_average",
            comparison_basis=f"latest closed-bar volume versus simple mean of the preceding {window} closed bars",
            required_rows=window + 1, actual_rows=len(volume), freshness=freshness,
        )
    current = _number(volume.iloc[-1], 2)
    average = _number(volume.iloc[-window - 1 : -1].mean(), 2)
    ratio = current / average if current is not None and average not in {None, 0} else None
    delta = current - average if current is not None and average is not None else None
    return _available(
        stock_id=stock_id, market=market, data_date=data_date, indicator_id=f"volume_vs_avg_{window}", name=f"量能 vs 前{window}根均量",
        parameters={"window": window, "baseline": "previous_closed_bars", "average": "simple_mean"}, unit="ratio_current_to_average",
        comparison_basis=f"latest closed-bar volume versus simple mean of the preceding {window} closed bars", value=ratio,
        comparison_values={"current_volume": current, "average_volume": average, "delta_volume": delta, "delta_percent": ((ratio - 1) * 100) if ratio is not None else None, "direction": "above_average" if ratio is not None and ratio > 1 else "below_or_equal_average" if ratio is not None else "unavailable"}, freshness=freshness,
    )


def build_technical_evidence(frame: pd.DataFrame, *, stock_id: str, market: str = "listed", freshness: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Build daily technical evidence cards from completed OHLCV rows."""
    if frame.empty:
        raise ValueError("technical evidence requires at least one OHLCV row")
    data_date = iso_date(frame.iloc[-1]["date"])
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    return [
        _indicator_rsi(close, stock_id=stock_id, market=market, data_date=data_date, freshness=freshness),
        _indicator_macd(close, stock_id=stock_id, market=market, data_date=data_date, freshness=freshness),
        _indicator_bollinger(close, stock_id=stock_id, market=market, data_date=data_date, freshness=freshness),
        *[_indicator_volume(volume, window=window, stock_id=stock_id, market=market, data_date=data_date, freshness=freshness) for window in (3, 5, 10)],
    ]
