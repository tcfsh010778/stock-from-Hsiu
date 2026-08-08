from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = ("date", "open", "high", "low", "close", "volume")


class AnalysisInputError(ValueError):
    """Raised when OHLCV input cannot be analyzed safely."""


def prepare_ohlcv(data: pd.DataFrame | list[dict[str, Any]]) -> pd.DataFrame:
    frame = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise AnalysisInputError(f"missing OHLCV columns: {', '.join(missing)}")
    frame = frame.loc[:, REQUIRED_COLUMNS].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=REQUIRED_COLUMNS).sort_values("date")
    frame = frame.drop_duplicates("date", keep="last").reset_index(drop=True)
    if len(frame) < 30:
        raise AnalysisInputError("at least 30 complete OHLCV rows are required")
    invalid = (
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
        | (frame["volume"] < 0)
    )
    if bool(invalid.any()):
        raise AnalysisInputError("OHLCV rows contain invalid high/low/volume values")
    return frame


def with_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    previous_close = out["close"].shift(1)
    true_range = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - previous_close).abs(),
            (out["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr14"] = true_range.rolling(14, min_periods=3).mean().bfill()
    out["volume_sma20"] = out["volume"].rolling(20, min_periods=3).mean()
    out["relative_volume"] = (out["volume"] / out["volume_sma20"]).replace([np.inf, -np.inf], np.nan)
    for window in (5, 20, 60, 120, 240):
        out[f"sma{window}"] = out["close"].rolling(window, min_periods=max(3, window // 3)).mean()
    return out


def finite_float(value: Any, digits: int = 6) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, digits)


def iso_date(value: Any) -> str:
    return pd.Timestamp(value).date().isoformat()


def clamp_score(value: float) -> float:
    return round(max(0.0, min(100.0, value)), 2)
