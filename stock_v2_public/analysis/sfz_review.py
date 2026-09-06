"""Deterministic Stock from Zero technical review from daily closed bars.

This module is an independent evidence screener.  Its engineering thresholds
are versioned candidates inspired by trend, box, breakout, and retest concepts;
they are not claimed to reproduce a proprietary formula or to authorize an
order.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .core import AnalysisInputError, iso_date

RULE_VERSION = "sfz-technical-review-candidate-v1.0.0"
BOX_WINDOW = 20
BREAKOUT_LOOKBACK = 8
BOX_MAX_WIDTH_PCT = 12.0
TOUCH_TOLERANCE_PCT = 2.0
BREAKOUT_BUFFER_PCT = 1.0
BREAKOUT_VOLUME_RATIO = 1.2
RETEST_UPPER_TOLERANCE_PCT = 2.0
RETEST_LOWER_TOLERANCE_PCT = 1.0
INVALIDATION_BUFFER_PCT = 2.0
MIN_HISTORY = 60
REQUIRED_COLUMNS = ("date", "open", "high", "low", "close", "volume")


def _evidence(evidence_id: str, summary: str, **metrics: Any) -> dict[str, Any]:
    return {"id": evidence_id, "summary": summary, "metrics": metrics}


def _prepare(data: pd.DataFrame | list[dict[str, Any]], cutoff: pd.Timestamp) -> pd.DataFrame:
    frame = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise AnalysisInputError(f"missing OHLCV columns: {', '.join(missing)}")
    frame = frame.loc[:, REQUIRED_COLUMNS].copy()
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    if bool(parsed_dates.isna().any()):
        raise AnalysisInputError("OHLCV rows contain an invalid date")
    frame["date"] = parsed_dates
    frame = frame.loc[frame["date"] <= cutoff].copy()
    if frame.empty:
        raise AnalysisInputError("no complete OHLCV rows are available on or before as_of")
    if bool(frame["date"].duplicated(keep=False).any()):
        raise AnalysisInputError("OHLCV rows contain duplicate dates on or before as_of")
    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    numeric = frame.loc[:, REQUIRED_COLUMNS[1:]]
    if bool(numeric.isna().any().any()) or not bool(np.isfinite(numeric.to_numpy(dtype=float)).all()):
        raise AnalysisInputError("OHLCV rows contain missing or non-finite numeric values")
    frame = frame.sort_values("date").reset_index(drop=True)
    invalid = (
        (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
        |
        (frame["high"] < frame[["open", "close", "low"]].max(axis=1))
        | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))
        | (frame["volume"] < 0)
    )
    if bool(invalid.any()):
        raise AnalysisInputError("OHLCV rows contain invalid high/low/volume values")
    return frame


def _box_at(frame: pd.DataFrame, breakout_index: int) -> dict[str, Any] | None:
    if breakout_index < BOX_WINDOW:
        return None
    prior = frame.iloc[breakout_index - BOX_WINDOW : breakout_index]
    high = float(prior["high"].max())
    low = float(prior["low"].min())
    midpoint = (high + low) / 2.0
    width_pct = (high - low) / midpoint * 100.0 if midpoint > 0 else float("inf")
    high_touches = int((prior["high"] >= high * (1.0 - TOUCH_TOLERANCE_PCT / 100.0)).sum())
    low_touches = int((prior["low"] <= low * (1.0 + TOUCH_TOLERANCE_PCT / 100.0)).sum())
    recent_volume = float(prior["volume"].iloc[-10:].mean())
    earlier_volume = float(prior["volume"].iloc[:10].mean())
    if width_pct > BOX_MAX_WIDTH_PCT or high_touches < 2 or low_touches < 2:
        return None
    return {
        "start_date": iso_date(prior.iloc[0]["date"]),
        "end_date": iso_date(prior.iloc[-1]["date"]),
        "high": high,
        "low": low,
        "width_pct": round(width_pct, 4),
        "high_touches": high_touches,
        "low_touches": low_touches,
        "volume_contracted": recent_volume <= earlier_volume,
        "mean_volume": float(prior["volume"].mean()),
    }


def review_sfz_technical(
    data: pd.DataFrame | list[dict[str, Any]],
    *,
    stock_id: str,
    as_of: str,
) -> dict[str, Any]:
    """Review one symbol using only rows dated on or before ``as_of``."""

    try:
        cutoff = pd.Timestamp(as_of)
    except (TypeError, ValueError) as exc:
        raise ValueError("as_of must be a valid date") from exc
    if pd.isna(cutoff):
        raise ValueError("as_of must be a valid date")
    frame = _prepare(data, cutoff)

    data_date = iso_date(frame.iloc[-1]["date"])
    evidence: list[dict[str, Any]] = []
    conflicts: list[str] = []
    missing: list[str] = []
    stage = "no_setup"
    candidate = False
    next_observation = "等待形成可重現的箱型邊界與突破證據。"

    close = frame["close"]
    sma20 = close.rolling(20, min_periods=20).mean()
    sma60 = close.rolling(60, min_periods=60).mean()
    trend_ready = len(frame) >= MIN_HISTORY
    trend_supportive = bool(
        trend_ready
        and sma20.notna().iloc[-1]
        and sma60.notna().iloc[-1]
        and float(close.iloc[-1]) > float(sma20.iloc[-1]) > float(sma60.iloc[-1])
        and float(sma20.iloc[-1]) > float(sma20.iloc[-6])
    )
    if trend_ready:
        evidence.append(
            _evidence(
                "trend_context",
                "收盤、SMA20 與 SMA60 的順序及 SMA20 近五根方向。",
                close=round(float(close.iloc[-1]), 6),
                sma20=round(float(sma20.iloc[-1]), 6),
                sma60=round(float(sma60.iloc[-1]), 6),
                sma20_change_5bars=round(float(sma20.iloc[-1] - sma20.iloc[-6]), 6),
                supportive=trend_supportive,
            )
        )
        if not trend_supportive:
            conflicts.append("趨勢條件未同時滿足 close > SMA20 > SMA60 且 SMA20 上升。")
    else:
        missing.append(f"趨勢判讀需要至少 {MIN_HISTORY} 根已收盤日 K；目前 {len(frame)} 根。")

    breakout: tuple[int, dict[str, Any]] | None = None
    search_start = max(BOX_WINDOW, len(frame) - BREAKOUT_LOOKBACK)
    for index in range(search_start, len(frame)):
        box = _box_at(frame, index)
        if box is None:
            continue
        row = frame.iloc[index]
        volume_ratio = float(row["volume"]) / box["mean_volume"] if box["mean_volume"] > 0 else 0.0
        if float(row["close"]) > box["high"] * (1.0 + BREAKOUT_BUFFER_PCT / 100.0) and volume_ratio >= BREAKOUT_VOLUME_RATIO:
            box["breakout_date"] = iso_date(row["date"])
            box["breakout_close"] = round(float(row["close"]), 6)
            box["breakout_volume_ratio"] = round(volume_ratio, 4)
            breakout = (index, box)

    if breakout is None:
        probe = _box_at(frame, len(frame))
        if probe is not None:
            stage = "box_forming"
            evidence.append(_evidence("box", "最近二十根形成可量測箱型，尚未出現有效突破。", **probe))
            next_observation = "觀察收盤是否帶量站上先前箱頂；邊界只使用突破前資料。"
        else:
            if len(frame) < BOX_WINDOW:
                missing.append(f"箱型判讀需要至少 {BOX_WINDOW} 根已收盤日 K；目前 {len(frame)} 根。")
            else:
                evidence.append(
                    _evidence(
                        "no_box_setup",
                        "最近觀察窗沒有同時形成窄幅箱型與上下緣至少兩次接觸。",
                        box_window=BOX_WINDOW,
                        breakout_lookback=BREAKOUT_LOOKBACK,
                    )
                )
    else:
        breakout_index, box = breakout
        evidence.append(_evidence("box", "箱型邊界完全取自突破日前二十根日 K。", **{key: value for key, value in box.items() if not key.startswith("breakout_")}))
        evidence.append(
            _evidence(
                "breakout",
                "收盤越過先前箱頂緩衝且成交量高於箱型均量。",
                breakout_date=box["breakout_date"],
                close=box["breakout_close"],
                box_high=round(box["high"], 6),
                volume_ratio=box["breakout_volume_ratio"],
            )
        )
        after = frame.iloc[breakout_index + 1 :]
        invalidated = bool((after["close"] < box["high"] * (1.0 - INVALIDATION_BUFFER_PCT / 100.0)).any())
        retest_rows = after.loc[
            (after["low"] <= box["high"] * (1.0 + RETEST_UPPER_TOLERANCE_PCT / 100.0))
            & (after["close"] >= box["high"] * (1.0 - RETEST_LOWER_TOLERANCE_PCT / 100.0))
        ]
        if invalidated:
            stage = "invalidated"
            conflicts.append("突破後曾收盤跌回箱頂下方超過失效緩衝。")
            next_observation = "等待重新建立箱型或新的突破結構。"
        elif after.empty:
            stage = "breakout_wait_retest"
            next_observation = "等待突破後回測箱頂；目前不把突破當成自動買進動作。"
        elif not retest_rows.empty and float(close.iloc[-1]) >= box["high"] and float(close.iloc[-1]) >= float(close.iloc[-2]):
            stage = "retest_confirmed"
            evidence.append(
                _evidence(
                    "retest",
                    "突破後價格回測箱頂附近並重新收在箱頂之上。",
                    first_retest_date=iso_date(retest_rows.iloc[0]["date"]),
                    latest_close=round(float(close.iloc[-1]), 6),
                    box_high=round(box["high"], 6),
                )
            )
            candidate = trend_supportive
            next_observation = "持續觀察箱頂是否守住及趨勢結構是否維持；本結果不構成下單指令。"
        else:
            stage = "breakout_wait_retest"
            next_observation = "等待回測箱頂後的止穩證據；不追認尚未確認的轉折。"

    if stage == "retest_confirmed" and not trend_supportive:
        next_observation = "回測結構已出現，但趨勢條件衝突；等待趨勢重新一致。"

    return {
        "stock_id": str(stock_id),
        "data_date": data_date,
        "rule_version": RULE_VERSION,
        "candidate": candidate,
        "stage": stage,
        "evidence": evidence,
        "conflicts": conflicts,
        "next_observation": next_observation,
        "missing": missing,
    }


def analyze_sfz(
    frame: pd.DataFrame | list[dict[str, Any]],
    stock_id: str,
    as_of: str,
) -> dict[str, Any]:
    """Stable public entry point for the standalone SFZ review contract."""

    return review_sfz_technical(frame, stock_id=stock_id, as_of=as_of)
