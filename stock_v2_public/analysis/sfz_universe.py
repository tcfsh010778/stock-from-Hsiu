"""Source-grounded Stock From Zero universe screening.

This module stops at universe membership.  It intentionally does not turn a
screen result into an entry or an order instruction.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd


RULE_VERSION = "sfz-universe-textbook-example-v1.0.0"
SOURCE_PAGES = {
    "computer_screen": [8, 12, 18],
    "top50_route": [30],
    "manual_structure_and_entry": [37, 38, 39, 43, 44, 45, 46],
}
REQUIRED_COLUMNS = ("date", "open", "high", "low", "close", "volume")


class SFZUniverseInputError(ValueError):
    """Raised when input bars cannot support an auditable calculation."""


@dataclass(frozen=True)
class SFZUniverseConfig:
    """Versioned example settings transcribed from textbook page 8.

    The textbook says volume and momentum settings should be adjusted for the
    market.  Callers should therefore persist this configuration with output.
    """

    min_five_day_volume_lots: float = 10_000.0
    min_eight_week_average_gain_pct: float = 5.0
    min_close_price: float = 20.0
    require_three_week_closing_high: bool = False


def _check(check_id: str, status: str, summary: str, **metrics: Any) -> dict[str, Any]:
    return {"id": check_id, "status": status, "summary": summary, "metrics": metrics}


def _prepare(data: pd.DataFrame | list[dict[str, Any]], cutoff: pd.Timestamp) -> pd.DataFrame:
    frame = data.copy() if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise SFZUniverseInputError(f"missing OHLCV columns: {', '.join(missing)}")

    parsed = pd.to_datetime(frame["date"], errors="coerce")
    if bool(parsed.isna().any()):
        raise SFZUniverseInputError("OHLCV rows contain an invalid date")
    if parsed.dt.tz is not None:
        parsed = parsed.dt.tz_localize(None)
    frame = frame.loc[parsed.dt.normalize() <= cutoff, REQUIRED_COLUMNS].copy()
    frame["date"] = parsed.loc[frame.index].dt.normalize()
    frame = frame.sort_values("date")
    if bool(frame["date"].duplicated().any()):
        raise SFZUniverseInputError("OHLCV rows contain duplicate dates at or before as_of")

    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    numeric = frame.loc[:, REQUIRED_COLUMNS[1:]].to_numpy(dtype=float)
    if not bool(np.isfinite(numeric).all()):
        raise SFZUniverseInputError("OHLCV rows contain non-finite values at or before as_of")
    if bool((frame[["open", "high", "low", "close"]] <= 0).any().any()) or bool((frame["volume"] < 0).any()):
        raise SFZUniverseInputError("OHLCV rows contain non-positive prices or negative volume")
    if bool((frame["high"] < frame[["open", "close", "low"]].max(axis=1)).any()) or bool(
        (frame["low"] > frame[["open", "close", "high"]].min(axis=1)).any()
    ):
        raise SFZUniverseInputError("OHLCV rows violate high/low bounds")
    return frame.reset_index(drop=True)


def _completed_weeks(frame: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=("open", "high", "low", "close", "volume"), index=pd.DatetimeIndex([], name="date"))
    indexed = frame.set_index("date")
    weekly = indexed.resample("W-FRI", label="right", closed="right").agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum")
    )
    # A partial current week must never masquerade as a completed weekly bar.
    weekly = weekly.loc[(weekly.index <= cutoff) & weekly["close"].notna()].copy()
    return weekly


def analyze_sfz_universe(
    daily: pd.DataFrame | list[dict[str, Any]],
    stock_id: str,
    as_of: str,
    *,
    config: SFZUniverseConfig | None = None,
    pool_provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate the page-8 SFZ example screen using closed bars through ``as_of``.

    A market-wide top-50 route is accepted only as caller-supplied provenance;
    this single-symbol function never fabricates a cross-sectional rank.
    """

    cutoff = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(cutoff):
        raise SFZUniverseInputError("as_of is not a valid date")
    cutoff = pd.Timestamp(cutoff).tz_localize(None) if pd.Timestamp(cutoff).tzinfo else pd.Timestamp(cutoff)
    cutoff = cutoff.normalize()
    cfg = config or SFZUniverseConfig()
    config_values = np.asarray(
        [cfg.min_five_day_volume_lots, cfg.min_eight_week_average_gain_pct, cfg.min_close_price], dtype=float
    )
    if not bool(np.isfinite(config_values).all()):
        raise SFZUniverseInputError("configuration thresholds must be finite")
    if cfg.min_five_day_volume_lots < 0 or cfg.min_close_price <= 0:
        raise SFZUniverseInputError("configuration thresholds must be non-negative and price must be positive")

    frame = _prepare(daily, cutoff)
    weeks = _completed_weeks(frame, cutoff)
    checks: list[dict[str, Any]] = []

    if len(weeks) < 89:
        checks.append(_check("weekly_ma_5_21_89", "unknown", "需要完整 89 週，不能以 MA34 取代 MA89。", required_weeks=89, actual_weeks=len(weeks)))
    else:
        close_w = weeks["close"]
        ma5, ma21, ma89 = (float(close_w.rolling(n, min_periods=n).mean().iloc[-1]) for n in (5, 21, 89))
        passed = ma5 > ma21 > ma89
        checks.append(_check("weekly_ma_5_21_89", "pass" if passed else "fail", "週線 MA5、MA21、MA89 教材初篩。", ma5=ma5, ma21=ma21, ma89=ma89))

    if len(frame) < 5:
        checks.append(_check("five_day_volume", "unknown", "需要五根已收盤日 K。", required_bars=5, actual_bars=len(frame)))
    else:
        volume_shares = float(frame["volume"].tail(5).sum())
        volume_lots = volume_shares / 1000.0
        checks.append(_check("five_day_volume", "pass" if volume_lots > cfg.min_five_day_volume_lots else "fail", "輸入為原始股數；最近五日合計除以 1,000 換算張數。門檻是可調教材示例。", value_shares=volume_shares, value_lots=volume_lots, threshold_lots=cfg.min_five_day_volume_lots))

    if len(weeks) < 9:
        checks.append(_check("eight_week_average_gain", "unknown", "八個週報酬需要九個完整週收盤端點。", required_weeks=9, actual_weeks=len(weeks)))
    else:
        gains = weeks["close"].pct_change().tail(8) * 100.0
        avg_gain = float(gains.mean())
        checks.append(_check("eight_week_average_gain", "pass" if avg_gain > cfg.min_eight_week_average_gain_pct else "fail", "最近八個完整週報酬的算術平均是舊程式沿用的工程詮釋；門檻是可調教材示例。", average_gain_pct=avg_gain, threshold_pct=cfg.min_eight_week_average_gain_pct, formula_provenance="legacy_screener_engineering_interpretation"))

    if frame.empty:
        for check_id, summary in (("minimum_price", "需要已收盤日 K。"), ("daily_ma34_position", "需要 34 根已收盤日 K。"), ("daily_ma34_rising", "需要 35 根已收盤日 K。")):
            checks.append(_check(check_id, "unknown", summary, actual_bars=0))
    else:
        last_close = float(frame["close"].iloc[-1])
        checks.append(_check("minimum_price", "pass" if last_close > cfg.min_close_price else "fail", "收盤價教材示例門檻。", close=last_close, threshold=cfg.min_close_price))
        ma34 = frame["close"].rolling(34, min_periods=34).mean()
        if len(frame) < 34:
            checks.append(_check("daily_ma34_position", "unknown", "需要 34 根已收盤日 K。", required_bars=34, actual_bars=len(frame)))
        else:
            latest_ma34 = float(ma34.iloc[-1])
            checks.append(_check("daily_ma34_position", "pass" if last_close > latest_ma34 else "fail", "收盤位於日 MA34 上方。", close=last_close, ma34=latest_ma34))
        if len(frame) < 35:
            checks.append(_check("daily_ma34_rising", "unknown", "MA34 方向需要至少 35 根已收盤日 K。", required_bars=35, actual_bars=len(frame)))
        else:
            delta = float(ma34.iloc[-1] - ma34.iloc[-2])
            checks.append(_check("daily_ma34_rising", "pass" if delta > 0 else "fail", "日 MA34 較前一日上升。", ma34_change_one_bar=delta))

    if cfg.require_three_week_closing_high:
        if len(weeks) < 3:
            checks.append(_check("optional_three_week_closing_high", "unknown", "需要三個完整週收盤。", required_weeks=3, actual_weeks=len(weeks)))
        else:
            current, prior_max = float(weeks["close"].iloc[-1]), float(weeks["close"].iloc[-3:-1].max())
            checks.append(_check("optional_three_week_closing_high", "pass" if current > prior_max else "fail", "選配近三週收盤新高；教材指出仍需人工核對週 K 高點。", current_close=current, prior_two_closes_max=prior_max))

    statuses = [item["status"] for item in checks]
    candidate = bool(statuses) and all(status == "pass" for status in statuses)
    stage = "insufficient_data" if "unknown" in statuses else ("universe_candidate" if candidate else "filtered")
    provenance = dict(pool_provenance or {})
    top50 = provenance.get("route") == "market_top50_momentum"

    return {
        "stock_id": str(stock_id),
        "as_of": cutoff.date().isoformat(),
        "data_date": None if frame.empty else frame["date"].iloc[-1].date().isoformat(),
        "rule_version": RULE_VERSION,
        "source_pages": SOURCE_PAGES,
        "config": asdict(cfg),
        "pool_provenance": provenance,
        "checks": checks,
        "candidate": candidate,
        "stage": stage,
        "weekly_context": {"completed_weeks": len(weeks), "last_completed_week": None if weeks.empty else weeks.index[-1].date().isoformat()},
        "entry_observations": {
            "enabled": False,
            "status": "manual",
            "reason": "股票池資格不等於進場；四類盤整、行進/盤整籃、一過二與 MA34 反應需另行判讀。",
            "manual_patterns": ["水平盤整", "前高上盤整", "前低上盤整", "前低下盤整"],
        },
        "notes": (["全市場前 50 名排名由 caller 提供，本函式未自行推算。"] if top50 else []),
    }
