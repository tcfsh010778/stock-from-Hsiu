"""Shared, side-effect-free stock classification rules.

The collectors, screener, and site generator all consume this module so policy
thresholds have one owner.  Keep rendering and file I/O in the callers.
"""

from __future__ import annotations

import math
import re
from typing import Any, Mapping


OVERHEAT_GAIN_6W_PCT = 100.0
OVERHEAT_RSI_14 = 85.0
OVERHEAT_BBAND_PCT = 110.0
OVERHEAT_GAIN_3D_PCT = 20.0
OVERHEAT_CONTEXT_RSI_14 = 80.0

SITE_MARCHING_GAIN_6W_PCT = 18.0
SITE_MARCHING_SCORE = 85.0

TRAFFIC_RR_MINIMUM = 1.5
TRAFFIC_RR_GO = 2.0
TRAFFIC_WR_BUY_MIN = -85.0
TRAFFIC_WR_BUY_MAX = -65.0
TRAFFIC_K_HARD_BREAK = 20.0


def to_number(value: Any, default: float | None = None) -> float | None:
    """Parse the numeric forms used by all three legacy callers."""

    if value in (None, ""):
        return default
    try:
        parsed = float(str(value).replace(",", "").replace("%", "").replace("+", "").strip())
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _metric(stock: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = stock.get(key)
        if value not in (None, ""):
            return to_number(value)
    return None


def overheat_assessment(stock: Mapping[str, Any]) -> dict[str, Any]:
    """Return the canonical overheat decision and its legacy-compatible reasons."""

    gain_6w = _metric(stock, "gain_6w")
    rsi_14 = _metric(stock, "rsi", "rsi_14")
    bband_pct = _metric(stock, "bb_pct", "bband_pct", "percent_b")
    gain_3d = _metric(stock, "gain_3d")
    overheated = bool(
        (gain_6w is not None and gain_6w >= OVERHEAT_GAIN_6W_PCT)
        or (rsi_14 is not None and rsi_14 >= OVERHEAT_RSI_14)
        or (bband_pct is not None and bband_pct >= OVERHEAT_BBAND_PCT)
        or (gain_3d is not None and gain_3d >= OVERHEAT_GAIN_3D_PCT)
    )
    reasons: list[str] = []
    if gain_6w is not None and gain_6w >= OVERHEAT_GAIN_6W_PCT:
        reasons.append(f"近6週 {gain_6w:+.2f}% (門檻 {OVERHEAT_GAIN_6W_PCT:.0f}%)")
    # Preserve the historical explanatory RSI context for extreme six-week gains.
    if rsi_14 is not None and (
        rsi_14 >= OVERHEAT_RSI_14
        or (
            gain_6w is not None
            and gain_6w >= OVERHEAT_GAIN_6W_PCT
            and rsi_14 >= OVERHEAT_CONTEXT_RSI_14
        )
    ):
        reasons.append(f"RSI {rsi_14:.1f} (門檻 {OVERHEAT_RSI_14:.0f})")
    if bband_pct is not None and bband_pct >= OVERHEAT_BBAND_PCT:
        reasons.append(f"%B {bband_pct:.1f}% (門檻 {OVERHEAT_BBAND_PCT:.0f}%)")
    if gain_3d is not None and gain_3d >= OVERHEAT_GAIN_3D_PCT:
        reasons.append(f"近3日 {gain_3d:+.2f}% (門檻 {OVERHEAT_GAIN_3D_PCT:.0f}%)")
    return {
        "overheated": overheated,
        "reasons": reasons,
        "metrics": {
            "gain_6w": gain_6w,
            "rsi_14": rsi_14,
            "bband_pct": bband_pct,
            "gain_3d": gain_3d,
        },
    }


def is_overheated(stock: Mapping[str, Any]) -> bool:
    return bool(overheat_assessment(stock)["overheated"])


def overheat_reasons(stock: Mapping[str, Any]) -> list[str]:
    return list(overheat_assessment(stock)["reasons"])


def overheat_reason_text(stock: Mapping[str, Any]) -> str:
    reasons = overheat_reasons(stock)
    return "強制過熱排除：" + " + ".join(reasons) if reasons else ""


def apply_overheat_guard(stock: Mapping[str, Any], base_basket: str) -> tuple[str, str]:
    """Return (effective basket, original basket) without mutating the stock."""

    return ("過熱/風險", base_basket) if is_overheated(stock) else (base_basket, "")


def holding_group(level: str) -> str:
    """Normalize TDCC holding-level labels into the shared four-way grouping."""

    text = str(level or "").strip()
    if text == "total" or "差異" in text:
        return "other"
    numbers = [int(item.replace(",", "")) for item in re.findall(r"\d[\d,]*", text)]
    lower = numbers[0] if numbers else None
    upper = numbers[-1] if numbers else None
    if "more than" in text or (numbers and max(numbers) >= 400_001):
        return "major"
    if lower is not None and upper is not None and lower >= 200_001 and upper <= 400_000:
        return "middle"
    if numbers and max(numbers) <= 10_000:
        return "retail"
    return "other"


def mda_stock_status(stock: Mapping[str, Any]) -> tuple[str, str]:
    """Canonical screener icon/status mapping."""

    if is_overheated(stock):
        return "🔴", "過熱/風險"
    basket = str(stock.get("basket") or "")
    if basket == "已發動籃":
        return "🟡", "強勢追蹤"
    if basket in {"空轉多觀察籃", "未發動觀察籃"}:
        return "🟢", "健康整理"
    return "🔴", "過熱/風險"


def site_basket_assessment(stock: Mapping[str, Any]) -> dict[str, Any]:
    """Return the site basket plus presentation factors from one policy owner."""

    gain_6w = _metric(stock, "gain_6w") or 0.0
    score = _metric(stock, "score") or 0.0
    score_marching = score >= SITE_MARCHING_SCORE
    gain_marching = gain_6w >= SITE_MARCHING_GAIN_6W_PCT
    if is_overheated(stock):
        basket = "risk"
    else:
        icon = str(stock.get("icon") or "")
        status = str(stock.get("status") or "")
        if icon == "🔴" or "超買" in status:
            basket = "risk"
        elif icon == "🟡" or gain_marching or score_marching:
            basket = "marching"
        else:
            basket = "consolidation"
    return {
        "basket": basket,
        "score_marching": score_marching,
        "gain_marching": gain_marching,
        "score_label": f"評分>={SITE_MARCHING_SCORE:.0f}",
        "gain_label": f"近6週漲幅>={SITE_MARCHING_GAIN_6W_PCT:.0f}%",
    }


def site_basket_key(stock: Mapping[str, Any]) -> str:
    """Canonical site-only three-basket presentation mapping."""

    return str(site_basket_assessment(stock)["basket"])


def _display_number(value: Any, digits: int = 1) -> str:
    parsed = to_number(value)
    if parsed is None:
        return "-"
    return f"{parsed:.{digits}f}".rstrip("0").rstrip(".")


def evaluate_traffic_light(
    stock: Mapping[str, Any],
    tech: Mapping[str, Any],
    decision: Mapping[str, Any],
    indicator: Mapping[str, Any],
    chip_total_5d: float | None,
) -> dict[str, Any]:
    """Evaluate actionable state without reading files or rendering HTML."""

    volume_price = str(tech.get("volume_price") or "")
    rr = to_number(decision.get("rr"))
    rr_text = str(decision.get("rr_text") or "─")
    wr = to_number(indicator.get("wr"))
    k_value = to_number(indicator.get("k"))
    macd_state = str(indicator.get("macd_state") or "")
    kd_state = str(indicator.get("kd_state") or "")
    trend = str(tech.get("trend") or "")
    close = to_number(tech.get("close"))
    ma20 = to_number(tech.get("ma20"))
    ma60 = to_number(tech.get("ma60"))
    basket_key = site_basket_key(stock)
    basket = {"marching": "行進籃", "consolidation": "盤整籃", "risk": "過熱/風險"}.get(
        basket_key, "未分類"
    )

    trend_bull = bool(
        "多" in trend
        or "轉強" in trend
        or (ma20 is not None and ma60 is not None and close is not None and close > ma20 > ma60)
    )
    trend_bear = bool(
        "空" in trend
        or "轉弱" in trend
        or (ma20 is not None and close is not None and close < ma20)
    )
    volume_ok = volume_price in {"量增價漲", "量縮價漲", "均量上彎"}
    wr_buy = wr is not None and TRAFFIC_WR_BUY_MIN <= wr <= TRAFFIC_WR_BUY_MAX
    chip_ok = chip_total_5d is None or chip_total_5d >= 0
    rr_low = rr is not None and rr < TRAFFIC_RR_MINIMUM
    forced_overheat = is_overheated(stock)
    basket_risk = basket == "過熱/風險"
    hard_momentum_break = bool(
        "賣出" in macd_state
        and k_value is not None
        and k_value < TRAFFIC_K_HARD_BREAK
        and not trend_bull
    )
    no_go = bool(trend_bear or forced_overheat or basket_risk or rr_low or hard_momentum_break)
    go = bool(
        not no_go
        and trend_bull
        and volume_ok
        and rr is not None
        and rr >= TRAFFIC_RR_GO
        and wr_buy
        and chip_ok
    )

    if no_go:
        state, css_class, light_class, icon, headline, label = (
            "NO-GO",
            "nogo",
            "light-red",
            "&#128308;",
            "NO-GO · 暫不建倉",
            "紅燈",
        )
    elif go:
        state, css_class, light_class, icon, headline, label = (
            "GO",
            "go",
            "light-green",
            "&#128994;",
            "GO · 可建倉",
            "綠燈",
        )
    else:
        state, css_class, light_class, icon, headline, label = (
            "WATCH",
            "watch",
            "light-yellow",
            "&#128993;",
            "WATCH · 等確認",
            "黃燈",
        )

    if forced_overheat:
        hot = overheat_reasons(stock)
        reason = f"強制過熱排除：{' + '.join(hot[:3]) or basket} → 等待回測 MA20 後重新評估"
    elif rr_low:
        reason = f"R:R {rr_text} 邊際不足 → 等改善至 1:2 以上再評估"
    elif basket_risk:
        reason = f"風險籃（{basket}）→ 暫不追價，等待回測或訊號轉強"
    elif trend_bear:
        reason = f"趨勢偏空（{trend or '跌破均線'}）→ 暫不建倉，先等站回 MA20"
    elif hard_momentum_break:
        reason = f"MACD 賣出區 + KD {_display_number(k_value)} 偏弱 → 先等動能止跌"
    elif go:
        reason = f"趨勢多方 + {volume_price} + R:R {rr_text} + Williams 在買進區"
    elif "賣出" in macd_state or "弱" in kd_state:
        parts: list[str] = []
        if trend_bull:
            parts.append("趨勢多方")
        if "弱" in kd_state:
            parts.append("KD 偏弱")
        if "賣出" in macd_state:
            parts.append("MACD 仍在賣出區")
        reason = " + ".join(parts[:3]) + " → 建議小部位試單或等 MACD 翻紅"
    else:
        factors: list[str] = []
        if trend_bull:
            factors.append("趨勢偏多")
        if volume_ok:
            factors.append(volume_price)
        if rr is not None:
            factors.append(f"R:R {rr_text}")
        if wr is not None:
            factors.append(f"Williams {_display_number(wr)}")
        reason = " + ".join(factors[:3]) + " → 條件未完全同向，先觀察或等回測"

    blockers = [
        key
        for key, active in (
            ("trend_bear", trend_bear),
            ("forced_overheat", forced_overheat),
            ("basket_risk", basket_risk),
            ("rr_low", rr_low),
            ("hard_momentum_break", hard_momentum_break),
        )
        if active
    ]
    candidate = not no_go
    armed = bool(candidate and trend_bull and volume_ok and rr is not None and rr >= TRAFFIC_RR_GO and chip_ok)
    return {
        "state": state,
        "candidate": candidate,
        "armed": armed,
        "entry": go,
        "exit": bool(trend_bear or forced_overheat or basket_risk or hard_momentum_break),
        "css_class": css_class,
        "light_class": light_class,
        "icon_entity": icon,
        "headline": headline,
        "label": label,
        "reason": reason,
        "basket": basket_key,
        "checks": {
            "trend_bull": trend_bull,
            "trend_bear": trend_bear,
            "volume_ok": volume_ok,
            "rr_go": rr is not None and rr >= TRAFFIC_RR_GO,
            "wr_buy": wr_buy,
            "chip_ok": chip_ok,
        },
        "blockers": blockers,
    }
