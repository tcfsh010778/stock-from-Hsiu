"""Pure product contracts. No strategy scores or prescribed trading levels."""
from __future__ import annotations

import calendar
import hashlib
import json
import math
from datetime import date

import pandas as pd

VERSION = "sfz-mda-workspace-1"
PATTERNS = {
    "range": "水平盤整", "higher_lows": "低點墊高", "double_bottom": "雙底候選",
    "double_top": "雙頂候選", "ascending_triangle": "上升三角候選",
    "descending_triangle": "下降三角候選", "bullish_engulfing": "多方吞噬",
    "bearish_engulfing": "空方吞噬", "hammer": "錘子線", "doji": "十字線",
}
PATTERNS.update( {'rising_channel':'上升通道候選','falling_channel':'下降通道候選',
    'symmetrical_triangle':'收斂三角候選','bull_pennant':'多方三角旗候選','bear_pennant':'空方三角旗候選',
    'head_shoulders':'頭肩頂候選','inverse_head_shoulders':'頭肩底候選',
    'morning_star':'晨星候選','evening_star':'暮星候選','shooting_star':'流星線',
    'bullish_harami':'多方母子線','bearish_harami':'空方母子線','three_white':'三紅兵','three_black':'三黑兵'})


def number(value):
    try:
        if value is None or isinstance(value, bool):
            return None
        value = float(str(value).replace(",", "").strip())
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def growth(current, previous):
    a, b = number(current), number(previous)
    return None if a is None or b is None or b <= 0 else (a / b - 1) * 100


def prepare_bars(rows, as_of):
    cutoff = date.fromisoformat(as_of)
    frame = rows.copy().reset_index(drop=True) if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    required = ["date", "open", "high", "low", "close", "volume"]
    if not set(required) <= set(frame):
        raise ValueError("OHLCV fields missing")
    parsed = pd.to_datetime(frame.date, errors="raise").dt.normalize()
    frame = frame.loc[parsed.dt.date <= cutoff, required].copy()
    frame["date"] = parsed.loc[frame.index]
    if frame.empty or frame.date.duplicated().any():
        raise ValueError("empty or duplicate price dates")
    frame = frame.sort_values("date")
    for key in required[1:]:
        frame[key] = frame[key].map(number)
    if frame[required[1:]].isna().any().any():
        raise ValueError("nonfinite OHLCV")
    if (frame[["open", "high", "low", "close"]] <= 0).any().any() or (frame.volume < 0).any():
        raise ValueError("invalid price or volume")
    if (frame.high < frame[["open", "close", "low"]].max(axis=1)).any() or (frame.low > frame[["open", "close"]].min(axis=1)).any():
        raise ValueError("invalid OHLC bounds")
    return frame.reset_index(drop=True)


def candles(rows, as_of, frequency="day", limit=500):
    """Aggregate full history before MAs; preserve actual date and partial bars."""
    frame = prepare_bars(rows, as_of)
    frame["source_date"] = frame.date
    if frequency != "day":
        rule = {"week": "W-FRI", "month": "ME"}[frequency]
        frame = frame.set_index("date").resample(rule).agg(
            open=("open", "first"), high=("high", "max"), low=("low", "min"),
            close=("close", "last"), volume=("volume", "sum"), source_date=("source_date", "last")
        ).dropna(subset=["close"]).reset_index()
    for n in (5, 20, 34, 60, 120, 240):
        frame[f"ma{n}"] = frame.close.rolling(n, min_periods=n).mean()
    output = []
    for row in frame.tail(limit).to_dict("records"):
        period_end = row["date"].date()
        actual = row["source_date"].date()
        output.append({"time": actual.isoformat(), "period_end": period_end.isoformat(),
                       "complete": period_end <= date.fromisoformat(as_of),
                       **{k: number(row[k]) for k in ("open", "high", "low", "close", "volume", "ma5", "ma20", "ma34", "ma60", "ma120", "ma240")}})
    return output


def chip_windows(history, sessions, stock_id):
    """Require each exchange session; absence is unknown, never zero-filled."""
    sessions = sorted(set(sessions))
    by_date = {}
    for item in history:
        if item.get("date") not in sessions:
            continue
        rows = [r for r in item.get("rows", []) if str(r.get("security_id")) == stock_id]
        if len(rows) > 1:
            raise ValueError("duplicate institutional row")
        if rows:
            by_date[item["date"]] = rows[0]
    fields = ("foreign_net", "investment_trust_net", "dealer_net", "institutional_total_net")
    windows = {}
    for n in (1, 5, 10):
        dates = sessions[-n:]
        values = {}
        gaps = [d for d in dates if d not in by_date]
        for field in fields:
            series = [number(by_date.get(d, {}).get(field)) for d in dates]
            values[field] = sum(series) if len(series) == n and all(v is not None for v in series) else None
        windows[str(n)] = {"start": dates[0] if dates else None, "end": dates[-1] if dates else None,
                           "required_sessions": n, "observed_sessions": len(dates) - len(gaps),
                           "missing_dates": gaps, "unit": "shares", "values": values,
                           "complete": len(dates) == n and not gaps and all(v is not None for v in values.values())}
    series = [{"date": d, **{f: number(by_date.get(d, {}).get(f)) for f in fields}} for d in sessions]
    return {"windows": windows, "series": series, "unit": "shares"}


def revenue_row(row, market, period, source_url, observed_at):
    sid = str(row["stock_id"])
    if not (len(sid) == 4 and sid.isdigit()) or date.fromisoformat(period + "-01").strftime("%Y-%m") != period:
        raise ValueError("invalid revenue identity or period")
    current, prior, year = (number(row.get(k)) for k in ("revenue", "previous_month", "previous_year"))
    return {"stock_id": sid, "market": market, "period": period,
            "revenue": current, "previous_month": prior, "previous_year": year,
            "mom_pct": growth(current, prior), "yoy_pct": growth(current, year),
            "unit": "TWD_thousands", "source_url": source_url, "observed_at": observed_at}


def detect_patterns(rows, as_of):
    """Versioned geometric candidates, independent of SFZ/MDA qualification."""
    f = prepare_bars(rows, as_of)
    observations = []
    def add(pid, start, end, evidence):
        observations.append({"id": pid, "name": PATTERNS[pid], "start": f.date.iloc[start].date().isoformat(),
                             "end": f.date.iloc[end].date().isoformat(), "evidence": evidence,
                             "basis": "engineering_geometry_v1", "role": "pattern_filter_only"})
    i = len(f) - 1
    if len(f) >= 2:
        r, p = f.iloc[-1], f.iloc[-2]
        span = r.high-r.low
        body = abs(r.close-r.open)
        if span > 0 and body <= span*.1:
            add("doji", i, i, "實體不超過當根高低範圍的 10%")
        if span > 0 and min(r.open,r.close)-r.low >= max(body*2,span*.5) and r.high-max(r.open,r.close) <= span*.2:
            add("hammer", i, i, "下影線至少為實體兩倍，上影線較短")
        if p.close < p.open and r.close > r.open and r.open <= p.close and r.close >= p.open:
            add("bullish_engulfing", i-1, i, "今日紅 K 實體包覆前一根黑 K")
        if p.close > p.open and r.close < r.open and r.open >= p.close and r.close <= p.open:
            add("bearish_engulfing", i-1, i, "今日黑 K 實體包覆前一根紅 K")
    if len(f) >= 40:
        w = f.tail(40)
        if w.high.max()/w.low.min()-1 <= .12:
            add("range", i-39, i, "近 40 根高低價範圍不超過 12%")
        # Pivots only exist after three right-hand confirmation bars.
        lows = [j for j in range(max(3,i-60),i-2) if f.low.iloc[j] < f.low.iloc[j-3:j].min() and f.low.iloc[j] <= f.low.iloc[j+1:j+4].min()]
        highs = [j for j in range(max(3,i-60),i-2) if f.high.iloc[j] > f.high.iloc[j-3:j].max() and f.high.iloc[j] >= f.high.iloc[j+1:j+4].max()]
        rising = len(lows)>=2 and f.low.iloc[lows[-1]] > f.low.iloc[lows[-2]]*1.01
        falling = len(highs)>=2 and f.high.iloc[highs[-1]] < f.high.iloc[highs[-2]]*.99
        flat_lows = len(lows)>=2 and lows[-1]-lows[-2]>=5 and abs(f.low.iloc[lows[-1]]/f.low.iloc[lows[-2]]-1)<=.02
        flat_highs = len(highs)>=2 and highs[-1]-highs[-2]>=5 and abs(f.high.iloc[highs[-1]]/f.high.iloc[highs[-2]]-1)<=.02
        if rising:
            add("higher_lows", lows[-2], i, "兩個已確認低點上移超過 1%")
        if flat_lows and f.high.iloc[lows[-2]:lows[-1]+1].max()/max(f.low.iloc[lows[-2]],f.low.iloc[lows[-1]]) >= 1.05:
            add("double_bottom", lows[-2], i, "兩低點差距不超過 2%，中間反彈至少 5%；突破尚待核對")
        if flat_highs and min(f.high.iloc[highs[-2]],f.high.iloc[highs[-1]])/f.low.iloc[highs[-2]:highs[-1]+1].min() >= 1.05:
            add("double_top", highs[-2], i, "兩高點差距不超過 2%，中間回落至少 5%；跌破尚待核對")
        if rising and flat_highs:
            add("ascending_triangle", min(lows[-2],highs[-2]),i,"低點上移，高點近似水平")
        if falling and flat_lows:
            add("descending_triangle", min(lows[-2],highs[-2]),i,"高點下移，低點近似水平")
    return {"version": "pattern-geometry-v1", "data_date": as_of, "observations": observations,
            "note": "幾何條件可用於篩選；不改變 SFZ／M 大原始條件。"}
