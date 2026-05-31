from __future__ import annotations

import csv
import json
import math
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "market_sentiment.json"
SFZ_ALL_PATH = DATA_DIR / "sfz_all.json"

TWSE_TAIEX_URL = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
TWSE_MARGIN_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TWSE_FOREIGN_URL = "https://www.twse.com.tw/rwd/zh/fund/TWT38U"
CBOE_VIX_CSV_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"

USER_AGENT = "stock-from-Hsiu market-sentiment/1.0"
TAIPEI_TZ = timezone(timedelta(hours=8))

INDICATOR_WEIGHTS = {
    "taiex_ma": 30,
    "margin_weekly": 15,
    "short_weekly": 15,
    "foreign_5d": 15,
    "breadth": 10,
    "us_vix": 15,
}


def parse_number(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        text = str(value).replace(",", "").replace("%", "").replace("+", "").strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def roc_date_to_iso(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "/" in text:
        parts = text.split("/")
        if len(parts[0]) == 3:
            return f"{int(parts[0]) + 1911:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        if len(parts[0]) == 4:
            return f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 7:
        return f"{int(digits[:3]) + 1911:04d}-{int(digits[3:5]):02d}-{int(digits[5:7]):02d}"
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return text


def _status(label: str, ok: bool, message: str) -> str:
    prefix = "OK" if ok else "WARN"
    return f"{prefix}: {label} - {message}"


def indicator(
    key: str,
    label: str,
    score: float,
    signal: str,
    display: str,
    detail: str = "",
    *,
    available: bool = True,
    source: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "label": label,
        "score": round(float(score), 2),
        "weight": INDICATOR_WEIGHTS[key],
        "signal": signal,
        "display": display,
        "detail": detail,
        "available": available,
        "source": source,
    }
    if extra:
        data.update(extra)
    return data


def neutral_indicator(key: str, label: str, detail: str, source: str = "") -> dict[str, Any]:
    weight = INDICATOR_WEIGHTS[key]
    return indicator(
        key,
        label,
        weight / 2,
        "neutral",
        "資料不足",
        detail,
        available=False,
        source=source,
    )


def _http_json(url: str, params: dict[str, Any], timeout: int = 20) -> dict[str, Any]:
    response = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("stat") not in (None, "OK"):
        raise RuntimeError(str(payload.get("stat")))
    return payload


def _http_text(url: str, timeout: int = 30) -> str:
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    response.raise_for_status()
    return response.text


def _month_starts(today: date, months: int) -> list[date]:
    out: list[date] = []
    year = today.year
    month = today.month
    for _ in range(months):
        out.append(date(year, month, 1))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return out


def fetch_taiex_history(today: date | None = None, months: int = 5) -> list[dict[str, Any]]:
    today = today or datetime.now(TAIPEI_TZ).date()
    rows: dict[str, dict[str, Any]] = {}
    for month_start in _month_starts(today, months):
        try:
            payload = _http_json(
                TWSE_TAIEX_URL,
                {"date": month_start.strftime("%Y%m%d"), "response": "json"},
                timeout=20,
            )
        except Exception:
            continue
        for row in payload.get("data") or []:
            if len(row) < 5:
                continue
            iso = roc_date_to_iso(row[0])
            close = parse_number(row[4])
            if iso and close:
                rows[iso] = {
                    "date": iso,
                    "open": parse_number(row[1]),
                    "high": parse_number(row[2]),
                    "low": parse_number(row[3]),
                    "close": close,
                }
    return [rows[key] for key in sorted(rows)]


def moving_average(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def compute_taiex_ma_indicator(rows: list[dict[str, Any]]) -> dict[str, Any]:
    clean_rows = sorted(
        [r for r in rows if r.get("date") and parse_number(r.get("close")) is not None],
        key=lambda r: str(r["date"]),
    )
    if len(clean_rows) < 5:
        return neutral_indicator("taiex_ma", "TAIEX MA", "TAIEX historical closes unavailable", TWSE_TAIEX_URL)
    closes = [float(parse_number(r["close"], 0) or 0) for r in clean_rows]
    latest_close = closes[-1]
    ma_values = {f"ma{window}": moving_average(closes, window) for window in (5, 20, 60)}
    lights = {key: bool(value is not None and latest_close > value) for key, value in ma_values.items()}
    missing = sum(1 for value in ma_values.values() if value is None)
    score = sum(10 for ok in lights.values() if ok) + missing * 5
    green_count = sum(1 for ok in lights.values() if ok)
    signal = "bullish" if green_count >= 2 and lights.get("ma60") else ("bearish" if green_count <= 1 else "neutral")
    return indicator(
        "taiex_ma",
        "TAIEX 均線",
        score,
        signal,
        f"{green_count}/3",
        f"收盤 {latest_close:.2f}；MA5 {ma_values['ma5'] or 0:.2f} / MA20 {ma_values['ma20'] or 0:.2f} / MA60 {ma_values['ma60'] or 0:.2f}",
        available=len(clean_rows) >= 60,
        source=TWSE_TAIEX_URL,
        extra={
            "date": clean_rows[-1]["date"],
            "close": round(latest_close, 2),
            "ma5": round(ma_values["ma5"], 2) if ma_values["ma5"] else None,
            "ma20": round(ma_values["ma20"], 2) if ma_values["ma20"] else None,
            "ma60": round(ma_values["ma60"], 2) if ma_values["ma60"] else None,
            "lights": lights,
        },
    )


def _recent_dates(today: date | None = None, calendar_days: int = 18) -> list[date]:
    today = today or datetime.now(TAIPEI_TZ).date()
    return [today - timedelta(days=offset) for offset in range(calendar_days)]


def fetch_margin_history(today: date | None = None, limit: int = 6) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for current in _recent_dates(today, 18):
        try:
            payload = _http_json(
                TWSE_MARGIN_URL,
                {"date": current.strftime("%Y%m%d"), "selectType": "MS", "response": "json"},
                timeout=15,
            )
        except Exception:
            continue
        date_text = roc_date_to_iso(payload.get("date") or current.strftime("%Y%m%d"))
        margin_balance = None
        short_balance = None
        for table in payload.get("tables") or []:
            for row in table.get("data") or []:
                if not row:
                    continue
                name = str(row[0])
                if name.startswith("融資") and "交易單位" in name and len(row) >= 6:
                    margin_balance = parse_number(row[5])
                elif name.startswith("融券") and "交易單位" in name and len(row) >= 6:
                    short_balance = parse_number(row[5])
        if date_text and margin_balance is not None and short_balance is not None:
            rows.append({"date": date_text, "margin_balance": margin_balance, "short_balance": short_balance})
        if len(rows) >= limit:
            break
    return sorted(rows, key=lambda r: r["date"])


def _weekly_change(rows: list[dict[str, Any]], key: str) -> tuple[float | None, float | None, float | None, str]:
    clean = sorted([r for r in rows if parse_number(r.get(key)) is not None], key=lambda r: str(r["date"]))
    if len(clean) < 2:
        return None, None, None, ""
    latest = float(parse_number(clean[-1][key], 0) or 0)
    previous = float(parse_number(clean[0][key], 0) or 0)
    if previous == 0:
        return latest, previous, None, clean[-1]["date"]
    return latest, previous, (latest / previous - 1) * 100, clean[-1]["date"]


def compute_margin_indicator(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest, previous, change, date_text = _weekly_change(rows, "margin_balance")
    if change is None:
        return neutral_indicator("margin_weekly", "融資週變化", "TWSE margin balance unavailable", TWSE_MARGIN_URL)
    score = clamp(7.5 + change * 2.5, 0, 15)
    signal = "bullish" if change > 0 else ("bearish" if change < -1 else "neutral")
    return indicator(
        "margin_weekly",
        "融資週變化",
        score,
        signal,
        f"{change:+.2f}%",
        f"融資餘額 {latest:,.0f}；前值 {previous:,.0f}",
        source=TWSE_MARGIN_URL,
        extra={"date": date_text, "latest": latest, "previous": previous, "weekly_change_pct": round(change, 2)},
    )


def compute_short_indicator(rows: list[dict[str, Any]]) -> dict[str, Any]:
    latest, previous, change, date_text = _weekly_change(rows, "short_balance")
    if change is None:
        return neutral_indicator("short_weekly", "融券週變化", "TWSE short balance unavailable", TWSE_MARGIN_URL)
    score = 15 if change <= 0 else clamp(15 - change * 0.75, 0, 15)
    signal = "bearish" if change >= 10 else ("neutral" if change > 0 else "bullish")
    return indicator(
        "short_weekly",
        "融券週變化",
        score,
        signal,
        f"{change:+.2f}%",
        f"融券餘額 {latest:,.0f}；快速增加視為偏空",
        source=TWSE_MARGIN_URL,
        extra={"date": date_text, "latest": latest, "previous": previous, "weekly_change_pct": round(change, 2)},
    )


def fetch_foreign_net_history(today: date | None = None, limit: int = 5) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for current in _recent_dates(today, 18):
        try:
            payload = _http_json(
                TWSE_FOREIGN_URL,
                {"date": current.strftime("%Y%m%d"), "response": "json"},
                timeout=15,
            )
        except Exception:
            continue
        date_text = roc_date_to_iso(payload.get("date") or current.strftime("%Y%m%d"))
        net = 0.0
        count = 0
        for row in payload.get("data") or []:
            if len(row) >= 6:
                value = parse_number(row[5])
                if value is not None:
                    net += value
                    count += 1
        if date_text and count:
            rows.append({"date": date_text, "foreign_net_shares": net})
        if len(rows) >= limit:
            break
    return sorted(rows, key=lambda r: r["date"])


def compute_foreign_indicator(rows: list[dict[str, Any]]) -> dict[str, Any]:
    clean = sorted([r for r in rows if parse_number(r.get("foreign_net_shares")) is not None], key=lambda r: str(r["date"]))
    if not clean:
        return neutral_indicator("foreign_5d", "外資 5 日", "TWSE foreign net buy/sell unavailable", TWSE_FOREIGN_URL)
    cumulative = sum(float(parse_number(r["foreign_net_shares"], 0) or 0) for r in clean[-5:])
    score = 15 if cumulative > 0 else (7.5 if cumulative == 0 else 0)
    signal = "bullish" if cumulative > 0 else ("bearish" if cumulative < 0 else "neutral")
    display = f"{cumulative / 100_000_000:+.2f} 億股"
    return indicator(
        "foreign_5d",
        "外資 5 日",
        score,
        signal,
        display,
        "上市外資買賣超彙總，正值代表偏多",
        source=TWSE_FOREIGN_URL,
        extra={"date": clean[-1]["date"], "days": len(clean[-5:]), "cumulative_net_shares": round(cumulative)},
    )


def load_sfz_rows(path: Path = SFZ_ALL_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return []
    if isinstance(payload, dict):
        rows = payload.get("stocks") or []
    else:
        rows = payload
    return rows if isinstance(rows, list) else []


def compute_breadth_indicator(rows: list[dict[str, Any]]) -> dict[str, Any]:
    advances = 0
    declines = 0
    for row in rows:
        value = parse_number(row.get("gain_3d"), None)
        if value is None:
            value = parse_number(row.get("gain_6w"), None)
        if value is None:
            continue
        if value > 0:
            advances += 1
        elif value < 0:
            declines += 1
    if advances + declines == 0:
        return neutral_indicator("breadth", "Breadth", "No local breadth rows available", str(SFZ_ALL_PATH))
    ratio = advances / max(declines, 1)
    score = clamp(5 + (ratio - 1) * 3, 0, 10)
    signal = "bullish" if ratio > 1.2 else ("bearish" if ratio < 0.8 else "neutral")
    return indicator(
        "breadth",
        "上漲/下跌比",
        score,
        signal,
        f"{advances}/{declines}",
        "以站內最新 SFZ 全量清單估算市場廣度",
        source=str(SFZ_ALL_PATH),
        extra={"advances": advances, "declines": declines, "ratio": round(ratio, 2)},
    )


def fetch_vix_history(limit: int = 40) -> list[dict[str, Any]]:
    text = _http_text(CBOE_VIX_CSV_URL, timeout=30)
    rows: list[dict[str, Any]] = []
    for row in csv.DictReader(StringIO(text)):
        try:
            dt = datetime.strptime(row.get("DATE", ""), "%m/%d/%Y").date()
            close = parse_number(row.get("CLOSE"))
            if close is not None:
                rows.append({"date": dt.isoformat(), "close": close})
        except Exception:
            continue
    rows.sort(key=lambda r: r["date"])
    return rows[-limit:]


def compute_vix_indicator(rows: list[dict[str, Any]]) -> dict[str, Any]:
    clean = sorted([r for r in rows if parse_number(r.get("close")) is not None], key=lambda r: str(r["date"]))
    if not clean:
        return neutral_indicator("us_vix", "US VIX", "Cboe VIX history unavailable", CBOE_VIX_CSV_URL)
    latest = float(parse_number(clean[-1]["close"], 0) or 0)
    previous = float(parse_number(clean[-6]["close"], latest) or latest) if len(clean) >= 6 else latest
    change_5d = (latest / previous - 1) * 100 if previous else 0.0
    if latest < 15:
        score = 15
    elif latest < 20:
        score = 12
    elif latest < 25:
        score = 8
    elif latest < 30:
        score = 4
    else:
        score = 0
    if change_5d > 20:
        score -= 4
    elif change_5d < -10:
        score += 2
    score = clamp(score, 0, 15)
    signal = "bullish" if score >= 12 else ("bearish" if score < 6 else "neutral")
    return indicator(
        "us_vix",
        "US VIX",
        score,
        signal,
        f"{latest:.2f}",
        f"5日變化 {change_5d:+.2f}%",
        source=CBOE_VIX_CSV_URL,
        extra={"date": clean[-1]["date"], "latest": latest, "change_5d_pct": round(change_5d, 2)},
    )


def regime_for_score(score: float) -> tuple[str, str, str]:
    if score < 40:
        return "bearish", "偏空", "red"
    if score <= 60:
        return "neutral", "中性", "yellow"
    return "bullish", "偏多", "green"


def build_payload(indicators: dict[str, dict[str, Any]], source_status: list[str] | None = None) -> dict[str, Any]:
    ordered = {key: indicators[key] for key in INDICATOR_WEIGHTS if key in indicators}
    total_weight = sum(float(item.get("weight", 0) or 0) for item in ordered.values())
    raw_score = sum(float(item.get("score", 0) or 0) for item in ordered.values())
    score = clamp((raw_score / total_weight * 100) if total_weight else 50, 0, 100)
    regime, regime_label, color = regime_for_score(score)
    updated_at = datetime.now(TAIPEI_TZ).isoformat(timespec="seconds")
    dates = [str(item.get("date")) for item in ordered.values() if item.get("date")]
    data_date = max(dates) if dates else updated_at[:10]
    summary = f"市場情緒 {round(score)} 分，{regime_label}；VIX 與台股均線/籌碼一起納入判斷。"
    return {
        "schema_version": 1,
        "date": data_date,
        "updated_at": updated_at,
        "score": int(round(score)),
        "regime": regime,
        "regime_label": regime_label,
        "color": color,
        "summary": summary,
        "indicators": ordered,
        "source_status": source_status or [],
        "future_extensions": ["us_market_rotation", "japan_market_rotation", "korea_market_rotation"],
    }


def write_payload(indicators: dict[str, dict[str, Any]], path: Path = OUTPUT_PATH, source_status: list[str] | None = None) -> dict[str, Any]:
    payload = build_payload(indicators, source_status=source_status)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def collect_indicators() -> tuple[dict[str, dict[str, Any]], list[str]]:
    source_status: list[str] = []

    try:
        taiex_rows = fetch_taiex_history()
        taiex = compute_taiex_ma_indicator(taiex_rows)
        source_status.append(_status("TAIEX", bool(taiex_rows), f"{len(taiex_rows)} rows"))
    except Exception as exc:
        taiex = neutral_indicator("taiex_ma", "TAIEX 均線", f"fetch failed: {exc}", TWSE_TAIEX_URL)
        source_status.append(_status("TAIEX", False, str(exc)))

    try:
        margin_rows = fetch_margin_history()
        margin = compute_margin_indicator(margin_rows)
        short = compute_short_indicator(margin_rows)
        source_status.append(_status("TWSE margin", bool(margin_rows), f"{len(margin_rows)} trading days"))
    except Exception as exc:
        margin = neutral_indicator("margin_weekly", "融資週變化", f"fetch failed: {exc}", TWSE_MARGIN_URL)
        short = neutral_indicator("short_weekly", "融券週變化", f"fetch failed: {exc}", TWSE_MARGIN_URL)
        source_status.append(_status("TWSE margin", False, str(exc)))

    try:
        foreign_rows = fetch_foreign_net_history()
        foreign = compute_foreign_indicator(foreign_rows)
        source_status.append(_status("TWSE foreign", bool(foreign_rows), f"{len(foreign_rows)} trading days"))
    except Exception as exc:
        foreign = neutral_indicator("foreign_5d", "外資 5 日", f"fetch failed: {exc}", TWSE_FOREIGN_URL)
        source_status.append(_status("TWSE foreign", False, str(exc)))

    try:
        sfz_rows = load_sfz_rows()
        breadth = compute_breadth_indicator(sfz_rows)
        source_status.append(_status("local breadth", bool(sfz_rows), f"{len(sfz_rows)} SFZ rows"))
    except Exception as exc:
        breadth = neutral_indicator("breadth", "上漲/下跌比", f"local breadth failed: {exc}", str(SFZ_ALL_PATH))
        source_status.append(_status("local breadth", False, str(exc)))

    try:
        vix_rows = fetch_vix_history()
        vix = compute_vix_indicator(vix_rows)
        source_status.append(_status("Cboe VIX", bool(vix_rows), f"{len(vix_rows)} rows"))
    except Exception as exc:
        vix = neutral_indicator("us_vix", "US VIX", f"fetch failed: {exc}", CBOE_VIX_CSV_URL)
        source_status.append(_status("Cboe VIX", False, str(exc)))

    return {
        "taiex_ma": taiex,
        "margin_weekly": margin,
        "short_weekly": short,
        "foreign_5d": foreign,
        "breadth": breadth,
        "us_vix": vix,
    }, source_status


def main() -> None:
    indicators, source_status = collect_indicators()
    payload = write_payload(indicators, OUTPUT_PATH, source_status)
    print(f"[market_sentiment] wrote {OUTPUT_PATH} score={payload['score']} regime={payload['regime']}")


if __name__ == "__main__":
    main()
