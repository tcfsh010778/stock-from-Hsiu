from __future__ import annotations

import csv
import json
import math
import os
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
PRICE_DIR = DATA_DIR / "prices"
SCAN_PATH = DATA_DIR / "mda_universe_scan.json"
MARKETS_PATH = DATA_DIR / "stock_markets.json"
MIN_CLOSE = float(os.environ.get("DAILY_TOP20_MIN_CLOSE", "20"))
TOP_N = int(os.environ.get("DAILY_TOP20_N", "20"))


def to_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        value = str(value).replace(",", "").replace("%", "").replace("+", "").strip()
        return float(value)
    except Exception:
        return default


def fmt_num(value, digits=2, unit=""):
    value = to_float(value, None)
    if value is None or not math.isfinite(value):
        return "-"
    if digits <= 0:
        text = f"{value:.0f}"
    else:
        text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return f"{text}{unit}"


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp950"):
        try:
            with path.open("r", encoding=encoding, newline="") as fh:
                return list(csv.DictReader(fh))
        except UnicodeDecodeError:
            continue
    return []


def price_rows(stock_id: str) -> list[dict]:
    rows = []
    for row in read_csv_rows(PRICE_DIR / f"{stock_id}.csv"):
        try:
            rows.append({
                "date": row.get("date", ""),
                "open": float(row.get("open") or row.get("Open") or 0),
                "high": float(row.get("high") or row.get("max") or row.get("High") or 0),
                "low": float(row.get("low") or row.get("min") or row.get("Low") or 0),
                "close": float(row.get("close") or row.get("Close") or 0),
                "volume": float(row.get("volume") or row.get("Trading_Volume") or row.get("Volume") or 0),
            })
        except Exception:
            continue
    return sorted([r for r in rows if r["date"] and r["close"] > 0], key=lambda r: r["date"])


def sma(values: list[float], window: int):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def rsi(values: list[float], window: int = 14):
    if len(values) <= window:
        return None
    gains = []
    losses = []
    for i in range(len(values) - window, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / window
    avg_loss = sum(losses) / window
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def bb_pct(values: list[float], window: int = 20):
    if len(values) < window:
        return None
    recent = values[-window:]
    mid = sum(recent) / window
    var = sum((x - mid) ** 2 for x in recent) / window
    std = math.sqrt(var)
    upper = mid + 2 * std
    lower = mid - 2 * std
    if upper <= lower:
        return None
    return (values[-1] - lower) / (upper - lower) * 100


def pct_change(now, before):
    if now is None or before in (None, 0):
        return None
    return (now / before - 1) * 100


def load_scan_rows() -> list[dict]:
    if not SCAN_PATH.exists():
        return []
    rows = json.loads(SCAN_PATH.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        return []
    markets = {}
    if MARKETS_PATH.exists():
        try:
            markets = (json.loads(MARKETS_PATH.read_text(encoding="utf-8")).get("stocks") or {})
        except Exception:
            markets = {}
    for row in rows:
        sid = str(row.get("stock_id") or "")
        info = markets.get(sid) or {}
        if info.get("name") and not row.get("name"):
            row["name"] = info["name"]
    return [r for r in rows if to_float(r.get("close"), 0) >= MIN_CLOSE]


def stock_status(row: dict) -> tuple[str, str]:
    basket = row.get("basket", "")
    if basket == "已發動籃":
        return "🟡", "強勢追蹤"
    if basket in {"空轉多觀察籃", "未發動觀察籃"}:
        return "🟢", "健康整理"
    return "🔴", "過熱/風險"


def enrich(row: dict) -> dict:
    sid = str(row.get("stock_id") or "")
    rows = price_rows(sid)
    closes = [r["close"] for r in rows]
    latest = rows[-1] if rows else {}
    close = latest.get("close") or to_float(row.get("close"), None)
    high21 = max((r["high"] for r in rows[-21:]), default=None)
    low21 = min((r["low"] for r in rows[-21:]), default=None)
    ma5 = sma(closes, 5)
    ma34 = sma(closes, 34)
    gain_6w = pct_change(close, closes[-31]) if close and len(closes) >= 31 else None
    vol5 = (sum(r["volume"] for r in rows[-5:]) / 5 / 1000) if len(rows) >= 5 else None
    entry = ma5 * 0.985 if ma5 else close
    target = high21 * 1.02 if high21 else (close * 1.12 if close else None)
    stop = low21 * 0.995 if low21 else (close * 0.9 if close else None)
    icon, status = stock_status(row)
    return {
        **row,
        "stock_id": sid,
        "name": row.get("name") or sid,
        "date": latest.get("date") or row.get("date") or "",
        "close": close,
        "icon": icon,
        "status": status,
        "gain_6w": gain_6w,
        "rsi": rsi(closes),
        "bb_pct": bb_pct(closes),
        "ma5": ma5,
        "ma34": ma34,
        "vol5_lot": vol5,
        "high21": high21,
        "low21": low21,
        "entry": entry,
        "target": target,
        "stop": stop,
    }


def select_top20(rows: list[dict]) -> list[dict]:
    basket_rank = {"已發動籃": 0, "空轉多觀察籃": 1, "未發動觀察籃": 2, "未入籃": 3}
    enriched = [enrich(r) for r in rows]
    enriched = [r for r in enriched if r.get("close") and r.get("date")]
    enriched.sort(key=lambda r: (
        basket_rank.get(r.get("basket", ""), 9),
        -to_float(r.get("score"), 0),
        str(r.get("stock_id") or ""),
    ))
    return enriched[:TOP_N]


def report_markdown(rows: list[dict], report_date: str) -> str:
    latest_price_date = max((r.get("date", "") for r in rows), default=report_date)
    lines = [
        f"# 每日選股報告｜{report_date}",
        "",
        "> 自動更新版：先由 M大全市場候選池挑出每日 Top20，再交給網站做 SFZ雙籃、買點雷達、入選追蹤與個股頁更新。",
        "",
        "---",
        "",
        f"## 大盤概況｜{latest_price_date}",
        "本頁由 GitHub Actions 自動產生，重點是讓網站每天有新的母名單可追蹤。實際買賣仍回到個股頁與買點雷達判讀。",
        "",
        "---",
        "",
        "## 篩選條件",
        "",
        "| 條件 | 說明 |",
        "|------|------|",
        f"| 價格 | 收盤價 >= {fmt_num(MIN_CLOSE, 0)} 元 |",
        "| 母名單 | M大全市場掃描候選 |",
        "| 排序 | 已發動優先，其次空轉多與未發動；同籃依 M大分數排序 |",
        "",
        f"**Top {len(rows)}**",
        "",
        "---",
        "",
        f"## Top {len(rows)} 精選個股｜{latest_price_date}",
        "",
    ]
    for idx, r in enumerate(rows, 1):
        sid = r["stock_id"]
        lines.extend([
            f"### {idx}. {r['icon']} {sid} {r.get('name','')} ｜{r['status']}｜ Score: {fmt_num(r.get('score'), 2)}",
            "",
            "| 指標 | 數值 |",
            "|------|------|",
            f"| 收盤價 | **{fmt_num(r.get('close'), 2)} 元** |",
            f"| 近6週漲幅 | **{fmt_num(r.get('gain_6w'), 2, '%')}** |",
            f"| RSI\\(14\\) | {fmt_num(r.get('rsi'), 1)} |",
            f"| 布林 %B | {fmt_num(r.get('bb_pct'), 1, '%')} |",
            f"| 近5日量 | {fmt_num(r.get('vol5_lot'), 0)} 張 |",
            f"| 近21日壓力 | {fmt_num(r.get('high21'), 2)} |",
            f"| 近21日支撐 | {fmt_num(r.get('low21'), 2)} |",
            f"| 外資近5日 | ─ |",
            f"| 外資連買天數 | ─ |",
            f"| 📌 進場參考 | {fmt_num(r.get('entry'), 2)} |",
            f"| 🎯 目標價 | {fmt_num(r.get('target'), 2)} |",
            f"| 🛑 停損價 | {fmt_num(r.get('stop'), 2)} |",
            "",
            f"> 籃子：{r.get('basket','')}；判讀：{r.get('reason','')}",
            "",
            "---",
            "",
        ])
    return "\n".join(lines)


def main() -> None:
    rows = select_top20(load_scan_rows())
    if not rows:
        raise SystemExit("No rows available. Run mda_full_market_refresh.py and mda_universe_scan.py first.")
    report_date = max((r.get("date", "") for r in rows), default=date.today().isoformat())
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORTS_DIR / f"每日選股報告_{report_date}.md"
    out.write_text(report_markdown(rows, report_date), encoding="utf-8")
    print(f"[run_screener] wrote {out} rows={len(rows)}")


if __name__ == "__main__":
    main()
