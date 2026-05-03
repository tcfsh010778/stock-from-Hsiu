from __future__ import annotations

import csv
import html
import json
import math
import re
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
PRICE_DIR = DATA_DIR / "prices"
HOLDING_DIR = DATA_DIR / "holding_shares"
REPORTS_PATH = DATA_DIR / "site_reports.json"
MARKETS_PATH = DATA_DIR / "stock_markets.json"

CSV_OUT = DATA_DIR / "mda_universe_scan.csv"
JSON_OUT = DATA_DIR / "mda_universe_scan.json"
HTML_OUT = DOCS_DIR / "mda_universe_scan.html"


def to_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        out = float(str(value).replace(",", ""))
        return out if math.isfinite(out) else default
    except Exception:
        return default


def to_int(value, default=None):
    try:
        if value in (None, ""):
            return default
        return int(float(str(value).replace(",", "")))
    except Exception:
        return default


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp950"):
        try:
            with path.open("r", encoding=encoding, newline="") as fh:
                return list(csv.DictReader(fh))
        except UnicodeDecodeError:
            continue
    return []


def moving_average(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    total = 0.0
    for idx, value in enumerate(values):
        total += value
        if idx >= window:
            total -= values[idx - window]
        out.append(total / window if idx + 1 >= window else None)
    return out


def pct_change(now: float | None, prev: float | None) -> float | None:
    if now is None or prev in (None, 0):
        return None
    return (now / prev - 1.0) * 100.0


def holding_group(level: str) -> str:
    text = str(level or "")
    if text == "total" or "差異" in text:
        return "other"
    nums = [int(x.replace(",", "")) for x in re.findall(r"\d[\d,]*", text)]
    if "more than" in text or (nums and max(nums) >= 1_000_001):
        return "major"
    if nums and max(nums) <= 10_000:
        return "retail"
    return "middle"


def read_holding_series(stock_id: str) -> list[dict]:
    rows = read_csv(HOLDING_DIR / f"{stock_id}.csv")
    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        date = str(row.get("date") or "").strip()
        if date:
            by_date[date].append(row)

    series = []
    for date in sorted(by_date):
        item = {"date": date, "major": 0.0, "retail": 0.0, "total_people": None}
        for row in by_date[date]:
            level = str(row.get("HoldingSharesLevel") or "").strip()
            pct = to_float(row.get("percent"), 0.0)
            people = to_int(row.get("people"))
            if level == "total":
                item["total_people"] = people
                continue
            group = holding_group(level)
            if group in {"major", "retail"}:
                item[group] += pct or 0.0
        series.append(item)
    return series


def delta(rows: list[dict], key: str, weeks: int):
    if len(rows) < weeks + 1:
        return None
    latest = rows[-1].get(key)
    base = rows[-(weeks + 1)].get(key)
    if latest is None or base is None:
        return None
    return latest - base


def clean_stock_name(name: str) -> str:
    text = str(name or "").strip()
    text = re.sub(r"\s*｜\s*[^\s｜]*\s*[^｜]*", "", text)
    text = re.sub(r"\s*｜\s*綜合評分[:：]?\s*[\d.]+", "", text)
    return text.strip()


def load_names() -> dict[str, str]:
    names: dict[str, str] = {}
    try:
        market_cache = json.loads(MARKETS_PATH.read_text(encoding="utf-8"))
    except Exception:
        market_cache = {}
    for sid, item in (market_cache.get("stocks") or {}).items():
        name = clean_stock_name((item or {}).get("name", ""))
        if re.fullmatch(r"\d{4}", str(sid)) and name:
            names[str(sid)] = name

    try:
        reports = json.loads(REPORTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        reports = []
    for report in reports:
        for stock in report.get("stocks", []) or []:
            sid = str(stock.get("id") or "").strip()
            name = clean_stock_name(stock.get("name") or "")
            if re.fullmatch(r"\d{4}", sid) and name and sid not in names:
                names[sid] = name
    return names


def read_price_rows(stock_id: str) -> list[dict]:
    out = []
    for row in read_csv(PRICE_DIR / f"{stock_id}.csv"):
        close = to_float(row.get("close"))
        high = to_float(row.get("high"))
        low = to_float(row.get("low"))
        volume = to_float(row.get("volume"))
        date = str(row.get("date") or "").strip()
        if date and close is not None:
            out.append({"date": date, "close": close, "high": high or close, "low": low or close, "volume": volume or 0.0})
    return sorted(out, key=lambda x: x["date"])


def scan_stock(stock_id: str, name: str) -> dict | None:
    prices = read_price_rows(stock_id)
    holdings = read_holding_series(stock_id)
    if len(prices) < 130 or len(holdings) < 5:
        return None

    closes = [x["close"] for x in prices]
    highs = [x["high"] for x in prices]
    lows = [x["low"] for x in prices]
    volumes = [x["volume"] for x in prices]
    ma120 = moving_average(closes, 120)
    ma240 = moving_average(closes, 240)

    close = closes[-1]
    ma120_now = ma120[-1]
    ma120_prev20 = ma120[-21] if len(ma120) >= 21 else None
    ma240_now = ma240[-1] if ma240 else None
    ma240_prev20 = ma240[-21] if len(ma240) >= 21 else None
    ma120_slope = pct_change(ma120_now, ma120_prev20)
    ma240_slope = pct_change(ma240_now, ma240_prev20)

    major_4w = delta(holdings, "major", 4)
    major_8w = delta(holdings, "major", 8)
    retail_4w = delta(holdings, "retail", 4)
    retail_8w = delta(holdings, "retail", 8)
    people_4w = delta(holdings, "total_people", 4)
    people_8w = delta(holdings, "total_people", 8)

    major_accumulating = (major_4w is not None and major_4w >= 0.5) or (major_8w is not None and major_8w >= 1.0)
    retail_or_people_support = (
        (retail_4w is not None and retail_4w <= -0.3)
        or (retail_8w is not None and retail_8w <= -0.8)
        or (people_4w is not None and people_4w < 0)
        or (people_8w is not None and people_8w < 0)
    )
    ma120_up = ma120_slope is not None and ma120_slope > 0
    price_above_ma120 = ma120_now is not None and close > ma120_now
    base_mda_watch = ma120_up and price_above_ma120 and major_accumulating

    high_240 = max(highs[-240:]) if len(highs) >= 240 else max(highs)
    one_year_high_gap = pct_change(close, high_240)
    deduct240_gap = pct_change(close, closes[-241]) if len(closes) >= 241 else None
    low20 = min(lows[-20:])
    low60 = min(lows[-60:]) if len(lows) >= 60 else min(lows)
    no_new_low = low20 >= low60 * 0.98
    range60 = (max(highs[-60:]) / min(lows[-60:]) - 1.0) * 100.0 if len(lows) >= 60 and min(lows[-60:]) > 0 else None
    vol20 = sum(volumes[-20:]) / min(20, len(volumes))
    vol120 = sum(volumes[-120:]) / min(120, len(volumes))
    volume_shrink = pct_change(vol20, vol120)

    ma240_up = ma240_slope is not None and ma240_slope >= 0
    price_above_ma240 = ma240_now is not None and close > ma240_now
    near_high = one_year_high_gap is not None and one_year_high_gap >= -12.0
    deduct_favorable = deduct240_gap is not None and deduct240_gap >= 3.0

    if base_mda_watch and price_above_ma240 and ma240_up and near_high:
        basket = "已發動籃"
    elif base_mda_watch and no_new_low and (deduct_favorable or price_above_ma240 or (one_year_high_gap is not None and one_year_high_gap >= -28.0)):
        basket = "空轉多觀察籃"
    elif major_accumulating and no_new_low and (ma120_up or (ma120_slope is not None and ma120_slope > -1.0)):
        basket = "未發動觀察籃"
    else:
        basket = "未入籃"

    score = 0
    score += 30 if base_mda_watch else 0
    score += 20 if retail_or_people_support else 0
    score += 15 if price_above_ma240 else 0
    score += 15 if ma240_up else 0
    score += 10 if no_new_low else 0
    score += 10 if volume_shrink is not None and volume_shrink <= -20 else 0

    reasons = []
    if ma120_up and price_above_ma120:
        reasons.append("MA120上彎且收盤站上")
    if major_accumulating:
        reasons.append("大戶4/8週累積")
    if retail_or_people_support:
        reasons.append("散戶或總股東下降支撐")
    if no_new_low:
        reasons.append("近20日未破60日低位區")
    if volume_shrink is not None and volume_shrink <= -20:
        reasons.append("20日量低於120日均量")
    if deduct_favorable:
        reasons.append("240扣抵偏有利")

    return {
        "stock_id": stock_id,
        "name": name,
        "basket": basket,
        "score": score,
        "date": prices[-1]["date"],
        "close": close,
        "ma120": ma120_now,
        "ma240": ma240_now,
        "ma120_slope_pct": ma120_slope,
        "ma240_slope_pct": ma240_slope,
        "one_year_high_gap_pct": one_year_high_gap,
        "deduct240_gap_pct": deduct240_gap,
        "range60_pct": range60,
        "volume20_vs_120_pct": volume_shrink,
        "major_4w_pctpt": major_4w,
        "major_8w_pctpt": major_8w,
        "retail_4w_pctpt": retail_4w,
        "retail_8w_pctpt": retail_8w,
        "people_4w": people_4w,
        "people_8w": people_8w,
        "major_accumulating": major_accumulating,
        "retail_or_people_support": retail_or_people_support,
        "base_mda_watch": base_mda_watch,
        "no_new_low": no_new_low,
        "reason": "、".join(reasons) if reasons else "條件不足",
    }


def fmt(value, digits=2, suffix=""):
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "Y" if value else ""
    if isinstance(value, (int, float)):
        return f"{value:,.{digits}f}{suffix}"
    return str(value)


def safe_print(text: str) -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode("cp950", errors="replace").decode("cp950", errors="replace"))


def build_html(rows: list[dict]) -> str:
    groups = ["已發動籃", "空轉多觀察籃", "未發動觀察籃", "未入籃"]
    counts = {g: sum(1 for r in rows if r["basket"] == g) for g in groups}
    latest_date = max((r["date"] for r in rows), default="-")
    body = []
    for group in groups:
        group_rows = [r for r in rows if r["basket"] == group]
        if not group_rows:
            continue
        body.append(f"<section><h2>{html.escape(group)} <span>{len(group_rows)}</span></h2>")
        body.append("<div class=\"table-wrap\"><table><thead><tr>"
                    "<th>股票</th><th>分數</th><th>收盤</th><th>MA120斜率</th><th>MA240斜率</th>"
                    "<th>距一年高</th><th>大戶4週</th><th>大戶8週</th><th>散戶4週</th><th>散戶8週</th>"
                    "<th>股東4週</th><th>量能</th><th>判讀</th></tr></thead><tbody>")
        for r in group_rows:
            sid = html.escape(r["stock_id"])
            name = html.escape(r.get("name") or "")
            link = f"stocks/{sid}.html"
            body.append("<tr>"
                        f"<td><a href=\"{link}\">{sid} {name}</a></td>"
                        f"<td>{fmt(r['score'], 0)}</td>"
                        f"<td>{fmt(r['close'], 2)}</td>"
                        f"<td>{fmt(r['ma120_slope_pct'], 2, '%')}</td>"
                        f"<td>{fmt(r['ma240_slope_pct'], 2, '%')}</td>"
                        f"<td>{fmt(r['one_year_high_gap_pct'], 2, '%')}</td>"
                        f"<td>{fmt(r['major_4w_pctpt'], 2, 'pt')}</td>"
                        f"<td>{fmt(r['major_8w_pctpt'], 2, 'pt')}</td>"
                        f"<td>{fmt(r['retail_4w_pctpt'], 2, 'pt')}</td>"
                        f"<td>{fmt(r['retail_8w_pctpt'], 2, 'pt')}</td>"
                        f"<td>{fmt(r['people_4w'], 0)}</td>"
                        f"<td>{fmt(r['volume20_vs_120_pct'], 1, '%')}</td>"
                        f"<td>{html.escape(r['reason'])}</td>"
                        "</tr>")
        body.append("</tbody></table></div></section>")

    return f"""<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>M大主篩掃描報表</title>
  <style>
    nav{{background:#0a0a16;border-bottom:1px solid #30363d;padding:6px 12px;display:flex;flex-wrap:wrap;align-items:center;gap:6px;position:sticky;top:0;z-index:999}}
    .nav-brand{{font-weight:800;color:#e6edf3;font-size:14px;margin-right:8px}}
    nav a.tab{{padding:4px 12px;border-radius:16px;font-size:12px;font-weight:600;white-space:nowrap;color:#8b949e;background:#1a1a2e;transition:all .2s}}
    nav a.tab:hover,nav a.tab.active{{background:#1a6bc4;color:#fff;text-decoration:none}}
    body{{margin:0;background:#f6f7f9;color:#18202a;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",Arial,sans-serif}}
    header{{padding:28px 24px 18px;background:#ffffff;border-bottom:1px solid #dde3ea}}
    main{{padding:20px 24px 36px}}
    h1{{margin:0 0 8px;font-size:28px;letter-spacing:0}}
    h2{{margin:28px 0 10px;font-size:20px}}
    h2 span{{color:#5c6672;font-size:15px}}
    .meta{{display:flex;flex-wrap:wrap;gap:8px;color:#5c6672;font-size:14px}}
    .pill{{background:#edf2f7;border:1px solid #d8e1eb;border-radius:999px;padding:5px 10px}}
    .table-wrap{{overflow:auto;background:#fff;border:1px solid #dde3ea;border-radius:8px}}
    table{{width:100%;border-collapse:collapse;min-width:1180px;font-size:13px}}
    th,td{{padding:9px 10px;border-bottom:1px solid #edf1f5;text-align:right;white-space:nowrap}}
    th:first-child,td:first-child,th:last-child,td:last-child{{text-align:left}}
    th{{background:#f9fbfd;color:#4d5967;font-weight:650}}
    tr:hover{{background:#fbfdff}}
    a{{color:#1468b8;text-decoration:none;font-weight:650}}
    .note{{margin-top:10px;color:#5c6672;font-size:13px;line-height:1.7;max-width:960px}}
  </style>
</head>
<body>
  <nav>
    <span class="nav-brand">Stockfrom脩</span>
    <a href="index.html" class="tab">首頁</a>
    <a href="daily.html" class="tab">今日選股</a>
    <a href="mda.html" class="tab">M大選股</a>
    <a href="baskets.html" class="tab">雙籃儀表板</a>
    <a href="signals.html" class="tab">訊號追蹤</a>
    <a href="radar.html" class="tab">買點雷達</a>
    <a href="mda_universe_scan.html" class="tab active">M大掃描</a>
    <a href="stocks.html" class="tab">個股查詢</a>
    <a href="backtest.html" class="tab">歷史回測</a>
    <a href="history.html" class="tab">歷史報告</a>
  </nav>
  <header>
    <h1>M大主篩掃描報表</h1>
    <div class="meta">
      <span class="pill">掃描檔數：{len(rows)}</span>
      <span class="pill">最新股價日：{html.escape(latest_date)}</span>
      <span class="pill">已發動：{counts['已發動籃']}</span>
      <span class="pill">空轉多：{counts['空轉多觀察籃']}</span>
      <span class="pill">未發動：{counts['未發動觀察籃']}</span>
    </div>
    <div class="note">目前這頁先用網站快取內同時具備股價與股權分散資料的股票掃描。核心條件是 MA120 上彎、收盤站上 MA120、千張大戶 4 週增加 0.5pt 或 8 週增加 1.0pt；散戶下降與總股東人數下降作為支撐判讀，不再使用平均每人持股。</div>
  </header>
  <main>
    {''.join(body)}
  </main>
</body>
</html>
"""


def main() -> None:
    names = load_names()
    ids = sorted({p.stem for p in PRICE_DIR.glob("*.csv")} & {p.stem for p in HOLDING_DIR.glob("*.csv")})
    rows = [r for sid in ids if (r := scan_stock(sid, names.get(sid, "")))]
    rows.sort(key=lambda r: ({"已發動籃": 0, "空轉多觀察籃": 1, "未發動觀察籃": 2, "未入籃": 3}[r["basket"]], -r["score"], r["stock_id"]))

    DATA_DIR.mkdir(exist_ok=True)
    DOCS_DIR.mkdir(exist_ok=True)
    JSON_OUT.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    if rows:
        with CSV_OUT.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    HTML_OUT.write_text(build_html(rows), encoding="utf-8")

    counts = defaultdict(int)
    for row in rows:
        counts[row["basket"]] += 1
    safe_print(f"scanned={len(rows)} launched={counts['已發動籃']} turning={counts['空轉多觀察籃']} dormant={counts['未發動觀察籃']} not_in={counts['未入籃']}")
    for row in rows[:20]:
        safe_print(f"{row['basket']} {row['stock_id']} {row.get('name','')} score={row['score']} close={row['close']} reason={row['reason']}")


if __name__ == "__main__":
    main()
