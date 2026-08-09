# -*- coding: utf-8 -*-
"""
generate_site.py - Stockfrom Site Generator v1.0
Usage: python generate_site.py
Output: ./docs/  (GitHub Pages root)
"""

import re
import os
import sys
import glob
import shutil
import csv
import json
import html
import sqlite3
import math
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

from stock_rules import evaluate_traffic_light
from stock_rules import holding_group as _shared_holding_group
from stock_rules import is_overheated as _shared_is_overheated
from stock_rules import overheat_reasons as _shared_overheat_reasons
from stock_rules import site_basket_assessment
from stock_rules import site_basket_key
from tools.pit_universe import get_eligible_universe, is_in_universe  # noqa: F401  # used in next step

# ──────────────────────────────────────────────
#  路徑設定（Windows / Linux 自動切換）
# ──────────────────────────────────────────────
_WIN_REPORTS  = Path(r"C:\Users\USER\OneDrive\文件\Claude\Projects\Stock from Zero")
_LINUX_REPORTS = Path("/sessions/adoring-amazing-mayer/mnt/Stock from Zero")
_REPO_REPORTS = Path(__file__).parent / "reports"
PUSH_LOG_PATH = Path(__file__).parent / "signal_push_log.csv"
V44_ROOT = Path(os.environ.get("V44_ROOT", r"C:\Users\USER\OneDrive\桌面\股票\自動交易程式"))
LOCAL_DATA_DIR = Path(__file__).parent / "data"
CHART_LOOKBACK_BARS = 520
CHART_DEFAULT_VISIBLE_DAYS = 31
MDA_MIN_CLOSE = 20.0
LOCAL_PRICE_DIR = LOCAL_DATA_DIR / "prices"
LOCAL_CHIP_DIR = LOCAL_DATA_DIR / "chips"
LOCAL_HOLDING_DIR = LOCAL_DATA_DIR / "holding_shares"
LOCAL_FOREIGN_SHAREHOLDING_DIR = LOCAL_DATA_DIR / "foreign_shareholding"
LOCAL_MARGIN_DIR = LOCAL_DATA_DIR / "margin"
REPORTS_CACHE_PATH = LOCAL_DATA_DIR / "site_reports.json"
SFZ_ALL_PATH = LOCAL_DATA_DIR / "sfz_all.json"
MARKET_SENTIMENT_PATH = LOCAL_DATA_DIR / "market_sentiment.json"
CARYBOT_SIGNALS_PATH = LOCAL_DATA_DIR / "carybot_signals.json"
BACKTEST_DASHBOARD_PATH = LOCAL_DATA_DIR / "backtest_results.json"
DAILY_DECISIONS_PATH = LOCAL_DATA_DIR / "daily_decisions.json"
ATTENTION_DISPOSITION_PATH = LOCAL_DATA_DIR / "attention_disposition.json"
DAILY_MARKET_FLOW_PATH = LOCAL_DATA_DIR / "daily_market_flow.json"
WEEKLY_HOLDER_RISERS_PATH = LOCAL_DATA_DIR / "weekly_holder_risers.json"
FRESHNESS_MANIFEST_PATH = LOCAL_DATA_DIR / "freshness_manifest.json"
PRICE_REFRESH_SUMMARY_PATH = LOCAL_DATA_DIR / "price_refresh_summary.json"
MARKET_CACHE_PATH = LOCAL_DATA_DIR / "stock_markets.json"
INDUSTRY_CACHE_PATH = LOCAL_DATA_DIR / "stock_industries.json"
PUBLIC_DATA_FILES = [
    SFZ_ALL_PATH,
    MARKET_SENTIMENT_PATH,
    CARYBOT_SIGNALS_PATH,
    BACKTEST_DASHBOARD_PATH,
    DAILY_DECISIONS_PATH,
    ATTENTION_DISPOSITION_PATH,
    DAILY_MARKET_FLOW_PATH,
    WEEKLY_HOLDER_RISERS_PATH,
    FRESHNESS_MANIFEST_PATH,
    PRICE_REFRESH_SUMMARY_PATH,
]
V44_PRICE_DIR = V44_ROOT / "回測" / "v6_outputs" / "prices"
V44_CHIP_DIR = V44_ROOT / "回測" / "v6_outputs" / "chips"
V44_HOLDING_DIR = V44_ROOT / "回測" / "v6_outputs" / "holding_shares"
V44_FOREIGN_SHAREHOLDING_DIR = V44_ROOT / "回測" / "v6_outputs" / "foreign_shareholding"
V44_MARGIN_DIR = V44_ROOT / "回測" / "v6_outputs" / "margin"
V44_BACKTEST_OUTPUT_DIR = V44_ROOT / "回測" / "v6_outputs"
V44_DB_PATH = V44_ROOT / "v9_reports" / "stockfromshu_records.sqlite"
_V44_FETCHER = None
_PRICE_HISTORY_CACHE: dict[str, list[dict]] = {}
SITE_LATEST_REPORT_DATE = ""
_INDUSTRY_CACHE: dict[str, dict] | None = None

if os.environ.get("REPORTS_DIR"):
    REPORTS_DIR = Path(os.environ["REPORTS_DIR"])
elif _REPO_REPORTS.exists():
    REPORTS_DIR = _REPO_REPORTS
elif sys.platform == "win32":
    REPORTS_DIR = _WIN_REPORTS
else:
    REPORTS_DIR = _LINUX_REPORTS if _LINUX_REPORTS.exists() else _WIN_REPORTS

OUTPUT_DIR = Path(__file__).parent / "docs"

ALLOWED_MARKETS = {"上市", "上櫃"}
TWSE_STOCK_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_DAILY_CLOSE_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"

# ──────────────────────────────────────────────
#  MD 解析器
# ──────────────────────────────────────────────

def _clean_cell(s: str) -> str:
    """清除 MD 表格值的 ** 與 元 字尾"""
    return s.strip().replace("**", "").replace(" 元", "").strip()


def _parse_format_v1(text: str, result: dict) -> dict:
    """
    新格式（v44 最新）：
    ### 1. 🟢 6213 聯茂 ｜健康整理 ｜ Score: 100.0
    | 收盤價 | **253.5 元** |
    """
    stock_pattern = re.compile(
        r"### \d+\. ([🟢🟡🔴]) (\d{4}) (.+?) ｜(.+?)｜ Score: ([\d.]+)(.*?)(?=\n### \d+\.|\n---|\Z)",
        re.DOTALL
    )
    for m in stock_pattern.finditer(text):
        status_icon = m.group(1)
        stock_id    = m.group(2)
        stock_name  = m.group(3).strip()
        score       = float(m.group(5))
        block       = m.group(6)

        def ext(label, blk=block):
            p = re.search(rf"\| {re.escape(label)} \| \*?\*?(.*?)\*?\*? \|", blk)
            return _clean_cell(p.group(1)) if p else "─"

        def ext_any(labels):
            for label in labels:
                value = ext(label)
                if value != "─":
                    return value
            return "─"

        # 外資月累計（不同月份標籤）
        fi_month = "─"
        for lbl in ["外資4月累計", "外資3月累計", "外資2月累計", "外資累計"]:
            v = ext(lbl)
            if v != "─":
                fi_month = v
                break

        result["stocks"].append({
            "icon":          status_icon,
            "id":            stock_id,
            "name":          stock_name,
            "status":        m.group(4).strip(),
            "score":         normalize_score_value(score, stock_name, m.group(4).strip()),
            "price":         ext("收盤價"),
            "gain_6w":       ext("近6週漲幅"),
            "gain_3d":       ext("近3日漲幅"),
            "rsi":           ext_any(["RSI(14)", "RSI\\(14\\)"]),
            "bband_pct":     ext("布林 %B"),
            "vol_5d":        ext("近5日量"),
            "foreign_month": fi_month,
            "foreign_5d":    ext("外資近5日"),
            "foreign_streak":ext("外資連買天數"),
            "resistance":    ext("近21日壓力"),
            "support":       ext("近21日支撐"),
            "entry":         ext("📌 進場參考"),
            "target":        ext("🎯 目標價"),
            "stop":          ext("🛑 停損價"),
            "score_source":   "原始報告 Score",
        })
    return result


def _parse_format_v3(text: str, result: dict) -> dict:
    """
    Compact 2026 report format:
    #### **#1 3026 禾伸堂** ｜ 收盤：212.5 元 ｜ 狀態：🟢 健康整理 ｜ 評分：13.09
    """
    stock_blocks = re.split(r"\n(?=#### \*\*#\d+ )", text)
    for idx, sblk in enumerate(stock_blocks, 1):
        m_head = re.match(
            r"#### \*\*#(\d+) (\d{4}[\w-]*) (.+?)\*\*.*?收盤：\s*([\d.]+).*?狀態：\s*([🟢🟡🔴])\s*([^｜\n]+).*?評分：\s*([\d.]+)",
            sblk,
            re.DOTALL,
        )
        if not m_head:
            continue
        stock_no = int(m_head.group(1))
        stock_id = m_head.group(2).strip()
        stock_name = m_head.group(3).strip()
        price = m_head.group(4).strip()
        status_icon = m_head.group(5).strip()
        status = m_head.group(6).strip()
        score = float(m_head.group(7))

        def ext(label, blk=sblk):
            p = re.search(rf"\| {re.escape(label)} \| \*?\*?(.*?)\*?\*? \|", blk)
            return _clean_cell(p.group(1)) if p else "─"

        def ext_any(labels):
            for label in labels:
                value = ext(label)
                if value != "─":
                    return value
            return "─"

        trade = re.search(
            r"進場參考\*\*：\s*([\d.]+).*?目標價\*\*：\s*([\d.]+).*?停損\*\*：\s*([\d.]+)",
            sblk,
            re.DOTALL,
        )
        entry, target, stop = trade.groups() if trade else ("─", "─", "─")

        result["stocks"].append({
            "icon": status_icon,
            "id": stock_id,
            "name": stock_name,
            "status": status,
            "score": normalize_score_value(score, stock_name, status) if score > 0 else rank_fallback_score(stock_no),
            "price": ext("收盤價") if ext("收盤價") != "─" else f"{price} 元",
            "gain_6w": ext_any(["近6週均漲幅", "近6週漲幅"]),
            "gain_3d": ext("近3日漲幅"),
            "rsi": ext("RSI(14)"),
            "bband_pct": ext_any(["布林%B", "布林 %B"]),
            "vol_5d": ext("5日成交量"),
            "foreign_month": ext_any(["外資月累淨買", "外資4月淨買", "外資累計"]),
            "foreign_5d": ext_any(["外資近5日淨買", "外資近5日"]),
            "foreign_streak": ext_any(["外資連買", "外資連買天數"]),
            "resistance": ext_any(["壓力（21日高）", "近21日壓力", "壓力"]),
            "support": ext_any(["支撐（21日低）", "近21日支撐", "支撐"]),
            "entry": entry,
            "target": target,
            "stop": stop,
            "score_source": "原始報告 Score",
        })
    return result


def _parse_format_v2(text: str, result: dict) -> dict:
    """
    舊格式（v42~v43）：
    ### 🟢 健康整理（優先布局）
    #### 1. 6213 聯茂
    | 指標 | 數值 | 指標 | 數值 |
    | 壓力 | 支撐 | 📌 進場參考 | 🎯 目標價 | 🛑 停損 |
    """
    icon_label = {"健康整理": "🟢", "強勢追漲": "🟡", "超買": "🔴", "觀察": "🟡", "注意": "🔴"}

    # 把整份文字切成「狀態區塊」—— 依 ### （非####）分割
    # 用 (?<!#)### 確保只切 3 個 # 開頭、不切 ####
    status_blocks = re.split(r"\n(?=### [^#])", text)

    current_icon = "🟢"
    stock_no = 1

    for block in status_blocks:
        # 找出這個 block 對應的狀態
        hdr = re.match(r"### ([🟢🟡🔴]?) ?(.+)", block)
        if hdr:
            icon_raw = hdr.group(1).strip()
            label    = hdr.group(2)
            if icon_raw in ("🟢", "🟡", "🔴"):
                current_icon = icon_raw
            else:
                for kw, ic in icon_label.items():
                    if kw in label:
                        current_icon = ic
                        break

        # 在這個 block 內找所有 #### 個股
        stock_blocks = re.split(r"\n(?=#### \d+\.)", block)
        for sblk in stock_blocks:
            m_head = re.match(r"#### \d+\. (\d{4}[\w-]*) (.+)", sblk)
            if not m_head:
                continue
            stock_id   = m_head.group(1).strip()
            stock_name = m_head.group(2).strip()

            def ext2(label, blk=sblk):
                p = re.search(rf"\| {re.escape(label)} \| \*?\*?(.*?)\*?\*? \|", blk)
                return _clean_cell(p.group(1)) if p else "─"

            # 壓力/支撐/進場/目標/停損同一行（5欄格式）
            ps_row = re.search(
                r"\|\s*([\d.]+)\s*\|\s*([\d.]+)\s*\|\s*\*?\*?([\d.]+)\*?\*?\s*\|\s*\*?\*?([\d.]+)\*?\*?\s*\|\s*\*?\*?([\d.]+)\*?\*?\s*\|",
                sblk
            )
            if ps_row:
                resistance, support = ps_row.group(1), ps_row.group(2)
                entry_p, target, stop = ps_row.group(3), ps_row.group(4), ps_row.group(5)
            else:
                resistance = ext2("壓力")
                support    = ext2("支撐")
                entry_p    = ext2("📌 進場參考")
                target     = ext2("🎯 目標價")
                stop       = ext2("🛑 停損")

            # 欄位別名對應
            gain = next((v for v in [ext2("近6週均漲幅"), ext2("近6週漲幅")] if v != "─"), "─")
            vol  = next((v for v in [ext2("5日成交量"), ext2("近5日量")] if v != "─"), "─")
            bband = next((v for v in [ext2("布林%B"), ext2("布林 %B")] if v != "─"), "─")
            fi_m  = next((v for v in [ext2("外資4月淨買"), ext2("外資3月淨買"), ext2("外資累計"), ext2("外資4月累計")] if v != "─"), "─")
            fi5   = next((v for v in [ext2("外資近5日"), ext2("外資5日")] if v != "─"), "─")

            result["stocks"].append({
                "icon":          current_icon,
                "id":            stock_id,
                "name":          stock_name,
                "status":        {"🟢": "健康整理", "🟡": "強勢追漲", "🔴": "超買"}.get(current_icon, ""),
                "score":         rank_fallback_score(stock_no),
                "price":         ext2("收盤價"),
                "gain_6w":       gain,
                "rsi":           ext2("RSI(14)") if ext2("RSI(14)") != "─" else ext2("RSI\\(14\\)"),
                "bband_pct":     bband,
                "vol_5d":        vol,
                "foreign_month": fi_m,
                "foreign_5d":    fi5,
                "foreign_streak":ext2("外資連買"),
                "resistance":    resistance,
                "support":       support,
                "entry":         entry_p,
                "target":        target,
                "stop":          stop,
                "score_source":   "0-100 rank fallback",
            })
            stock_no += 1

    return result


def parse_report(md_path: Path) -> dict:
    """把 每日選股報告 MD 解析成結構化 dict（自動偵測格式）"""
    text = md_path.read_text(encoding="utf-8")
    result = {
        "date": normalize_date(md_path.stem),
        "market_overview": "",
        "filter_summary": [],
        "stocks": [],
        "notes": "",
        "raw_path": str(md_path)
    }

    # 大盤市況
    mo = re.search(r"## 🌐 大盤市況[^\n]*\n+(.*?)(?=\n---|\n##)", text, re.DOTALL)
    if mo:
        result["market_overview"] = mo.group(1).strip()

    # 篩選條件步驟表（新格式）
    filter_block = re.search(r"\| 步驟 \| 條件 \| 留存數 \|(.*?)\n\n", text, re.DOTALL)
    if filter_block:
        rows = re.findall(r"\| (.+?) \| (.+?) \| (.+?) \|", filter_block.group(1))
        result["filter_summary"] = [{"step": r[0], "condition": r[1], "count": r[2]} for r in rows]

    # 自動偵測格式
    if re.search(r"### \d+\. [🟢🟡🔴]", text):
        _parse_format_v1(text, result)
    elif re.search(r"#### \*\*#\d+ \d{4}", text):
        _parse_format_v3(text, result)
    else:
        _parse_format_v2(text, result)

    # 操作提醒
    notes_m = re.search(r"## ⚠️ 操作提醒\n+(.*?)(?=\n---|\Z)", text, re.DOTALL)
    if notes_m:
        result["notes"] = notes_m.group(1).strip()

    return result


def normalize_date(stem: str) -> str:
    """把 20260424 或 2026-04-24 統一轉成 2026-04-24"""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", stem)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"(\d{4})(\d{2})(\d{2})", stem)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


def find_all_reports() -> list[Path]:
    """找到所有每日選股報告 MD（支援 YYYY-MM-DD 和 YYYYMMDD 兩種格式），依日期降序"""
    patterns = [
        str(REPORTS_DIR / "*.md"),
    ]
    seen_dates = {}  # date_str -> Path，去重複
    for pat in patterns:
        for f in glob.glob(pat):
            p = Path(f)
            if not p.stem.startswith("每日選股報告"):  # 每日選股報告
                continue
            date_str = normalize_date(p.stem)
            if not date_str:
                continue
            # 如果同一天有兩個檔案，優先留 YYYY-MM-DD 格式
            if date_str not in seen_dates or "-" in p.stem:
                seen_dates[date_str] = p

    # 依日期降序排列
    return [v for _, v in sorted(seen_dates.items(), reverse=True)]


def _fetch_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8-sig"))


def load_stock_reference_map() -> dict[str, dict]:
    """Load listed/OTC market and name map from official daily quote APIs."""
    if MARKET_CACHE_PATH.exists():
        try:
            cache = json.loads(MARKET_CACHE_PATH.read_text(encoding="utf-8"))
            updated_at = datetime.fromisoformat(cache.get("updated_at", "1970-01-01T00:00:00"))
            stocks = cache.get("stocks")
            if stocks and datetime.now() - updated_at < timedelta(days=1):
                return stocks
        except Exception:
            pass

    stocks: dict[str, dict] = {}
    errors: list[str] = []
    try:
        for row in _fetch_json(TWSE_STOCK_DAY_ALL_URL):
            code = str(row.get("Code", "")).strip()
            if re.fullmatch(r"\d{4}", code):
                stocks[code] = {"market": "上市", "name": clean_stock_name(row.get("Name", ""))}
    except Exception as exc:
        errors.append(f"TWSE {exc}")

    try:
        for row in _fetch_json(TPEX_DAILY_CLOSE_URL):
            code = str(row.get("SecuritiesCompanyCode", "")).strip()
            if re.fullmatch(r"\d{4}", code):
                stocks[code] = {"market": "上櫃", "name": clean_stock_name(row.get("CompanyName", ""))}
    except Exception as exc:
        errors.append(f"TPEX {exc}")

    if stocks:
        markets = {code: item.get("market", "") for code, item in stocks.items()}
        LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        MARKET_CACHE_PATH.write_text(
            json.dumps(
                {"updated_at": datetime.now().isoformat(timespec="seconds"), "markets": markets, "stocks": stocks},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"   [Market] loaded {len(stocks)} listed/OTC codes", flush=True)
        return stocks

    if MARKET_CACHE_PATH.exists():
        cache = json.loads(MARKET_CACHE_PATH.read_text(encoding="utf-8"))
        print(f"   [Market] using cached market map after fetch failure: {'; '.join(errors)}", flush=True)
        stocks = cache.get("stocks")
        if stocks:
            return stocks
        return {code: {"market": market, "name": ""} for code, market in cache.get("markets", {}).items()}

    print(f"   [Market][WARN] market map unavailable, skip listed/OTC filter: {'; '.join(errors)}", flush=True)
    return {}


def load_stock_market_map() -> dict[str, str]:
    refs = load_stock_reference_map()
    return {code: item.get("market", "") for code, item in refs.items()}


def with_report_date(stocks: list[dict], date_str: str) -> list[dict]:
    out = []
    for stock in stocks or []:
        item = dict(stock)
        if date_str and not item.get("report_date"):
            item["report_date"] = date_str
        out.append(item)
    return out


def attach_report_dates(reports: list[dict]) -> list[dict]:
    out = []
    for report in reports:
        item = dict(report)
        item["stocks"] = with_report_date(report.get("stocks", []), item.get("date", ""))
        out.append(item)
    return out


def filter_listed_otc_reports(reports: list[dict]) -> list[dict]:
    markets = load_stock_market_map()
    if not markets:
        return reports

    filtered_reports = []
    removed: list[str] = []
    for report in reports:
        r = dict(report)
        kept = []
        for stock in report.get("stocks", []):
            sid = str(stock.get("id", "")).strip()
            market = markets.get(sid)
            if market in ALLOWED_MARKETS:
                item = dict(stock)
                if r.get("date", "") and not item.get("report_date"):
                    item["report_date"] = r.get("date", "")
                item["market"] = market
                kept.append(item)
            else:
                removed.append(f"{sid} {stock.get('name', '')}".strip())
        r["stocks"] = kept
        filtered_reports.append(r)

    if removed:
        sample = "、".join(removed[:12])
        suffix = "..." if len(removed) > 12 else ""
        print(f"   [Market] excluded {len(removed)} non-listed/OTC picks: {sample}{suffix}", flush=True)
    return filtered_reports


def load_reports() -> list[dict]:
    md_files = find_all_reports()
    reports = []
    if md_files:
        print(f"\n[Read] Found {len(md_files)} reports...", flush=True)
        for f in md_files:
            try:
                r = parse_report(f)
                reports.append(r)
                print(f"   [OK] {r['date']} - {len(r['stocks'])} stocks", flush=True)
            except Exception as e:
                print(f"   [WARN] {f.name}: {e}", flush=True)
        if reports:
            if REPORTS_CACHE_PATH.exists():
                try:
                    cached = json.loads(REPORTS_CACHE_PATH.read_text(encoding="utf-8"))
                    seen = {r.get("date") for r in reports}
                    reports.extend(r for r in cached if r.get("date") not in seen)
                    reports.sort(key=lambda r: r.get("date", ""), reverse=True)
                except Exception:
                    pass
            reports = normalize_report_scores(attach_report_dates(reports))
            LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
            REPORTS_CACHE_PATH.write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
            return filter_listed_otc_reports(reports)

    if REPORTS_CACHE_PATH.exists():
        print(f"\n[Read] No MD reports found; using cache {REPORTS_CACHE_PATH}", flush=True)
        return filter_listed_otc_reports(normalize_report_scores(attach_report_dates(json.loads(REPORTS_CACHE_PATH.read_text(encoding="utf-8")))))

    return []


def latest_stock_report(reports: list[dict]) -> dict:
    """Return the newest report that actually contains stock picks."""
    return next((r for r in reports if r.get("stocks")), reports[0] if reports else {})


# ──────────────────────────────────────────────
#  HTML 元件
# ──────────────────────────────────────────────

CSS = """
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0d1117;color:#e6edf3;font-family:-apple-system,'Segoe UI','PingFang TC','Microsoft JhengHei',sans-serif;line-height:1.6;min-height:100vh}
a{color:#58a6ff;text-decoration:none}
a:hover{text-decoration:underline}

/* Nav */
nav{background:#0a0a16;border-bottom:1px solid #30363d;padding:6px 12px;display:flex;flex-wrap:wrap;align-items:center;gap:6px;position:sticky;top:0;z-index:999}
.nav-brand{font-weight:800;color:#e6edf3;font-size:14px;margin-right:8px}
nav a.tab{padding:4px 12px;border-radius:16px;font-size:12px;font-weight:600;white-space:nowrap;color:#8b949e;background:#1a1a2e;transition:all .2s}
nav a.tab:hover,nav a.tab.active{background:#1a6bc4;color:#fff;text-decoration:none}

/* Layout */
.container{max-width:1100px;margin:0 auto;padding:24px 16px}
.page-title{font-size:clamp(20px,3vw,28px);font-weight:800;margin-bottom:4px}
.page-sub{color:#6e7681;font-size:13px;margin-bottom:24px}

/* Cards */
.card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:20px;margin-bottom:16px}
.card-title{font-size:14px;font-weight:700;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px}
.grid{display:grid;gap:16px}
.grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}
.grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}
.grid-4{grid-template-columns:repeat(4,minmax(0,1fr))}
.metric{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px}
.metric-num{font-size:26px;font-weight:800;color:#e6edf3}
.metric-label{font-size:12px;color:#6e7681;margin-top:2px}
.action-list{display:grid;gap:10px;margin-top:12px}
.action-row{display:grid;grid-template-columns:1.2fr repeat(5,minmax(76px,1fr));gap:10px;align-items:center;background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:10px}
.action-row .label{font-size:11px;color:#6e7681}
.action-row .value{font-size:13px;color:#e6edf3;font-weight:800;margin-top:2px}
.action-row .note{font-size:12px;color:#8b949e;line-height:1.5}
.daily-decision-row{grid-template-columns:1.65fr 96px 112px 1fr}
.daily-decision-row .decision-reason{font-size:12px;color:#c9d1d9;line-height:1.55}
.market-light{display:grid;grid-template-columns:180px 1fr;gap:14px;align-items:stretch}
.market-badge{display:flex;align-items:center;justify-content:center;border-radius:10px;border:1px solid #30363d;background:#0d1117;font-size:28px;font-weight:900}
.market-badge.pos{border-color:rgba(248,81,73,.45);background:rgba(248,81,73,.09)}
.market-badge.neu{border-color:rgba(210,153,34,.45);background:rgba(210,153,34,.09);color:#d2a520}
.market-badge.neg{border-color:rgba(63,185,80,.45);background:rgba(63,185,80,.09)}
.check-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
.check-item{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:9px 10px}
.check-item .k{font-size:11px;color:#6e7681}
.check-item .v{font-size:13px;color:#e6edf3;font-weight:800;margin-top:3px;line-height:1.45}
.alert-row{display:grid;grid-template-columns:1fr 92px 92px 1.7fr;gap:10px;align-items:center;background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:10px;margin-top:8px}
.alert-level{font-size:12px;font-weight:800;border-radius:999px;padding:4px 8px;text-align:center;background:#161b22;color:#8b949e;border:1px solid #30363d}
.alert-level.watch{color:#d2a520;border-color:rgba(210,153,34,.45);background:rgba(210,153,34,.09)}
.alert-level.exit{color:#f85149;border-color:rgba(248,81,73,.45);background:rgba(248,81,73,.09)}
.chip-line{font-size:12px;color:#8b949e;margin-top:6px;line-height:1.6}
.rr-good{color:#3fb950!important}
.rr-mid{color:#d2a520!important}
.rr-bad{color:#f85149!important}
.basket-card{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px;margin-bottom:10px}
.basket-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:8px}
.basket-code{font-size:17px;font-weight:800;color:#e6edf3}
.basket-name{font-size:12px;color:#8b949e;margin-top:2px}
.basket-title-link{display:inline-flex;align-items:baseline;gap:6px;color:#e6edf3;text-decoration:none;border-bottom:1px solid transparent}
.basket-title-link:hover{color:#58a6ff;border-bottom-color:#58a6ff}
.basket-title-link:hover .basket-name{color:#58a6ff}
.basket-action{font-size:12px;font-weight:700;padding:3px 8px;border-radius:999px;background:#1a1a2e;color:#58a6ff;white-space:nowrap}
.basket-price-row{display:flex;align-items:flex-end;gap:10px;flex-wrap:wrap;margin:8px 0 6px}
.basket-price{font-size:24px;font-weight:900;color:#e6edf3;line-height:1}
.basket-change{font-size:13px;font-weight:800}
.tag-row{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
.tag{font-size:11px;color:#8b949e;background:#161b22;border:1px solid #30363d;border-radius:999px;padding:3px 7px}
.tag-green{color:#3fb950;border-color:rgba(63,185,80,.35);background:rgba(63,185,80,.08)}
.tag-blue{color:#58a6ff;border-color:rgba(88,166,255,.35);background:rgba(88,166,255,.08)}
.tag-yellow{color:#d2a520;border-color:rgba(210,153,34,.35);background:rgba(210,153,34,.08)}
.tag-red{color:#f85149;border-color:rgba(248,81,73,.35);background:rgba(248,81,73,.08)}
.strategy-note{font-size:13px;color:#c9d1d9;line-height:1.75}
.signal-foot{font-size:12px;color:#8b949e;margin-top:8px;border-top:1px solid #21262d;padding-top:8px}
.signal-foot strong{color:#e6edf3}
.signal-table td{vertical-align:top}
.signal-table tr.clickable-row{cursor:pointer}
.signal-table tr.clickable-row:hover .stock-link{color:#58a6ff;text-decoration:none}
.signal-dates{font-size:12px;color:#8b949e;line-height:1.7}
.push-ok{color:#3fb950;font-weight:700}
.push-wait{color:#d2a520;font-weight:700}
.push-miss{color:#f85149;font-weight:700}
.stock-link{color:#e6edf3;font-weight:800}
.stock-link:hover{color:#58a6ff;text-decoration:none}
.detail-hero{display:grid;grid-template-columns:1.2fr .8fr;gap:16px}
.info-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.info-cell{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px}
.info-cell .k{font-size:11px;color:#6e7681}
.info-cell .v{font-size:16px;color:#e6edf3;font-weight:800;margin-top:2px}
.telegram-report-card{display:grid;gap:14px}
.telegram-head{display:flex;justify-content:space-between;gap:14px;align-items:flex-start;border-bottom:1px solid #30363d;padding-bottom:12px}
.telegram-title{font-size:18px;font-weight:800;color:#e6edf3}
.telegram-meta{font-size:12px;color:#8b949e;margin-top:4px;line-height:1.55}
.telegram-rating{font-size:18px;font-weight:900;text-align:right;white-space:nowrap}
.telegram-phase{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:12px}
.telegram-phase h3{font-size:14px;color:#e6edf3;margin:0 0 8px}
.telegram-line{display:grid;grid-template-columns:104px 1fr;gap:10px;font-size:13px;line-height:1.6;border-top:1px solid rgba(48,54,61,.55);padding-top:7px;margin-top:7px}
.telegram-line:first-of-type{border-top:0;padding-top:0;margin-top:0}
.telegram-line .k{color:#8b949e}
.telegram-line .v{color:#c9d1d9}
.telegram-price-line{display:flex;align-items:flex-end;gap:12px;flex-wrap:wrap;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px 12px;margin-bottom:8px}
.telegram-price-line .k{font-size:11px;color:#8b949e}
.telegram-price-line .price{font-size:28px;font-weight:900;color:#e6edf3;line-height:1}
.telegram-price-line .change{font-size:13px;font-weight:800}
.telegram-note{font-size:12px;line-height:1.65;color:#8b949e;background:#161b22;border-left:3px solid #58a6ff;padding:8px 10px;border-radius:6px}
.diagnosis-head{display:grid;grid-template-columns:1.3fr .9fr;gap:14px;margin-bottom:14px}
.diagnosis-verdict{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:14px}
.diagnosis-verdict .label{font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:1px}
.diagnosis-verdict .main{font-size:20px;font-weight:900;color:#e6edf3;margin-top:4px;line-height:1.35}
.diagnosis-verdict .sub{font-size:13px;color:#c9d1d9;margin-top:8px;line-height:1.65}
.diagnosis-score{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}
.diagnosis-score .box{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px}
.diagnosis-score .num{font-size:20px;font-weight:900;color:#e6edf3}
.diagnosis-score .k{font-size:11px;color:#8b949e;margin-top:2px}
.diagnosis-list{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}
.diagnosis-list .panel{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:12px}
.diagnosis-list h3{font-size:13px;color:#e6edf3;margin:0 0 8px}
.diagnosis-list ul{margin:0;padding-left:18px;color:#c9d1d9;font-size:13px;line-height:1.75}
.diagnosis-prompt{white-space:pre-wrap;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:12px;color:#c9d1d9;font-size:13px;line-height:1.7;margin-top:12px}
.chart-box{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:12px;margin-top:10px}
.chart-tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.chart-tabs button{background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:8px;padding:6px 10px;cursor:pointer}
.chart-tabs button.active{background:#1a6bc4;color:#fff;border-color:#1a6bc4}
.hover-chart,.indicator-hover{position:relative}
.chart-crosshair{position:absolute;top:0;bottom:0;width:1px;background:rgba(88,166,255,.85);display:none;pointer-events:none}
.chart-tooltip{position:absolute;z-index:5;display:none;min-width:190px;max-width:240px;background:rgba(13,17,23,.96);border:1px solid #30363d;border-radius:8px;padding:9px 10px;color:#c9d1d9;font-size:12px;line-height:1.55;box-shadow:0 10px 28px rgba(0,0,0,.35);pointer-events:none}
.chart-tooltip .t-date{color:#e6edf3;font-weight:800;margin-bottom:4px}
.chart-tooltip .t-grid{display:grid;grid-template-columns:1fr 1fr;gap:2px 10px}
.chart-tooltip .t-ma{margin-top:5px;padding-top:5px;border-top:1px solid #30363d;color:#8b949e}
.chart-stack{display:grid;grid-template-columns:1fr;gap:12px;margin-top:12px}
.tv-chart-grid{display:grid;grid-template-columns:1fr;gap:10px;margin-top:12px}
.tv-chart-panel{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:10px;position:relative}
.tv-chart-title{font-size:12px;font-weight:800;color:#e6edf3;margin:0 0 6px}
.tv-chart{height:150px;min-height:150px;position:relative}
.tv-chart.main{height:360px;min-height:360px}
.tv-chart-note{font-size:11px;color:#6e7681;margin-top:8px;line-height:1.5}
.tv-tooltip{position:absolute;z-index:8;display:none;top:34px;left:14px;background:rgba(13,17,23,.96);border:1px solid #30363d;border-radius:8px;padding:8px 10px;color:#c9d1d9;font-size:12px;line-height:1.55;pointer-events:none;box-shadow:0 10px 28px rgba(0,0,0,.35)}
.tv-draw-toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:-2px 0 8px}
.tv-draw-btn{border:1px solid #30363d;background:#161b22;color:#8b949e;border-radius:8px;padding:5px 10px;font-size:12px;font-weight:800;cursor:pointer}
.tv-draw-btn:hover,.tv-draw-btn.active{border-color:#58a6ff;color:#58a6ff;background:#0d2142}
.tv-draw-layer{position:absolute;inset:0;z-index:7;pointer-events:none}
.tv-draw-layer.active{pointer-events:auto;cursor:crosshair}
.tv-draw-layer line{vector-effect:non-scaling-stroke}
.tv-draw-layer .draft{stroke-dasharray:5 4;opacity:.9}
.tv-chip-grid{display:grid;grid-template-columns:1fr;gap:12px;margin-top:12px}
.tv-chip-chart{height:280px;min-height:280px}
.tv-chip-chart.compact{height:240px;min-height:240px}
.holding-stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-bottom:8px}
.holding-info-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:10px}
.tech-panel{display:grid;grid-template-columns:280px 1fr;gap:14px;align-items:start}
.tech-summary-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:10px}
.indicator-stack{display:grid;grid-template-columns:1fr;gap:8px;margin-top:10px}
.chip-indicator-stack{display:grid;grid-template-columns:1fr;gap:8px;margin-top:12px}
.indicator-box{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:8px}
.mini-report{white-space:pre-wrap;font-size:13px;color:#c9d1d9;line-height:1.75;background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px;max-height:360px;overflow:auto}
.pill-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.searchbar{width:100%;background:#0d1117;border:1px solid #30363d;border-radius:8px;color:#e6edf3;padding:10px 12px;font-size:14px;margin:10px 0 14px}
.ma-strip{display:flex;flex-direction:column;gap:8px;margin-top:10px;max-width:280px}
.ma-pill{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:9px 10px;display:flex;align-items:center;justify-content:space-between}
.ma-name{font-size:12px;color:#8b949e}
.ma-value{font-size:15px;font-weight:800;color:#e6edf3}
.arrow-up{color:#f85149;font-weight:900}
.arrow-down{color:#3fb950;font-weight:900}
.arrow-flat{color:#8b949e;font-weight:900}

/* Market overview */
.market-text{font-size:15px;color:#c9d1d9;line-height:1.85}
.market-env{border-color:rgba(88,166,255,.25)}
.market-env-head{display:grid;grid-template-columns:150px 1fr;gap:14px;align-items:stretch}
.market-env-score{display:flex;flex-direction:column;align-items:center;justify-content:center;border:1px solid #30363d;border-radius:8px;background:#0d1117;padding:12px;min-height:130px}
.market-env-score .num{font-size:42px;line-height:1;font-weight:900;color:#e6edf3}
.market-env-score .label{font-size:13px;font-weight:800;margin-top:8px}
.market-env-score.green{border-color:rgba(63,185,80,.55);background:rgba(63,185,80,.08)}
.market-env-score.yellow{border-color:rgba(210,153,34,.55);background:rgba(210,153,34,.08)}
.market-env-score.red{border-color:rgba(248,81,73,.55);background:rgba(248,81,73,.08)}
.market-env-score.green .label{color:#3fb950}.market-env-score.yellow .label{color:#d2a520}.market-env-score.red .label{color:#f85149}
.market-env-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:10px}
.market-env-item{background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:9px 10px;min-height:78px}
.market-env-item .k{font-size:11px;color:#8b949e;font-weight:800;display:flex;align-items:center;gap:6px}
.market-env-item .v{font-size:14px;color:#e6edf3;font-weight:900;margin-top:4px}
.market-env-item .d{font-size:11px;color:#6e7681;line-height:1.45;margin-top:3px}
.env-dot{width:9px;height:9px;border-radius:50%;display:inline-block;background:#6e7681;box-shadow:0 0 0 3px rgba(110,118,129,.12)}
.env-dot.green{background:#3fb950;box-shadow:0 0 0 3px rgba(63,185,80,.16)}
.env-dot.yellow{background:#d2a520;box-shadow:0 0 0 3px rgba(210,153,34,.16)}
.env-dot.red{background:#f85149;box-shadow:0 0 0 3px rgba(248,81,73,.16)}
.market-env-updated{font-size:12px;color:#8b949e;margin-top:8px}
.market-env-summary{font-size:13px;color:#c9d1d9;line-height:1.65}
.flow-market-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:12px}
.flow-market-card{border:1px solid #30363d;border-radius:8px;background:#0d1117;padding:12px}
.flow-metric-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin:10px 0 12px}
.flow-metric-grid>div{background:#161b22;border-radius:7px;padding:8px}.flow-net{font-size:16px;font-weight:900;color:#e6edf3;margin-top:4px}
.flow-top-title{font-size:11px;color:#8b949e;font-weight:800;margin:8px 0 3px}.flow-top-row{display:flex;justify-content:space-between;gap:8px;border-top:1px solid #21262d;padding:4px 0;font-size:12px}.flow-top-row span{font-variant-numeric:tabular-nums;color:#c9d1d9}
.flow-page-link{display:inline-flex;align-items:center;justify-content:center;margin-top:10px;border:1px solid #58a6ff;border-radius:7px;padding:7px 11px;color:#58a6ff;font-size:12px;font-weight:900;text-decoration:none}.flow-page-link:hover{background:rgba(88,166,255,.12);text-decoration:none}
.flow-page-toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin:12px 0}.flow-page-toolbar input{min-width:260px;background:#0d1117;border:1px solid #30363d;border-radius:7px;color:#e6edf3;padding:8px 10px}.flow-page-count{font-size:12px;color:#8b949e;font-weight:800}
.complete-table-note{border-left:3px solid #58a6ff;background:rgba(88,166,255,.08);border-radius:6px;padding:9px 11px;color:#c9d1d9;font-size:12px;line-height:1.65;margin:10px 0}
.flow-anchor-nav{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0 16px}.flow-anchor-nav a{border:1px solid #30363d;background:#161b22;border-radius:999px;padding:7px 11px;color:#c9d1d9;font-size:12px;font-weight:800;text-decoration:none}.flow-anchor-nav a:hover{border-color:#58a6ff;color:#58a6ff;text-decoration:none}.ranking-section{scroll-margin-top:64px}.ranking-section+.ranking-section{margin-top:16px}
.flow-overview-card{padding:16px}.flow-overview-card .flow-market-card{padding:10px 12px}.flow-overview-card .flow-metric-grid{margin-bottom:4px}
.ranking-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;align-items:start}.ranking-grid .ranking-section{margin:0;min-width:0;padding:0;overflow:hidden;border-radius:4px;border-color:#64748b}.ranking-grid .ranking-section+.ranking-section{margin-top:0}
.ranking-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:8px 10px;border-bottom:1px solid #64748b;background:#161b22}.ranking-head .section-label{margin:0;font-size:13px}.ranking-meta{font-size:11px;font-weight:900;color:#e6edf3;white-space:nowrap}.ranking-meta span{color:#8b949e;font-weight:700}
.flow-table-wrap{overflow:auto;max-height:720px;background:#fff}.stock-table.flow-ranking-table{font-size:11px;line-height:1.2;table-layout:fixed;border-collapse:separate;border-spacing:0;color:#111827;background:#fff}.stock-table.flow-ranking-table th,.stock-table.flow-ranking-table td{padding:3px 5px;border-right:1px solid #94a3b8;border-bottom:1px solid #94a3b8}.stock-table.flow-ranking-table th{position:sticky;top:0;z-index:2;padding-top:4px;padding-bottom:4px;background:#e2e8f0;color:#111827;font-size:10px;font-weight:900}.stock-table.flow-ranking-table th:last-child,.stock-table.flow-ranking-table td:last-child{border-right:0}.stock-table.flow-ranking-table tr:last-child td{border-bottom:0}.stock-table.flow-ranking-table tr:nth-child(even) td:not(.pos):not(.neg){background:#f8fafc}.stock-table.flow-ranking-table tr:hover td{background:#fff4cc}.stock-table.flow-ranking-table .pos{color:#b42318;background:#ffe0e5;font-weight:900}.stock-table.flow-ranking-table .neg{color:#067647;background:#dcfae6;font-weight:900}.stock-table.flow-ranking-table th:nth-child(1),.stock-table.flow-ranking-table td:nth-child(1){width:36px;text-align:right;color:#475569}.stock-table.flow-ranking-table th:nth-child(3),.stock-table.flow-ranking-table td:nth-child(3){width:48px;text-align:center}.stock-table.flow-ranking-table th:nth-child(4),.stock-table.flow-ranking-table td:nth-child(4){width:90px;text-align:right;font-variant-numeric:tabular-nums}.stock-table.flow-ranking-table td:nth-child(2){font-weight:800}.stock-table.flow-ranking-table td:nth-child(2) a{color:#0f3f8c;white-space:nowrap;text-decoration:none}.stock-table.flow-ranking-table td:nth-child(2) a:hover{text-decoration:underline}
.holder-history-card{padding:0;overflow:hidden;border-radius:4px}.holder-history-wrap{overflow:auto;max-height:760px;background:#fff}.stock-table.holder-history-table{min-width:960px;font-size:10px;line-height:1.2;border-collapse:separate;border-spacing:0;color:#111827;background:#fff}.stock-table.holder-history-table th,.stock-table.holder-history-table td{padding:3px 5px;border-right:1px solid #94a3b8;border-bottom:1px solid #94a3b8;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}.stock-table.holder-history-table th{position:sticky;top:0;z-index:2;background:#e2e8f0;color:#111827;font-weight:900}.stock-table.holder-history-table th:last-child,.stock-table.holder-history-table td:last-child{border-right:0}.stock-table.holder-history-table tr:last-child td{border-bottom:0}.stock-table.holder-history-table tr:nth-child(even) td:not(.holder-change):not(.holder-total){background:#f8fafc}.stock-table.holder-history-table tr:hover td{outline:1px solid #f59e0b;outline-offset:-1px}.stock-table.holder-history-table th:nth-child(1),.stock-table.holder-history-table td:nth-child(1){width:38px;color:#475569}.stock-table.holder-history-table th:nth-child(2),.stock-table.holder-history-table td:nth-child(2){min-width:150px;text-align:left;font-weight:800}.stock-table.holder-history-table th:nth-child(3),.stock-table.holder-history-table td:nth-child(3){width:48px;text-align:center}.stock-table.holder-history-table td:nth-child(2) a{color:#0f3f8c;text-decoration:none}.stock-table.holder-history-table td:nth-child(2) a:hover{text-decoration:underline}.holder-change.pos{color:#b42318;background:#ffe0e5;font-weight:900}.holder-change.neg{color:#067647;background:#dcfae6;font-weight:900}.holder-change.zero,.holder-change.missing{color:#64748b;background:#fff}.holder-total{color:#7c5800;background:#fff0a6;font-weight:900}.holder-positive-count{font-weight:900;color:#0f3f8c}

/* Backtest dashboard */
.backtest-layout{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(300px,.65fr);gap:14px;align-items:start}
.backtest-toolbar{display:flex;gap:10px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin:10px 0 12px}
.backtest-toolbar select{background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#e6edf3;padding:8px 10px;min-width:260px}
.backtest-table-wrap{overflow:auto;max-height:520px;margin-top:12px}
.backtest-chart-box{min-height:330px}
.backtest-chart-box canvas{width:100%;min-height:300px}
.backtest-heatmap{display:grid;grid-template-columns:repeat(12,minmax(38px,1fr));gap:6px;margin-top:10px}
.heat-cell{border:1px solid #30363d;border-radius:6px;background:#161b22;min-height:42px;padding:6px;font-size:11px;color:#8b949e}
.heat-cell .m{font-weight:800;color:#e6edf3}
.heat-cell .r{font-size:12px;font-weight:900;margin-top:3px}
.heat-pos{border-color:rgba(248,81,73,.32);background:rgba(248,81,73,.08)}
.heat-neg{border-color:rgba(63,185,80,.32);background:rgba(63,185,80,.08)}
.heat-flat{border-color:rgba(139,148,158,.28)}
.strategy-params{white-space:pre-wrap;font-size:12px;color:#c9d1d9;line-height:1.6;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px;max-height:280px;overflow:auto}
.stock-table th[data-sort-key]{cursor:pointer}
.stock-table th[data-sort-key]::after{content:" ⇅";font-size:10px;color:#6e7681}
.stock-table th[data-sort-key].sort-asc::after{content:" ↑";color:#58a6ff}
.stock-table th[data-sort-key].sort-desc::after{content:" ↓";color:#58a6ff}

/* Filter steps */
.filter-steps{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:0}
.filter-step{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:10px 14px;flex:1;min-width:140px}
.filter-step .step-count{font-size:22px;font-weight:800;color:#58a6ff}
.filter-step .step-label{font-size:11px;color:#6e7681;margin-top:2px}
.filter-step .step-cond{font-size:11px;color:#8b949e;margin-top:4px}

/* Stock table */
.stock-table{width:100%;border-collapse:collapse;font-size:13px}
.stock-table th{padding:8px 10px;text-align:left;color:#6e7681;font-weight:600;font-size:11px;border-bottom:1px solid #30363d;white-space:nowrap}
.stock-table td{padding:10px 10px;border-bottom:1px solid #1c2128;vertical-align:middle}
.stock-table tr:hover td{background:#1c2128}
.stock-table tr:last-child td{border-bottom:none}
.daily-top20-card .stock-table{font-size:15px}
.daily-top20-card .stock-table th{font-size:12px;padding:10px 12px}
.daily-top20-card .stock-table td{padding:12px 12px}
.daily-top20-card .stock-link{font-size:17px;font-weight:900}
.score-note-card{border-color:rgba(88,166,255,.32)}
.score-note-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:12px}
.score-note{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:12px}
.score-note .k{font-size:12px;color:#8b949e;font-weight:700;margin-bottom:5px}
.score-note .v{font-size:15px;color:#e6edf3;font-weight:800;line-height:1.5}
.score-note .desc{font-size:12px;color:#8b949e;line-height:1.65;margin-top:6px}
.score-rule{margin-top:10px;font-size:13px;line-height:1.75;color:#c9d1d9}
.carybot-cell{min-width:150px}
.carybot-line{font-size:12px;color:#8b949e;line-height:1.55;white-space:nowrap}
.carybot-line strong{color:#e6edf3}
.carybot-missing{font-size:12px;color:#6e7681}
.sfz-control-bar{position:sticky;top:46px;z-index:22;display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:10px;background:rgba(13,17,23,.96);border:1px solid #30363d;border-radius:8px;padding:10px;margin:12px 0;backdrop-filter:blur(8px)}
.sfz-control{display:flex;flex-direction:column;gap:4px}
.sfz-control label{font-size:11px;color:#8b949e;font-weight:800}
.sfz-control select,.sfz-control input{background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#e6edf3;padding:7px 9px;font-size:13px}
.sfz-actions{display:flex;gap:8px;align-items:end;flex-wrap:wrap}
.sfz-actions button{background:#21262d;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:7px 10px;cursor:pointer;font-weight:800}
.sfz-actions button:hover{border-color:#58a6ff;color:#58a6ff}
.sfz-count-line{display:flex;gap:10px;align-items:center;justify-content:space-between;flex-wrap:wrap;color:#8b949e;font-size:12px;margin:8px 0 10px}
.sfz-pager{display:flex;justify-content:flex-end;align-items:center;gap:8px;margin-top:10px;color:#8b949e;font-size:12px}
.sfz-pager button{background:#21262d;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:6px 10px;cursor:pointer}
.sfz-pager button:disabled{opacity:.45;cursor:not-allowed}
.sfz-bull-note{display:none;color:#d2a520;font-weight:800}

/* Status badges */
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700}
.badge-green{background:rgba(63,185,80,0.15);color:#3fb950;border:1px solid rgba(63,185,80,0.3)}
.badge-yellow{background:rgba(210,153,34,0.15);color:#d2a520;border:1px solid rgba(210,153,34,0.3)}
.badge-red{background:rgba(248,81,73,0.15);color:#f85149;border:1px solid rgba(248,81,73,0.3)}

/* Score bar */
.score-bar{display:flex;align-items:center;gap:6px}
.score-num{font-weight:700;color:#e6edf3;min-width:50px}

/* Price info */
.price-main{font-weight:700;font-size:14px;color:#e6edf3}
.price-change{font-size:12px}
.pos{color:#f85149}
.neg{color:#3fb950}

/* Entry/Target/Stop */
.price-row{display:flex;flex-direction:column;gap:2px}
.price-entry{color:#58a6ff;font-size:12px}
.price-target{color:#f85149;font-size:12px}
.price-stop{color:#3fb950;font-size:12px}
.price-support{color:#8b949e;font-size:12px}
.price-rr{font-size:12px;font-weight:800}
.m-score{font-size:24px;font-weight:900;color:#e6edf3}
.m-checks{display:flex;flex-wrap:wrap;gap:6px}
.m-check{display:inline-flex;align-items:center;border:1px solid #30363d;border-radius:999px;padding:2px 8px;font-size:11px;color:#c9d1d9;background:#0d1117}
.m-check.ok{border-color:rgba(63,185,80,.45);color:#3fb950}
.m-check.warn{border-color:rgba(210,153,34,.45);color:#d2a520}
.m-check.bad{border-color:rgba(248,81,73,.45);color:#f85149}

/* Notes */
.notes-list{list-style:none;padding:0}
.notes-list li{padding:6px 0;border-bottom:1px solid #1c2128;font-size:13px;color:#c9d1d9;line-height:1.7}
.notes-list li:last-child{border-bottom:none}
.notes-list li::before{content:"⚠ ";color:#d2a520}

/* History list */
.history-list{list-style:none}
.history-item{display:flex;align-items:center;justify-content:space-between;padding:12px 0;border-bottom:1px solid #1c2128}
.history-item:last-child{border-bottom:none}
.history-date{font-weight:700;color:#e6edf3}
.history-meta{font-size:12px;color:#6e7681}
.history-link{font-size:12px;padding:4px 12px;border-radius:8px;background:#1a1a2e;color:#58a6ff;font-weight:600}
.history-link:hover{background:#1a6bc4;color:#fff;text-decoration:none}

/* Section label */
.section-label{font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#58a6ff;margin-bottom:6px}
.section-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:8px}
.section-date{font-size:12px;color:#8b949e;background:#0d1117;border:1px solid #30363d;border-radius:999px;padding:3px 9px;white-space:nowrap}
.subsection-title{font-size:15px;margin:16px 0 0;color:#e6edf3}
.pending-box{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px;margin-top:12px}
.placeholder-block{background:#0d1117;border:1px solid #30363d;border-radius:8px;margin-top:12px;color:#c9d1d9;overflow:hidden}
.placeholder-block[open]{border-color:#3b4958;background:#111720}.placeholder-block.ready{border-color:rgba(63,185,80,.45)}
.placeholder-block summary{list-style:none;cursor:pointer;display:flex;align-items:center;justify-content:space-between;gap:10px;padding:12px 14px;color:#e6edf3;font-weight:800}.placeholder-block summary::-webkit-details-marker{display:none}
.placeholder-body{padding:0 14px 14px}.coming-soon-badge{display:inline-flex;align-items:center;border:1px solid #30363d;background:#21262d;color:#8b949e;border-radius:999px;padding:2px 8px;font-size:11px;font-weight:800;white-space:nowrap}.placeholder-block.ready .coming-soon-badge{border-color:rgba(63,185,80,.45);background:rgba(63,185,80,.10);color:#7ee787}.inline-placeholder{margin:0;background:transparent;border-color:#30363d}.inline-placeholder summary{padding:6px 0;font-size:12px}
.rr-warning{color:#f85149;font-size:12px;margin-left:6px;font-weight:800;white-space:nowrap}
.warning-banner,.warning-bar{border:1px solid rgba(248,81,73,.55);background:rgba(248,81,73,.10);color:#ffdcd7;border-left:4px solid #d32f2f;border-radius:8px;padding:10px 12px;margin:0 0 12px;font-size:13px;font-weight:800}
.traffic-light,.signal-light{border:1px solid #30363d;border-left:4px solid #30363d;border-radius:8px;padding:12px;margin-bottom:12px;color:#1f2328}.traffic-light .signal,.signal-light .signal{display:flex;align-items:center;gap:8px;font-size:20px;font-weight:900}.traffic-light .signal-label,.signal-light .signal-label{font-size:11px;font-weight:900;letter-spacing:1px;color:rgba(31,35,40,.72);text-transform:uppercase}.traffic-light .reason,.signal-light .reason{font-size:13px;line-height:1.6;margin-top:6px}.traffic-light.go,.signal-light.light-green{background:#ffebee;border-color:#c62828}.traffic-light.watch,.signal-light.light-yellow{background:#fff8e1;border-color:#f57c00}.traffic-light.nogo,.signal-light.light-red{background:#e8f5e9;border-color:#2e7d32}
.ledger-controls,.radar-filter-bar,.radar-filter{position:sticky;top:0;z-index:20;display:flex;gap:8px;flex-wrap:wrap;align-items:center;background:rgba(13,17,23,.96);border:1px solid #30363d;border-radius:8px;padding:10px;margin-bottom:12px;backdrop-filter:blur(8px)}.ledger-controls input,.ledger-controls select,.radar-filter-bar input,.radar-filter-bar select,.radar-filter input,.radar-filter select{background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#e6edf3;padding:7px 9px;font-size:13px}.radar-filter-bar fieldset,.radar-filter fieldset{border:0;display:flex;gap:8px;flex-wrap:wrap;margin:0;padding:0}.radar-filter-bar legend,.radar-filter legend{font-size:11px;font-weight:800;color:#8b949e;margin-right:2px}.filter-chip{display:inline-flex;align-items:center;gap:6px;border:1px solid #30363d;border-radius:999px;padding:6px 9px;color:#c9d1d9;font-size:12px;font-weight:700;cursor:pointer}.filter-chip input{accent-color:#58a6ff}.filter-count{margin-left:auto;color:#c9d1d9;font-size:12px;font-weight:800}.filter-reset,.pager button,.page-num{background:#21262d;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:6px 10px;cursor:pointer}.pager{display:flex;justify-content:flex-end;align-items:center;gap:8px;margin-top:10px;color:#8b949e;font-size:12px;flex-wrap:wrap}.pager button:disabled{opacity:.45;cursor:not-allowed}.page-num.active{background:#1a6bc4;border-color:#1a6bc4}.stock-table th[data-ledger-sort]{cursor:pointer;white-space:nowrap}.stock-table th[data-ledger-sort]::after{content:" ⇅";color:#6e7681;font-size:10px}.stock-table th.sort-asc::after{content:" ↑";color:#58a6ff}.stock-table th.sort-desc::after{content:" ↓";color:#58a6ff}
.heat-strip{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:12px}.heat-pill{border:1px solid #30363d;background:#0d1117;border-radius:8px;padding:9px 10px}.heat-pill .k{font-size:11px;color:#8b949e}.heat-pill .v{font-size:14px;color:#e6edf3;font-weight:900;margin-top:2px}
.disclaimer-modal{position:fixed;inset:0;display:none;align-items:center;justify-content:center;background:rgba(1,4,9,.72);z-index:2000;padding:18px}.disclaimer-modal.show{display:flex}.disclaimer-box{max-width:440px;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:18px;box-shadow:0 18px 60px rgba(0,0,0,.45)}.disclaimer-box h2{margin:0 0 8px;font-size:18px}.disclaimer-box p{margin:0 0 14px;color:#c9d1d9;line-height:1.7;font-size:13px}.disclaimer-box button{background:#1a6bc4;border:0;color:white;border-radius:6px;padding:9px 12px;font-weight:800;cursor:pointer}

/* Tabs */
.tab-bar{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:18px;border-bottom:2px solid #21262d;padding-bottom:0}
.tab-btn{background:none;border:none;border-bottom:2px solid transparent;margin-bottom:-2px;padding:8px 16px;font-size:13px;font-weight:700;color:#8b949e;cursor:pointer;white-space:nowrap;transition:color .2s,border-color .2s}
.tab-btn:hover{color:#c9d1d9}
.tab-btn.active{color:#58a6ff;border-bottom-color:#58a6ff}
.tab-panel{display:none}
.tab-panel.active{display:block}

/* Footer */
footer{text-align:center;padding:24px 20px;color:#484f58;font-size:12px;border-top:1px solid #30363d;margin-top:48px}
footer .disclaimer{color:#e74c3c;margin-top:6px;font-size:11px}

/* Responsive */
@media(max-width:768px){
  .stock-table{font-size:12px}
  .stock-table.responsive-card{min-width:0}  .stock-table.responsive-card thead{display:none}
  .stock-table.responsive-card,.stock-table.responsive-card tbody,.stock-table.responsive-card tr,.stock-table.responsive-card td{display:block;width:100%}
  .stock-table.responsive-card tr{border:1px solid #30363d;border-radius:8px;margin-bottom:10px;background:#0d1117;padding:8px}
  .stock-table.responsive-card td{border:0;border-bottom:1px solid rgba(48,54,61,.55);display:grid;grid-template-columns:92px 1fr;gap:10px;padding:8px 4px}.stock-table.responsive-card td:last-child{border-bottom:0}.stock-table.responsive-card td::before{content:attr(data-label);color:#8b949e;font-size:11px;font-weight:800}.ledger-controls,.radar-filter-bar{align-items:stretch}.filter-count{margin-left:0}.heat-strip{grid-template-columns:repeat(2,minmax(0,1fr))}
  .backtest-layout{grid-template-columns:1fr}.backtest-heatmap{grid-template-columns:repeat(3,minmax(0,1fr))}.backtest-toolbar select{min-width:0;width:100%}.flow-market-grid{grid-template-columns:1fr}.flow-metric-grid{grid-template-columns:1fr}.flow-page-toolbar input{min-width:0;width:100%}.ranking-grid{grid-template-columns:1fr}.ranking-head{padding:8px 10px}.flow-table-wrap{max-height:640px}.flow-ranking-table{font-size:11px}.flow-ranking-table th,.flow-ranking-table td{padding:4px 5px}.flow-ranking-table th:nth-child(3),.flow-ranking-table td:nth-child(3){display:none}.flow-ranking-table th:nth-child(4),.flow-ranking-table td:nth-child(4){width:92px}
  .daily-top20-card .stock-table{font-size:13px}
  .daily-top20-card .stock-link{font-size:15px}
  .score-note-grid{grid-template-columns:1fr}
  .stock-table .hide-mobile{display:none}
  .sfz-control-bar{position:static;grid-template-columns:1fr}
  .sfz-count-line{display:grid}
  .filter-steps{flex-direction:column}
  .grid-2,.grid-3,.grid-4{grid-template-columns:1fr}
  .ma-strip{grid-template-columns:repeat(2,minmax(0,1fr))}
  .detail-hero,.info-grid,.tech-panel,.tech-summary-grid,.telegram-head,.telegram-line,.action-row,.market-light,.market-env-head,.market-env-grid,.check-grid,.alert-row,.diagnosis-head,.diagnosis-list{grid-template-columns:1fr}
  .section-head{display:grid}
  .section-date{width:max-content}
  .telegram-head{display:grid}
  .telegram-rating{text-align:left}
  .tab-btn{padding:6px 10px;font-size:12px}
}
"""

COMPONENTS_CSS = """
.placeholder-block {
    border: 1px dashed #c9c9c9;
    background: #fafafa;
    padding: 1rem;
    border-radius: 6px;
    margin: 1rem 0;
    color: #555;
}
.placeholder-block > summary {
    cursor: pointer;
    color: #888;
    font-size: 0.9rem;
    list-style: none;
    font-weight: 600;
}
.placeholder-block > summary::-webkit-details-marker {
    display: none;
}
.placeholder-block > summary::before {
    content: "🚧 開發中 · ";
    color: #c79a4a;
    font-weight: 600;
}
.placeholder-block[open] {
    background: #fff;
    border-style: solid;
}
.placeholder-block.data-ready > summary::before {
    content: "✅ 資料已接入 · ";
    color: #2e7d32;
}
.placeholder-body {
    padding-top: 0.75rem;
    color: #555;
}
.placeholder-body .strategy-note {
    color: #555;
}
.inline-placeholder {
    padding: 0.5rem 0.75rem;
    margin: 0;
}
.table-placeholder-note {
    color: #888;
    font-size: 0.9rem;
    margin: 0.5rem 0 0.75rem;
}
.stock-table th[data-empty="true"],
.stock-table td[data-empty="true"] {
    display: none;
}
"""

AUTO_EXPAND_PLACEHOLDER_JS = """
document.addEventListener('DOMContentLoaded', async () => {
    const blocks = document.querySelectorAll('.placeholder-block[data-source]');
    for (const block of blocks) {
        const src = block.dataset.source;
        try {
            const res = await fetch(src, { method: 'HEAD', cache: 'no-store' });
            if (res.ok) {
                block.open = true;
                block.classList.add('data-ready');
            }
        } catch (e) {
            // Keep collapsed when the future data file is not published yet.
        }
    }
});
"""

def nav_html(active: str = "home", prefix: str = "") -> str:
    tabs = [
        ("home",      "index.html",     "首頁"),
        ("flow",      "institutional-flow.html", "法人排行"),
        ("selection", "selection.html", "選股池"),
        ("mda",       "mda.html",       "M大觀察"),
        ("timing",    "timing.html",    "買賣時機"),
        ("stocks",    "stocks.html",    "個股查詢"),
        ("history",   "history.html",   "歷史分析"),
    ]
    # legacy nav keys map to new pages
    _NAV_ALIASES = {
        "daily": "selection", "basket": "selection", "signals": "selection",
        "mda_launched": "mda", "mda_consolidation": "mda",
        "radar": "timing", "carybot": "timing",
        "backtest": "history",
    }
    active = _NAV_ALIASES.get(active, active)
    items = ""
    for key, href, label in tabs:
        cls = "tab active" if key == active else "tab"
        items += f'<a href="{prefix}{href}" class="{cls}">{label}</a>\n'
    return f"""
<nav>
  <span class="nav-brand">📊 Stockfrom脩</span>
  {items}
</nav>"""


def footer_html() -> str:
    freshness = ""
    if SITE_LATEST_REPORT_DATE:
        freshness = f'  <p class="site-freshness">資料更新：{esc(SITE_LATEST_REPORT_DATE)} 收盤後</p>\n'
    return f"""
<footer>
{freshness}  <p>資料來源：FinMind 付費版 · TWSE · Yahoo Finance</p>
  <p class="disclaimer">本站資訊僅供研究參考，不構成投資建議，投資人應自行判斷並承擔風險。</p>
  <p style="margin-top:6px">© {datetime.now().year} Stockfrom脩 · 每個交易日自動更新</p>
</footer>"""


def status_badge(icon: str, text: str) -> str:
    if icon == "🟢":
        return f'<span class="badge badge-green">原始綠燈</span>'
    elif icon == "🟡":
        return f'<span class="badge badge-yellow">原始黃燈</span>'
    else:
        return f'<span class="badge badge-red">風險</span>'


def clean_stock_name(name: str) -> str:
    text = str(name or "").strip()
    text = re.sub(r"\s*｜\s*[🟢🟡🔴]\s*[^｜]+", "", text)
    text = re.sub(r"\s*｜\s*綜合評分[:：]?\s*[\d.]+", "", text)
    return text.strip()


def basket_badge(s: dict) -> str:
    basket = classify_basket(s)
    if basket == "marching":
        return '<span class="tag tag-green">行進籃</span>'
    if basket == "consolidation":
        return '<span class="tag tag-blue">盤整籃</span>'
    return '<span class="tag tag-red">過熱/風險</span>'


def gain_color(gain_str: str) -> str:
    """根據漲幅字串決定顏色"""
    try:
        v = float(gain_str.replace("%", "").replace("+", ""))
        return "pos" if v >= 0 else "neg"
    except:
        return ""


def is_blank(value) -> bool:
    return value is None or str(value).strip() in {"", "─", "-", "nan", "None"}


def esc(value) -> str:
    return html.escape(str(value if value is not None else ""))


def set_site_latest_report_date(reports: list[dict]) -> None:
    global SITE_LATEST_REPORT_DATE
    dates = sorted(str(r.get("date") or "") for r in reports if r.get("date"))
    SITE_LATEST_REPORT_DATE = dates[-1] if dates else ""


def industry_cache() -> dict[str, dict]:
    global _INDUSTRY_CACHE
    if _INDUSTRY_CACHE is not None:
        return _INDUSTRY_CACHE
    try:
        payload = json.loads(INDUSTRY_CACHE_PATH.read_text(encoding="utf-8-sig"))
        stocks = payload.get("stocks") or {}
        _INDUSTRY_CACHE = stocks if isinstance(stocks, dict) else {}
    except Exception:
        _INDUSTRY_CACHE = {}
    return _INDUSTRY_CACHE


def stock_sector(stock_id: str, fallback: str = "") -> str:
    info = industry_cache().get(str(stock_id)) or {}
    sector = info.get("industry_category") or info.get("sector") or fallback
    sector = str(sector or "").strip()
    if not sector or sector.upper() == "ETF":
        return "未分類"
    return sector


def stock_href(stock_id: str, prefix: str = "stocks") -> str:
    return f"{prefix}/{esc(stock_id)}.html"


_CARYBOT_MARKER_CACHE: dict[str, dict] | None = None
_CARYBOT_SIGNAL_PAYLOAD_CACHE: dict | None = None


def default_carybot_signals_payload() -> dict:
    return {
        "date": "",
        "signals": [],
        "history": [],
        "sources": {"mode": "missing"},
        "freshness": {"status": "missing", "data_date": None, "expected_data_date": None},
    }


def load_carybot_signals_payload(path: Path | str = CARYBOT_SIGNALS_PATH) -> dict:
    global _CARYBOT_SIGNAL_PAYLOAD_CACHE
    path_obj = Path(path)
    if path_obj == CARYBOT_SIGNALS_PATH and _CARYBOT_SIGNAL_PAYLOAD_CACHE is not None:
        return _CARYBOT_SIGNAL_PAYLOAD_CACHE
    if not path_obj.exists():
        payload = default_carybot_signals_payload()
    else:
        try:
            payload = json.loads(path_obj.read_text(encoding="utf-8"))
        except Exception:
            payload = default_carybot_signals_payload()
    if not isinstance(payload, dict):
        payload = default_carybot_signals_payload()
    payload.setdefault("signals", [])
    payload.setdefault("history", [])
    payload.setdefault("date", "")
    payload.setdefault("sources", {})
    payload.setdefault("freshness", {"status": "missing", "data_date": None, "expected_data_date": None})
    if not isinstance(payload.get("signals"), list):
        payload["signals"] = []
    if not isinstance(payload.get("history"), list):
        payload["history"] = []
    if path_obj == CARYBOT_SIGNALS_PATH:
        _CARYBOT_SIGNAL_PAYLOAD_CACHE = payload
    return payload


def _carybot_signal_rank(signal: dict) -> tuple:
    type_rank = {"B1": 2, "B2": 1}.get(str(signal.get("signal_type") or ""), 0)
    score = _num_or_none(signal.get("score")) or 0
    rank = _num_or_none(signal.get("rank")) or 999999
    return (str(signal.get("date") or ""), type_rank, score, -rank)


def latest_carybot_signals_by_stock(payload: dict | None = None) -> dict[str, dict]:
    payload = payload or load_carybot_signals_payload()
    latest: dict[str, dict] = {}
    for row in payload.get("signals") or []:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("stock_id") or row.get("stock") or "").strip()
        if not sid:
            continue
        old = latest.get(sid)
        if old is None or _carybot_signal_rank(row) >= _carybot_signal_rank(old):
            latest[sid] = row
    return latest


def carybot_signals_for_stock(stock_id: str, payload: dict | None = None, limit: int = 8) -> list[dict]:
    payload = payload or load_carybot_signals_payload()
    sid = str(stock_id)
    seen: set[tuple[str, str]] = set()
    rows: list[dict] = []
    for row in list(payload.get("signals") or []) + list(payload.get("history") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("stock_id") or row.get("stock") or "") != sid:
            continue
        key = (str(row.get("date") or ""), str(row.get("signal_type") or ""))
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    rows.sort(key=_carybot_signal_rank, reverse=True)
    return rows[:limit]


def carybot_signal_badge(signal: dict, double_confirm: bool = False) -> str:
    signal_type = str(signal.get("signal_type") or "")
    tag_cls = "tag-green" if signal_type == "B1" else "tag-blue" if signal_type == "B2" else ""
    icon = "&#128994;" if signal_type == "B1" else "&#128309;" if signal_type == "B2" else ""
    score = _num_or_none(signal.get("score"))
    temp = _num_or_none(signal.get("thermometer_score"))
    date_text = str(signal.get("date") or "-")
    bits = []
    if double_confirm:
        bits.append('<span class="tag tag-yellow">&#11088; SFZ + CaryBot</span>')
    bits.append(f'<span class="tag {tag_cls}">{icon} {esc(signal_type or "CaryBot")}</span>')
    meta = [date_text]
    if score is not None:
        meta.append(f"分數 {score:.0f}")
    if temp is not None:
        meta.append(f"溫度 {temp:.0f}")
    bits.append(f'<div class="signal-dates">{"｜".join(esc(x) for x in meta if x)}</div>')
    return "".join(bits)


def build_carybot_signal_history_panel(stock_id: str, payload: dict | None = None) -> str:
    rows = carybot_signals_for_stock(stock_id, payload)
    payload = payload or load_carybot_signals_payload()
    source_date = payload.get("date") or "-"
    freshness_warning = artifact_freshness_warning(payload, "CaryBot 訊號")
    if not rows:
        return f"""
<div class="card" data-carybot-history>
  <div class="section-label">CaryBot 買點歷史</div>
  {freshness_warning}
  <div class="strategy-note">目前 data/carybot_signals.json 尚無 {esc(stock_id)} 的 B1/B2 買點紀錄；此區只作 timing / confirmation layer，不取代 SFZ 趨勢篩選。</div>
</div>"""
    body_rows = ""
    for row in rows:
        signal_type = str(row.get("signal_type") or "")
        tag_cls = "tag-green" if signal_type == "B1" else "tag-blue"
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        qz = _fmt_short(metrics.get("QZ") if metrics else row.get("QZ"))
        qtyr = _fmt_short(metrics.get("QTYR") if metrics else row.get("QTYR"))
        vam20 = _fmt_short(metrics.get("VAM20") if metrics else row.get("VAM20"))
        phase = row.get("phase") or row.get("carybot_phase") or "-"
        reason = row.get("reason") or row.get("transition_5d") or "-"
        body_rows += f"""
<tr>
  <td>{esc(row.get("date") or "-")}</td>
  <td><span class="tag {tag_cls}">{esc(signal_type)}</span><div class="signal-dates">raw {esc(row.get("raw_signal_type") or "")}</div></td>
  <td><strong>{esc(row.get("score") or "-")}</strong><div class="signal-dates">溫度 {esc(row.get("thermometer_score") or "-")}</div></td>
  <td>QZ {esc(qz)}｜QTYR {esc(qtyr)}｜VAM20 {esc(vam20)}</td>
  <td>{esc(phase)}<div class="signal-dates">{esc(reason)}</div></td>
</tr>"""
    return f"""
<div class="card" data-carybot-history>
  {freshness_warning}
  <div class="section-head">
    <div>
      <div class="section-label">CaryBot 買點歷史</div>
      <div class="strategy-note">讀取 <code>data/carybot_signals.json</code> 的 B1/B2 買點與溫度計分數；CaryBot 是買點確認層，不取代 SFZ / M大股票池。</div>
    </div>
    <div class="section-date">資料日 {esc(source_date)}</div>
  </div>
  <div style="overflow-x:auto">
    <table class="stock-table"><thead><tr><th>日期</th><th>訊號</th><th>分數</th><th>溫度計指標</th><th>階段 / 原因</th></tr></thead><tbody>{body_rows}</tbody></table>
  </div>
</div>"""


def _num_or_none(value):
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", "").replace("%", "").strip())
    except Exception:
        return None


def _fmt_short(value, digits: int = 2) -> str:
    v = _num_or_none(value)
    if v is None or not math.isfinite(v):
        return "-"
    return f"{v:.{digits}f}".rstrip("0").rstrip(".")


def latest_carybot_markers_by_stock() -> dict[str, dict]:
    """Prefer latest v50 AI_Buy/PreBuy marker, fallback to older v42 buy markers."""
    global _CARYBOT_MARKER_CACHE
    if _CARYBOT_MARKER_CACHE is not None:
        return _CARYBOT_MARKER_CACHE

    paths = [
        V44_BACKTEST_OUTPUT_DIR / "carybot_signal_master_v50.csv",
        V44_BACKTEST_OUTPUT_DIR / "carybot_buy_markers_v42_features.csv",
    ]
    latest: dict[str, dict] = {}
    for path in paths:
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    sid = str(row.get("stock") or "").strip()
                    marker_type = str(row.get("signal_type") or row.get("marker_type") or "").strip()
                    marker_side = str(row.get("marker_side") or ("buy" if marker_type in {"AI_Buy", "PreBuy"} else "")).strip()
                    marker_date = str(row.get("date") or row.get("marker_date") or row.get("Date") or "").strip()
                    if not sid or marker_type not in {"AI_Buy", "PreBuy"}:
                        continue
                    if marker_side and marker_side != "buy":
                        continue
                    old = latest.get(sid)
                    rank = (1 if marker_type == "AI_Buy" else 0, marker_date)
                    old_rank = (
                        1 if ((old or {}).get("signal_type") or (old or {}).get("marker_type")) == "AI_Buy" else 0,
                        str((old or {}).get("date") or (old or {}).get("marker_date") or ""),
                    )
                    if old is None or rank >= old_rank:
                        latest[sid] = row
        except Exception:
            latest = latest or {}
        if latest:
            break
    _CARYBOT_MARKER_CACHE = latest
    return latest


def carybot_marker_cell(stock_id: str, hide_empty_col: bool = False) -> str:
    signal = latest_carybot_signals_by_stock().get(str(stock_id))
    if signal:
        metrics = signal.get("metrics") if isinstance(signal.get("metrics"), dict) else {}
        qz = metrics.get("QZ") if metrics else signal.get("QZ")
        qz_num = _num_or_none(qz)
        qz_cls = "pos" if qz_num is not None and qz_num > 0 else "neg" if qz_num is not None and qz_num < 0 else ""
        qtyr = metrics.get("QTYR") if metrics else signal.get("QTYR")
        vam20 = metrics.get("VAM20") if metrics else signal.get("VAM20")
        vam60 = metrics.get("VAM60") if metrics else signal.get("VAM60")
        phase = signal.get("phase") or signal.get("carybot_phase") or "-"
        return f"""<td class="hide-mobile carybot-cell">
  {carybot_signal_badge(signal)}
  <div class="carybot-line">QZ <strong class="{qz_cls}">{_fmt_short(qz)}</strong>｜QTYR <strong>{_fmt_short(qtyr)}</strong></div>
  <div class="carybot-line">VAM20 {_fmt_short(vam20)}｜VAM60 {_fmt_short(vam60)}</div>
  <div class="carybot-line">{esc(phase)}</div>
</td>"""

    marker = latest_carybot_markers_by_stock().get(str(stock_id))
    if not marker:
        empty_attr = ' data-empty="true"' if hide_empty_col else ""
        return f'<td class="hide-mobile carybot-cell carybot-pending-cell"{empty_attr}><span class="carybot-missing">尚無藍點資料</span></td>'

    marker_type = marker.get("signal_type") or marker.get("marker_type", "")
    tag_cls = "tag-green" if marker_type == "AI_Buy" else "tag-blue"
    date_text = marker.get("date") or marker.get("marker_date") or marker.get("Date") or "-"
    qz = _num_or_none(marker.get("QZ"))
    qz_cls = "pos" if qz is not None and qz > 0 else "neg" if qz is not None and qz < 0 else ""
    phase = marker.get("carybot_phase") or "-"
    return f"""<td class="hide-mobile carybot-cell">
  <span class="tag {tag_cls}">{esc(marker_type)}</span>
  <div class="signal-dates">{esc(date_text)}</div>
  <div class="carybot-line">QZ <strong class="{qz_cls}">{_fmt_short(qz)}</strong>｜QTYR <strong>{_fmt_short(marker.get("QTYR"))}</strong></div>
  <div class="carybot-line">VAM20 {_fmt_short(marker.get("VAM20"))}｜VAM60 {_fmt_short(marker.get("VAM60"))}</div>
  <div class="carybot-line">{esc(phase)}</div>
</td>"""


def html_page(title: str, nav_key: str, body: str, nav_prefix: str = "") -> str:
    component_href = f"{nav_prefix}css/components.css"
    placeholder_js = f"{nav_prefix}js/auto-expand-placeholder.js"
    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Stockfrom脩 選股站</title>
<meta name="description" content="量化選股 · 每日精選 Top 20 · ABC籌碼分析 · 台股研究">
<link rel="icon" href="data:,">
<style>{CSS}</style>
<link rel="stylesheet" href="{component_href}">
<script defer src="{placeholder_js}"></script>
</head>
<body>
{nav_html(nav_key, nav_prefix)}
{body}
{footer_html()}
</body>
</html>"""



TAB_JS = """
<script>
function initTabs(containerId){var c=document.getElementById(containerId);if(!c)return;var btns=c.querySelectorAll('.tab-btn'),panels=c.querySelectorAll('.tab-panel');var aliases={top20:'daily-top20',sfz:'sfz-baskets',tracking:'signal-ledger',radar:'buy-radar',carybot:'carybot',backtest:'backtest',reports:'reports'};function activate(id,writeHash){id=aliases[id]||id;if(!id||!c.querySelector('#'+CSS.escape(id)))id=btns[0]&&btns[0].dataset.tab;btns.forEach(function(b){b.classList.toggle('active',(aliases[b.dataset.tab]||b.dataset.tab)===id)});panels.forEach(function(p){p.classList.toggle('active',p.id===id);p.hidden=p.id!==id;});if(writeHash)history.replaceState(null,'',location.pathname+'#'+id);}btns.forEach(function(b){b.addEventListener('click',function(){activate(b.dataset.tab,true)})});window.addEventListener('hashchange',function(){activate((location.hash||'').replace('#',''),false)});activate((location.hash||'').replace('#','')||new URLSearchParams(location.search).get('tab')||'',false);}
function initPlaceholders(){}
function initResponsiveTables(){document.querySelectorAll('table.stock-table:not(.flow-ranking-table):not(.holder-history-table)').forEach(function(table){table.classList.add('responsive-card');var heads=Array.from(table.querySelectorAll('thead th')).map(function(th){return th.textContent.trim();});table.querySelectorAll('tbody tr').forEach(function(tr){Array.from(tr.children).forEach(function(td,i){if(!td.dataset.label)td.dataset.label=heads[i]||'';});});});}
function initSignalLedger(){document.querySelectorAll('[data-ledger]').forEach(function(root){var tbody=root.querySelector('tbody');var rows=Array.from(root.querySelectorAll('[data-ledger-row]'));var input=root.querySelector('[data-ledger-search]');var current=root.querySelector('[data-ledger-current]');var history=root.querySelector('[data-ledger-history]');var prev=root.querySelector('[data-page-prev]');var next=root.querySelector('[data-page-next]');var info=root.querySelector('[data-page-info]');var pageNums=root.querySelector('[data-page-nums]');var sortKey='latest',sortDir='desc',page=1,per=30;function val(r,k){if(['count','push'].includes(k))return Number(r.dataset[k]||0);return (r.dataset[k]||'').toLowerCase();}function selectedRows(){var q=(input&&input.value||'').trim().toLowerCase();var showCurrent=!current||current.checked;var showHistory=!!(history&&history.checked);return rows.filter(function(r){var okText=!q||(r.dataset.text||'').toLowerCase().indexOf(q)>=0;var okFilter=q||((showCurrent&&r.dataset.current==='1')||(showHistory&&r.dataset.current!=='1'));return okText&&okFilter;}).sort(function(a,b){var av=val(a,sortKey),bv=val(b,sortKey);if(av<bv)return sortDir==='asc'?-1:1;if(av>bv)return sortDir==='asc'?1:-1;return 0;});}function renderPages(pages){if(!pageNums)return;pageNums.innerHTML='';var start=Math.max(1,page-2),end=Math.min(pages,start+4);for(var i=start;i<=end;i++){var btn=document.createElement('button');btn.type='button';btn.className='page-num'+(i===page?' active':'');btn.textContent=i;btn.dataset.page=i;btn.addEventListener('click',function(){page=Number(this.dataset.page);render();});pageNums.appendChild(btn);}}function render(){var shown=selectedRows();var pages=Math.max(1,Math.ceil(shown.length/per));if(page>pages)page=pages;rows.forEach(function(r){r.style.display='none';});shown.forEach(function(r){if(tbody)tbody.appendChild(r);});shown.slice((page-1)*per,page*per).forEach(function(r){r.style.display='';});if(info)info.textContent='第 '+page+' / '+pages+' 頁 · 目前 '+shown.length+' / '+rows.length+' 檔';if(prev)prev.disabled=page<=1;if(next)next.disabled=page>=pages;renderPages(pages);}root.querySelectorAll('[data-ledger-sort]').forEach(function(th){th.addEventListener('click',function(){var key=th.dataset.ledgerSort;if(sortKey===key){sortDir=sortDir==='asc'?'desc':'asc';}else{sortKey=key;sortDir=key==='code'||key==='name'?'asc':'desc';}root.querySelectorAll('[data-ledger-sort]').forEach(function(x){x.classList.remove('sort-asc','sort-desc');});th.classList.add(sortDir==='asc'?'sort-asc':'sort-desc');page=1;render();});});[input,current,history].forEach(function(el){if(el)el.addEventListener(input&&el===input?'input':'change',function(){page=1;render();});});if(prev)prev.addEventListener('click',function(){page=Math.max(1,page-1);render();});if(next)next.addEventListener('click',function(){page=page+1;render();});render();});}
function initRadarFilters(){document.querySelectorAll('[data-radar]').forEach(function(root){var rows=Array.from(root.querySelectorAll('[data-radar-row]'));var checks=Array.from(root.querySelectorAll('[data-radar-status]'));var basket=root.querySelector('[data-radar-basket]');var sector=root.querySelector('[data-radar-sector]');var rr=root.querySelector('[data-radar-min-rr]');var count=root.querySelector('[data-radar-count]');var reset=root.querySelector('[data-radar-reset]');function render(){var enabled=new Set(checks.filter(function(c){return c.checked;}).map(function(c){return c.value;}));var minRr=rr&&rr.value!==''?Number(rr.value):0;var visible=0;rows.forEach(function(r){var ok=enabled.has(r.dataset.status||'far');if(basket&&basket.value&&basket.value!=='all')ok=ok&&r.dataset.basket===basket.value;if(sector&&sector.value&&sector.value!=='all')ok=ok&&r.dataset.sector===sector.value;if(Number.isFinite(minRr)&&minRr>0)ok=ok&&Number(r.dataset.rr||0)>=minRr;r.style.display=ok?'':'none';if(ok)visible+=1;});if(count)count.textContent='目前 '+visible+' / '+rows.length+' 檔';}function setDefaults(){checks.forEach(function(c){c.checked=c.value==='near'||c.value==='pullback';});if(basket)basket.value='all';if(sector)sector.value='all';if(rr)rr.value='2.0';render();}checks.forEach(function(c){c.addEventListener('change',render);});[basket,sector,rr].forEach(function(el){if(el)el.addEventListener('change',render);});if(rr)rr.addEventListener('input',render);if(reset)reset.addEventListener('click',setDefaults);setDefaults();});}
function initDisclaimer(){var modal=document.querySelector('[data-disclaimer-modal]');if(!modal)return;if(localStorage.getItem('stockfromDisclaimerOk')==='1')return;modal.classList.add('show');var btn=modal.querySelector('button');if(btn)btn.addEventListener('click',function(){localStorage.setItem('stockfromDisclaimerOk','1');modal.classList.remove('show');});}
document.addEventListener('DOMContentLoaded',function(){initPlaceholders();initResponsiveTables();initSignalLedger();initRadarFilters();initDisclaimer();});
</script>
"""


def redirect_page(target_url: str, title: str = "Redirecting") -> str:
    """Generate a lightweight redirect HTML page."""
    freshness = f"<p>\u8cc7\u6599\u66f4\u65b0\uff1a{esc(SITE_LATEST_REPORT_DATE)} \u6536\u76e4\u5f8c</p>" if SITE_LATEST_REPORT_DATE else ""
    canonical = target_url.replace("../", "")
    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="0;url={esc(target_url)}">
<link rel="canonical" href="{esc(canonical)}">
<title>{esc(title)}</title>
</head>
<body>
{freshness}
<p>Page moved: <a href="{esc(target_url)}">{esc(target_url)}</a></p>
</body>
</html>"""


def write_static_assets() -> None:
    css_dir = OUTPUT_DIR / "css"
    js_dir = OUTPUT_DIR / "js"
    css_dir.mkdir(parents=True, exist_ok=True)
    js_dir.mkdir(parents=True, exist_ok=True)
    (css_dir / "components.css").write_text(COMPONENTS_CSS.strip() + "\n", encoding="utf-8")
    (js_dir / "auto-expand-placeholder.js").write_text(AUTO_EXPAND_PLACEHOLDER_JS.strip() + "\n", encoding="utf-8")


def publish_data_assets(paths: list[Path] | None = None) -> list[str]:
    public_dir = OUTPUT_DIR / "data"
    public_dir.mkdir(parents=True, exist_ok=True)
    published: list[str] = []
    for source in paths or PUBLIC_DATA_FILES:
        if not source.exists():
            continue
        target = public_dir / source.name
        shutil.copy2(source, target)
        published.append(target.name)
    return published


# ──────────────────────────────────────────────
#  各頁面生成
# ──────────────────────────────────────────────

def build_stock_table(
    stocks: list[dict],
    compact: bool = False,
    stock_link_prefix: str = "stocks",
    show_basket: bool = True,
    show_status: bool = True,
    show_carybot: bool = False,
    table_class: str = "stock-table",
) -> str:
    """生成股票表格 HTML"""
    rows = ""
    carybot_signal_map = latest_carybot_signals_by_stock()
    carybot_marker_map = latest_carybot_markers_by_stock()
    carybot_empty_col = bool(
        show_carybot
        and not any(str(row.get("id", "")) in carybot_signal_map or str(row.get("id", "")) in carybot_marker_map for row in stocks)
    )
    for i, s in enumerate(stocks, 1):
        s = enrich_stock_fields(s)
        _, tech, decision = stock_trade_context(s)
        plan = decision
        badge = basket_badge(s)
        gain_cls = gain_color(s["gain_6w"])
        badge_line = f'<div style="margin-top:3px">{badge}</div>' if show_basket else ""
        basket_cell = f"<td>{badge}</td>" if show_status else ""
        foreign_color = "#f85149" if s["foreign_5d"].startswith("+") else "#3fb950" if s["foreign_5d"].startswith("-") else "#8b949e"
        carybot_cell = carybot_marker_cell(s["id"], carybot_empty_col) if show_carybot else ""

        if compact:
            rows += f"""
<tr>
  <td><span style="color:#6e7681;font-size:11px">#{i}</span></td>
  <td>
    <div><a class="stock-link" href="{stock_href(s['id'], stock_link_prefix)}">{s['id']} {s['name']}</a></div>
    <div class="signal-dates">{esc(s.get('sector', '未分類'))}</div>
{badge_line}
  </td>
  <td class="price-main">{s['price']}</td>
  <td class="{gain_cls}">{s['gain_6w']}</td>
  <td><span style="color:#58a6ff;font-weight:700">{s['score']}</span></td>
  <td>
    <div class="price-entry">進 {plan['entry_text']}</div>
    <div class="price-target">目 {plan['target_text']}</div>
    <div class="price-stop">初停 {plan['initial_stop_text']}</div>
    <div class="price-rr {plan['rr_class']}">R:R {plan['rr_text']}</div>
  </td>
</tr>"""
        else:
            rows += f"""
<tr>
  <td><span style="color:#6e7681;font-size:11px">#{i}</span></td>
  <td>
    <div style="font-size:14px"><a class="stock-link" href="{stock_href(s['id'], stock_link_prefix)}">{s['id']}</a></div>
    <div style="color:#8b949e;font-size:12px">{s['name']}</div>
    <div class="signal-dates">{esc(s.get('sector', '未分類'))}</div>
  </td>
{basket_cell}
  <td class="price-main">{s['price']}</td>
  <td class="{gain_cls}" style="font-weight:600">{s['gain_6w']}</td>
  <td style="color:#8b949e">{s['rsi']}</td>
  <td style="color:#8b949e">{s['bband_pct']}</td>
  <td class="hide-mobile" style="color:#8b949e">{s['vol_5d']}</td>
  <td class="hide-mobile" style="color:{foreign_color}">{s['foreign_5d']}</td>
{carybot_cell}
  <td><span style="color:#58a6ff;font-weight:700;font-size:14px">{s['score']}</span></td>
  <td>
    <div class="price-entry">進 {plan['entry_text']}</div>
    <div class="price-target">目 {plan['target_text']}</div>
    <div class="price-stop">初停 {plan['initial_stop_text']}</div>
    <div class="price-support">支撐 {plan['reference_support_text']}</div>
    <div class="price-rr {plan['rr_class']}">R:R {plan['rr_text']}</div>
  </td>
</tr>"""

    if compact:
        header = """<tr>
  <th>#</th><th>個股</th><th>收盤</th><th>近6週漲幅</th><th>評分</th><th>進場/目標/初停/R:R</th>
</tr>"""
    else:
        status_header = "<th>狀態</th>" if show_status else ""
        empty_attr = ' data-empty="true"' if show_carybot and carybot_empty_col else ""
        carybot_header = f'<th class="hide-mobile carybot-col"{empty_attr}>CaryBot暫接</th>' if show_carybot else ""
        header = f"""<tr>
  <th>#</th><th>代號/名稱</th>{status_header}<th>收盤</th>
  <th>近6週漲幅</th><th>RSI</th><th>%B</th>
  <th class="hide-mobile">近5日量</th><th class="hide-mobile">外資近5日</th>{carybot_header}
  <th>評分</th><th>進場/目標/初停/R:R</th>
</tr>"""

    carybot_notice = '<div class="table-placeholder-note">CaryBot 訊號欄位接入中</div>' if show_carybot and carybot_empty_col else ""
    return f"""{carybot_notice}<div style="overflow-x:auto">
<table class="{table_class}">
<thead>{header}</thead>
<tbody>{rows}</tbody>
</table>
</div>"""


def load_sfz_all_payload(path: Path | str = SFZ_ALL_PATH) -> dict:
    path = Path(path)
    if not path.exists():
        return {"date": "", "count": 0, "default_limit": 20, "stocks": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {"date": "", "count": 0, "default_limit": 20, "stocks": []}
    if not isinstance(payload, dict):
        return {"date": "", "count": 0, "default_limit": 20, "stocks": []}
    stocks = payload.get("stocks")
    if not isinstance(stocks, list):
        payload["stocks"] = []
    payload["count"] = len(payload.get("stocks", []))
    payload.setdefault("default_limit", 20)
    payload.setdefault("date", "")
    return payload


def default_market_sentiment_payload() -> dict:
    return {
        "score": 50,
        "regime": "neutral",
        "regime_label": "中性",
        "color": "yellow",
        "updated_at": "",
        "summary": "市場情緒資料尚未產生，先以中性看待。",
        "indicators": {},
        "source_status": ["WARN: market sentiment JSON missing"],
    }


def load_market_sentiment_payload(path: Path | str = MARKET_SENTIMENT_PATH) -> dict:
    path = Path(path)
    if not path.exists():
        return default_market_sentiment_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default_market_sentiment_payload()
    if not isinstance(payload, dict):
        return default_market_sentiment_payload()
    payload.setdefault("score", 50)
    payload.setdefault("regime", "neutral")
    payload.setdefault("regime_label", "中性")
    payload.setdefault("color", "yellow")
    payload.setdefault("indicators", {})
    payload.setdefault("source_status", [])
    return payload


DAILY_DECISION_STATE_META = {
    "ENTRY_CANDIDATE": ("可進一步確認", "tag-green", "SFZ、交通燈與 CaryBot B1 已對齊"),
    "SETUP": ("準備中", "tag-yellow", "部分條件成立，等待確認"),
    "WATCH": ("觀察", "tag-blue", "尚未形成可執行條件"),
    "NO-GO": ("先不做", "tag-red", "存在阻擋條件或風險"),
    "HOLD": ("續抱", "tag-green", "持倉整合尚未接入前僅作保留狀態"),
    "RISK_REDUCE": ("降低風險", "tag-red", "需要檢查部位與風險證據"),
    "EXIT_CANDIDATE": ("出場候選", "tag-red", "需要檢查出場證據"),
}


def default_daily_decisions_payload() -> dict:
    return {
        "date": "",
        "updated_at": "",
        "count": 0,
        "action_counts": {},
        "decisions": [],
        "data_quality": {
            "state": "missing",
            "warnings": ["daily_decisions.json 尚未產生"],
        },
        "freshness": {"status": "missing"},
    }


def load_daily_decisions_payload(path: Path | str = DAILY_DECISIONS_PATH) -> dict:
    """Load the operation-advice contract without deriving new signals in HTML."""
    path = Path(path)
    if not path.exists():
        return default_daily_decisions_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default_daily_decisions_payload()
    if not isinstance(payload, dict):
        return default_daily_decisions_payload()
    decisions = payload.get("decisions")
    payload["decisions"] = [row for row in decisions if isinstance(row, dict)] if isinstance(decisions, list) else []
    payload["count"] = len(payload["decisions"])
    counts = payload.get("action_counts")
    payload["action_counts"] = dict(counts) if isinstance(counts, dict) else {}
    quality = payload.get("data_quality")
    payload["data_quality"] = dict(quality) if isinstance(quality, dict) else {"state": "unknown", "warnings": []}
    payload["data_quality"].setdefault("state", "unknown")
    payload["data_quality"].setdefault("warnings", [])
    payload.setdefault("date", "")
    payload.setdefault("updated_at", "")
    payload.setdefault("freshness", {})
    return payload


def daily_decision_state_meta(state: str) -> tuple[str, str, str]:
    return DAILY_DECISION_STATE_META.get(
        str(state or ""),
        (str(state or "未分類"), "tag", "等待更多資料"),
    )


def _daily_decision_warning_text(payload: dict) -> str:
    quality = payload.get("data_quality") if isinstance(payload.get("data_quality"), dict) else {}
    warnings = quality.get("warnings") if isinstance(quality.get("warnings"), list) else []
    state = str(quality.get("state") or "unknown")
    if state == "ok" and not warnings:
        return ""
    friendly = []
    for warning in warnings[:3]:
        text = str(warning)
        text = text.replace(" freshness is fallback_stale", " 目前使用舊版 fallback")
        text = text.replace(" freshness is missing", " 尚未附 freshness")
        text = text.replace(" freshness is stale", " 已過期")
        text = text.replace(" freshness is schema_error", " schema 不正確")
        friendly.append(text)
    if not friendly:
        friendly.append("daily_decisions.json 尚未完成資料品質確認")
    return "；".join(friendly)


DAILY_RISK_META = {
    "none": ("官方風險：無已發布風險", "tag-green"),
    "attention": ("官方注意", "tag-yellow"),
    "near_disposition": ("官方處置預警", "tag-yellow"),
    "disposition": ("官方處置", "tag-red"),
    "unknown": ("官方風險資料不完整", "tag-yellow"),
}


def daily_decision_risk(row: dict) -> dict:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    risk = evidence.get("market_risk") if isinstance(evidence.get("market_risk"), dict) else {}
    risk = dict(risk)
    level = str(risk.get("risk_level") or "unknown")
    source_state = str(risk.get("source_state") or "unknown")
    if level == "none" and source_state != "complete":
        level = "unknown"
    label, tag_cls = DAILY_RISK_META.get(level, DAILY_RISK_META["unknown"])
    return {
        "level": level,
        "source_state": source_state,
        "label": label,
        "tag_cls": tag_cls,
        "reasons": [str(item) for item in risk.get("reasons") or [] if str(item)],
        "warnings": [str(item) for item in risk.get("warnings") or [] if str(item)],
    }


def daily_decision_map(payload: dict | None = None) -> dict[str, dict]:
    payload = payload or load_daily_decisions_payload()
    return {
        str(row.get("stock_id") or row.get("security_id") or "").strip(): row
        for row in payload.get("decisions") or []
        if isinstance(row, dict) and str(row.get("stock_id") or row.get("security_id") or "").strip()
    }


def build_daily_decision_badge(stock_id: str, payload: dict | None = None) -> str:
    row = daily_decision_map(payload).get(str(stock_id or "").strip())
    if not row:
        return '<span class="tag tag-yellow" data-decision-state="missing">決策狀態未載入</span>'
    state = str(row.get("action_state") or "WATCH")
    label, tag_cls, _ = daily_decision_state_meta(state)
    risk = daily_decision_risk(row)
    title = "；".join(risk["reasons"] + risk["warnings"])
    title_attr = f' title="{esc(title)}"' if title else ""
    return (
        f'<span class="tag {tag_cls}" data-decision-state="{esc(state)}">決策：{esc(label)}</span>'
        f'<span class="tag {risk["tag_cls"]}" data-market-risk="{esc(risk["level"])}"{title_attr}>{esc(risk["label"])}</span>'
    )


def _default_market_flow_summary() -> dict:
    return {
        "stock_count": 0,
        "ranking_eligible_count": 0,
        "ranking_excluded_count": 0,
        "foreign_buy": 0,
        "foreign_sell": 0,
        "foreign_net": 0,
        "investment_trust_buy": 0,
        "investment_trust_sell": 0,
        "investment_trust_net": 0,
        "institutional_total_net": 0,
        "foreign_top_buy": [],
        "foreign_top_sell": [],
        "trust_top_buy": [],
        "trust_top_sell": [],
        "amounts": {},
    }


def default_daily_market_flow_payload() -> dict:
    return {
        "date": "",
        "updated_at": "",
        "markets": {"listed": _default_market_flow_summary(), "otc": _default_market_flow_summary()},
        "rankings": {
            "eligibility_policy": "ordinary_equity_v1",
            "eligible_count": 0,
            "excluded_count": 0,
            "foreign_buy": [],
            "foreign_sell": [],
            "investment_trust_buy": [],
            "investment_trust_sell": [],
        },
        "source_artifacts": [],
        "data_quality": {"state": "missing", "warnings": ["daily_market_flow.json 尚未產生"]},
        "freshness": {"status": "missing"},
    }


def load_daily_market_flow_payload(path: Path | str = DAILY_MARKET_FLOW_PATH) -> dict:
    path = Path(path)
    if not path.exists():
        return default_daily_market_flow_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default_daily_market_flow_payload()
    if not isinstance(payload, dict):
        return default_daily_market_flow_payload()
    markets = payload.get("markets") if isinstance(payload.get("markets"), dict) else {}
    payload["markets"] = {
        market: dict(_default_market_flow_summary(), **(markets.get(market) if isinstance(markets.get(market), dict) else {}))
        for market in ("listed", "otc")
    }
    rankings = payload.get("rankings") if isinstance(payload.get("rankings"), dict) else {}
    payload["rankings"] = {
        "eligibility_policy": str(rankings.get("eligibility_policy") or "ordinary_equity_v1"),
        "eligible_count": int(rankings.get("eligible_count") or 0),
        "excluded_count": int(rankings.get("excluded_count") or 0),
        "foreign_buy": [row for row in rankings.get("foreign_buy") or [] if isinstance(row, dict)],
        "foreign_sell": [row for row in rankings.get("foreign_sell") or [] if isinstance(row, dict)],
        "investment_trust_buy": [row for row in rankings.get("investment_trust_buy") or [] if isinstance(row, dict)],
        "investment_trust_sell": [row for row in rankings.get("investment_trust_sell") or [] if isinstance(row, dict)],
    }
    payload.setdefault("date", "")
    payload.setdefault("updated_at", "")
    payload.setdefault("source_artifacts", [])
    payload.setdefault("data_quality", {"state": "unknown", "warnings": []})
    payload.setdefault("freshness", {})
    return payload


def _flow_number(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "─"
    return f"{number / 1000:+,.0f} 張"


def _flow_amount(value: Any) -> str:
    if value in (None, ""):
        return "─"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "─"
    return f"{number / 100_000_000:+,.2f} 億元"


def _market_amount_cards(payload: dict) -> str:
    market_labels = {"listed": "上市", "otc": "上櫃"}
    cards = ""
    for market in ("listed", "otc"):
        summary = payload["markets"].get(market) or _default_market_flow_summary()
        amounts = summary.get("amounts") if isinstance(summary.get("amounts"), dict) else {}
        cards += f"""
<div class="flow-market-card" data-market-flow="{market}">
  <div class="section-label">{market_labels[market]} · 官方全市場金額</div>
  <div class="flow-metric-grid">
    <div><div class="label">外資買賣超</div><div class="flow-net">{_flow_amount(amounts.get('foreign_net_amount'))}</div></div>
    <div><div class="label">投信買賣超</div><div class="flow-net">{_flow_amount(amounts.get('investment_trust_net_amount'))}</div></div>
    <div><div class="label">三大法人</div><div class="flow-net">{_flow_amount(amounts.get('institutional_total_net_amount'))}</div></div>
  </div>
  <div class="strategy-note">金額為交易所官方市場彙總，單位新台幣；涵蓋範圍依官方彙總表。</div>
</div>"""
    return cards


def _data_quality_warning(payload: dict, dataset_name: str) -> str:
    quality = payload.get("data_quality") if isinstance(payload.get("data_quality"), dict) else {}
    warnings = quality.get("warnings") if isinstance(quality.get("warnings"), list) else []
    freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
    freshness_status = str(freshness.get("status") or "")
    if freshness_status and freshness_status not in {"fresh", "fallback_fresh"}:
        warnings = list(warnings) + [f"{dataset_name} freshness is {freshness_status}"]
    return f'<div class="strategy-note" style="margin-top:10px"><span class="tag tag-yellow">資料品質提醒</span> {esc("；".join(str(item) for item in warnings[:2]))}</div>' if warnings else ""


def build_daily_market_flow_panel(payload: dict | None = None) -> str:
    payload = payload or load_daily_market_flow_payload()
    cards = _market_amount_cards(payload)
    warning_html = _data_quality_warning(payload, "daily_market_flow")
    date_text = str(payload.get("date") or "─")
    return f"""
<div class="card market-flow-card" data-daily-market-flow data-market-flow-date="{esc(date_text)}">
  <div class="section-head"><div><div class="section-label">每日上市／上櫃法人流向</div><div class="strategy-note">首頁改呈現交易所官方買賣超金額；個股排行另頁顯示，並排除 ETF 等非一般股票。</div></div><div class="section-date">資料日：{esc(date_text)}</div></div>
  <div class="flow-market-grid">{cards}</div>
  <a class="flow-page-link" href="institutional-flow.html">查看外資／投信完整排行 →</a>
  {warning_html}
  <div class="signal-foot">金額來源：TWSE BFI82U／TPEx 三大法人買賣金額彙總表；排行來源：TWSE T86／TPEx 三大法人明細 · 不改寫選股規則</div>
</div>"""


def _institutional_ranking_table(rows: list[dict], empty_text: str) -> str:
    row_html = "".join(
        f'<tr data-flow-rank-row data-search="{esc(str(row.get("security_id") or "") + " " + str(row.get("name") or "") + " " + str(row.get("market") or ""))}"><td>{rank:,}</td><td><a href="{stock_href(str(row.get("security_id") or ""))}">{esc(str(row.get("security_id") or ""))} {esc(str(row.get("name") or ""))}</a></td><td>{"上市" if str(row.get("market")) == "listed" else "上櫃" if str(row.get("market")) == "otc" else esc(str(row.get("market") or "─"))}</td><td class="{"pos" if float(row.get("net_shares") or 0) > 0 else "neg"}">{_flow_number(row.get("net_shares"))}</td></tr>'
        for rank, row in enumerate(rows, 1)
    )
    if not row_html:
        row_html = f'<tr><td colspan="4" style="color:#8b949e">{esc(empty_text)}</td></tr>'
    return f'<div class="flow-table-wrap"><table class="stock-table flow-ranking-table"><thead><tr><th>排名</th><th>個股</th><th>市場</th><th>淨買賣超</th></tr></thead><tbody>{row_html}</tbody></table></div>'


def build_institutional_flow_page(payload: dict | None = None) -> str:
    payload = payload or load_daily_market_flow_payload()
    rankings = payload.get("rankings") if isinstance(payload.get("rankings"), dict) else {}
    eligible_count = int(rankings.get("eligible_count") or 0)
    excluded_count = int(rankings.get("excluded_count") or 0)
    tabs = [
        ("foreign-buy", "外資買超", rankings.get("foreign_buy") or []),
        ("foreign-sell", "外資賣超", rankings.get("foreign_sell") or []),
        ("trust-buy", "投信買超", rankings.get("investment_trust_buy") or []),
        ("trust-sell", "投信賣超", rankings.get("investment_trust_sell") or []),
    ]
    display_tabs = [(tab_id, label, rows[:50], len(rows)) for tab_id, label, rows in tabs]
    ranking_sections = "".join(
        f'<section class="card ranking-section" id="{tab_id}"><div class="ranking-head"><div class="section-label">{esc(label)}</div><div class="ranking-meta">Top {len(rows):,} <span>／全榜 {total_count:,} 檔</span></div></div>{_institutional_ranking_table(rows, "當日沒有符合條件的一般股票")}</section>'
        for tab_id, label, rows, total_count in display_tabs
    )
    date_text = str(payload.get("date") or "─")
    warning_html = _data_quality_warning(payload, "daily_market_flow")
    body = f"""
<div class="container" id="page-top" data-flow-ranking-page>
  <div class="page-title">法人買賣超排行</div>
  <div class="page-sub">上市＋上櫃合併排行 · 一般股票 · 資料日：{esc(date_text)}</div>
  <div class="card flow-overview-card">
    <div class="section-head"><div class="section-label">今日法人動向</div><div class="section-date">單位：億元</div></div>
    <div class="flow-market-grid">{_market_amount_cards(payload)}</div>
    <div class="complete-table-note">官方全市場金額彙總；排行榜僅列一般股票 Top 50。符合 {eligible_count:,} 檔，已排除 ETF、ETN、權證、TDR 等 {excluded_count:,} 檔。</div>
{warning_html}
  </div>
  <div class="flow-page-toolbar"><input type="search" data-flow-rank-search placeholder="搜尋代號或名稱"><span class="flow-page-count" data-flow-rank-count>顯示 {sum(len(rows) for _, _, rows, _ in display_tabs):,} 筆 · 每榜 Top 50</span></div>
  <div class="ranking-grid">{ranking_sections}</div>
</div>
{TAB_JS}
<script>
(function(){{var root=document.querySelector('[data-flow-ranking-page]');if(!root)return;var input=root.querySelector('[data-flow-rank-search]'),count=root.querySelector('[data-flow-rank-count]');function run(){{var q=(input.value||'').trim().toLowerCase(),visible=0;root.querySelectorAll('[data-flow-rank-row]').forEach(function(row){{var show=!q||(row.dataset.search||'').toLowerCase().indexOf(q)>=0;row.style.display=show?'':'none';if(show)visible+=1;}});count.textContent='目前顯示 '+visible+' 筆排行列';}}input.addEventListener('input',run);}})();
</script>"""
    return html_page("外資／投信買賣超排行", "flow", body)


def default_weekly_holder_risers_payload() -> dict:
    return {"date": "", "previous_date": "", "weekly_dates": [], "updated_at": "", "rows": [], "data_quality": {"state": "missing", "warnings": ["weekly_holder_risers.json 尚未產生"]}, "freshness": {"status": "missing"}}


def load_weekly_holder_risers_payload(path: Path | str = WEEKLY_HOLDER_RISERS_PATH) -> dict:
    path = Path(path)
    if not path.exists():
        return default_weekly_holder_risers_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default_weekly_holder_risers_payload()
    if not isinstance(payload, dict):
        return default_weekly_holder_risers_payload()
    payload["rows"] = [row for row in payload.get("rows") or [] if isinstance(row, dict)]
    payload.setdefault("date", "")
    payload.setdefault("previous_date", "")
    payload.setdefault("weekly_dates", [])
    payload.setdefault("updated_at", "")
    payload.setdefault("data_quality", {"state": "unknown", "warnings": []})
    payload.setdefault("freshness", {})
    return payload


def build_weekly_holder_risers_panel(payload: dict | None = None) -> str:
    payload = payload or load_weekly_holder_risers_payload()
    rows = payload.get("rows") or []
    warning_html = _data_quality_warning(payload, "weekly_holder_risers")
    largest = rows[0] if rows else {}
    largest_text = f'{esc(str(largest.get("security_id") or ""))} {esc(str(largest.get("name") or ""))}（+{fmt_num(largest.get("major_delta_pctpt"), 2)} pt）' if largest else "─"
    return f"""
<div class="card weekly-holder-risers-card" data-weekly-holder-risers data-weekly-holder-date="{esc(str(payload.get('date') or ''))}">
  <div class="section-head"><div><div class="section-label">回顧 6 週大戶股權變化</div><div class="strategy-note">並排最近 6 個週五的 400 張以上大戶持股比例變化；這是觀察池，不等於買進訊號。</div></div><div class="section-date">截至 {esc(str(payload.get('date') or '─'))}</div></div>
  <div class="flow-metric-grid"><div><div class="label">本週上升檔數</div><div class="flow-net">{len(rows):,} 檔</div></div><div><div class="label">最大升幅</div><div class="flow-net">{largest_text}</div></div><div><div class="label">清單狀態</div><div class="flow-net">全部列出</div></div></div>
  <a class="flow-page-link" href="holder-risers.html">查看完整上升表格 →</a>
  {warning_html}
  <div class="signal-foot">資料來源：TDCC 集保戶股權分散表的必要衍生彙總 · 不會直接改變 SFZ／MDA／CaryBot 狀態</div>
</div>"""


def _holder_change_cell(value) -> str:
    if value is None:
        return '<td class="holder-change missing">─</td>'
    number = float(value)
    cls = "pos" if number > 0 else "neg" if number < 0 else "zero"
    prefix = "+" if number > 0 else ""
    return f'<td class="holder-change {cls}">{prefix}{number:.2f}</td>'


def build_weekly_holder_risers_page(payload: dict | None = None) -> str:
    payload = payload or load_weekly_holder_risers_payload()
    rows = payload.get("rows") or []
    market_label = {"上市": "上市", "上櫃": "上櫃", "listed": "上市", "otc": "上櫃"}
    weekly_dates = [str(item) for item in payload.get("weekly_dates") or []][-6:]
    if not weekly_dates:
        weekly_dates = sorted({str(change.get("date") or "") for row in rows for change in row.get("weekly_changes") or [] if change.get("date")})[-6:]
    date_headers = "".join(f'<th title="{esc(data_date)}">{esc(data_date[5:].replace("-", "/"))}</th>' for data_date in weekly_dates)
    row_html = "".join(
        f'<tr data-holder-riser-row data-search="{esc(str(row.get("security_id") or "") + " " + str(row.get("name") or "") + " " + str(row.get("market") or ""))}"><td>{rank:,}</td><td><a href="{stock_href(str(row.get("security_id") or ""))}">{esc(str(row.get("security_id") or ""))} {esc(str(row.get("name") or ""))}</a></td><td>{market_label.get(str(row.get("market") or ""), "─")}</td>{"".join(_holder_change_cell({str(change.get("date") or ""): change.get("delta_pctpt") for change in row.get("weekly_changes") or []}.get(data_date)) for data_date in weekly_dates)}<td class="holder-total">{float(row.get("six_week_delta_pctpt") or 0):+.2f}</td><td>{fmt_num(row.get("major_percent"), 2)}%</td><td class="holder-positive-count">{int(row.get("positive_week_count") or 0)} / {len(weekly_dates) or 6}</td><td>{fmt_num(row.get("major_people"), 0)}</td></tr>'
        for rank, row in enumerate(rows, 1)
    )
    if not row_html:
        row_html = f'<tr><td colspan="{7 + len(weekly_dates)}" style="color:#8b949e">尚無完整股權快照，或本週沒有大戶比例上升的股票。</td></tr>'
    warning_html = _data_quality_warning(payload, "weekly_holder_risers")
    body = f"""
<div class="container" data-holder-risers-page>
  <div class="page-title">回顧 6 週大戶股權變化</div>
  <div class="page-sub">每欄代表該週最後營業日相對前一週的 400 張以上大戶持股比例變動（百分點）。</div>
  <div class="complete-table-note">截至：{esc(str(payload.get('date') or '─'))}｜本週大戶比例上升共 {len(rows):,} 檔，完整列出、不做 Top N 截斷。淡紅為增加、淡綠為減少、黃色為 6 週累積；這是籌碼觀察表，不等於買進訊號。</div>
  {warning_html}
  <div class="flow-page-toolbar"><input type="search" data-holder-riser-search placeholder="搜尋代號、名稱或市場"><span class="flow-page-count" data-holder-riser-count>目前顯示 {len(rows):,} / {len(rows):,} 檔</span></div>
  <div class="card holder-history-card"><div class="holder-history-wrap"><table class="stock-table holder-history-table"><thead><tr><th>#</th><th>股票代號／名稱</th><th>市場</th>{date_headers}<th>6週累積</th><th>大戶持股%</th><th>增加週數</th><th>大戶人數</th></tr></thead><tbody>{row_html}</tbody></table></div></div>
</div>
{TAB_JS}
<script>
(function(){{var root=document.querySelector('[data-holder-risers-page]');if(!root)return;var input=root.querySelector('[data-holder-riser-search]'),count=root.querySelector('[data-holder-riser-count]'),rows=Array.from(root.querySelectorAll('[data-holder-riser-row]'));function run(){{var q=(input.value||'').trim().toLowerCase(),visible=0;rows.forEach(function(row){{var show=!q||(row.dataset.search||'').toLowerCase().indexOf(q)>=0;row.style.display=show?'':'none';if(show)visible+=1;}});count.textContent='目前顯示 '+visible+' / '+rows.length+' 檔';}}input.addEventListener('input',run);}})();
</script>"""
    return html_page("回顧 6 週大戶股權變化", "holder-risers", body)


def build_daily_decisions_panel(payload: dict | None = None, compact: bool = False, limit: int = 5) -> str:
    """Render the existing daily_decisions contract as a human-readable queue."""
    payload = payload or load_daily_decisions_payload()
    decisions = [row for row in (payload.get("decisions") or []) if isinstance(row, dict)]
    counts = payload.get("action_counts") if isinstance(payload.get("action_counts"), dict) else {}
    state_order = ["ENTRY_CANDIDATE", "SETUP", "WATCH", "NO-GO", "RISK_REDUCE", "EXIT_CANDIDATE", "HOLD"]
    state_labels = {
        "ENTRY_CANDIDATE": "可確認",
        "SETUP": "準備中",
        "WATCH": "觀察",
        "NO-GO": "先不做",
    }
    state_colors = {
        "ENTRY_CANDIDATE": "#3fb950",
        "SETUP": "#d2a520",
        "WATCH": "#58a6ff",
        "NO-GO": "#f85149",
    }
    metrics = "".join(
        f'<div class="metric"><div class="metric-num" style="color:{state_colors[state]}">{int(_to_float(counts.get(state), 0) or 0)}</div>'
        f'<div class="metric-label">{state_labels[state]}</div></div>'
        for state in state_order[:4]
    )

    def rank_key(row: dict) -> tuple:
        state = str(row.get("action_state") or "WATCH")
        rank = _to_float(row.get("rank"), 999999) or 999999
        return (state_order.index(state) if state in state_order else len(state_order), rank)

    preferred = [row for row in decisions if str(row.get("action_state") or "") in {"ENTRY_CANDIDATE", "SETUP"}]
    if not preferred:
        preferred = [row for row in decisions if str(row.get("action_state") or "") == "WATCH"]
    if not preferred:
        preferred = [row for row in decisions if str(row.get("action_state") or "") in {"NO-GO", "RISK_REDUCE", "EXIT_CANDIDATE"}]
    selected = sorted(preferred, key=rank_key)[:max(1, limit)]
    decision_rows = ""
    for row in selected:
        state = str(row.get("action_state") or "WATCH")
        label, tag_cls, _ = daily_decision_state_meta(state)
        stock_id = str(row.get("stock_id") or row.get("security_id") or "").strip()
        name = str(row.get("name") or stock_id)
        traffic = row.get("traffic_light") if isinstance(row.get("traffic_light"), dict) else {}
        reason = str(traffic.get("reason") or "")
        if not reason:
            reasons = row.get("reasons") if isinstance(row.get("reasons"), list) else []
            reason = str(reasons[0]) if reasons else "等待更多證據"
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        carybot = evidence.get("carybot") if isinstance(evidence.get("carybot"), dict) else {}
        mda = evidence.get("mda") if isinstance(evidence.get("mda"), dict) else {}
        carybot_type = str(carybot.get("signal_type") or "")
        mda_basket = str(mda.get("basket") or "")
        risk = daily_decision_risk(row)
        detail_tags = []
        if mda_basket:
            detail_tags.append(f'<span class="tag">{esc(mda_basket)}</span>')
        if carybot_type:
            detail_tags.append(f'<span class="tag tag-green">CaryBot {esc(carybot_type)}</span>')
        risk_title = "；".join(risk["reasons"] + risk["warnings"])
        risk_title_attr = f' title="{esc(risk_title)}"' if risk_title else ""
        detail_tags.append(f'<span class="tag {risk["tag_cls"]}" data-market-risk="{esc(risk["level"])}"{risk_title_attr}>{esc(risk["label"])}</span>')
        details = "".join(detail_tags)
        href = stock_href(stock_id) if stock_id else "stocks.html"
        decision_rows += f"""
<div class="action-row daily-decision-row" data-action-state="{esc(state)}">
  <div>
    <a class="stock-link" href="{href}">{esc(stock_id)} {esc(name)}</a>
    <div class="tag-row">{details}<span class="tag {tag_cls}">{esc(label)}</span></div>
  </div>
  <div><div class="label">決策</div><div class="value"><span class="tag {tag_cls}">{esc(label)}</span></div></div>
  <div><div class="label">狀態</div><div class="value">{esc(state)}</div></div>
  <div class="decision-reason">{esc(reason)}</div>
</div>"""
    if not decision_rows:
        decision_rows = '<div class="strategy-note" style="margin-top:10px">目前沒有準備中或觀察中的標的；請先查看 SFZ 完整候選清單。</div>'

    warning = _daily_decision_warning_text(payload)
    warning_html = (
        f'<div class="strategy-note" style="margin-top:12px"><span class="tag tag-yellow">資料品質提醒</span> {esc(warning)}；本區只呈現合約結果，不會自行補推訊號。</div>'
        if warning
        else '<div class="strategy-note" style="margin-top:12px">本區只呈現既有 SFZ／MDA／CaryBot 證據的操作狀態，不會改寫選股門檻。</div>'
    )
    date_text = str(payload.get("date") or "─")
    updated_text = str(payload.get("updated_at") or "")
    updated_line = f"｜產生於 {updated_text}" if updated_text else ""
    compact_note = "先看可確認／準備中，再回到個股卡檢查買點與風控。" if compact else "首頁先給工作順序；完整候選與原始證據請回到 SFZ 雙籃。"
    return f"""
<div class="card daily-decision-card" data-daily-decisions data-daily-decisions-date="{esc(date_text)}">
  <div class="section-head">
    <div>
      <div class="section-label">今日操作總覽</div>
      <div class="strategy-note">{compact_note}</div>
    </div>
    <div class="section-date">資料日：{esc(date_text)}{esc(updated_line)}</div>
  </div>
  <div class="grid grid-4" style="margin-top:12px">{metrics}</div>
  {warning_html}
  <div class="action-list">{decision_rows}</div>
  <div class="signal-foot"><a href="selection.html#sfz-baskets">查看 SFZ 完整候選與雙籃分流 →</a>　<a href="stocks.html">搜尋個股詳細證據 →</a></div>
</div>"""


def _market_color_class(value: str) -> str:
    text = str(value or "").lower()
    if text in {"green", "bullish", "calm"}:
        return "green"
    if text in {"red", "bearish", "fear"}:
        return "red"
    return "yellow"


def _indicator_signal_label(signal: str) -> str:
    text = str(signal or "").lower()
    if text in {"bullish", "calm"}:
        return "偏多"
    if text in {"bearish", "fear"}:
        return "偏空"
    return "中性"


def build_market_sentiment_panel(payload: dict | None = None, compact: bool = False) -> str:
    payload = payload or load_market_sentiment_payload()
    score_value = _num_or_none(payload.get("score"))
    score = int(score_value if score_value is not None else 50)
    color = _market_color_class(payload.get("color") or payload.get("regime"))
    regime_label = payload.get("regime_label") or {"bullish": "偏多", "bearish": "偏空"}.get(str(payload.get("regime")), "中性")
    updated_at = payload.get("updated_at") or "-"
    summary = payload.get("summary") or ""
    indicators = payload.get("indicators") if isinstance(payload.get("indicators"), dict) else {}
    order = ["taiex_ma", "margin_weekly", "short_weekly", "foreign_5d", "breadth", "us_vix"]
    indicator_html = ""
    for key in order:
        item = indicators.get(key) or {}
        label = item.get("label") or key
        signal = item.get("signal") or "neutral"
        dot = _market_color_class(signal)
        display = item.get("display")
        if display in (None, ""):
            value = _num_or_none(item.get("score"))
            display = "-" if value is None else f"{value:.1f}"
        detail = item.get("detail") or _indicator_signal_label(signal)
        indicator_html += f"""
        <div class="market-env-item">
          <div class="k"><span class="env-dot {dot}"></span>{esc(label)}</div>
          <div class="v">{esc(display)}</div>
          <div class="d">{esc(detail)}</div>
        </div>"""
    if not indicator_html:
        indicator_html = """
        <div class="market-env-item">
          <div class="k"><span class="env-dot yellow"></span>資料狀態</div>
          <div class="v">中性</div>
          <div class="d">尚未產生 market_sentiment.json</div>
        </div>"""
    source_notes = payload.get("source_status") or []
    source_text = "；".join(str(x) for x in source_notes[:3]) if source_notes else "資料源檢查正常"
    if compact:
        source_line = ""
    else:
        source_line = f'<div class="market-env-updated">最後更新 {esc(updated_at)}｜{esc(source_text)}</div>'
    return f"""
<div class="card market-env" data-market-sentiment data-market-regime="{esc(payload.get('regime') or 'neutral')}" data-market-score="{score}">
  <div class="section-head">
    <div>
      <div class="section-label">市場環境燈號</div>
      <div class="market-env-summary">{esc(summary)}</div>
    </div>
    <div class="section-date">{esc(payload.get('date') or '-')}</div>
  </div>
  <div class="market-env-head">
    <div class="market-env-score {color}">
      <div class="num">{score}</div>
      <div class="label">{esc(regime_label)}</div>
    </div>
    <div>
      <div class="market-env-grid">{indicator_html}</div>
      {source_line}
    </div>
  </div>
</div>"""


def _sfz_fmt(value, digits: int = 1, suffix: str = "") -> str:
    value = _num_or_none(value)
    if value is None:
        return "-"
    return f"{value:.{digits}f}{suffix}"


def _sfz_money(value) -> str:
    value = _num_or_none(value)
    if value is None:
        return "-"
    if value >= 100_000_000:
        return f"{value / 100_000_000:.1f} 億"
    if value >= 10_000:
        return f"{value / 10_000:.0f} 萬"
    return f"{value:.0f}"


def _sfz_market_cap_label(bucket: str) -> str:
    return {
        "large": "大型股",
        "mid": "中型股",
        "small": "小型股",
        "unknown": "未分類",
    }.get(str(bucket or "unknown"), "未分類")


def _sfz_tag_class(row: dict) -> str:
    text = f"{row.get('basket', '')} {row.get('status', '')}"
    if "風險" in text or "過熱" in text:
        return "tag-red"
    if "盤整" in text:
        return "tag-blue"
    return "tag-green"


def build_sfz_all_controls(
    payload: dict,
    market_sentiment: dict | None = None,
    carybot_payload: dict | None = None,
) -> str:
    stocks = payload.get("stocks") or []
    date_str = payload.get("date") or "-"
    total = len(stocks)
    if not stocks:
        return """
<div class="card">
  <div class="section-label">SFZ 全量候選</div>
  <div class="strategy-note">尚未找到 <code>data/sfz_all.json</code>；請先執行 <code>python run_screener.py</code> 產生全量候選資料。</div>
</div>"""

    carybot_payload = carybot_payload or load_carybot_signals_payload()
    carybot_map = latest_carybot_signals_by_stock(carybot_payload)
    legacy_carybot_map = latest_carybot_markers_by_stock()
    use_legacy_carybot = not carybot_map
    market_sentiment = market_sentiment or load_market_sentiment_payload()
    market_score = _num_or_none(market_sentiment.get("score"))
    market_score = 50 if market_score is None else market_score
    market_bullish = market_score > 60 or str(market_sentiment.get("regime") or "").lower() == "bullish"
    market_bullish_attr = "1" if market_bullish else "0"
    bull_note_style = ' style="display:inline"' if market_bullish else ""
    rows_html = ""
    sorted_stocks = sorted(
        stocks,
        key=lambda row: (
            0
            if str(row.get("stock_id") or row.get("id") or "") in carybot_map
            or (use_legacy_carybot and str(row.get("stock_id") or row.get("id") or "") in legacy_carybot_map)
            else 1,
            _num_or_none(row.get("rank")) or 999999,
        ),
    )
    for row in sorted_stocks:
        sid = str(row.get("stock_id") or row.get("id") or "")
        name = row.get("name") or sid
        basket_text = row.get("basket") or row.get("rank_basket") or row.get("status") or "-"
        tag_cls = _sfz_tag_class(row)
        score = _num_or_none(row.get("score"))
        close = _num_or_none(row.get("close"))
        gain = _num_or_none(row.get("gain_6w"))
        turnover = _num_or_none(row.get("turnover_value")) or 0
        cap_bucket = str(row.get("market_cap_bucket") or "unknown")
        carybot_signal = carybot_map.get(sid)
        has_carybot = "1" if carybot_signal or (use_legacy_carybot and sid in legacy_carybot_map) else "0"
        if carybot_signal:
            carybot_cell = carybot_signal_badge(carybot_signal, double_confirm=True)
        elif use_legacy_carybot and sid in legacy_carybot_map:
            carybot_cell = '<span class="tag tag-blue">CaryBot</span><div class="signal-dates">legacy marker</div>'
        else:
            carybot_cell = '<span class="tag">待接入</span>'
        search = f"{sid} {name} {basket_text} {row.get('sector', '')}".lower()
        rows_html += f"""
<tr data-sfz-row data-rank="{esc(row.get('rank') or 0)}" data-code="{esc(sid)}" data-name="{esc(name)}" data-score="{esc(score if score is not None else 0)}" data-turnover="{esc(turnover)}" data-gain="{esc(gain if gain is not None else 0)}" data-market-cap="{esc(cap_bucket)}" data-carybot="{has_carybot}" data-bullish="{market_bullish_attr}" data-text="{esc(search)}">
  <td><strong>#{esc(row.get('rank') or '')}</strong></td>
  <td><a class="stock-link" href="stocks/{esc(sid)}.html">{esc(sid)} {esc(name)}</a><div class="signal-dates">{esc(row.get('sector') or '-')}</div></td>
  <td><span class="tag {tag_cls}">{esc(basket_text)}</span><div class="signal-dates">{_sfz_market_cap_label(cap_bucket)}</div></td>
  <td class="price-main">{_sfz_fmt(close, 2)}</td>
  <td><span style="color:#58a6ff;font-weight:800">{_sfz_fmt(score, 1)}</span></td>
  <td class="{('pos' if (gain or 0) >= 0 else 'neg')}">{_sfz_fmt(gain, 1, '%')}</td>
  <td>{_sfz_money(turnover)}<div class="signal-dates">5日均量 {_sfz_fmt(row.get('vol5_lot'), 0)} 張</div></td>
  <td>{carybot_cell}</td>
</tr>"""

    return f"""
<div class="card" data-sfz-table data-market-bullish="{market_bullish_attr}">
  <div class="section-head">
    <div>
      <div class="section-label">SFZ 全量候選</div>
      <div class="strategy-note">資料日 {esc(date_str)}，保留每日 Top20 體驗；這裡顯示全部 {total} 檔通過 SFZ/M大條件的候選，再用前端做二次篩選。</div>
    </div>
    <div class="section-date">共 {total} 檔</div>
  </div>
  <div class="sfz-control-bar">
    <div class="sfz-control"><label for="sfzSearch">搜尋</label><input id="sfzSearch" data-sfz-search placeholder="代號、名稱、產業、籃別"></div>
    <div class="sfz-control"><label for="sfzBasketFilter">籃別</label><select id="sfzBasketFilter"><option value="all">全部</option><option value="marching">已發動 / 行進</option><option value="consolidation">盤整 / 觀察</option><option value="risk">過熱 / 風險</option></select></div>
    <div class="sfz-control"><label for="sfzMarketCapFilter">市值</label><select id="sfzMarketCapFilter"><option value="all">全部</option><option value="large">大型股</option><option value="mid">中型股</option><option value="small">小型股</option><option value="unknown">未分類</option></select></div>
    <div class="sfz-control"><label for="sfzVolumeFilter">成交金額</label><select id="sfzVolumeFilter"><option value="all">全部</option><option value="100000000">&gt; 1億</option><option value="50000000">&gt; 5千萬</option></select></div>
    <div class="sfz-control"><label for="sfzCarybotFilter">CaryBot</label><select id="sfzCarybotFilter"><option value="all">全部</option><option value="yes">已有 CaryBot 訊號</option><option value="no">尚未接入</option></select></div>
    <div class="sfz-control"><label for="sfzBullishFilter">大盤情緒</label><select id="sfzBullishFilter"><option value="all">全部</option><option value="yes">大盤偏多訊號</option><option value="no">非偏多環境</option></select></div>
    <div class="sfz-control"><label for="sfzSort">排序</label><select id="sfzSort"><option value="rank">預設排名</option><option value="score">分數高到低</option><option value="turnover">成交金額高到低</option><option value="gain">漲幅高到低</option></select></div>
    <div class="sfz-control"><label for="sfzPageSize">每頁</label><select id="sfzPageSize"><option value="20">20</option><option value="50">50</option><option value="all">全部</option></select></div>
    <div class="sfz-actions"><button type="button" id="sfzShowAll">顯示全部 {total} 檔</button><button type="button" id="sfzReset">重設</button></div>
  </div>
  <div class="sfz-count-line"><span data-sfz-count>目前顯示 Top 20 / {total} 檔</span><span class="sfz-bull-note" data-sfz-bull-note{bull_note_style}>目前大盤偏多，共篩出 {total} 檔，建議搭配 CaryBot 訊號做二次確認</span></div>
  <div style="overflow-x:auto">
    <table class="stock-table">
      <thead><tr><th>#</th><th>股票</th><th>籃別 / 市值</th><th>收盤</th><th>分數</th><th>6週漲幅</th><th>成交金額</th><th>CaryBot</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
  <div class="sfz-pager"><button type="button" data-sfz-prev>上一頁</button><span data-sfz-page>第 1 / 1 頁</span><button type="button" data-sfz-next>下一頁</button></div>
</div>
<script>
(function(){{
  function bucketOf(row){{
    const text=(row.querySelector('td:nth-child(3)')?.textContent||'');
    if(text.indexOf('風險')>=0 || text.indexOf('過熱')>=0) return 'risk';
    if(text.indexOf('盤整')>=0 || text.indexOf('觀察')>=0) return 'consolidation';
    return 'marching';
  }}
  function init(){{
    document.querySelectorAll('[data-sfz-table]').forEach(function(root){{
      const tbody=root.querySelector('tbody');
      const rows=Array.from(root.querySelectorAll('[data-sfz-row]'));
      const q=root.querySelector('[data-sfz-search]');
      const basket=root.querySelector('#sfzBasketFilter');
      const cap=root.querySelector('#sfzMarketCapFilter');
      const volume=root.querySelector('#sfzVolumeFilter');
      const carybot=root.querySelector('#sfzCarybotFilter');
      const bullish=root.querySelector('#sfzBullishFilter');
      const sort=root.querySelector('#sfzSort');
      const pageSize=root.querySelector('#sfzPageSize');
      const count=root.querySelector('[data-sfz-count]');
      const pageText=root.querySelector('[data-sfz-page]');
      const prev=root.querySelector('[data-sfz-prev]');
      const next=root.querySelector('[data-sfz-next]');
      const showAll=root.querySelector('#sfzShowAll');
      const reset=root.querySelector('#sfzReset');
      let page=1;
      function num(v){{ const n=Number(v); return Number.isFinite(n)?n:0; }}
      function carybotFirst(a,b){{ const av=a.dataset.carybot==='1'?0:1; const bv=b.dataset.carybot==='1'?0:1; return av-bv; }}
      function selected(){{
        const query=(q&&q.value||'').trim().toLowerCase();
        const minTurnover=volume&&volume.value!=='all'?Number(volume.value):0;
        const capValue=cap&&cap.value||'all';
        const basketValue=basket&&basket.value||'all';
        const carybotValue=carybot&&carybot.value||'all';
        const bullishValue=bullish&&bullish.value||'all';
        return rows.filter(function(row){{
          let ok=!query || (row.dataset.text||'').indexOf(query)>=0;
          if(capValue!=='all') ok=ok && row.dataset.marketCap===capValue;
          if(minTurnover>0) ok=ok && num(row.dataset.turnover)>=minTurnover;
          if(basketValue!=='all') ok=ok && bucketOf(row)===basketValue;
          if(carybotValue==='yes') ok=ok && row.dataset.carybot==='1';
          if(carybotValue==='no') ok=ok && row.dataset.carybot!=='1';
          if(bullishValue==='yes') ok=ok && row.dataset.bullish==='1';
          if(bullishValue==='no') ok=ok && row.dataset.bullish!=='1';
          return ok;
        }}).sort(function(a,b){{
          const key=sort&&sort.value||'rank';
          if(key==='score') return num(b.dataset.score)-num(a.dataset.score);
          if(key==='turnover') return num(b.dataset.turnover)-num(a.dataset.turnover);
          if(key==='gain') return num(b.dataset.gain)-num(a.dataset.gain);
          return carybotFirst(a,b)||num(a.dataset.rank)-num(b.dataset.rank);
        }});
      }}
      function render(){{
        const shown=selected();
        const sizeValue=pageSize&&pageSize.value||'20';
        const per=sizeValue==='all'?Math.max(shown.length,1):Number(sizeValue||20);
        const pages=Math.max(1,Math.ceil(shown.length/per));
        if(page>pages) page=pages;
        const pageRows=shown.slice((page-1)*per,page*per);
        rows.forEach(function(row){{ row.style.display='none'; }});
        shown.forEach(function(row){{ tbody.appendChild(row); }});
        pageRows.forEach(function(row){{ row.style.display=''; }});
        if(count) count.textContent='本頁 '+pageRows.length+' 檔 / 篩選 '+shown.length+' 檔 / 全部 '+rows.length+' 檔';
        if(pageText) pageText.textContent='第 '+page+' / '+pages+' 頁';
        if(prev) prev.disabled=page<=1;
        if(next) next.disabled=page>=pages;
      }}
      [q,basket,cap,volume,carybot,bullish,sort,pageSize].forEach(function(el){{ if(el) el.addEventListener(el===q?'input':'change',function(){{ page=1; render(); }}); }});
      if(prev) prev.addEventListener('click',function(){{ page=Math.max(1,page-1); render(); }});
      if(next) next.addEventListener('click',function(){{ page=page+1; render(); }});
      if(showAll) showAll.addEventListener('click',function(){{ if(pageSize) pageSize.value='all'; page=1; render(); }});
      if(reset) reset.addEventListener('click',function(){{ if(q) q.value=''; if(basket) basket.value='all'; if(cap) cap.value='all'; if(volume) volume.value='all'; if(carybot) carybot.value='all'; if(bullish) bullish.value='all'; if(sort) sort.value='rank'; if(pageSize) pageSize.value='20'; page=1; render(); }});
      render();
    }});
  }}
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded', init); else init();
}})();
</script>"""


def build_filter_steps(steps: list[dict]) -> str:
    if not steps:
        return ""
    cards = ""
    colors = ["#58a6ff", "#3fb950", "#f0883e", "#a78bfa"]
    for i, step in enumerate(steps):
        color = colors[i % len(colors)]
        cards += f"""
<div class="filter-step">
  <div class="step-count" style="color:{color}">{step['count']}</div>
  <div class="step-label">{step['step']}</div>
  <div class="step-cond">{step['condition']}</div>
</div>"""
    return f'<div class="filter-steps">{cards}</div>'


def build_notes(notes_text: str) -> str:
    if not notes_text:
        return ""
    def keep_note(text: str) -> bool:
        return "停損紀律" not in text

    def clean_note_detail(text: str) -> str:
        text = text.strip().replace(chr(10), " ")
        if "MA5×0.985" in text or "MA5×98.5%" in text:
            return "等待 Williams 買入區或 MA20 站回，不追高。"
        text = text.replace("強勢追漲中（🟡）", "漲幅偏高")
        text = text.replace("強勢追漲中", "漲幅偏高")
        text = text.replace("健康整理（🟢）", "整理觀察")
        text = text.replace("健康整理", "整理觀察")
        return text

    items = re.findall(r"\d+\.\s+\*\*(.+?)\*\*[：:]\s*(.*?)(?=\n\d+\.|\Z)", notes_text, re.DOTALL)
    if not items:
        # fallback: split by numbered lines
        lines = [l.strip() for l in notes_text.split("\n") if l.strip() and re.match(r"\d+\.", l.strip())]
        def clean_line(l):
            return re.sub(r'^\d+\.\s*', '', l).replace('**', '')
        items_html = "\n".join(f"<li>{clean_note_detail(clean_line(l))}</li>" for l in lines if keep_note(clean_line(l)))
    else:
        items_html = "\n".join(
            f"<li><strong>{t}：</strong>{clean_note_detail(d)}</li>"
            for t, d in items
            if keep_note(f"{t} {d}")
        )
    return f'<ul class="notes-list">{items_html}</ul>'


def _to_float(value: str, default: float = 0.0) -> float:
    try:
        s = str(value).replace("%", "").replace("+", "").replace(",", "").strip()
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if m:
            s = m.group(0)
        return float(s)
    except Exception:
        return default

def rank_fallback_score(stock_no: int) -> float:
    return max(0.0, min(100.0, round((21 - stock_no) * 5.0, 1)))


def _legacy_score_from_text(*values) -> float | None:
    joined = " ".join(str(v or "") for v in values)
    for token in ("綜合評分", "評分"):
        idx = joined.find(token)
        if idx >= 0:
            m = re.search(r"([0-9]+(?:\.[0-9]+)?)", joined[idx: idx + 48])
            if m:
                val = _to_float(m.group(1), None)
                return None if val is None else max(0.0, min(100.0, round(val, 1)))
    m = re.search(r"\bscore\s*[:：]\s*([0-9]+(?:\.[0-9]+)?)", joined, re.I)
    if m:
        val = _to_float(m.group(1), None)
        return None if val is None else max(0.0, min(100.0, round(val, 1)))
    return None

def normalize_score_value(value, *context) -> float:
    raw = _to_float(value, 0.0)
    embedded = _legacy_score_from_text(*context)
    if embedded is not None:
        return embedded
    if raw > 100:
        return max(0.0, min(100.0, round(raw / 2.0, 1)))
    return max(0.0, min(100.0, round(raw, 1)))


def normalize_report_scores(reports: list[dict]) -> list[dict]:
    for report in reports:
        for stock in report.get("stocks", []):
            old_score = stock.get("score")
            source = str(stock.get("score_source") or "")
            normalized = normalize_score_value(old_score, stock.get("name", ""), stock.get("status", ""))
            if _to_float(old_score, 0) != normalized:
                stock["score"] = normalized
                stock["score_source"] = "0-100 normalized Score"
            if "?" in str(stock.get("name", "")) and _legacy_score_from_text(stock.get("name", "")):
                stock["name"] = str(stock.get("name", "")).split("?", 1)[0].strip()
            if "rank fallback" in source.lower() and _to_float(old_score, 0) <= 100:
                stock["score_source"] = "0-100 rank fallback"
    return reports


def is_overheated_stock(s: dict) -> bool:
    return _shared_is_overheated(s)


def overheat_reasons(s: dict) -> list[str]:
    return _shared_overheat_reasons(s)


def coming_soon_block(title: str, body: str, data_check: str = "", ready: bool = False, inline: bool = False) -> str:
    data_attr = f' data-source="{esc(data_check)}"' if data_check else ""
    open_attr = " open" if ready else ""
    ready_cls = " data-ready ready" if ready else ""
    inline_cls = " inline-placeholder" if inline else ""
    return f'''<details class="coming-soon placeholder-block{ready_cls}{inline_cls}"{data_attr}{open_attr}>
  <summary>{esc(title)}</summary>
  <div class="placeholder-body">{body}</div>
</details>'''


def rr_warning_bar(decision: dict) -> str:
    rr = decision.get("rr")
    rr_text = decision.get("rr_text") or (f"1:{float(rr):.1f}" if rr is not None else "1:─")
    return f'<div class="warning-banner">⚠ R:R = {esc(rr_text)} 低於建議門檻 1.5，不建議建倉</div>' if rr is not None and rr < 1.5 else ""


def stock_traffic_light(stock_id: str, s: dict, tech: dict, decision: dict, daily: list[dict], chip_series: list[dict]) -> str:
    indicator = indicator_snapshot(daily)
    chip_total = _sum_recent(chip_series, "total", 5)
    state = evaluate_traffic_light(s, tech, decision, indicator, chip_total)
    return f'''<div class="traffic-light {state["css_class"]} signal-light {state["light_class"]}"><div class="signal"><span>{state["icon_entity"]}</span><span>{state["headline"]}</span><span class="signal-label">{state["label"]}</span></div><div class="reason">{esc(state["reason"])}</div></div>'''

def build_stock_mda_abc_block(stock_id: str, s: dict, daily: list[dict], tech: dict, chip_series: list[dict], holding: dict) -> str:
    if not daily:
        return ""
    scored = mda_score_stock(s, True)
    abc = mda_abc_checks(s, daily, tech, chip_series, holding)
    pressure = pressure_absorption_analysis(stock_id, daily, chip_series, read_margin_series(stock_id), tech)
    checks = "".join(_m_check(label, cls) for label, cls in abc.get("items", []))
    reason_html = scored.get("reason", "")
    risk_html = scored.get("risk_reason", "")
    pressure_summary = esc(pressure.get("summary", ""))
    return f'''<div class="card"><div class="section-label">M大 ABC 拆分</div><div class="grid grid-3"><div class="metric"><div class="metric-num">{fmt_num(scored.get("score"), 0)}</div><div class="metric-label">M大分數</div></div><div class="metric"><div class="metric-num">{fmt_num(abc.get("a_score"), 0)}</div><div class="metric-label">A 趨勢</div></div><div class="metric"><div class="metric-num">{fmt_num(abc.get("b1_score"), 0)} / {fmt_num(abc.get("b2_score"), 0)}</div><div class="metric-label">B1 籌碼 / B2 壓力</div></div></div><div class="m-checks" style="margin-top:10px">{checks}</div><div class="strategy-note" style="margin-top:10px">{reason_html}<br>{risk_html}<br>{pressure_summary}</div></div>'''

def disclaimer_modal_html() -> str:
    return '''<div class="disclaimer-modal" data-disclaimer-modal><div class="disclaimer-box"><h2>免責聲明</h2><p>本站內容僅供研究與交易紀律檢核，不構成投資建議；下單前請自行確認風險。</p><button type="button">已了解，不再顯示</button></div></div>'''


def classify_basket(s: dict) -> str:
    return site_basket_key(s)


def basket_label(basket: str) -> str:
    return {
        "marching": "行進籃",
        "consolidation": "盤整籃",
        "risk": "過熱/風險",
    }.get(basket, "未分類")


def stock_trade_context(s: dict) -> tuple[list[dict], dict, dict]:
    sid = s.get("id", "")
    daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily") if sid else []
    tech = technical_snapshot(daily, s) if daily else {}
    decision = build_trade_decision(tech, s)
    return daily, tech, decision


def daily_change_text(rows: list[dict]) -> tuple[str, str]:
    if len(rows) < 2:
        return "單日 ─", ""
    close = rows[-1].get("close")
    prev = rows[-2].get("close")
    if not close or not prev:
        return "單日 ─", ""
    diff = close - prev
    pct = (close / prev - 1) * 100
    cls = "pos" if diff >= 0 else "neg"
    return f"{diff:+.2f}（{pct:+.2f}%）", cls


def b1_force_status(s: dict, chip_series: list[dict] | None = None, holding: dict | None = None) -> str:
    sid = s.get("id", "")
    chip_series = chip_series if chip_series is not None else read_chip_series(sid)
    holding = holding if holding is not None else read_holding_summary(sid)
    if not chip_series and not holding:
        return "B1資料不足"
    metrics = chip_trend_metrics(chip_series or [], holding or {})
    holding_series = read_holding_series(sid) if sid else []
    total_10d = metrics.get("total_10d")
    foreign_10d = metrics.get("foreign_10d")
    major_delta = metrics.get("major_delta")
    retail_delta = metrics.get("retail_delta")
    major_4w_delta = None
    retail_4w_delta = None
    latest_major = None
    if len(holding_series) >= 5:
        latest_major = holding_series[-1].get("major")
        latest_retail = holding_series[-1].get("retail")
        base_major = holding_series[-5].get("major")
        base_retail = holding_series[-5].get("retail")
        if latest_major is not None and base_major is not None:
            major_4w_delta = latest_major - base_major
        if latest_retail is not None and base_retail is not None:
            retail_4w_delta = latest_retail - base_retail
    elif holding:
        latest_major = (holding.get("latest") or {}).get("major")

    structure_ok = (
        (major_4w_delta is not None and major_4w_delta >= 0.5)
        or (latest_major is not None and latest_major >= 55 and (major_delta is None or major_delta >= -0.3))
    )
    structure_bad = (
        (major_4w_delta is not None and major_4w_delta <= -1.0)
        or (major_delta is not None and major_delta <= -0.8)
    )
    retail_bad = (
        (retail_4w_delta is not None and retail_4w_delta >= 1.0)
        or (retail_delta is not None and retail_delta >= 0.8)
    )
    flow_bad = (total_10d is not None and total_10d < 0) and (foreign_10d is not None and foreign_10d < 0)

    if structure_bad and (retail_bad or flow_bad):
        return "B1/B2籌碼偏弱"
    if structure_ok:
        return "B1主力未離開"
    if flow_bad:
        return "B1短線轉弱"
    return "B1資料不足"


def basket_reason(s: dict, tech: dict | None = None, chip_series: list[dict] | None = None, holding: dict | None = None) -> str:
    basket_assessment = site_basket_assessment(s)
    basket = basket_assessment["basket"]
    status = s.get("status", "")
    icon = s.get("icon", "")
    tech = tech or {}
    checks = []
    trend = tech.get("trend") or tech.get("trend_pattern")
    volume_price = tech.get("volume_price") or "量價資料不足"
    sid = s.get("id", "")
    force_status = b1_force_status(s, chip_series, holding)
    daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily") if sid else []
    pressure = pressure_absorption_analysis(sid, daily, chip_series or [], read_margin_series(sid), tech) if sid else {}

    if basket == "marching":
        checks.append("行進籃")
        if basket_assessment["score_marching"]:
            checks.append(basket_assessment["score_label"])
        if basket_assessment["gain_marching"]:
            checks.append(basket_assessment["gain_label"])
        if trend and "多" in str(trend):
            checks.append(str(trend))
        checks.append(str(volume_price))
        checks.append(force_status)
    elif basket == "consolidation":
        checks.append("未進入過熱區")
        if trend:
            checks.append(str(trend))
        checks.append(str(volume_price))
        checks.append(force_status)
        if pressure.get("level"):
            checks.append(str(pressure.get("level")))
    else:
        for reason in overheat_reasons(s):
            checks.append(reason)
        if icon == "🔴" or "超買" in status:
            checks.append("原報告風險/超買")
        if basket_assessment["gain_marching"]:
            checks.append("漲幅偏大")
        checks.append(str(volume_price))
        checks.append(force_status)
        checks.append("不追高，等回測")

    seen = []
    for item in checks:
        if item and item not in seen:
            seen.append(item)
    return " / ".join(seen) if seen else "等待更多技術與籌碼確認"


def split_baskets(stocks: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """用現有每日報告欄位先做網站層分籃；正式版可改讀 JSON。"""
    marching, consolidation, risk = [], [], []
    for s in stocks:
        s = enrich_stock_fields(s)
        basket = classify_basket(s)
        if basket == "risk":
            risk.append(s)
        elif basket == "marching":
            marching.append(s)
        else:
            consolidation.append(s)
    return marching, consolidation, risk


def load_push_log() -> dict[tuple[str, str], list[dict]]:
    """讀取可選的推播台帳：date,stock_id,status,sent_at,channel。"""
    if not PUSH_LOG_PATH.exists():
        return {}
    rows: dict[tuple[str, str], list[dict]] = {}
    with PUSH_LOG_PATH.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            date = (row.get("date") or row.get("signal_date") or "").strip()
            stock_id = (row.get("stock_id") or row.get("id") or "").strip()
            if not date or not stock_id:
                continue
            rows.setdefault((date, stock_id), []).append(row)
    return rows


def event_trade_snapshot(s: dict, report_date: str) -> dict:
    event_stock = dict(s)
    event_stock["report_date"] = report_date
    event_stock = enrich_stock_fields(event_stock)
    sid = event_stock.get("id", "")
    rows = []
    if sid:
        rows = merge_report_close(read_price_history(sid), event_stock)
        rows = sorted(rows, key=lambda r: r.get("date", ""))
        rows = [r for r in rows if r.get("date", "") <= report_date]
    tech = technical_snapshot(rows, event_stock) if rows else {}
    decision = build_trade_decision(tech, event_stock)
    return {
        "buy_zone": decision.get("entry_range") or decision.get("entry_text") or event_stock.get("entry", "─"),
        "raw_entry": event_stock.get("entry", "─"),
    }


def build_signal_ledger(reports: list[dict]) -> dict[str, dict]:
    push_log = load_push_log()
    ledger: dict[str, dict] = {}
    for report in sorted(reports, key=lambda r: r.get("date", "")):
        date = report.get("date", "")
        for s in report.get("stocks", []):
            s = enrich_stock_fields(s)
            stock_id = s.get("id", "")
            if not stock_id:
                continue
            item = ledger.setdefault(stock_id, {
                "id": stock_id,
                "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
                "events": [],
                "push_count": 0,
            })
            logs = push_log.get((date, stock_id), [])
            pushed = any((x.get("status", "").lower() in {"ok", "sent", "success", "pushed", "done"} or x.get("sent_at")) for x in logs)
            trade_snapshot = event_trade_snapshot(s, date)
            item["events"].append({
                "date": date,
                "basket": classify_basket(s),
                "entry": trade_snapshot["buy_zone"],
                "raw_entry": trade_snapshot["raw_entry"],
                "price": s.get("price", "─"),
                "score": s.get("score", "─"),
                "score_source": s.get("score_source", "原始報告 Score"),
                "pushed": pushed,
                "log_count": len(logs),
            })
            if pushed:
                item["push_count"] += 1
            item["name"] = s.get("name", item["name"])
    return ledger


def signal_summary_html(stock_id: str, ledger: dict[str, dict]) -> str:
    item = ledger.get(stock_id)
    if not item:
        return '<div class="signal-foot">歷史訊號：首次出現，尚無摘要。</div>'
    events = item["events"]
    first = events[0]
    latest = events[-1]
    baskets = " / ".join(dict.fromkeys(basket_label(e["basket"]) for e in events))
    if PUSH_LOG_PATH.exists():
        push = f'推播 <span class="push-ok">{item["push_count"]}</span> / {len(events)}'
    else:
        push = '<span class="push-wait">推播台帳待串接</span>'
    return (
        '<div class="signal-foot">'
        + f'歷史訊號：<strong>{len(events)}</strong> 次 ｜ 首見 {first["date"]} ｜ 最近 {latest["date"]} ｜ {baskets} ｜ {push}'
        + '</div>'
    )


def find_latest_stock_map(reports: list[dict]) -> dict[str, dict]:
    stocks: dict[str, dict] = {}
    for report in reversed(reports):
        for s in report.get("stocks", []):
            sid = s.get("id", "")
            if sid:
                item = dict(s)
                if report.get("date", "") and not item.get("report_date"):
                    item["report_date"] = report.get("date", "")
                item = enrich_stock_fields(item)
                stocks[sid] = item
    return stocks


def cached_stock_ids() -> set[str]:
    ids: set[str] = set()
    local_dirs = [LOCAL_PRICE_DIR, LOCAL_CHIP_DIR, LOCAL_HOLDING_DIR, LOCAL_FOREIGN_SHAREHOLDING_DIR, LOCAL_MARGIN_DIR]
    for folder in local_dirs:
        if folder.exists():
            for path in folder.glob("*.csv"):
                sid = path.stem.strip()
                if re.fullmatch(r"\d{4,6}", sid):
                    ids.add(sid)
    if V44_PRICE_DIR.exists():
        for path in V44_PRICE_DIR.glob("*.csv"):
            sid = path.stem.strip()
            if re.fullmatch(r"\d{4,6}", sid) and _price_cache_is_fresh(path):
                ids.add(sid)
    return ids


def _price_cache_last_date(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            last = ""
            for line in fh:
                if line.strip():
                    last = line.strip()
        if not last or last.lower().startswith("date"):
            return ""
        return last.split(",", 1)[0]
    except Exception:
        return ""


def _price_cache_is_fresh(path: Path) -> bool:
    last_date = _price_cache_last_date(path)
    if not last_date:
        return False
    stale_days = int(os.environ.get("SITE_PRICE_STALE_DAYS", "14"))
    min_date = os.environ.get(
        "SITE_PRICE_MIN_DATE",
        (datetime.today() - timedelta(days=stale_days)).strftime("%Y-%m-%d"),
    )
    return last_date >= min_date


def build_stock_query_map(reports: list[dict]) -> dict[str, dict]:
    stock_map = find_latest_stock_map(reports)
    refs = load_stock_reference_map()
    for sid in sorted(cached_stock_ids()):
        if sid in stock_map:
            if not stock_map[sid].get("name") and refs.get(sid, {}).get("name"):
                stock_map[sid]["name"] = refs[sid]["name"]
            if not stock_map[sid].get("market") and refs.get(sid, {}).get("market"):
                stock_map[sid]["market"] = refs[sid]["market"]
            stock_map[sid]["query_only"] = False
            continue
        ref = refs.get(sid, {})
        item = {
            "id": sid,
            "name": ref.get("name", ""),
            "market": ref.get("market", ""),
            "icon": "",
            "status": "個股查詢",
            "score": "─",
            "score_source": "快取個股",
            "query_only": True,
        }
        stock_map[sid] = enrich_stock_fields(item)
    for item in stock_map.values():
        item.setdefault("query_only", False)
    return stock_map


def read_price_history(stock_id: str, limit: int = 760) -> list[dict]:
    stock_id = str(stock_id)
    if stock_id in _PRICE_HISTORY_CACHE:
        return _PRICE_HISTORY_CACHE[stock_id][-limit:]
    path = LOCAL_PRICE_DIR / f"{stock_id}.csv"
    if not path.exists():
        path = V44_PRICE_DIR / f"{stock_id}.csv"
    rows = []
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    rows.append({
                        "date": row.get("date") or row.get("Date"),
                        "open": float(row.get("open") or row.get("Open")),
                        "high": float(row.get("high") or row.get("max") or row.get("High")),
                        "low": float(row.get("low") or row.get("min") or row.get("Low")),
                        "close": float(row.get("close") or row.get("Close")),
                        "volume": float(row.get("volume") or row.get("Trading_Volume") or row.get("Volume") or 0),
                    })
                except Exception:
                    continue
    if _price_rows_need_refresh(rows):
        months = int(os.environ.get("V44_FETCH_MONTHS", "24"))
        fresh_rows = fetch_v44_price_history(stock_id, months=months)
        if fresh_rows and (not rows or fresh_rows[-1].get("date", "") > rows[-1].get("date", "")):
            rows = fresh_rows
            write_price_history(stock_id, rows)
    _PRICE_HISTORY_CACHE[stock_id] = rows
    return rows[-limit:]


def _price_rows_need_refresh(rows: list[dict]) -> bool:
    if os.environ.get("SITE_REFRESH_STALE_PRICES", "0") == "0":
        return not rows
    if not rows:
        return True
    stale_days = int(os.environ.get("SITE_PRICE_STALE_DAYS", "14"))
    min_date = os.environ.get(
        "SITE_PRICE_MIN_DATE",
        (datetime.today() - timedelta(days=stale_days)).strftime("%Y-%m-%d"),
    )
    return str(rows[-1].get("date", "")) < min_date


def write_price_history(stock_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    try:
        LOCAL_PRICE_DIR.mkdir(parents=True, exist_ok=True)
        path = LOCAL_PRICE_DIR / f"{stock_id}.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["date", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    "date": row.get("date", ""),
                    "open": row.get("open", ""),
                    "high": row.get("high", ""),
                    "low": row.get("low", ""),
                    "close": row.get("close", ""),
                    "volume": row.get("volume", 0),
                })
    except Exception as e:
        print(f"   [WARN] write price cache failed {stock_id}: {e}", flush=True)


def read_csv_rows(primary: Path, fallback: Path | None = None) -> list[dict]:
    path = primary if primary.exists() else fallback
    if not path or not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def read_chip_summary(stock_id: str) -> dict:
    rows = read_csv_rows(LOCAL_CHIP_DIR / f"{stock_id}.csv", V44_CHIP_DIR / f"{stock_id}.csv")
    if not rows:
        return {}
    by_date: dict[str, dict] = {}
    for r in rows:
        date = r.get("date", "")
        name = r.get("name", "")
        try:
            net = (float(r.get("buy") or 0) - float(r.get("sell") or 0)) / 1000
        except Exception:
            continue
        d = by_date.setdefault(date, {"foreign": 0.0, "trust": 0.0, "dealer": 0.0, "total": 0.0})
        if "Foreign" in name:
            d["foreign"] += net
        elif "Investment_Trust" in name:
            d["trust"] += net
        elif "Dealer" in name:
            d["dealer"] += net
        d["total"] += net
    if not by_date:
        return {}
    dates = sorted(by_date)
    latest = by_date[dates[-1]]
    last5 = dates[-5:]
    last10 = dates[-10:]
    sum5 = {k: sum(by_date[d].get(k, 0.0) for d in last5) for k in ["foreign", "trust", "dealer", "total"]}
    sum10 = {k: sum(by_date[d].get(k, 0.0) for d in last10) for k in ["foreign", "trust", "dealer", "total"]}
    return {"date": dates[-1], "latest": latest, "sum5": sum5, "sum10": sum10}


def read_chip_series(stock_id: str) -> list[dict]:
    rows = read_csv_rows(LOCAL_CHIP_DIR / f"{stock_id}.csv", V44_CHIP_DIR / f"{stock_id}.csv")
    if not rows:
        return []
    by_date: dict[str, dict] = {}
    for r in rows:
        date = r.get("date", "")
        name = r.get("name", "")
        try:
            net = (float(r.get("buy") or 0) - float(r.get("sell") or 0)) / 1000
        except Exception:
            continue
        d = by_date.setdefault(date, {"date": date, "foreign": 0.0, "trust": 0.0, "dealer": 0.0, "total": 0.0})
        if "Foreign" in name:
            d["foreign"] += net
        elif "Investment_Trust" in name:
            d["trust"] += net
        elif "Dealer" in name:
            d["dealer"] += net
        d["total"] += net
    return [by_date[d] for d in sorted(by_date)]


def chip_trend_metrics(chip_series: list[dict], holding: dict) -> dict:
    last10 = chip_series[-10:]
    foreign_vals = [float(x.get("foreign") or 0) for x in last10]
    total_vals = [float(x.get("total") or 0) for x in last10]

    buy_streak = 0
    for v in reversed(foreign_vals):
        if v > 0:
            buy_streak += 1
        else:
            break

    sell_streak = 0
    for v in reversed(foreign_vals):
        if v < 0:
            sell_streak += 1
        else:
            break

    latest_h = holding.get("latest", {}) if holding else {}
    prev_h = holding.get("prev", {}) if holding else {}
    major_delta = None
    retail_delta = None
    if latest_h and prev_h:
        if latest_h.get("major") is not None and prev_h.get("major") is not None:
            major_delta = latest_h.get("major", 0) - prev_h.get("major", 0)
        if latest_h.get("retail") is not None and prev_h.get("retail") is not None:
            retail_delta = latest_h.get("retail", 0) - prev_h.get("retail", 0)

    return {
        "foreign_buy_streak": buy_streak,
        "foreign_sell_streak": sell_streak,
        "foreign_10d": sum(foreign_vals),
        "total_10d": sum(total_vals),
        "major_delta": major_delta,
        "retail_delta": retail_delta,
    }


def chip_flow_payload(series: list[dict]) -> list[dict]:
    return [
        {
            "date": item.get("date", ""),
            "foreign": item.get("foreign"),
            "trust": item.get("trust"),
            "dealer": item.get("dealer"),
            "total": item.get("total"),
        }
        for item in series[-10:]
    ]


def main_force_payload(chip_series: list[dict], price_rows: list[dict]) -> list[dict]:
    close_by_date = {r.get("date"): r.get("close") for r in price_rows}
    change_pct_by_date = price_change_pct_by_date(price_rows)
    rows = []
    last_close = None
    for item in chip_series[-30:]:
        date = item.get("date", "")
        close = close_by_date.get(date)
        if close is not None:
            last_close = close
        rows.append({
            "date": date,
            "total": item.get("total"),
            "close": last_close,
            "changePct": change_pct_by_date.get(date),
        })
    return [r for r in rows if r.get("close") is not None]


def price_change_pct_by_date(price_rows: list[dict]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    prev_close = None
    for row in price_rows:
        date = row.get("date")
        close = row.get("close")
        try:
            close_f = float(close)
            prev_f = float(prev_close) if prev_close is not None else None
        except Exception:
            close_f = None
            prev_f = None
        if date:
            result[date] = ((close_f / prev_f - 1) * 100) if close_f is not None and prev_f else None
        if close_f is not None:
            prev_close = close_f
    return result


def _holding_group(level: str) -> str:
    return _shared_holding_group(level)


def read_holding_summary(stock_id: str) -> dict:
    rows = read_csv_rows(LOCAL_HOLDING_DIR / f"{stock_id}.csv", V44_HOLDING_DIR / f"{stock_id}.csv")
    if not rows:
        return {}
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r.get("date", ""), []).append(r)
    dates = sorted(d for d in by_date if d)
    if not dates:
        return {}

    def summarize(date: str) -> dict:
        result = {
            "major": 0.0,
            "large": 0.0,
            "middle": 0.0,
            "retail": 0.0,
            "total_people": None,
            "major_people": 0,
            "middle_people": 0,
            "retail_people": 0,
        }
        for r in by_date.get(date, []):
            level = r.get("HoldingSharesLevel", "")
            try:
                pct = float(r.get("percent") or 0)
                people = int(float(r.get("people") or 0))
            except Exception:
                continue
            if level == "total":
                result["total_people"] = people
                continue
            group = _holding_group(level)
            if group in result:
                result[group] += pct
            if group == "major":
                result["major_people"] += people
            elif group == "middle":
                result["middle_people"] += people
            elif group == "retail":
                result["retail_people"] += people
        return result

    latest_date = dates[-1]
    latest = summarize(latest_date)
    prev = summarize(dates[-2]) if len(dates) >= 2 else {}
    return {"date": latest_date, "latest": latest, "prev": prev}


def read_holding_series(stock_id: str) -> list[dict]:
    rows = read_csv_rows(LOCAL_HOLDING_DIR / f"{stock_id}.csv", V44_HOLDING_DIR / f"{stock_id}.csv")
    if not rows:
        return []
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(r.get("date", ""), []).append(r)
    series = []
    for date in sorted(d for d in by_date if d):
        item = {
            "date": date,
            "major": 0.0,
            "large": 0.0,
            "middle": 0.0,
            "retail": 0.0,
            "total_people": None,
            "major_people": 0,
            "middle_people": 0,
            "retail_people": 0,
        }
        for r in by_date.get(date, []):
            level = r.get("HoldingSharesLevel", "")
            try:
                pct = float(r.get("percent") or 0)
                people = int(float(r.get("people") or 0))
            except Exception:
                continue
            if level == "total":
                item["total_people"] = people
                continue
            group = _holding_group(level)
            if group in {"major", "middle", "retail"}:
                item[group] += pct
            if group == "major":
                item["major_people"] += people
            elif group == "middle":
                item["middle_people"] += people
            elif group == "retail":
                item["retail_people"] += people
        series.append(item)
    return series


def read_foreign_shareholding_series(stock_id: str) -> list[dict]:
    rows = read_csv_rows(
        LOCAL_FOREIGN_SHAREHOLDING_DIR / f"{stock_id}.csv",
        V44_FOREIGN_SHAREHOLDING_DIR / f"{stock_id}.csv",
    )
    out = []
    for r in rows:
        date_str = r.get("date", "")
        shares = _to_float(r.get("foreign_shares_lot"), None)
        if shares is None:
            raw_shares = _to_float(r.get("foreign_shares") or r.get("ForeignInvestmentShares"), None)
            shares = raw_shares / 1000 if raw_shares is not None else None
        ratio = _to_float(r.get("foreign_ratio") or r.get("ForeignInvestmentSharesRatio"), None)
        if date_str and (shares is not None or ratio is not None):
            out.append({"date": date_str, "foreign_shares": shares, "foreign_ratio": ratio})
    return sorted(out, key=lambda x: x.get("date", ""))


def read_margin_series(stock_id: str) -> list[dict]:
    rows = read_csv_rows(LOCAL_MARGIN_DIR / f"{stock_id}.csv", V44_MARGIN_DIR / f"{stock_id}.csv")
    out = []
    for r in rows:
        date_str = r.get("date", "")
        margin = _to_float(r.get("margin_balance") or r.get("MarginPurchaseTodayBalance"), None)
        short = _to_float(r.get("short_balance") or r.get("ShortSaleTodayBalance"), None)
        if date_str and (margin is not None or short is not None):
            out.append({"date": date_str, "margin_balance": margin, "short_balance": short})
    return sorted(out, key=lambda x: x.get("date", ""))


def _last_delta(rows: list[dict], key: str, span: int = 10) -> float | None:
    vals = [x.get(key) for x in rows if x.get(key) is not None]
    if len(vals) < 2:
        return None
    prev_idx = max(0, len(vals) - 1 - span)
    try:
        return float(vals[-1]) - float(vals[prev_idx])
    except Exception:
        return None


def holding_delta(rows: list[dict], key: str, weeks: int) -> float | None:
    if len(rows) < weeks + 1:
        return None
    latest = rows[-1].get(key)
    base = rows[-(weeks + 1)].get(key)
    if latest is None or base is None:
        return None
    try:
        return float(latest) - float(base)
    except Exception:
        return None


def _sum_recent(rows: list[dict], key: str, span: int = 10) -> float | None:
    vals = [x.get(key) for x in rows[-span:] if x.get(key) is not None]
    if not vals:
        return None
    try:
        return sum(float(v) for v in vals)
    except Exception:
        return None


def pressure_absorption_analysis(
    stock_id: str,
    daily: list[dict] | None = None,
    chip_series: list[dict] | None = None,
    margin_series: list[dict] | None = None,
    tech: dict | None = None,
) -> dict:
    """Read B2 through long trend, chip behavior, deduction context and rebound quality.

    M大祕密花園的公開檢核重點不是單日訊號，而是先拉長時間尺度，
    再看下跌/拉升過程籌碼、20日扣抵量/240扣抵價，以及止跌回均線的速度。
    """
    daily = daily if daily is not None else aggregate_ohlcv(read_price_history(stock_id), "daily")
    chip_series = chip_series if chip_series is not None else read_chip_series(stock_id)
    margin_series = margin_series if margin_series is not None else read_margin_series(stock_id)
    tech = tech or (technical_snapshot(daily, {}) if daily else {})

    if len(daily) < 25:
        return {
            "level": "B2資料不足",
            "class": "",
            "summary": "B2資料不足：至少需要約25個交易日價量資料",
            "line": "價量資料不足，暫時無法判讀賣壓是否消失。",
            "items": [],
            "score": 0,
        }

    recent = daily[-10:]
    prev = daily[-25:-10]
    recent_low = min(x.get("low") for x in recent if x.get("low") is not None)
    prev_low = min(x.get("low") for x in prev if x.get("low") is not None)
    close = daily[-1].get("close")
    ma20 = tech.get("ma20") if tech else None
    ma60 = tech.get("ma60") if tech else None
    ma120 = tech.get("ma120") if tech else None
    ma240 = tech.get("ma240") if tech else None
    slopes = tech.get("ma_slopes") or {}
    ma120_up = slopes.get("ma120") is not None and slopes.get("ma120") > 0
    ma240_up = slopes.get("ma240") is not None and slopes.get("ma240") > 0
    long_trend_ok = bool(close and ((ma120 and ma240 and close > ma120 and close > ma240) or ma120_up or ma240_up))
    not_break = bool(recent_low is not None and prev_low is not None and recent_low >= prev_low * 0.98)

    vol_recent = sum(float(x.get("volume") or 0) for x in recent[-5:]) / max(1, len(recent[-5:]))
    vol_prev = sum(float(x.get("volume") or 0) for x in daily[-10:-5]) / max(1, len(daily[-10:-5]))
    vol_delta_pct = ((vol_recent / vol_prev - 1) * 100) if vol_prev else None
    lower_volume = bool(vol_delta_pct is not None and vol_delta_pct <= -8)
    volume_price = tech.get("volume_price") if tech else volume_price_relation(daily, None)
    small_volume_hold = bool(volume_price in {"量縮價穩", "量縮價漲"} or (not_break and lower_volume))

    same_zone_push = False
    same_zone_volume_note = "同價位量能資料不足"
    if close and prev_low:
        recent_zone = [
            x for x in daily[-10:]
            if x.get("close") is not None and close * 0.92 <= x.get("close") <= close * 1.08
        ]
        prior_zone = [
            x for x in daily[-80:-20]
            if x.get("close") is not None and close * 0.92 <= x.get("close") <= close * 1.08
        ]
        recent_zone_vol = sum(float(x.get("volume") or 0) for x in recent_zone) / max(1, len(recent_zone))
        prior_zone_vol = sum(float(x.get("volume") or 0) for x in prior_zone) / max(1, len(prior_zone))
        same_zone_push = bool(
            close >= prev_low * 1.02
            and small_volume_hold
            and prior_zone_vol
            and recent_zone_vol <= prior_zone_vol * 0.78
        )
        if prior_zone_vol:
            same_zone_volume_note = f"同價位前段均量 {fmt_num(prior_zone_vol/1000, 0)} 張 / 後段 {fmt_num(recent_zone_vol/1000, 0)} 張"

    holding_series = read_holding_series(stock_id) if stock_id else []
    major_4w_delta = holding_delta(holding_series, "major", 4)
    major_8w_delta = holding_delta(holding_series, "major", 8)
    retail_4w_delta = holding_delta(holding_series, "retail", 4)
    retail_8w_delta = holding_delta(holding_series, "retail", 8)
    people_4w_delta = holding_delta(holding_series, "total_people", 4)
    people_8w_delta = holding_delta(holding_series, "total_people", 8)
    major_accumulating = bool(
        (major_4w_delta is not None and major_4w_delta >= 0.5)
        or (major_8w_delta is not None and major_8w_delta >= 1.0)
    )
    retail_support = bool(
        (retail_4w_delta is not None and retail_4w_delta <= -0.3)
        or (retail_8w_delta is not None and retail_8w_delta <= -0.8)
    )
    people_support = bool(
        (people_4w_delta is not None and people_4w_delta < 0)
        or (people_8w_delta is not None and people_8w_delta < 0)
    )
    long_chip_ok = bool(
        major_accumulating and (retail_support or people_support)
    )

    foreign_10d = _sum_recent(chip_series, "foreign", 10)
    foreign_5d = _sum_recent(chip_series, "foreign", 5)
    total_10d = _sum_recent(chip_series, "total", 10)
    foreign_stopping = bool(
        foreign_10d is None
        or foreign_10d >= 0
        or (foreign_5d is not None and foreign_5d >= 0)
        or (chip_series and float(chip_series[-1].get("foreign") or 0) >= 0)
    )
    chip_by_date = {x.get("date"): x for x in chip_series if x.get("date")}
    down_total = up_total = down_foreign = up_foreign = 0.0
    for row in daily[-20:]:
        open_price = row.get("open")
        close_price = row.get("close")
        chip = chip_by_date.get(row.get("date"), {})
        total = float(chip.get("total") or 0)
        foreign = float(chip.get("foreign") or 0)
        if open_price is not None and close_price is not None and close_price < open_price:
            down_total += total
            down_foreign += foreign
        elif open_price is not None and close_price is not None and close_price >= open_price:
            up_total += total
            up_foreign += foreign
    short_chip_ok = bool((up_total >= 0 and down_total >= -abs(up_total) * 1.3) or (up_foreign >= 0 and down_foreign >= -abs(up_foreign) * 1.3))
    foreign_repeat_buy = bool(up_foreign > 0 and down_foreign >= -abs(up_foreign) * 0.7)

    margin_10d = _last_delta(margin_series, "margin_balance", 10)
    margin_20d = _last_delta(margin_series, "margin_balance", 20)
    margin_not_hot = bool(margin_10d is None or margin_10d <= 0 or (not_break and margin_10d <= max(1000, abs(margin_20d or 0) * 0.35)))
    margin_masked = bool(margin_10d is not None and margin_10d > 0 and margin_10d <= max(1000, abs(margin_20d or 0) * 0.35) and not_break)
    margin_absorbed = bool(margin_10d is not None and margin_10d < 0 and not_break)

    avg_vol20 = sum(float(x.get("volume") or 0) for x in daily[-20:]) / 20 if len(daily) >= 20 else None
    deduct_vol20 = float(daily[-21].get("volume") or 0) if len(daily) > 20 else None
    volume_deduction_ok = bool(
        deduct_vol20 is not None
        and avg_vol20
        and (deduct_vol20 <= avg_vol20 * 1.15 or vol_recent >= deduct_vol20 * 0.75)
    )
    deduct_price240 = daily[-241].get("close") if len(daily) > 240 else None
    price_deduction_ok = bool(close and deduct_price240 and close > deduct_price240)

    red_k_count = sum(1 for x in recent if x.get("close") is not None and x.get("open") is not None and x["close"] >= x["open"])
    lower_shadow_count = 0
    for x in recent:
        open_price = x.get("open")
        close_price = x.get("close")
        low_price = x.get("low")
        high_price = x.get("high")
        if None in (open_price, close_price, low_price, high_price):
            continue
        body = abs(close_price - open_price)
        lower_shadow = min(open_price, close_price) - low_price
        candle_range = high_price - low_price
        if candle_range > 0 and lower_shadow >= max(body * 0.7, candle_range * 0.18):
            lower_shadow_count += 1
    stand_back_ma = bool(close and ((ma20 and close >= ma20) or (ma60 and close >= ma60)))
    recent_high = max(x.get("high") for x in recent if x.get("high") is not None)
    prev_high = max(x.get("high") for x in prev if x.get("high") is not None)
    slow_break_high = bool(recent_high and prev_high and recent_high >= prev_high * 0.995)
    rebound_quality = bool(stand_back_ma and (red_k_count >= 5 or lower_shadow_count >= 3 or slow_break_high))

    score = 0
    if long_trend_ok:
        score += 12
    if long_chip_ok:
        score += 12
    if not_break:
        score += 18
    if small_volume_hold:
        score += 16
    if same_zone_push:
        score += 10
    if short_chip_ok:
        score += 10
    if foreign_repeat_buy:
        score += 8
    if foreign_stopping:
        score += 8
    if margin_not_hot:
        score += 8
    if margin_absorbed:
        score += 6
    if volume_deduction_ok:
        score += 8
    if price_deduction_ok:
        score += 8
    if rebound_quality:
        score += 12
    if not_break and small_volume_hold and (same_zone_push or volume_deduction_ok or stand_back_ma):
        score += 8

    if score >= 78:
        level, cls = "B2賣壓疑似消失", "pos"
    elif score >= 58:
        level, cls = "B2賣壓收斂中", ""
    else:
        level, cls = "B2賣壓未確認", "neg"

    margin_text = "融資資料不足"
    if margin_10d is not None:
        margin_text = f"融資10日 {margin_10d:+,.0f} 張"
        if margin_masked:
            margin_text += "（小增但價格不破，列為觀察）"
        elif margin_absorbed:
            margin_text += "（融資退場但價格不破，賣壓被吸收）"

    items = [
        ("長週期轉多/長多", long_trend_ok, f"MA120 {fmt_num(ma120)} / MA240 {fmt_num(ma240)}；斜率 {fmt_num(slopes.get('ma120'))}/{fmt_num(slopes.get('ma240'))}"),
        ("主力長期動態", long_chip_ok, f"大戶4週 {fmt_num(major_4w_delta, 2)}% / 8週 {fmt_num(major_8w_delta, 2)}%；散戶4週 {fmt_num(retail_4w_delta, 2)}% / 8週 {fmt_num(retail_8w_delta, 2)}%；股東4週 {fmt_num(people_4w_delta, 0)}人 / 8週 {fmt_num(people_8w_delta, 0)}人"),
        ("價不破低", not_break, f"近10日低點 {fmt_num(recent_low)} / 前段低點 {fmt_num(prev_low)}"),
        ("量縮仍能撐住", small_volume_hold, f"{volume_price}；5日均量 {fmt_num(vol_delta_pct, 1)}%"),
        ("同區間小量推升", same_zone_push, same_zone_volume_note),
        ("跌升過程籌碼", short_chip_ok, f"跌日主力 {fmt_num(down_total, 0)} 張 / 紅K主力 {fmt_num(up_total, 0)} 張"),
        ("外資拉回不逃", foreign_repeat_buy, f"紅K外資 {fmt_num(up_foreign, 0)} 張 / 黑K或回檔外資 {fmt_num(down_foreign, 0)} 張"),
        ("外資賣壓停止", foreign_stopping, f"外資10日 {fmt_num(foreign_10d, 0)} 張 / 5日 {fmt_num(foreign_5d, 0)} 張 / 主力10日 {fmt_num(total_10d, 0)} 張"),
        ("融資沒有失控", margin_not_hot, margin_text),
        ("融資賣壓被吸收", margin_absorbed, margin_text),
        ("20日扣抵量有利", volume_deduction_ok, f"20日前量 {fmt_num((deduct_vol20 or 0)/1000, 0)} 張 / 近5日均量 {fmt_num(vol_recent/1000, 0)} 張"),
        ("240扣抵價有利", price_deduction_ok, f"240日前收盤 {fmt_num(deduct_price240)} / 現價 {fmt_num(close)}"),
        ("止跌回升品質", rebound_quality, f"紅K {red_k_count}/10；下影 {lower_shadow_count}/10；站回均線={'是' if stand_back_ma else '否'}；過前高={'是' if slow_break_high else '否'}"),
    ]
    summary = f"{level}｜長週期={'是' if long_trend_ok else '否'}｜價不破低={'是' if not_break else '否'}｜同區間小量={'是' if same_zone_push else '否'}｜外資拉回不逃={'是' if foreign_repeat_buy else '否'}｜{margin_text}"
    line = "；".join(f"{name}{'✅' if ok else '❌'}（{note}）" for name, ok, note in items)
    return {"level": level, "class": cls, "summary": summary, "line": line, "items": items, "score": score}


def holding_payload(series: list[dict]) -> list[dict]:
    return [
        {
            "date": item.get("date", ""),
            "major": item.get("major"),
            "middle": item.get("middle"),
            "retail": item.get("retail"),
            "totalPeople": item.get("total_people"),
            "majorPeople": item.get("major_people"),
            "middlePeople": item.get("middle_people"),
            "retailPeople": item.get("retail_people"),
        }
        for item in series[-80:]
    ]


def foreign_payload(series: list[dict]) -> list[dict]:
    return [
        {
            "date": item.get("date", ""),
            "foreign": item.get("foreign"),
        }
        for item in series[-80:]
    ]


def aligned_chip_payload(series: list[dict]) -> list[dict]:
    out = []
    foreign_cum = 0.0
    for item in series[-CHART_LOOKBACK_BARS:]:
        foreign = item.get("foreign")
        if foreign is not None:
            try:
                foreign_cum += float(foreign)
            except Exception:
                pass
        out.append({
            "date": item.get("date", ""),
            "major": item.get("major"),
            "middle": item.get("middle"),
            "retail": item.get("retail"),
            "totalPeople": item.get("total_people"),
            "majorPeople": item.get("major_people"),
            "middlePeople": item.get("middle_people"),
            "retailPeople": item.get("retail_people"),
            "holdingDate": item.get("holding_date", ""),
            "foreign": foreign,
            "foreignCum": foreign_cum,
        })
    return out


def align_chip_to_price_dates(price_rows: list[dict], holding_series: list[dict], chip_series: list[dict]) -> list[dict]:
    """Use trading dates as the x-axis; carry the latest weekly holding data forward."""
    dates = [r.get("date", "") for r in price_rows[-CHART_LOOKBACK_BARS:] if r.get("date")]
    if not dates:
        return []
    holding_sorted = sorted([x for x in holding_series if x.get("date")], key=lambda x: x.get("date", ""))
    chip_by_date = {x.get("date"): x for x in chip_series if x.get("date")}
    out = []
    h_idx = -1
    latest_h = None
    for date in dates:
        while h_idx + 1 < len(holding_sorted) and str(holding_sorted[h_idx + 1].get("date", "")) <= str(date):
            h_idx += 1
            latest_h = holding_sorted[h_idx]
        chip = chip_by_date.get(date, {})
        out.append({
            "date": date,
            "major": latest_h.get("major") if latest_h else None,
            "middle": latest_h.get("middle") if latest_h else None,
            "retail": latest_h.get("retail") if latest_h else None,
            "total_people": latest_h.get("total_people") if latest_h else None,
            "major_people": latest_h.get("major_people") if latest_h else None,
            "middle_people": latest_h.get("middle_people") if latest_h else None,
            "retail_people": latest_h.get("retail_people") if latest_h else None,
            "holding_date": latest_h.get("date") if latest_h else "",
            "foreign": chip.get("foreign"),
        })
    return out


def get_v44_fetcher():
    global _V44_FETCHER
    if _V44_FETCHER is not None:
        return _V44_FETCHER
    if os.environ.get("V44_LIVE_FETCH", "0") == "0" and os.environ.get("SITE_REFRESH_STALE_PRICES", "0") == "0":
        return None
    cell3 = V44_ROOT / "cell3_v44.py"
    cell4 = V44_ROOT / "cell4_v44.py"
    if not cell3.exists() or not cell4.exists():
        return None
    try:
        ns = {}
        for p in [cell3, cell4]:
            code = p.read_text(encoding="utf-8")
            exec(compile(code, str(p), "exec"), ns)
        _V44_FETCHER = ns["DataFetcher"]()
        return _V44_FETCHER
    except Exception as e:
        print(f"   [WARN] v44 fetcher unavailable: {e}", flush=True)
        _V44_FETCHER = False
        return None


def fetch_v44_price_history(stock_id: str, months: int = 36) -> list[dict]:
    fetcher = get_v44_fetcher()
    if not fetcher:
        return []
    try:
        df = fetcher.fetch_kline(stock_id, months=months)
        if df is None or df.empty:
            return []
        rows = []
        for idx, row in df.iterrows():
            try:
                date = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(row.get("date") or idx)
                rows.append({
                    "date": date[:10],
                    "open": float(row.get("Open") or row.get("open")),
                    "high": float(row.get("High") or row.get("max") or row.get("high")),
                    "low": float(row.get("Low") or row.get("min") or row.get("low")),
                    "close": float(row.get("Close") or row.get("close")),
                    "volume": float(row.get("Volume") or row.get("Trading_Volume") or 0),
                })
            except Exception:
                continue
        return rows
    except Exception as e:
        print(f"   [WARN] {stock_id} v44 kline failed: {e}", flush=True)
        return []


def merge_report_close(rows: list[dict], s: dict) -> list[dict]:
    """讓個股頁最後一筆價格至少與每日報告的收盤價一致。"""
    price = _to_float(s.get("price", ""), None)
    date = s.get("report_date") or ""
    if price is None or not date:
        return rows
    out = list(rows)
    if out and out[-1].get("date") == date:
        out[-1] = {**out[-1], "close": price, "high": max(out[-1]["high"], price), "low": min(out[-1]["low"], price)}
        return out
    if not out or str(out[-1].get("date", "")) < date:
        out.append({"date": date, "open": price, "high": price, "low": price, "close": price, "volume": 0})
    return out


def aggregate_ohlcv(rows: list[dict], mode: str) -> list[dict]:
    if mode == "daily":
        return rows
    buckets: dict[str, list[dict]] = {}
    for r in rows:
        try:
            dt = datetime.strptime(r["date"], "%Y-%m-%d")
        except Exception:
            continue
        if mode == "weekly":
            iso = dt.isocalendar()
            key = f"{iso.year}-W{iso.week:02d}"
        else:
            key = dt.strftime("%Y-%m")
        buckets.setdefault(key, []).append(r)
    out = []
    for key, items in buckets.items():
        out.append({
            "date": items[-1]["date"],
            "open": items[0]["open"],
            "high": max(x["high"] for x in items),
            "low": min(x["low"] for x in items),
            "close": items[-1]["close"],
            "volume": sum(x["volume"] for x in items),
        })
    return out


def ma_values(rows: list[dict], window: int) -> list[float | None]:
    closes = [r["close"] for r in rows]
    out: list[float | None] = []
    for i in range(len(closes)):
        if i + 1 < window:
            out.append(None)
        else:
            out.append(sum(closes[i + 1 - window:i + 1]) / window)
    return out


def latest_ma_and_slope(rows: list[dict], window: int, lookback: int = 5) -> tuple[float | None, float | None]:
    vals = ma_values(rows, window)
    latest = vals[-1] if vals else None
    prev = vals[-1 - lookback] if len(vals) > lookback else None
    slope = (latest - prev) if latest is not None and prev is not None else None
    return latest, slope


def ma_trend_direction(rows: list[dict], window: int) -> int | None:
    vals = [v for v in ma_values(rows, window) if v is not None]
    span = min(len(vals), max(5, min(window, 20)))
    if span < 3:
        return None
    recent = vals[-span:]
    xs = list(range(span))
    x_avg = sum(xs) / span
    y_avg = sum(recent) / span
    denom = sum((x - x_avg) ** 2 for x in xs)
    if denom == 0:
        return None
    slope = sum((x - x_avg) * (y - y_avg) for x, y in zip(xs, recent)) / denom
    if abs(slope) < 0.01:
        return 0
    return 1 if slope > 0 else -1


def volume_price_relation(rows: list[dict], volume_ratio: float | None) -> str:
    row = rows[-1] if rows else {}
    close = row.get("close")
    open_ = row.get("open")
    if close is None or open_ is None:
        return "資料不足"
    if volume_ratio is None:
        return "量能資料不足"
    prev_close = rows[-2].get("close") if len(rows) >= 2 else open_
    day_change = ((close / prev_close - 1) * 100) if prev_close else 0
    up = day_change > 0
    stable = abs(day_change) <= 1.5
    avg5 = None
    prev_avg5 = None
    if len(rows) >= 10:
        avg5 = sum(r.get("volume", 0) for r in rows[-5:]) / 5
        prev_avg5 = sum(r.get("volume", 0) for r in rows[-10:-5]) / 5
    avg_turning_up = bool(avg5 and prev_avg5 and avg5 >= prev_avg5 * 1.08)

    if volume_ratio <= 0.85 and up:
        return "量縮價漲"
    if volume_ratio >= 1.15 and up:
        return "量增價漲"
    if volume_ratio <= 0.9 and stable:
        return "量縮價穩"
    if avg_turning_up:
        return "均量上彎"
    return "量價未表態"


def volume_price_basis(rows: list[dict], volume_ratio: float | None) -> str:
    if not rows:
        return "資料不足"
    row = rows[-1]
    close = row.get("close")
    open_ = row.get("open")
    prev_close = rows[-2].get("close") if len(rows) >= 2 else open_
    day_change = ((close / prev_close - 1) * 100) if close and prev_close else None
    body_dir = "紅K" if close and open_ and close > open_ else "黑K" if close and open_ and close < open_ else "平盤K"
    avg_note = "5日均量資料不足"
    if len(rows) >= 10:
        avg5 = sum(r.get("volume", 0) for r in rows[-5:]) / 5
        prev_avg5 = sum(r.get("volume", 0) for r in rows[-10:-5]) / 5
        if prev_avg5:
            avg_note = f"5日均量較前5日 {((avg5 / prev_avg5 - 1) * 100):+.1f}%"
    return f"單日 {fmt_num(day_change, 2)}% / 量比 {fmt_num(volume_ratio, 2)}x / {body_dir} / {avg_note}"


def volume_price_reading(label: str) -> str:
    return {
        "量縮價漲": "最強，飆股型態，惜售無人賣，抱緊。",
        "量增價漲": "常態上漲，多頭順勢；若爆大量要注意高點。",
        "量縮價穩": "盤整蓄力，等突破方向確認。",
        "均量上彎": "趨勢出量訊號，搭配量增價漲確認攻擊啟動。",
        "量價未表態": "尚未出現明確量價優勢，先等量縮價穩、轉強或均量上彎再評估。",
    }.get(label, "資料不足，先等量價結構明確。")


def trend_pattern(rows: list[dict], ma5, ma10, ma20, ma60) -> str:
    if not rows:
        return "資料不足"
    close = rows[-1].get("close")
    if close and ma5 and ma10 and ma20 and ma60 and close > ma5 > ma10 > ma20 > ma60:
        return "短中多頭排列"
    if close and ma20 and ma60 and close > ma20 > ma60:
        return "多方趨勢"
    if close and ma5 and ma10 and ma20 and close < ma5 < ma10 < ma20:
        return "短線空頭排列"
    if close and ma20 and close < ma20:
        return "跌破月線整理"
    if ma5 and ma10 and abs(ma5 / ma10 - 1) <= 0.015:
        return "均線糾結"
    return "區間整理"


def candle_pattern(rows: list[dict]) -> str:
    if not rows:
        return "資料不足"
    r = rows[-1]
    open_, high, low, close = r.get("open"), r.get("high"), r.get("low"), r.get("close")
    if None in {open_, high, low, close}:
        return "資料不足"
    rng = max(high - low, 0.01)
    body = abs(close - open_)
    upper = high - max(open_, close)
    lower = min(open_, close) - low
    if body / rng <= 0.15:
        return "十字震盪"
    if lower / rng >= 0.45 and close >= open_:
        return "長下影承接"
    if upper / rng >= 0.45 and close <= open_:
        return "長上影賣壓"
    if close > open_ and body / rng >= 0.55:
        return "實體紅K"
    if close < open_ and body / rng >= 0.55:
        return "實體黑K"
    return "小實體整理"


def latest_large_volume_event(rows: list[dict], lookback: int = 60, threshold: float = 1.8) -> dict | None:
    start = max(19, len(rows) - lookback)
    latest = None
    for i in range(start, len(rows)):
        avg20 = sum(r.get("volume", 0) for r in rows[i - 19:i + 1]) / 20
        vol = rows[i].get("volume", 0)
        ratio = (vol / avg20) if avg20 else None
        if ratio and ratio >= threshold:
            latest = {
                "date": rows[i].get("date", ""),
                "ratio": ratio,
                "high": rows[i].get("high"),
                "low": rows[i].get("low"),
                "close": rows[i].get("close"),
            }
    return latest


def bollinger_values(rows: list[dict], window: int = 20, width: float = 2.0) -> tuple[list[float | None], list[float | None]]:
    closes = [r["close"] for r in rows]
    upper: list[float | None] = []
    lower: list[float | None] = []
    for i in range(len(closes)):
        if i + 1 < window:
            upper.append(None)
            lower.append(None)
            continue
        sample = closes[i + 1 - window:i + 1]
        avg = sum(sample) / window
        variance = sum((x - avg) ** 2 for x in sample) / window
        sd = variance ** 0.5
        upper.append(avg + width * sd)
        lower.append(avg - width * sd)
    return upper, lower


def chart_payload(rows: list[dict]) -> list[dict]:
    rows = rows[-CHART_LOOKBACK_BARS:]
    ma_map = {n: ma_values(rows, n) for n in (5, 10, 20, 60)}
    bb_upper, bb_lower = bollinger_values(rows, 20, 2.0)
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    k_vals: list[float | None] = []
    d_vals: list[float | None] = []
    wr_vals: list[float | None] = []
    k = 50.0
    d = 50.0
    for i in range(len(rows)):
        if i + 1 >= 9:
            hi9 = max(highs[i + 1 - 9:i + 1])
            lo9 = min(lows[i + 1 - 9:i + 1])
            rsv = 50.0 if hi9 == lo9 else (closes[i] - lo9) / (hi9 - lo9) * 100
            k = k * 2 / 3 + rsv / 3
            d = d * 2 / 3 + k / 3
            k_vals.append(k)
            d_vals.append(d)
        else:
            k_vals.append(None)
            d_vals.append(None)
        if i + 1 >= 14:
            hi14 = max(highs[i + 1 - 14:i + 1])
            lo14 = min(lows[i + 1 - 14:i + 1])
            wr_vals.append(None if hi14 == lo14 else (hi14 - closes[i]) / (hi14 - lo14) * -100)
        else:
            wr_vals.append(None)
    ema12 = ema_values(closes, 12)
    ema26 = ema_values(closes, 26)
    dif_vals = [(a - b) if a is not None and b is not None else None for a, b in zip(ema12, ema26)]
    dea_vals = ema_values([float(x or 0) for x in dif_vals], 9)
    macd_vals = [((dif_vals[i] - dea_vals[i]) * 2) if dif_vals[i] is not None and dea_vals[i] is not None else None for i in range(len(rows))]
    payload: list[dict] = []
    for i, r in enumerate(rows):
        payload.append({
            "date": r.get("date", ""),
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
            "volume": r.get("volume", 0),
            "ma5": ma_map[5][i],
            "ma10": ma_map[10][i],
            "ma20": ma_map[20][i],
            "ma60": ma_map[60][i],
            "bbUpper": bb_upper[i],
            "bbLower": bb_lower[i],
            "k": k_vals[i],
            "d": d_vals[i],
            "dif": dif_vals[i],
            "dea": dea_vals[i],
            "macd": macd_vals[i],
            "wr": wr_vals[i],
        })
    return payload


def chart_svg(rows: list[dict], title: str) -> str:
    rows = rows[-CHART_LOOKBACK_BARS:]
    if len(rows) < 2:
        return '<div class="strategy-note">尚未找到 v44 價格快取，之後接上每日更新後會顯示 K 線。</div>'
    w, h = 900, 360
    pad_l, pad_r, pad_t, pad_b = 50, 18, 18, 26
    price_h = 240
    vol_top = pad_t + price_h + 18
    vol_h = h - vol_top - pad_b
    values = [r["close"] for r in rows]
    ma5 = ma_values(rows, 5)
    ma10 = ma_values(rows, 10)
    ma20 = ma_values(rows, 20)
    ma60 = ma_values(rows, 60)
    bb_upper, bb_lower = bollinger_values(rows, 20, 2.0)
    band_values = [v for v in bb_upper + bb_lower if v is not None]
    lo, hi = min(values + band_values), max(values + band_values)
    if hi == lo:
        hi += 1
        lo -= 1
    def xy(idx, val):
        x = pad_l + idx * (w - pad_l - pad_r) / (len(rows) - 1)
        y = pad_t + (hi - val) * price_h / (hi - lo)
        return x, y
    def poly(vals, color, width=2):
        pts = []
        for i, v in enumerate(vals):
            if v is None:
                continue
            x, y = xy(i, float(v))
            pts.append(f"{x:.1f},{y:.1f}")
        return f'<polyline fill="none" stroke="{color}" stroke-width="{width}" points="{" ".join(pts)}" />' if pts else ""
    ma5_line = poly(ma5, "#58a6ff", 1.5)
    ma10_line = poly(ma10, "#d2a520", 1.5)
    ma20_line = poly(ma20, "#f0883e", 1.7)
    ma60_line = poly(ma60, "#3fb950", 1.7)
    bb_upper_line = poly(bb_upper, "#8b949e", 1.2)
    bb_lower_line = poly(bb_lower, "#8b949e", 1.2)
    grid = ""
    for pct in [0, .25, .5, .75, 1]:
        y = pad_t + pct * price_h
        price = hi - pct * (hi - lo)
        grid += f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" stroke="#21262d"/><text x="4" y="{y+4:.1f}" fill="#6e7681" font-size="11">{price:.1f}</text>'
    step = (w - pad_l - pad_r) / max(len(rows), 1)
    candle_w = max(2, min(8, step * 0.58))
    candles = ""
    vols = [r.get("volume", 0) for r in rows]
    max_vol = max(vols) if vols else 1
    for i, r in enumerate(rows):
        x, y_close = xy(i, r["close"])
        _, y_open = xy(i, r["open"])
        _, y_high = xy(i, r["high"])
        _, y_low = xy(i, r["low"])
        up = r["close"] >= r["open"]
        color = "#f85149" if up else "#3fb950"
        body_y = min(y_open, y_close)
        body_h = max(abs(y_close - y_open), 1.4)
        v_h = 0 if max_vol == 0 else (r.get("volume", 0) / max_vol) * vol_h
        candles += (
            f'<line x1="{x:.1f}" y1="{y_high:.1f}" x2="{x:.1f}" y2="{y_low:.1f}" stroke="{color}" stroke-width="1"/>'
            f'<rect x="{x-candle_w/2:.1f}" y="{body_y:.1f}" width="{candle_w:.1f}" height="{body_h:.1f}" fill="{color}" opacity=".78"/>'
            f'<rect x="{x-candle_w/2:.1f}" y="{vol_top + vol_h - v_h:.1f}" width="{candle_w:.1f}" height="{v_h:.1f}" fill="{color}" opacity=".35"/>'
        )
    deduct_colors = {5: "#58a6ff", 10: "#d2a520", 20: "#f0883e", 60: "#3fb950", 120: "#a78bfa"}
    deduct_marks = ""
    deduct_periods = [120, 60, 20, 10, 5] if title == "日K" else [60, 20, 10, 5]
    for period in deduct_periods:
        idx = len(rows) - period
        if idx < 0 or idx >= len(rows):
            continue
        x, y_close = xy(idx, rows[idx]["close"])
        color = deduct_colors[period]
        deduct_marks += (
            f'<rect x="{x-step/2:.1f}" y="{pad_t:.1f}" width="{max(step, 5):.1f}" height="{price_h:.1f}" fill="{color}" opacity=".08"/>'
            f'<line x1="{x:.1f}" y1="{pad_t:.1f}" x2="{x:.1f}" y2="{vol_top-2:.1f}" stroke="{color}" stroke-width="1" stroke-dasharray="4 4" opacity=".8"/>'
            f'<circle cx="{x:.1f}" cy="{vol_top-11:.1f}" r="9" fill="#0d1117" stroke="{color}" stroke-width="2"/>'
            f'<text x="{x:.1f}" y="{vol_top-7:.1f}" text-anchor="middle" fill="{color}" font-size="10" font-weight="700">{period}</text>'
            f'<text x="{x+8:.1f}" y="{max(pad_t+13, y_close-10):.1f}" fill="{color}" font-size="10">扣抵 {rows[idx]["close"]:.1f}</text>'
        )
    if deduct_marks:
        deduct_marks = (
            f'<text x="{pad_l}" y="{vol_top-30:.1f}" fill="#c9d1d9" font-size="11">預備扣抵值區域</text>'
            + deduct_marks
        )
    max_vol_lot = max_vol / 1000 if max_vol else 0
    last = rows[-1]
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  <line x1="{pad_l}" y1="{vol_top:.1f}" x2="{w-pad_r}" y2="{vol_top:.1f}" stroke="#30363d"/>
  <text x="4" y="{vol_top+12:.1f}" fill="#6e7681" font-size="11">量</text>
  <text x="4" y="{vol_top+28:.1f}" fill="#6e7681" font-size="11">{max_vol_lot:.0f}張</text>
  {deduct_marks}
  {candles}
  {bb_upper_line}{bb_lower_line}{ma5_line}{ma10_line}{ma20_line}{ma60_line}
  <text x="{pad_l}" y="{h-8}" fill="#6e7681" font-size="11">{esc(rows[0]["date"])}</text>
  <text x="{w-112}" y="{h-8}" fill="#6e7681" font-size="11">{esc(last["date"])}</text>
  <text x="{pad_l}" y="14" fill="#e6edf3" font-size="12">{esc(title)} ｜ 收 {last["close"]:.2f}</text>
  <text x="{w-315}" y="14" fill="#58a6ff" font-size="11">MA5</text>
  <text x="{w-265}" y="14" fill="#d2a520" font-size="11">MA10</text>
  <text x="{w-210}" y="14" fill="#f0883e" font-size="11">MA20</text>
  <text x="{w-155}" y="14" fill="#3fb950" font-size="11">MA60</text>
  <text x="{w-95}" y="14" fill="#8b949e" font-size="11">BB</text>
</svg>"""


def holding_line_svg(series: list[dict], title: str = "股權分配趨勢") -> str:
    series = series[-80:]
    if len(series) < 2:
        return '<div class="strategy-note">股權分配資料不足，暫時無法形成趨勢折線圖。</div>'
    w, h = 900, 240
    pad_l, pad_r, pad_t, pad_b = 50, 18, 18, 32
    keys = [("major", "大戶(400張以上)", "#f85149"), ("middle", "中實戶(200-400張)", "#d2a520"), ("retail", "散戶<1萬股", "#3fb950")]
    values = [float(x.get(k, 0) or 0) for x in series for k, _, _ in keys]
    people_values = [float(x.get("total_people", 0) or 0) for x in series if x.get("total_people") is not None]
    lo, hi = min(values), max(values)
    if hi == lo:
        hi += 1
        lo -= 1
    p_lo, p_hi = (min(people_values), max(people_values)) if people_values else (0, 1)
    if p_hi == p_lo:
        p_hi += 1
        p_lo -= 1

    def xy(idx, val):
        x = pad_l + idx * (w - pad_l - pad_r) / (len(series) - 1)
        y = pad_t + (hi - val) * (h - pad_t - pad_b) / (hi - lo)
        return x, y

    def y_people(val):
        return pad_t + (p_hi - val) * (h - pad_t - pad_b) / (p_hi - p_lo)

    grid = ""
    for pct in [0, .25, .5, .75, 1]:
        y = pad_t + pct * (h - pad_t - pad_b)
        v = hi - pct * (hi - lo)
        grid += f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" stroke="#21262d"/><text x="4" y="{y+4:.1f}" fill="#6e7681" font-size="11">{v:.1f}%</text>'
        if people_values:
            pv = p_hi - pct * (p_hi - p_lo)
            grid += f'<text x="{w-52}" y="{y+4:.1f}" fill="#6e7681" font-size="11">{pv/1000:.0f}k</text>'

    lines = ""
    for key, label, color in keys:
        pts = []
        for i, item in enumerate(series):
            x, y = xy(i, float(item.get(key, 0) or 0))
            pts.append(f"{x:.1f},{y:.1f}")
        lines += f'<polyline fill="none" stroke="{color}" stroke-width="2.1" points="{" ".join(pts)}" />'
    if people_values:
        pts = []
        for i, item in enumerate(series):
            if item.get("total_people") is None:
                continue
            x = pad_l + i * (w - pad_l - pad_r) / (len(series) - 1)
            pts.append(f"{x:.1f},{y_people(float(item.get('total_people') or 0)):.1f}")
        if pts:
            lines += f'<polyline fill="none" stroke="#58a6ff" stroke-width="1.8" stroke-dasharray="5 4" points="{" ".join(pts)}" />'

    latest = series[-1]
    legend = ""
    x0 = w - 410
    for idx, (key, label, color) in enumerate(keys):
        legend += f'<text x="{x0 + idx*105}" y="14" fill="{color}" font-size="11">{label}</text>'
    legend += f'<text x="{w-85}" y="14" fill="#58a6ff" font-size="11">總股東</text>'
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {lines}
  <text x="{pad_l}" y="14" fill="#e6edf3" font-size="12">{esc(title)} ｜ {esc(latest.get('date',''))}</text>
  {legend}
  <text x="{pad_l}" y="{h-8}" fill="#6e7681" font-size="11">{esc(series[0].get('date',''))}</text>
  <text x="{w-112}" y="{h-8}" fill="#6e7681" font-size="11">{esc(latest.get('date',''))}</text>
</svg>"""


def chip_flow_svg(series: list[dict], title: str = "10日籌碼動向折線圖") -> str:
    series = series[-10:]
    if len(series) < 2:
        return '<div class="strategy-note">籌碼資料不足，暫時無法形成 10 日籌碼動向圖。</div>'
    w, h = 900, 260
    pad_l, pad_r, pad_t, pad_b = 54, 18, 24, 36
    plot_h = h - pad_t - pad_b
    keys = [("foreign", "外資", "#58a6ff"), ("trust", "投信", "#d2a520"), ("dealer", "自營商", "#f85149")]
    stack_extents = []
    for item in series:
        vals = [float(item.get(k, 0) or 0) for k, _, _ in keys]
        pos = sum(v for v in vals if v > 0)
        neg = sum(v for v in vals if v < 0)
        total = sum(vals)
        stack_extents.extend([pos, neg, total])
    max_abs = nice_number((max(abs(v) for v in stack_extents) or 1) * 1.15)
    zero_y = pad_t + plot_h / 2

    def y(v):
        return zero_y - float(v) * (plot_h / 2) / max_abs

    grid = f'<line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w-pad_r}" y2="{zero_y:.1f}" stroke="#8b949e" stroke-width="1"/>'
    for v in [max_abs, max_abs / 2, -max_abs / 2, -max_abs]:
        yy = y(v)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-pad_r}" y2="{yy:.1f}" stroke="#21262d"/><text x="4" y="{yy+4:.1f}" fill="#6e7681" font-size="11">{compact_axis_label(v)}</text>'

    group_w = (w - pad_l - pad_r) / len(series)
    bar_w = max(12, min(28, group_w * 0.34))
    bars = ""
    labels = ""
    total_points = []
    for i, item in enumerate(series):
        cx = pad_l + group_w * (i + 0.5)
        pos_base = 0.0
        neg_base = 0.0
        total = 0.0
        x = cx - bar_w / 2
        for key, _, color in keys:
            v = float(item.get(key, 0) or 0)
            if v >= 0:
                y0 = y(pos_base)
                y1 = y(pos_base + v)
                pos_base += v
            else:
                y0 = y(neg_base)
                y1 = y(neg_base + v)
                neg_base += v
            total += v
            top = min(y0, y1)
            bh = max(abs(y0 - y1), 1.5)
            bars += f'<rect x="{x:.1f}" y="{top:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="{color}" opacity=".84"/>'
        total_points.append(f"{cx:.1f},{y(total):.1f}")
        if i in {0, len(series) - 1}:
            labels += f'<text x="{cx-32:.1f}" y="{h-10}" fill="#6e7681" font-size="11">{esc(item.get("date",""))}</text>'

    legend = ""
    for i, (_, label, color) in enumerate(keys):
        legend += f'<text x="{w-282+i*58}" y="16" fill="{color}" font-size="11">{label}</text>'
    legend += f'<text x="{w-72}" y="16" fill="#e6edf3" font-size="11">合計線</text>'
    total_line = f'<polyline points="{" ".join(total_points)}" fill="none" stroke="#e6edf3" stroke-width="2" opacity=".9"/>'
    total_dots = "".join(
        f'<circle cx="{pt.split(",")[0]}" cy="{pt.split(",")[1]}" r="2.4" fill="#e6edf3"/>'
        for pt in total_points
    )
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {bars}
  {total_line}
  {total_dots}
  <text x="{pad_l}" y="16" fill="#e6edf3" font-size="12">{esc(title)}（張）</text>
  {legend}
  {labels}
</svg>"""


def main_force_price_svg(chip_series: list[dict], price_rows: list[dict], title: str = "主力增減張數與收盤價關係") -> str:
    chip_series = chip_series[-30:]
    if len(chip_series) < 2 or len(price_rows) < 2:
        return '<div class="strategy-note">籌碼或收盤價資料不足，暫時無法形成主力與收盤價關係圖。</div>'
    close_by_date = {r.get("date"): r.get("close") for r in price_rows}
    rows = []
    last_close = None
    for item in chip_series:
        close = close_by_date.get(item.get("date"))
        if close is not None:
            last_close = close
        rows.append({**item, "close": last_close})
    rows = [r for r in rows if r.get("close") is not None]
    if len(rows) < 2:
        return '<div class="strategy-note">籌碼日期尚未對齊收盤價，暫時無法形成主力與收盤價關係圖。</div>'

    w, h = 900, 280
    pad_l, pad_r, pad_t, pad_b = 54, 54, 24, 36
    plot_h = h - pad_t - pad_b
    net_vals = [float(r.get("total", 0) or 0) for r in rows]
    closes = [float(r.get("close", 0) or 0) for r in rows]
    max_abs = max(abs(v) for v in net_vals) or 1
    max_abs *= 1.15
    lo, hi = min(closes), max(closes)
    if hi == lo:
        hi += 1
        lo -= 1
    zero_y = pad_t + plot_h / 2

    def x(i):
        return pad_l + i * (w - pad_l - pad_r) / (len(rows) - 1)

    def y_net(v):
        return zero_y - float(v) * (plot_h / 2) / max_abs

    def y_close(v):
        return pad_t + (hi - float(v)) * plot_h / (hi - lo)

    grid = f'<line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w-pad_r}" y2="{zero_y:.1f}" stroke="#8b949e" stroke-width="1"/>'
    for pct in [0, .25, .5, .75, 1]:
        yy = pad_t + pct * plot_h
        price = hi - pct * (hi - lo)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-pad_r}" y2="{yy:.1f}" stroke="#21262d"/><text x="{w-48}" y="{yy+4:.1f}" fill="#6e7681" font-size="11">{price:.1f}</text>'

    step = (w - pad_l - pad_r) / max(len(rows), 1)
    bar_w = max(3, min(10, step * 0.46))
    bars = ""
    points = []
    for i, r in enumerate(rows):
        v = float(r.get("total", 0) or 0)
        xx = x(i)
        yy = y_net(v)
        top = min(yy, zero_y)
        bh = max(abs(zero_y - yy), 1.5)
        color = "#f85149" if v >= 0 else "#3fb950"
        bars += f'<rect x="{xx-bar_w/2:.1f}" y="{top:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="{color}" opacity=".55"/>'
        points.append(f"{xx:.1f},{y_close(r['close']):.1f}")

    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {bars}
  <polyline fill="none" stroke="#58a6ff" stroke-width="2.2" points="{" ".join(points)}"/>
  <text x="{pad_l}" y="16" fill="#e6edf3" font-size="12">{esc(title)}</text>
  <text x="{w-245}" y="16" fill="#f85149" font-size="11">主力買超</text>
  <text x="{w-175}" y="16" fill="#3fb950" font-size="11">主力賣超</text>
  <text x="{w-105}" y="16" fill="#58a6ff" font-size="11">收盤價</text>
  <text x="{pad_l}" y="{h-10}" fill="#6e7681" font-size="11">{esc(rows[0].get("date",""))}</text>
  <text x="{w-112}" y="{h-10}" fill="#6e7681" font-size="11">{esc(rows[-1].get("date",""))}</text>
</svg>"""


def chip_lightweight_flow_panel(stock_id: str, chip_series: list[dict], price_rows: list[dict]) -> str:
    close_by_date = {r.get("date"): r.get("close") for r in price_rows if r.get("date")}
    change_pct_by_date = price_change_pct_by_date(price_rows)
    rows = []
    last_close = None
    for item in chip_series[-CHART_LOOKBACK_BARS:]:
        date = item.get("date", "")
        close = close_by_date.get(date)
        if close is not None:
            last_close = close
        rows.append({
            "date": date,
            "foreign": item.get("foreign"),
            "trust": item.get("trust"),
            "dealer": item.get("dealer"),
            "total": item.get("total"),
            "close": last_close,
            "changePct": change_pct_by_date.get(date),
        })
    rows = [r for r in rows if r.get("date")]
    if len(rows) < 2:
        return ""
    data = json.dumps(rows, ensure_ascii=False)
    panel_id = f"chip-tv-{stock_id}"
    script = f"""
<script src="https://unpkg.com/lightweight-charts@5.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
(function(){{
  const root=document.getElementById('{panel_id}');
  const rows={data};
  if(!root || !rows.length) return;
  const L=window.LightweightCharts;
  if(!L){{
    root.innerHTML='<div class="strategy-note">TradingView Lightweight Charts 載入失敗，請檢查網路或 CDN。</div>';
    return;
  }}
  const charts=[];
  let syncing=false;
  const maxLogical=Math.max(0,rows.length-1);
  const gridColor='#21262d';
  const textColor='#8b949e';
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  const fmt=(v,d=2)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW',{{maximumFractionDigits:d,minimumFractionDigits:d}}) : '-';
  const baseOptions=(height)=>({{
    height,
    layout:{{background:{{type:'solid',color:'#0d1117'}},textColor}},
    grid:{{vertLines:{{color:gridColor}},horzLines:{{color:gridColor}}}},
    rightPriceScale:{{borderColor:'#30363d'}},
    leftPriceScale:{{visible:true,borderColor:'#30363d'}},
    timeScale:{{borderColor:'#30363d',timeVisible:false,secondsVisible:false,fixLeftEdge:true,fixRightEdge:true}},
    crosshair:{{mode:L.CrosshairMode.Normal}},
    localization:{{locale:'zh-TW'}},
  }});
  function clampRange(range){{
    if(!range) return range;
    let from=Number(range.from), to=Number(range.to);
    if(!Number.isFinite(from) || !Number.isFinite(to)) return range;
    const span=to-from;
    if(span>=maxLogical) return {{from:0,to:maxLogical}};
    if(from<0){{ to-=from; from=0; }}
    if(to>maxLogical){{ from-=to-maxLogical; to=maxLogical; }}
    return {{from:Math.max(0,from),to:Math.min(maxLogical,to)}};
  }}
  function sameRange(a,b){{
    return a && b && Math.abs(Number(a.from)-Number(b.from))<0.01 && Math.abs(Number(a.to)-Number(b.to))<0.01;
  }}
  function histData(key,colorFn){{
    return rows.filter(x=>x[key]!=null).map(x=>({{time:x.date,value:Number(x[key]),color:colorFn ? colorFn(x) : '#58a6ff'}}));
  }}
  function lineData(key){{
    return rows.filter(x=>x[key]!=null).map(x=>({{time:x.date,value:Number(x[key])}}));
  }}
  function defaultLogicalRange(){{
    if(maxLogical <= 0) return {{from:0,to:maxLogical}};
    const lastTime=Date.parse(rows[rows.length-1]?.date || '');
    let from=Math.max(0, rows.length - 22);
    if(!Number.isNaN(lastTime)){{
      const cutoff=lastTime - {CHART_DEFAULT_VISIBLE_DAYS}*24*60*60*1000;
      from=0;
      for(let i=rows.length-1;i>=0;i--){{
        const t=Date.parse(rows[i]?.date || '');
        if(!Number.isNaN(t) && t < cutoff){{ from=Math.min(maxLogical, i+1); break; }}
      }}
    }}
    return {{from:Math.max(0,from),to:maxLogical}};
  }}
  function wireRange(chart){{
    chart.timeScale().setVisibleLogicalRange(defaultLogicalRange());
    chart.timeScale().subscribeVisibleLogicalRangeChange(range=>{{
      if(syncing || !range) return;
      const next=clampRange(range);
      syncing=true;
      if(!sameRange(range,next)) chart.timeScale().setVisibleLogicalRange(next);
      charts.forEach(other=>{{ if(other!==chart) other.timeScale().setVisibleLogicalRange(next); }});
      syncing=false;
    }});
  }}
  function chipTip(x){{
    return `<b>${{x.date}}</b><br>外資 ${{fmtInt(x.foreign)}} 張<br>投信 ${{fmtInt(x.trust)}} 張<br>自營商 ${{fmtInt(x.dealer)}} 張<br>合計 ${{fmtInt(x.total)}} 張`;
  }}
  function forceTip(x){{
    const pct=Number.isFinite(Number(x.changePct)) ? `${{Number(x.changePct).toFixed(2)}}%` : '-';
    return `<b>${{x.date}}</b><br>主力合計 ${{fmtInt(x.total)}} 張<br>收盤價 ${{fmt(x.close)}}<br>漲幅 ${{pct}}`;
  }}
  function addTip(chart, wrapper, tipFn){{
    const tip=wrapper.querySelector('.tv-tooltip');
    chart.subscribeCrosshairMove(param=>{{
      if(!tip) return;
      if(!param || !param.time){{ tip.style.display='none'; return; }}
      const x=rows.find(r=>r.date===param.time);
      if(!x){{ tip.style.display='none'; return; }}
      tip.innerHTML=tipFn(x);
      tip.style.display='block';
    }});
  }}
  const chipEl=document.getElementById('{panel_id}-flow');
  if(chipEl){{
    const chart=L.createChart(chipEl, baseOptions(280));
    chart.applyOptions({{rightPriceScale:{{visible:false}},leftPriceScale:{{visible:true,borderColor:'#30363d'}}}});
    const total=chart.addSeries(L.HistogramSeries,{{priceScaleId:'left',priceFormat:{{type:'volume'}},priceLineVisible:false,lastValueVisible:false}});
    total.setData(histData('total',x=>Number(x.total)>=0?'rgba(248,81,73,.55)':'rgba(63,185,80,.55)'));
    [['foreign','#58a6ff'],['trust','#d2a520'],['dealer','#f85149']].forEach(([key,color])=>{{
      const s=chart.addSeries(L.LineSeries,{{priceScaleId:'left',color,lineWidth:2,priceLineVisible:false,lastValueVisible:false}});
      s.setData(lineData(key));
    }});
    charts.push(chart);
    wireRange(chart);
    addTip(chart, chipEl.closest('.tv-chart-panel'), chipTip);
  }}
  const forceEl=document.getElementById('{panel_id}-force');
  if(forceEl){{
    const chart=L.createChart(forceEl, baseOptions(280));
    chart.applyOptions({{
      rightPriceScale:{{visible:true,borderColor:'#30363d'}},
      leftPriceScale:{{visible:true,borderColor:'#30363d'}},
    }});
    const force=chart.addSeries(L.HistogramSeries,{{priceScaleId:'left',priceFormat:{{type:'volume'}},priceLineVisible:false,lastValueVisible:false}});
    force.setData(histData('total',x=>Number(x.total)>=0?'#f85149':'#3fb950'));
    const close=chart.addSeries(L.LineSeries,{{priceScaleId:'right',color:'#58a6ff',lineWidth:2,priceLineVisible:false}});
    close.setData(lineData('close'));
    charts.push(chart);
    wireRange(chart);
    addTip(chart, forceEl.closest('.tv-chart-panel'), forceTip);
  }}
  window.addEventListener('resize',()=>{{
    const flow=document.getElementById('{panel_id}-flow');
    const force=document.getElementById('{panel_id}-force');
    charts.forEach((chart,i)=>chart.applyOptions({{width:(i===0?flow:force)?.clientWidth || 0}}));
  }});
}})();
</script>"""
    return f"""
<div id="{panel_id}" class="tv-chip-grid">
  <div class="tv-chart-panel">
    <div class="tv-chart-title">籌碼動向｜外資 / 投信 / 自營商 / 合計</div>
    <div id="{panel_id}-flow" class="tv-chip-chart"></div>
    <div class="tv-tooltip"></div>
  </div>
  <div class="tv-chart-panel">
    <div class="tv-chart-title">主力增減張數與收盤價關係</div>
    <div id="{panel_id}-force" class="tv-chip-chart"></div>
    <div class="tv-tooltip"></div>
  </div>
</div>
{script}"""


def read_ai_logs(stock_id: str, limit: int = 3) -> list[dict]:
    if not V44_DB_PATH.exists():
        return []
    try:
        conn = sqlite3.connect(str(V44_DB_PATH))
        rows = conn.execute(
            """
            SELECT created_at, kind, close, stage, buy_zone, stop_line, target_price, report
            FROM ai_analysis_logs
            WHERE stock_id=?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (stock_id, limit),
        ).fetchall()
        conn.close()
    except Exception:
        return []
    return [
        {
            "created_at": r[0],
            "kind": r[1],
            "close": r[2],
            "stage": r[3],
            "buy_zone": r[4],
            "stop_line": r[5],
            "target_price": r[6],
            "report": r[7],
        }
        for r in rows
    ]


def _value_or_dash(value) -> str:
    if value is None:
        return "─"
    text = str(value).strip()
    return text if text else "─"


def _line_html(label: str, value: str, cls: str = "") -> str:
    cls_attr = f' {cls}' if cls else ""
    return (
        '<div class="telegram-line">'
        f'<div class="k">{esc(label)}</div>'
        f'<div class="v{cls_attr}">{esc(value)}</div>'
        '</div>'
    )


def _signed_class(value) -> str:
    n = _to_float(value, None)
    if n is None:
        return ""
    return "pos" if n >= 0 else "neg"


def build_telegram_info_card(
    stock_id: str,
    s: dict,
    tech: dict,
    chip: dict,
    holding: dict,
    decision: dict,
    ledger_item: dict | None,
    sell_signal: dict | None = None,
) -> str:
    """Build a compact card that mirrors the Telegram deep-analysis rhythm."""
    indicator = indicator_snapshot(aggregate_ohlcv(merge_report_close(read_price_history(stock_id), s), "daily"))
    basket = basket_label(classify_basket(s))
    events = ledger_item.get("events", []) if ledger_item else []
    repeat_note = f"歷史入選 {len(events)} 次，最近 {events[-1]['date']}" if events else "首次或尚未建立歷史台帳"
    close = _value_or_dash(s.get("price"))
    price_date = _value_or_dash(s.get("price_date"))
    report_date = _value_or_dash(s.get("report_date"))
    meta_parts = [f"收盤日期 {price_date}"]
    meta_parts.append(f"報告日期 {report_date}" if not is_blank(report_date) else "個股查詢頁")
    telegram_meta = "｜".join(meta_parts)
    daily_rows = aggregate_ohlcv(merge_report_close(read_price_history(stock_id), s), "daily")
    price_change_text, price_change_cls = daily_change_text(daily_rows)
    score = _value_or_dash(s.get("score"))
    reason_line = basket_reason(s, tech, read_chip_series(stock_id), holding)
    trend = _value_or_dash(tech.get("trend_pattern") or tech.get("trend"))
    volume_price = _value_or_dash(tech.get("volume_price"))
    volume_basis = _value_or_dash(tech.get("volume_price_basis"))
    volume_reading = volume_price_reading(volume_price)
    kd = "─"
    if indicator.get("k") is not None and indicator.get("d") is not None:
        kd = f"K {fmt_num(indicator.get('k'), 1)} / D {fmt_num(indicator.get('d'), 1)}，{indicator.get('kd_state', '─')}"
    macd = "─"
    if indicator.get("dif") is not None and indicator.get("dea") is not None:
        macd = indicator.get("macd_state", "─")
    wr = "─"
    if indicator.get("wr") is not None:
        wr = f"{fmt_num(indicator.get('wr'), 1)}，{indicator.get('wr_state', '─')}"

    chip_latest = chip.get("latest", {}) if chip else {}
    chip_sum5 = chip.get("sum5", {}) if chip else {}
    h_latest = holding.get("latest", {}) if holding else {}
    foreign5 = s.get("foreign_5d")
    if chip_sum5.get("foreign") is not None:
        foreign5 = f"{chip_sum5.get('foreign'):+,.0f}張"
    force_parts = []
    if chip:
        force_parts.append(f"外資5日 {_value_or_dash(foreign5)}")
        force_parts.append(f"三大法人當日 {fmt_num(chip_latest.get('total'), 0)} 張")
    else:
        force_parts.append("法人買賣超尚無快取")
    if holding:
        force_parts.append(
            f"大戶(400張以上) {fmt_num(h_latest.get('major'))}% / "
            f"中實戶人數(200-400張) {fmt_num(h_latest.get('middle_people'), 0)} 人 / "
            f"散戶 {fmt_num(h_latest.get('retail'))}%"
        )
    else:
        force_parts.append("股權分散尚無快取")
    force_line = "；".join(force_parts)
    chip_metrics_line = ""
    if chip:
        sum10 = chip.get("sum10", {})
        chip_metrics_line = f"外資10日 {fmt_num(sum10.get('foreign'),0)} 張；主力10日 {fmt_num(sum10.get('total'),0)} 張"

    operation_note = {
        "行進籃": "SFZ 波段候選：原訊號可小部位，追高不追，等 MA5/MA10/箱頂回測或 TA3 確認。",
        "盤整籃": "MABC 觀察：先看 A/B 是否維持，等量縮價穩、站回成本區或 C 買點再處理。",
        "過熱/風險": "偏熱或風險區：不追高，等降溫、回測支撐不破，再重新評估。",
    }.get(basket, "先等資料補齊，再回到買點、失敗線與目標價判斷。")

    phase1 = (
        _line_html("篩選結論", f"{basket}｜Score {score}")
        + _line_html("操作評價", f"{decision['rating']}｜{decision['reason']}", decision.get("rating_class", ""))
        + _line_html("分籃理由", reason_line)
        + _line_html("台帳", repeat_note)
    )
    phase2 = (
        _line_html("趨勢結構", trend)
        + _line_html("量價關係", volume_price)
        + _line_html("判斷依據", volume_basis)
        + _line_html("量價判讀", volume_reading)
        + _line_html("KD", kd)
        + _line_html("MACD", macd)
        + _line_html("Williams", wr)
        + _line_html("籌碼", force_line, _signed_class(foreign5))
        + (_line_html("籌碼數字", chip_metrics_line) if chip_metrics_line else "")
    )
    return f"""
<div class="telegram-report-card">
  <div class="telegram-head">
    <div>
      <div class="telegram-title">{esc(stock_id)} {esc(s.get('name',''))} 個股資訊卡</div>
      <div class="telegram-meta">{esc(telegram_meta)}</div>
    </div>
    <div class="telegram-rating {decision.get('rating_class','')}">{esc(decision['rating'])}</div>
  </div>
  <div class="telegram-price-line"><div><div class="k">收盤價</div><div class="price">{esc(close)}</div></div><div class="change {price_change_cls}">單日 {esc(price_change_text)}</div></div>
  <div class="telegram-phase"><h3>① 量化篩選確認</h3>{phase1}</div>
  <div class="telegram-phase"><h3>② 技術 / 籌碼 / 指標判讀</h3>{phase2}</div>
  <div class="telegram-note">這張卡整理量化篩選、技術與籌碼；操作規劃與 Quick 分析保留在右側。</div>
</div>"""


def build_operation_plan_card(s: dict, tech: dict, decision: dict, sell_signal: dict | None = None) -> str:
    basket = basket_label(classify_basket(s))
    resistance = _value_or_dash(s.get("resistance") or fmt_num(tech.get("resistance")))
    operation_note = {
        "行進籃": "SFZ 波段候選：原訊號可小部位，追高不追，等 MA5/MA10/箱頂回測或 TA3 確認。",
        "盤整籃": "MABC 觀察：先看 A/B 是否維持，等量縮價穩、站回成本區或 C 買點再處理。",
        "過熱/風險": "偏熱或風險區：不追高，等降溫、回測支撐不破，再重新評估。",
    }.get(basket, "先等資料補齊，再回到買點、失敗線與目標價判斷。")
    return (
        '<div class="telegram-phase" style="margin-top:0">'
        + _line_html("是否進場", decision["rating"], decision.get("rating_class", ""))
        + _line_html("壓力 / 近支撐", f"壓力 {resistance}｜近支撐 {decision.get('initial_stop_text','─')}")
        + _line_html("較佳買入區", decision["entry_range"])
        + _line_html("停利", decision.get("target_text") or _value_or_dash(s.get("target")))
        + _line_html("初始停損", f"{decision.get('initial_stop_text','─')}（{decision.get('initial_stop_label','─')}，{decision.get('stop_pct_text','─')}）")
        + _line_html("R:R", decision.get("rr_text", "─"), decision.get("rr_class", ""))
        + (_line_html("賣出警示", f"{sell_signal.get('level')}｜{sell_signal.get('reason')}", sell_signal.get("class", "")) if sell_signal else "")
        + _line_html("追蹤重點", operation_note)
        + "</div>"
    )


def quick_analysis_text(s: dict, ledger_item: dict | None) -> str:
    query_only = bool(s.get("query_only"))
    basket = "個股查詢" if query_only else basket_label(classify_basket(s))
    events = ledger_item.get("events", []) if ledger_item else []
    repeat_note = f"歷史入選 {len(events)} 次，最近 {events[-1]['date']}。" if events else "未在目前籃中，從已快取價格/籌碼資料建立查詢頁。"
    sid = s.get("id", "")
    daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily") if sid else []
    decision = build_trade_decision(technical_snapshot(daily, s), s) if daily else {
        "rating": "觀望",
        "entry_range": "資料不足",
        "defense": "資料不足",
        "reason": "等待價格快取更新",
    }
    if query_only:
        action = "先當作獨立個股觀察：重點看價格是否不再破低，外資/融資賣壓是否收斂，以及少量買盤能否守住關鍵支撐。"
    elif basket == "行進籃":
        action = "偏向 SFZ 波段候選：原訊號可小部位，突破追不到不追，等回測 MA5/MA10/箱頂或 TA3-Strict 加碼確認。"
    elif basket == "盤整籃":
        action = "偏向盤整觀察：重點看 MABC 是否維持 A/B，量縮價穩或站回均線轉強時才處理早買點。"
    else:
        action = "偏熱或風險區：不追高，等降溫、回測支撐不破，或重新整理後再評估。"
    return (
        f"分類：{basket}\n"
        f"操作評價：{decision['rating']}｜買進區間：{decision['entry_range']}｜關鍵防守：{decision['defense']}\n"
        f"現況：收盤 {s.get('price','─')}，RSI {s.get('rsi','─')}，%B {s.get('bband_pct','─')}。\n"
        f"理由：{decision['reason']}\n"
        f"台帳：{repeat_note}\n"
        f"操作：{action}"
    )


def technical_snapshot(rows: list[dict], s: dict) -> dict:
    if not rows:
        return {}
    close = rows[-1]["close"]
    closes = [r["close"] for r in rows]
    def last_ma(n):
        if len(closes) < n:
            return None
        return sum(closes[-n:]) / n
    ma_windows = [5, 10, 20, 60, 120, 240]
    ma_pairs = {n: latest_ma_and_slope(rows, n) for n in ma_windows}
    ma5, ma10, ma20, ma60, ma120, ma240 = [ma_pairs[n][0] for n in ma_windows]
    ma_trends = {f"ma{n}": ma_trend_direction(rows, n) for n in ma_windows}
    bb_upper, bb_lower = bollinger_values(rows, 20, 2.0)
    avg_vol20 = None
    if len(rows) >= 20:
        avg_vol20 = sum(r.get("volume", 0) for r in rows[-20:]) / 20
    latest_vol = rows[-1].get("volume", 0)
    volume_ratio = (latest_vol / avg_vol20) if avg_vol20 else None
    large_volume = bool(volume_ratio and volume_ratio >= 1.8)
    large_volume_event = latest_large_volume_event(rows, 60, 1.8)
    recent = rows[-60:]
    support = min(r["low"] for r in recent) if recent else None
    resistance = max(r["high"] for r in recent) if recent else None
    formal_entry = formal_williams_entry_zone(rows, ma20)
    entry_gap = entry_zone_gap(close, formal_entry.get("low"), formal_entry.get("high"))
    detrend_120 = rows[-121].get("close") if len(rows) > 120 else None
    trend = "長多偏強" if ma20 and ma60 and close > ma20 > ma60 else "短線轉強" if ma20 and close > ma20 else "整理/修正"
    return {
        "close": close,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "ma120": ma120,
        "ma240": ma240,
        "bb_upper": bb_upper[-1] if bb_upper else None,
        "bb_lower": bb_lower[-1] if bb_lower else None,
        "ma_trends": ma_trends,
        "ma_slopes": {f"ma{n}": ma_pairs[n][1] for n in ma_windows},
        "detrend_120": detrend_120,
        "open": rows[-1].get("open"),
        "high": rows[-1].get("high"),
        "low": rows[-1].get("low"),
        "volume": latest_vol,
        "avg_vol20": avg_vol20,
        "volume_ratio": volume_ratio,
        "volume_price": volume_price_relation(rows, volume_ratio),
        "volume_price_basis": volume_price_basis(rows, volume_ratio),
        "trend_pattern": trend_pattern(rows, ma5, ma10, ma20, ma60),
        "candle_pattern": candle_pattern(rows),
        "large_volume": large_volume,
        "large_volume_event": large_volume_event,
        "support": support,
        "resistance": resistance,
        "formal_entry_low": formal_entry.get("low"),
        "formal_entry_high": formal_entry.get("high"),
        "formal_entry_mid": formal_entry.get("mid"),
        "formal_entry_filter_ok": formal_entry.get("filter_ok"),
        "formal_entry_basis": formal_entry.get("basis"),
        "entry_gap": entry_gap,
        "trend": trend,
    }


def fmt_num(v, digits: int = 2) -> str:
    if v is None:
        return "─"
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return "─"


def entry_zone_gap(close: float | None, low: float | None, high: float | None) -> float | None:
    if close is None or low is None or high is None or low <= 0 or high <= 0:
        return None
    if low <= close <= high:
        return 0.0
    anchor = high if close > high else low
    return (close / anchor - 1) * 100


def calc_trade_plan(tech: dict, s: dict) -> dict:
    entry = tech.get("formal_entry_mid") if tech else None
    if entry is None:
        entry = _to_float(s.get("entry", ""), None)
    target = _to_float(s.get("target", ""), None)
    report_stop = _to_float(s.get("stop", ""), None)
    close = tech.get("close") if tech else _to_float(s.get("price", ""), None)
    ma10 = tech.get("ma10") if tech else None
    ma20 = tech.get("ma20") if tech else None
    large_event = (tech.get("large_volume_event") or {}) if tech else {}
    large_low = large_event.get("low")

    if entry is None and close:
        entry = close

    reference_support = None
    for value in [report_stop, tech.get("support") if tech else None, ma20]:
        if value:
            reference_support = value
            break

    candidates: list[tuple[str, float]] = []
    for label, value in [
        ("原始防守", report_stop),
        ("爆量K低點", large_low),
        ("MA20", ma20),
        ("MA10", ma10),
    ]:
        if not entry or not value or value >= entry:
            continue
        risk_pct = (1 - value / entry) * 100
        if 3 <= risk_pct <= 12:
            candidates.append((label, value))

    if candidates:
        stop_label, initial_stop = max(candidates, key=lambda item: item[1])
    elif entry:
        stop_label, initial_stop = "買點-6%", entry * 0.94
    else:
        stop_label, initial_stop = "資料不足", None

    rr = None
    stop_pct = None
    if entry and initial_stop and target and target > entry and initial_stop < entry:
        rr = (target - entry) / (entry - initial_stop)
        stop_pct = (initial_stop / entry - 1) * 100
    elif entry and initial_stop and initial_stop < entry:
        stop_pct = (initial_stop / entry - 1) * 100

    rr_class = ""
    if rr is not None:
        if rr >= 2:
            rr_class = "rr-good"
        elif rr >= 1.45:
            rr_class = "rr-mid"
        else:
            rr_class = "rr-bad"

    return {
        "entry": entry,
        "target": target,
        "initial_stop": initial_stop,
        "initial_stop_label": stop_label,
        "reference_support": reference_support,
        "rr": rr,
        "stop_pct": stop_pct,
        "entry_text": fmt_num(entry),
        "target_text": fmt_num(target),
        "initial_stop_text": fmt_num(initial_stop),
        "reference_support_text": fmt_num(reference_support),
        "rr_text": "─" if rr is None else f"1:{rr:.1f}",
        "rr_class": rr_class,
        "stop_pct_text": "─" if stop_pct is None else f"{stop_pct:.1f}%",
    }


def build_trade_decision(tech: dict, s: dict) -> dict:
    if not tech:
        plan = calc_trade_plan({}, s)
        return {
            "rating": "觀望",
            "rating_class": "",
            "entry_range": "資料不足",
            "defense": "資料不足",
            "reason": "等待價格快取更新",
            **plan,
        }
    close = tech.get("close")
    ma10 = tech.get("ma10")
    ma20 = tech.get("ma20")
    ma60 = tech.get("ma60")
    large_event = tech.get("large_volume_event") or {}
    large_low = large_event.get("low")

    entry_low = tech.get("formal_entry_low")
    entry_high = tech.get("formal_entry_high")
    filter_ok = tech.get("formal_entry_filter_ok")
    if entry_low and entry_high:
        entry_range = f"{fmt_num(entry_low)} ~ {fmt_num(entry_high)}"
    elif ma20:
        entry_range = f"{fmt_num(ma20 * 0.99)} ~ {fmt_num(ma20 * 1.01)}"
    else:
        entry_range = "資料不足"

    defense_candidates = []
    for label, value in [("近期大量K低點", large_low), ("MA20", ma20), ("MA60", ma60)]:
        if value and close and value < close:
            defense_candidates.append((close - value, label, value))
    if defense_candidates:
        _, defense_label, defense_value = min(defense_candidates, key=lambda x: x[0])
        defense = f"{fmt_num(defense_value)}（{defense_label}）"
    elif ma20:
        defense = f"{fmt_num(ma20)}（MA20）"
    else:
        defense = "資料不足"

    if not entry_high and entry_range != "資料不足":
        nums = [_to_float(x, None) for x in re.findall(r"\d+(?:\.\d+)?", entry_range)]
        entry_high = max([x for x in nums if x is not None], default=None)
    defense_value = _to_float(defense, None)
    gap = entry_zone_gap(close, entry_low, entry_high)

    if close and defense_value and close < defense_value:
        rating, cls, reason = "賣出/避開", "neg", "跌破關鍵防守價位"
    elif tech.get("volume_price") == "放量下跌":
        rating, cls, reason = "觀望", "", "放量下跌，先等賣壓消化"
    elif filter_ok is False:
        rating, cls, reason = "觀望", "", "Williams 買點已算出，但收盤仍低於 MA20，先等站回"
    elif gap is not None and gap < -2:
        rating, cls, reason = "觀望", "", "跌破 Williams 買進區間，等止跌站回"
    elif gap is not None and gap <= 2:
        rating, cls, reason = "可買進", "pos", "收盤接近 Williams -65~-85 買進區間"
    elif gap is not None and gap <= 8:
        rating, cls, reason = "觀望", "", "略高於買進區間，等回測"
    else:
        rating, cls, reason = "觀望", "", "距買進區間偏遠，不追價"

    plan = calc_trade_plan(tech, s)
    return {
        "rating": rating,
        "rating_class": cls,
        "entry_range": entry_range,
        "defense": defense,
        "reason": reason,
        **plan,
    }


def calc_sell_signal(daily: list[dict], weekly: list[dict], chip_series: list[dict], s: dict, decision: dict) -> dict:
    if not daily:
        return {"level": "資料不足", "class": "", "reason": "等待價格資料", "ma20_gap": None, "profit": None}

    close = daily[-1].get("close")
    entry = decision.get("entry")
    ma20 = ma_values(daily, 20)[-1] if len(daily) >= 20 else None
    ma20_gap = ((close / ma20 - 1) * 100) if close and ma20 else None
    profit = ((close / entry - 1) * 100) if close and entry else None

    last2_drop = False
    if len(daily) >= 3:
        p1, p2, p3 = daily[-3], daily[-2], daily[-1]
        ret1 = (p2["close"] / p1["close"] - 1) if p1.get("close") and p2.get("close") else 0
        ret2 = (p3["close"] / p2["close"] - 1) if p2.get("close") and p3.get("close") else 0
        last2_drop = ret1 <= -0.03 and ret2 <= -0.03

    long_black = False
    if len(daily) >= 21:
        latest = daily[-1]
        avg_vol20 = sum(x.get("volume", 0) for x in daily[-21:-1]) / 20
        prev_close = daily[-2].get("close")
        day_ret = (latest["close"] / prev_close - 1) * 100 if latest.get("close") and prev_close else 0
        long_black = latest.get("close") < latest.get("open") and day_ret <= -3 and latest.get("volume", 0) >= avg_vol20 * 1.5

    weekly_turn = False
    if len(weekly) >= 2:
        weekly_turn = weekly[-1].get("close") < weekly[-2].get("low")

    chip_metrics = chip_trend_metrics(chip_series, {})
    foreign_sell_streak = chip_metrics.get("foreign_sell_streak", 0)

    reasons = []
    level, cls = "續抱觀察", ""
    if long_black and foreign_sell_streak >= 2:
        level, cls = "立即檢查", "exit"
        reasons.append("量大長黑且外資連賣")
    elif profit is not None and profit > 20 and ma20_gap is not None and ma20_gap < 0:
        level, cls = "跌破MA20", "exit"
        reasons.append("漲幅>20%，依MA20主線出場")
        if weekly_turn and last2_drop:
            reasons.append("週K轉折且日K連兩根跌逾3%")
    elif profit is not None and profit <= 10 and ma20_gap is not None and ma20_gap < 0:
        level, cls = "跌破MA20", "watch"
        reasons.append("漲幅10%內跌破MA20")
    elif ma20_gap is not None and ma20_gap < -2:
        level, cls = "月線轉弱", "watch"
        reasons.append("收盤低於MA20超過2%")
    elif ma20_gap is not None:
        reasons.append(f"距MA20 {ma20_gap:+.1f}%")
    else:
        reasons.append("MA20資料不足")

    if foreign_sell_streak >= 2 and cls != "exit":
        reasons.append(f"外資連賣{foreign_sell_streak}日")

    return {
        "level": level,
        "class": cls,
        "reason": "；".join(reasons),
        "ma20_gap": ma20_gap,
        "profit": profit,
        "foreign_sell_streak": foreign_sell_streak,
    }


def calc_rsi(closes: list[float], period: int = 14):
    if len(closes) <= period:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def ema_values(values: list[float], span: int) -> list[float | None]:
    if not values:
        return []
    alpha = 2 / (span + 1)
    out: list[float | None] = []
    ema = values[0]
    for i, v in enumerate(values):
        ema = v if i == 0 else alpha * v + (1 - alpha) * ema
        out.append(ema)
    return out


def indicator_snapshot(rows: list[dict]) -> dict:
    if len(rows) < 15:
        return {}
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]

    k_vals: list[float | None] = []
    d_vals: list[float | None] = []
    k = 50.0
    d = 50.0
    wr = None
    for i in range(len(rows)):
        if i + 1 < 9:
            k_vals.append(None)
            d_vals.append(None)
            continue
        hi = max(highs[i + 1 - 9:i + 1])
        lo = min(lows[i + 1 - 9:i + 1])
        rsv = 50.0 if hi == lo else (closes[i] - lo) / (hi - lo) * 100
        k = k * 2 / 3 + rsv / 3
        d = d * 2 / 3 + k / 3
        k_vals.append(k)
        d_vals.append(d)

    if len(rows) >= 14:
        hi14 = max(highs[-14:])
        lo14 = min(lows[-14:])
        wr = None if hi14 == lo14 else (hi14 - closes[-1]) / (hi14 - lo14) * -100

    ema12 = ema_values(closes, 12)
    ema26 = ema_values(closes, 26)
    dif = [(a - b) if a is not None and b is not None else None for a, b in zip(ema12, ema26)]
    dea_series = ema_values([float(x or 0) for x in dif], 9)
    macd_hist = None
    if dif and dea_series:
        macd_hist = (dif[-1] - dea_series[-1]) * 2

    latest_k = k_vals[-1]
    latest_d = d_vals[-1]
    prev_k = next((x for x in reversed(k_vals[:-1]) if x is not None), None)
    prev_d = next((x for x in reversed(d_vals[:-1]) if x is not None), None)
    kd_state = "資料不足"
    if latest_k is not None and latest_d is not None:
        if prev_k is not None and prev_d is not None and prev_k <= prev_d and latest_k > latest_d:
            kd_state = "黃金交叉"
        elif prev_k is not None and prev_d is not None and prev_k >= prev_d and latest_k < latest_d:
            kd_state = "死亡交叉"
        elif latest_k > 80 and latest_d > 80:
            kd_state = "高檔鈍化"
        elif latest_k < 20 and latest_d < 20:
            kd_state = "低檔轉折區"
        elif latest_k > latest_d:
            kd_state = "偏多"
        else:
            kd_state = "偏弱"

    macd_state = "資料不足"
    if dif and dea_series and dif[-1] is not None:
        if dif[-1] > dea_series[-1] and (macd_hist or 0) > 0:
            macd_state = "買進區"
        elif dif[-1] < dea_series[-1] and (macd_hist or 0) < 0:
            macd_state = "賣出區"
        else:
            macd_state = "觀察區"

    wr_state = "資料不足"
    if wr is not None:
        if wr > -20:
            wr_state = "偏熱"
        elif wr < -80:
            wr_state = "超賣"
        else:
            wr_state = "中性"

    return {
        "k": latest_k,
        "d": latest_d,
        "kd_state": kd_state,
        "dif": dif[-1] if dif else None,
        "dea": dea_series[-1] if dea_series else None,
        "macd": macd_hist,
        "macd_state": macd_state,
        "wr": wr,
        "wr_state": wr_state,
    }


def indicator_series(rows: list[dict]) -> dict:
    rows = rows[-120:]
    if len(rows) < 15:
        return {}
    closes = [float(r["close"]) for r in rows]
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]

    k_vals: list[float | None] = []
    d_vals: list[float | None] = []
    wr_vals: list[float | None] = []
    k = 50.0
    d = 50.0
    for i in range(len(rows)):
        if i + 1 >= 9:
            hi = max(highs[i + 1 - 9:i + 1])
            lo = min(lows[i + 1 - 9:i + 1])
            rsv = 50.0 if hi == lo else (closes[i] - lo) / (hi - lo) * 100
            k = k * 2 / 3 + rsv / 3
            d = d * 2 / 3 + k / 3
            k_vals.append(k)
            d_vals.append(d)
        else:
            k_vals.append(None)
            d_vals.append(None)
        if i + 1 >= 14:
            hi14 = max(highs[i + 1 - 14:i + 1])
            lo14 = min(lows[i + 1 - 14:i + 1])
            wr_vals.append(None if hi14 == lo14 else (hi14 - closes[i]) / (hi14 - lo14) * -100)
        else:
            wr_vals.append(None)

    ema12 = ema_values(closes, 12)
    ema26 = ema_values(closes, 26)
    dif = [(a - b) if a is not None and b is not None else None for a, b in zip(ema12, ema26)]
    dea = ema_values([float(x or 0) for x in dif], 9)
    hist = [(a - b) * 2 if a is not None and b is not None else None for a, b in zip(dif, dea)]
    return {
        "dates": [r.get("date", "") for r in rows],
        "k": k_vals,
        "d": d_vals,
        "dif": dif,
        "dea": dea,
        "hist": hist,
        "wr": wr_vals,
    }


def mini_line_svg(
    title: str,
    series_defs: list[tuple[str, list[float | None], str]],
    height: int = 118,
    fixed_range: tuple[float, float] | None = None,
    zero_line: bool = False,
    guide_lines: list[tuple[float, str, str]] | None = None,
) -> str:
    w, h = 900, height
    pad_l, pad_r, pad_t, pad_b = 50, 18, 20, 22
    values = [float(v) for _, vals, _ in series_defs for v in vals if v is not None]
    if not values:
        return '<div class="strategy-note">指標資料不足</div>'
    lo, hi = fixed_range if fixed_range else (min(values), max(values))
    if hi == lo:
        hi += 1
        lo -= 1

    max_len = max(len(vals) for _, vals, _ in series_defs)
    def xy(idx, val):
        x = pad_l + idx * (w - pad_l - pad_r) / max(1, max_len - 1)
        y = pad_t + (hi - float(val)) * (h - pad_t - pad_b) / (hi - lo)
        return x, y

    grid = ""
    for pct in [0, .5, 1]:
        y = pad_t + pct * (h - pad_t - pad_b)
        v = hi - pct * (hi - lo)
        grid += f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-pad_r}" y2="{y:.1f}" stroke="#21262d"/><text x="2" y="{y+4:.1f}" fill="#6e7681" font-size="10">{v:.0f}</text>'
    if zero_line and lo < 0 < hi:
        _, zy = xy(0, 0)
        grid += f'<line x1="{pad_l}" y1="{zy:.1f}" x2="{w-pad_r}" y2="{zy:.1f}" stroke="#8b949e" stroke-dasharray="3 3"/>'
    for guide_value, guide_label, guide_color in guide_lines or []:
        if lo <= guide_value <= hi:
            _, gy = xy(0, guide_value)
            grid += (
                f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{w-pad_r}" y2="{gy:.1f}" stroke="{guide_color}" stroke-dasharray="5 4" opacity=".75"/>'
                f'<text x="{pad_l+6}" y="{gy-4:.1f}" fill="{guide_color}" font-size="10">{esc(guide_label)}</text>'
            )

    lines = ""
    legend = ""
    for idx, (label, vals, color) in enumerate(series_defs):
        pts = []
        for i, v in enumerate(vals):
            if v is None:
                continue
            x, y = xy(i, v)
            pts.append(f"{x:.1f},{y:.1f}")
        if pts:
            lines += f'<polyline fill="none" stroke="{color}" stroke-width="1.7" points="{" ".join(pts)}"/>'
        legend += f'<text x="{w-210+idx*68}" y="14" fill="{color}" font-size="10">{esc(label)}</text>'
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {lines}
  <text x="{pad_l}" y="14" fill="#e6edf3" font-size="11">{esc(title)}</text>
  {legend}
</svg>"""


def nice_number(value: float) -> float:
    value = abs(float(value or 0))
    if value <= 0:
        return 1.0
    exp = math.floor(math.log10(value))
    frac = value / (10 ** exp)
    if frac <= 1:
        nice = 1
    elif frac <= 2:
        nice = 2
    elif frac <= 5:
        nice = 5
    else:
        nice = 10
    return nice * (10 ** exp)


def compact_axis_label(value: float) -> str:
    value = float(value)
    abs_v = abs(value)
    if abs_v >= 10000:
        return f"{value/1000:.0f}k"
    if abs_v >= 1000:
        return f"{value/1000:.1f}k"
    if abs_v >= 100:
        return f"{value:.0f}"
    return f"{value:.1f}".rstrip("0").rstrip(".")


def holding_compact_svg(series: list[dict], title: str = "股權分析") -> str:
    rows = [x for x in series[-CHART_LOOKBACK_BARS:] if x.get("major") is not None or x.get("middle") is not None or x.get("retail") is not None or x.get("total_people") is not None]
    if len(rows) < 2:
        return '<div class="strategy-note">股權分配資料不足。</div>'
    w, h = 900, 150
    pad_l, pad_r, pad_t, pad_b = 50, 56, 22, 24
    plot_h = h - pad_t - pad_b
    pct_series = [
        ("major", "大戶(400張以上)", "#f85149"),
        ("middle", "中實戶(200-400張)", "#d2a520"),
        ("retail", "散戶", "#3fb950"),
    ]
    pct_vals = [float(x.get(k)) for x in rows for k, _, _ in pct_series if x.get(k) is not None]
    people_vals = [float(x.get("total_people")) for x in rows if x.get("total_people") is not None]
    pct_lo, pct_hi = (min(pct_vals), max(pct_vals)) if pct_vals else (0.0, 1.0)
    pad = max(0.4, (pct_hi - pct_lo) * 0.12)
    pct_lo = math.floor((pct_lo - pad) * 2) / 2
    pct_hi = math.ceil((pct_hi + pad) * 2) / 2
    if pct_hi <= pct_lo:
        pct_hi = pct_lo + 1
    people_lo, people_hi = (min(people_vals), max(people_vals)) if people_vals else (0.0, 1.0)
    people_pad = max(1.0, (people_hi - people_lo) * 0.12)
    people_lo = math.floor((people_lo - people_pad) / 100) * 100
    people_hi = math.ceil((people_hi + people_pad) / 100) * 100
    if people_hi <= people_lo:
        people_hi = people_lo + 100

    def x_pos(i):
        return pad_l + i * (w - pad_l - pad_r) / max(1, len(rows) - 1)

    def y_pct(v):
        return pad_t + (pct_hi - float(v)) * plot_h / (pct_hi - pct_lo)

    def y_people(v):
        return pad_t + (people_hi - float(v)) * plot_h / (people_hi - people_lo)

    grid = ""
    for pct in [0, .5, 1]:
        yy = pad_t + pct * plot_h
        pv = pct_hi - pct * (pct_hi - pct_lo)
        hv = people_hi - pct * (people_hi - people_lo)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-pad_r}" y2="{yy:.1f}" stroke="#21262d"/>'
        grid += f'<text x="4" y="{yy+4:.1f}" fill="#6e7681" font-size="10">{pv:.1f}%</text>'
        grid += f'<text x="{w-48}" y="{yy+4:.1f}" fill="#6e7681" font-size="10">{compact_axis_label(hv)}</text>'

    lines = ""
    for key, _, color in pct_series:
        pts = []
        for i, item in enumerate(rows):
            if item.get(key) is None:
                continue
            pts.append(f"{x_pos(i):.1f},{y_pct(float(item.get(key))):.1f}")
        if pts:
            lines += f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{" ".join(pts)}"/>'
    people_pts = []
    for i, item in enumerate(rows):
        if item.get("total_people") is None:
            continue
        people_pts.append(f"{x_pos(i):.1f},{y_people(float(item.get('total_people'))):.1f}")
    if people_pts:
        lines += f'<polyline fill="none" stroke="#58a6ff" stroke-width="1.8" stroke-dasharray="5 4" points="{" ".join(people_pts)}"/>'
    legend = f'<text x="{w-300}" y="14" fill="#f85149" font-size="10">大戶</text><text x="{w-248}" y="14" fill="#d2a520" font-size="10">中實戶</text><text x="{w-180}" y="14" fill="#3fb950" font-size="10">散戶</text><text x="{w-120}" y="14" fill="#58a6ff" font-size="10">股東數</text>'
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {lines}
  <text x="{pad_l}" y="14" fill="#e6edf3" font-size="11">{esc(title)}</text>
  {legend}
</svg>"""


def foreign_flow_bar_line_svg(series: list[dict], title: str = "外資買賣超 / 區間累積") -> str:
    rows = [x for x in series[-CHART_LOOKBACK_BARS:] if x.get("foreign") is not None]
    if len(rows) < 2:
        return '<div class="strategy-note">外資買賣超資料不足。</div>'
    w, h = 900, 150
    pad_l, pad_r, pad_t, pad_b = 50, 56, 22, 24
    plot_h = h - pad_t - pad_b
    vals = [float(x.get("foreign") or 0) for x in rows]
    running = []
    total = 0.0
    for v in vals:
        total += v
        running.append(total)
    bar_abs = nice_number(max(abs(v) for v in vals) * 1.15)
    line_lo, line_hi = min(running), max(running)
    line_pad = max(1.0, (line_hi - line_lo) * 0.14)
    line_lo = -nice_number(abs(line_lo - line_pad)) if line_lo < 0 else 0
    line_hi = nice_number(line_hi + line_pad) if line_hi > 0 else 0
    if line_hi <= line_lo:
        line_hi = line_lo + 1
    zero_y = pad_t + plot_h / 2

    def x_pos(i):
        return pad_l + i * (w - pad_l - pad_r) / max(1, len(rows) - 1)

    def y_bar(v):
        return zero_y - float(v) * (plot_h / 2) / bar_abs

    def y_line(v):
        return pad_t + (line_hi - float(v)) * plot_h / (line_hi - line_lo)

    grid = f'<line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w-pad_r}" y2="{zero_y:.1f}" stroke="#8b949e" stroke-dasharray="3 3"/>'
    for v in [bar_abs, 0, -bar_abs]:
        yy = y_bar(v)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-pad_r}" y2="{yy:.1f}" stroke="#21262d"/><text x="3" y="{yy+4:.1f}" fill="#6e7681" font-size="10">{compact_axis_label(v)}</text>'
    for pct in [0, .5, 1]:
        yy = pad_t + pct * plot_h
        lv = line_hi - pct * (line_hi - line_lo)
        grid += f'<text x="{w-48}" y="{yy+4:.1f}" fill="#6e7681" font-size="10">{compact_axis_label(lv)}</text>'
    step = (w - pad_l - pad_r) / len(rows)
    bar_w = max(3, min(8, step * 0.56))
    bars = ""
    for i, v in enumerate(vals):
        cx = x_pos(i)
        yy = y_bar(v)
        top = min(yy, zero_y)
        bh = max(abs(zero_y - yy), 1.3)
        color = "#f85149" if v >= 0 else "#3fb950"
        bars += f'<rect x="{cx-bar_w/2:.1f}" y="{top:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="{color}" opacity=".78"/>'
    pts = " ".join(f"{x_pos(i):.1f},{y_line(v):.1f}" for i, v in enumerate(running))
    line = f'<polyline fill="none" stroke="#58a6ff" stroke-width="2" points="{pts}"/>'
    legend = f'<text x="{w-240}" y="14" fill="#f85149" font-size="10">買超</text><text x="{w-182}" y="14" fill="#3fb950" font-size="10">賣超</text><text x="{w-122}" y="14" fill="#58a6ff" font-size="10">累積線</text>'
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {bars}
  {line}
  <text x="{pad_l}" y="14" fill="#e6edf3" font-size="11">{esc(title)}（張）</text>
  {legend}
</svg>"""


def indicator_chart_panel(rows: list[dict], label: str, mode: str) -> str:
    data = indicator_series(rows)
    if not data:
        return '<div class="strategy-note" style="margin-top:10px">指標資料不足。</div>'
    kd = mini_line_svg(
        f"{label} KD",
        [("K", data["k"], "#58a6ff"), ("D", data["d"], "#d2a520")],
        fixed_range=(0, 100),
        guide_lines=[(80, "80 過熱/賣出觀察", "#f85149"), (20, "20 超賣/買進觀察", "#3fb950")],
    )
    macd = mini_line_svg(
        f"{label} MACD",
        [("DIF", data["dif"], "#58a6ff"), ("DEA", data["dea"], "#d2a520"), ("M", data["hist"], "#f85149")],
        zero_line=True,
        guide_lines=[(0, "0 軸 多空分界", "#8b949e")],
    )
    wr = mini_line_svg(
        f"{label} Williams %R",
        [("%R", data["wr"], "#a78bfa")],
        fixed_range=(-100, 0),
        guide_lines=[(-20, "-20 過熱/賣出觀察", "#f85149"), (-80, "-80 超賣/買進觀察", "#3fb950")],
    )
    return f"""<div class="indicator-stack">
  <div class="indicator-box indicator-hover" data-source="price" data-mode="{esc(mode)}" data-kind="wr">{wr}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>
  <div class="indicator-box indicator-hover" data-source="price" data-mode="{esc(mode)}" data-kind="kd">{kd}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>
  <div class="indicator-box indicator-hover" data-source="price" data-mode="{esc(mode)}" data-kind="macd">{macd}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>
</div>"""


def chip_indicator_panel(aligned_series: list[dict]) -> str:
    panels = []
    if aligned_series:
        panels.append(("aligned", "holdingPack", holding_compact_svg(aligned_series, "股權分析：大戶 / 散戶 / 股東數")))
        panels.append(("aligned", "foreignFlow", foreign_flow_bar_line_svg(aligned_series, "外資買賣超 / 區間累積")))
    if not panels:
        return '<div class="strategy-note" style="margin-top:10px">籌碼指標資料不足。</div>'
    return '<div class="chip-indicator-stack">' + "".join(
        f'<div class="indicator-box indicator-hover" data-source="{source}" data-kind="{kind}">{svg}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>'
        for source, kind, svg in panels
    ) + '</div>'


def enrich_stock_fields(s: dict) -> dict:
    out = dict(s)
    out["name"] = clean_stock_name(out.get("name", ""))
    sid = out.get("id", "")
    out["sector"] = out.get("sector") or stock_sector(sid)
    daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), out), "daily")
    if daily:
        closes = [r["close"] for r in daily]
        latest = daily[-1]
        close = latest["close"]
        if is_blank(out.get("price")):
            out["price"] = fmt_num(close)
        if is_blank(out.get("gain_6w")) and len(daily) >= 31:
            base = daily[-31]["close"]
            if base:
                out["gain_6w"] = f"{(close / base - 1) * 100:+.2f}%"
        if is_blank(out.get("gain_3d")) and len(daily) >= 4:
            base = daily[-4]["close"]
            if base:
                out["gain_3d"] = f"{(close / base - 1) * 100:+.2f}%"
        if is_blank(out.get("rsi")):
            rsi = calc_rsi(closes)
            out["rsi"] = fmt_num(rsi, 1)
        if is_blank(out.get("bband_pct")) and len(closes) >= 20:
            tail = closes[-20:]
            ma20 = sum(tail) / 20
            std = (sum((x - ma20) ** 2 for x in tail) / 20) ** 0.5
            upper, lower = ma20 + 2 * std, ma20 - 2 * std
            out["bband_pct"] = fmt_num(((close - lower) / (upper - lower)) * 100 if upper != lower else None, 1)
        if is_blank(out.get("vol_5d")) and len(daily) >= 5:
            vol5 = sum(r.get("volume", 0) for r in daily[-5:]) / 1000
            out["vol_5d"] = f"{vol5:,.0f}張"

        tech = technical_snapshot(daily, out)
        if is_blank(out.get("target")) and tech.get("resistance"):
            out["target"] = f"{tech['resistance'] * 1.02:.2f} (壓力×102%)"
        if is_blank(out.get("stop")) and tech.get("support"):
            out["stop"] = f"{tech['support'] * 0.995:.2f} (支撐×99.5%)"
        if is_blank(out.get("resistance")) and tech.get("resistance"):
            out["resistance"] = fmt_num(tech.get("resistance"))
        if is_blank(out.get("support")) and tech.get("support"):
            out["support"] = fmt_num(tech.get("support"))

    chip = read_chip_summary(sid)
    if chip and is_blank(out.get("foreign_5d")):
        foreign5 = chip.get("sum5", {}).get("foreign")
        if foreign5 is not None:
            out["foreign_5d"] = f"{foreign5:+,.0f}張"
    return out


def build_tech_panel(tech: dict) -> str:
    if not tech:
        return '<div class="strategy-note">技術資料不足，等待 FinMind 快取更新。</div>'
    ma_trends = tech.get("ma_trends") or {}
    volume_price = tech.get("volume_price", "─")
    volume_basis = tech.get("volume_price_basis", "─")
    ma_strip = ""
    for n in [5, 10, 20, 60, 120, 240]:
        val = tech.get(f"ma{n}")
        direction = ma_trends.get(f"ma{n}")
        if direction is None:
            arrow = '<span class="arrow-flat">→</span>'
        elif direction > 0:
            arrow = '<span class="arrow-up">▲</span>'
        elif direction < 0:
            arrow = '<span class="arrow-down">▼</span>'
        else:
            arrow = '<span class="arrow-flat">→</span>'
        ma_strip += f'<div class="ma-pill"><div class="ma-name">MA{n}</div><div class="ma-value">{fmt_num(val)} {arrow}</div></div>'
    close = tech.get("close")
    detrend_120 = tech.get("detrend_120")
    detrend_gap = ((close / detrend_120 - 1) * 100) if close and detrend_120 else None
    return f"""
<div class="tech-panel">
  <div class="ma-strip">{ma_strip}</div>
  <div class="tech-summary-grid">
    <div class="info-cell"><div class="k">量價評分</div><div class="v">{esc(volume_price)}</div></div>
    <div class="info-cell"><div class="k">判斷依據</div><div class="v">{esc(volume_basis)}</div></div>
    <div class="info-cell"><div class="k">趨勢型態</div><div class="v">{esc(tech.get('trend_pattern','─'))}</div></div>
    <div class="info-cell"><div class="k">120日扣抵值</div><div class="v">{fmt_num(detrend_120)}</div><div class="signal-dates">收盤距扣抵 {fmt_num(detrend_gap, 1)}%</div></div>
  </div>
</div>"""


def build_chip_panel(chip: dict, holding: dict) -> str:
    chip_latest = chip.get("latest", {})
    chip_sum5 = chip.get("sum5", {})
    chip_sum10 = chip.get("sum10", {})
    h_latest = holding.get("latest", {}) if holding else {}
    h_prev = holding.get("prev", {}) if holding else {}
    major_delta = h_latest.get("major", 0) - h_prev.get("major", 0) if h_latest and h_prev else None
    middle_people_delta = h_latest.get("middle_people", 0) - h_prev.get("middle_people", 0) if h_latest and h_prev else None
    retail_delta = h_latest.get("retail", 0) - h_prev.get("retail", 0) if h_latest and h_prev else None
    def chip_cell(value, suffix: str = "張", digits: int = 0) -> str:
        return f"{fmt_num(value, digits)}{suffix}" if value is not None else "尚無快取"

    def holding_cell(value, suffix: str = "", digits: int = 2) -> str:
        return f"{fmt_num(value, digits)}{suffix}" if value is not None else "尚無快取"

    def value_class(value) -> str:
        if value is None:
            return ""
        return "pos" if float(value or 0) >= 0 else "neg"

    chip_status = f"法人 {chip.get('date')}" if chip else "法人尚無快取"
    holding_status = f"股權 {holding.get('date')}" if holding else "股權尚無快取"
    chip_note_parts = []
    if chip:
        chip_note_parts.append(f"外資10日 {fmt_num(chip_sum10.get('foreign'),0)} 張")
        chip_note_parts.append(f"主力10日 {fmt_num(chip_sum10.get('total'),0)} 張")
    else:
        chip_note_parts.append("法人買賣超尚無快取")
    if holding:
        chip_note_parts.append(f"大戶週變化 {fmt_num(major_delta)}%")
        chip_note_parts.append(f"中實戶週變化 {fmt_num(middle_people_delta, 0)} 人")
        chip_note_parts.append(f"散戶週變化 {fmt_num(retail_delta)}%")
    else:
        chip_note_parts.append("股權分散尚無快取")
    chip_note = "｜".join(chip_note_parts)
    return f"""<div class="info-grid">
  <div class="info-cell"><div class="k">籌碼資料狀態</div><div class="v">{esc(chip_status)}｜{esc(holding_status)}</div></div>
  <div class="info-cell"><div class="k">外資買賣超</div><div class="v {value_class(chip_latest.get('foreign'))}">{chip_cell(chip_latest.get('foreign'))}</div></div>
  <div class="info-cell"><div class="k">投信買賣超</div><div class="v {value_class(chip_latest.get('trust'))}">{chip_cell(chip_latest.get('trust'))}</div></div>
  <div class="info-cell"><div class="k">自營商買賣超</div><div class="v {value_class(chip_latest.get('dealer'))}">{chip_cell(chip_latest.get('dealer'))}</div></div>
  <div class="info-cell"><div class="k">主力當日合計</div><div class="v {value_class(chip_latest.get('total'))}">{chip_cell(chip_latest.get('total'))}</div></div>
  <div class="info-cell"><div class="k">外資5日</div><div class="v {value_class(chip_sum5.get('foreign'))}">{chip_cell(chip_sum5.get('foreign'))}</div></div>
  <div class="info-cell"><div class="k">投信5日</div><div class="v {value_class(chip_sum5.get('trust'))}">{chip_cell(chip_sum5.get('trust'))}</div></div>
  <div class="info-cell"><div class="k">主力5日合計</div><div class="v {value_class(chip_sum5.get('total'))}">{chip_cell(chip_sum5.get('total'))}</div></div>
</div>
<div class="holding-info-grid">
  <div class="info-cell"><div class="k">大戶比例（400張以上）</div><div class="v">{holding_cell(h_latest.get('major'), '%')}</div></div>
  <div class="info-cell"><div class="k">大戶人數（400張以上）</div><div class="v">{holding_cell(h_latest.get('major_people'), '', 0)}</div></div>
  <div class="info-cell"><div class="k">中實戶持股人數（200-400張）</div><div class="v">{holding_cell(h_latest.get('middle_people'), '', 0)}</div></div>
  <div class="info-cell"><div class="k">中實戶比例（200-400張）</div><div class="v">{holding_cell(h_latest.get('middle'), '%')}</div></div>
  <div class="info-cell"><div class="k">散戶比例</div><div class="v">{holding_cell(h_latest.get('retail'), '%')}</div></div>
  <div class="info-cell"><div class="k">總股東人數</div><div class="v">{holding_cell(h_latest.get('total_people'), '', 0)}</div></div>
</div>
<div class="chip-line">{esc(chip_note)}</div>"""


def basket_card(s: dict, basket: str, ledger: dict[str, dict] | None = None) -> str:
    gain_cls = gain_color(s.get("gain_6w", ""))
    daily, tech, plan = stock_trade_context(s)
    chip_series = read_chip_series(s.get("id", ""))
    holding = read_holding_summary(s.get("id", ""))
    reason = basket_reason(s, tech, chip_series, holding)
    change_text, change_cls = daily_change_text(daily)
    close_text = fmt_num(tech.get("close") if tech else _to_float(s.get("price", ""), None))
    if basket == "marching":
        action = "SFZ試單/續抱"
        action_cls = "tag-green"
        tags = [
            ("行進籃", "tag-green"),
            ("TA3加碼觀察", "tag-yellow"),
            ("MA20主線", "tag"),
        ]
    elif basket == "consolidation":
        action = "MABC量價觀察"
        action_cls = "tag-blue"
        tags = [
            ("盤整籃", "tag-blue"),
            ("早買雷達", "tag"),
            ("等轉強", "tag"),
        ]
    else:
        action = "過熱不追"
        action_cls = "tag-red"
        tags = [
            ("風險區", "tag-red"),
            ("等回測", "tag"),
            ("不追高", "tag"),
        ]
    tag_html = "".join(f'<span class="tag {cls}">{label}</span>' for label, cls in tags)
    rr_warning = '<span class="rr-warning">⚠ R:R 過低</span>' if plan.get("rr") is not None and plan.get("rr") < 1.5 else ""
    return f"""
<div class="basket-card">
  <div class="basket-head">
    <div>
      <a class="basket-title-link" href="{stock_href(s.get('id',''))}" title="打開 {esc(s.get('id',''))} {esc(s.get('name',''))} 個股資訊頁">
        <span class="basket-code">{esc(s.get('id',''))}</span>
        <span class="basket-name">{esc(s.get('name',''))}</span>
      </a>
      <div style="font-size:12px;color:#8b949e;margin-top:4px">近6週 <span class="{gain_cls}">{s.get('gain_6w','─')}</span> ｜ 分數 {s.get('score','─')}</div>
    </div>
    <div class="basket-action {action_cls}">{action}</div>
  </div>
  <div class="basket-price-row">
    <div>
      <div style="font-size:11px;color:#6e7681">收盤價</div>
      <div class="basket-price">{close_text}</div>
    </div>
    <div class="basket-change {change_cls}">單日 {change_text}</div>
  </div>
  <div style="font-size:12px;color:#c9d1d9">買點 {plan['entry_text']} ｜ 目標 {plan['target_text']} ｜ 初始停損 {plan['initial_stop_text']} ｜ <span class="price-rr {plan['rr_class']}">R:R {plan['rr_text']}</span>{rr_warning}</div>
  <div style="font-size:12px;color:#8b949e;margin-top:4px">近支撐 {plan['initial_stop_text']} ｜ 符合條件：{esc(reason)}</div>
  <div class="tag-row">{tag_html}</div>
  {signal_summary_html(s.get('id',''), ledger or {})}
</div>"""


def build_basket_column(title: str, subtitle: str, stocks: list[dict], basket: str, ledger: dict[str, dict] | None = None) -> str:
    cards = "\n".join(basket_card(s, basket, ledger) for s in stocks[:12])
    if not cards:
        cards = '<div class="basket-card" style="color:#6e7681">今日沒有符合此籃條件的標的。</div>'
    title_color = {
        "marching": "#3fb950",
        "consolidation": "#58a6ff",
        "risk": "#f85149",
    }.get(basket, "#58a6ff")
    return f"""
<div class="card">
  <div class="section-label" style="color:{title_color}">{title}</div>
  <div class="strategy-note" style="margin-bottom:12px">{subtitle}</div>
  {cards}
</div>"""


def build_action_rows(items: list[dict], empty_text: str) -> str:
    if not items:
        return f'<div class="strategy-note" style="margin-top:10px">{empty_text}</div>'
    html_rows = ""
    for x in items:
        gap_txt = "─" if x["gap"] is None else f'{x["gap"]:+.1f}%'
        html_rows += f"""
<div class="action-row">
  <div>
    <a class="stock-link" href="stocks/{x['sid']}.html">{x['sid']} {esc(x['name'])}</a>
    <div class="note">{esc(x['basket'])}｜{esc(x['reason'])}</div>
  </div>
  <div><div class="label">收盤</div><div class="value">{fmt_num(x['close'])}</div></div>
  <div><div class="label">距買點</div><div class="value">{gap_txt}</div></div>
  <div><div class="label">買點</div><div class="value">{x['plan']['entry_text']}</div></div>
  <div><div class="label">初停</div><div class="value">{x['plan']['initial_stop_text']}</div></div>
  <div><div class="label">R:R</div><div class="value {x['plan']['rr_class']}">{x['plan']['rr_text']}</div></div>
</div>"""
    return f'<div class="action-list">{html_rows}</div>'


def next_business_day_text(date_str: str) -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return "─"
    d = d + timedelta(days=1)
    while d.weekday() >= 5:
        d = d + timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def report_date_badge(date_str: str, include_next: bool = False) -> str:
    text = f"資料日期：{esc(date_str)}"
    if include_next:
        text += f"｜下次交易日：{esc(next_business_day_text(date_str))}"
    return f'<span class="section-date">{text}</span>'


def build_sell_alert_rows(stocks: list[dict], limit: int = 5, only_actionable: bool = True) -> str:
    alerts = []
    for s in stocks:
        s = enrich_stock_fields(s)
        rows = merge_report_close(read_price_history(s.get("id", "")), s)
        daily = aggregate_ohlcv(rows, "daily")
        weekly = aggregate_ohlcv(rows, "weekly")
        tech = technical_snapshot(daily, s)
        decision = build_trade_decision(tech, s)
        chip_series = read_chip_series(s.get("id", ""))
        signal = calc_sell_signal(daily, weekly, chip_series, s, decision)
        severity = 2 if signal["class"] == "exit" else 1 if signal["class"] == "watch" else 0
        if only_actionable and severity == 0:
            continue
        alerts.append({
            "id": s.get("id", ""),
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "close": tech.get("close") if tech else None,
            "signal": signal,
            "severity": severity,
            "score": _to_float(s.get("score", "0")),
        })
    alerts.sort(key=lambda x: (-x["severity"], x["signal"].get("ma20_gap") if x["signal"].get("ma20_gap") is not None else 99, -x["score"]))
    rows = ""
    for x in alerts[:limit]:
        sig = x["signal"]
        ma20_gap = "─" if sig.get("ma20_gap") is None else f'{sig["ma20_gap"]:+.1f}%'
        profit = "─" if sig.get("profit") is None else f'{sig["profit"]:+.1f}%'
        cls = sig.get("class") or ""
        rows += f"""
<div class="alert-row">
  <div><a class="stock-link" href="stocks/{x['id']}.html">{x['id']} {esc(x['name'])}</a><div class="signal-dates">收盤 {fmt_num(x['close'])}</div></div>
  <div><div class="label">MA20距離</div><div class="value">{ma20_gap}</div></div>
  <div><div class="label">買點損益</div><div class="value">{profit}</div></div>
  <div><span class="alert-level {cls}">{esc(sig['level'])}</span><div class="signal-dates" style="margin-top:4px">{esc(sig['reason'])}</div></div>
</div>"""
    if not rows:
        return '<div class="strategy-note" style="margin-top:10px">目前沒有需要立刻處理的賣出警示。</div>'
    return rows


def build_today_action_card(stocks: list[dict], date_str: str) -> str:
    items = []
    for s in stocks:
        s = enrich_stock_fields(s)
        _, tech, decision = stock_trade_context(s)
        gap = tech.get("entry_gap") if tech else None
        if gap is None:
            continue
        sid = s.get("id", "")
        daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily") if sid else []
        chip_series = read_chip_series(sid) if sid else []
        pressure = pressure_absorption_analysis(sid, daily, chip_series, read_margin_series(sid), tech) if sid else {}
        reason_text = basket_reason(s, tech, chip_series, read_holding_summary(sid))
        pressure_level = pressure.get("level")
        if pressure_level and pressure_level not in reason_text:
            reason_text += f" / {pressure_level}"
        items.append({
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "basket": basket_label(classify_basket(s)),
            "reason": reason_text,
            "gap": gap,
            "close": tech.get("close"),
            "score": _to_float(s.get("score", "0")),
            "plan": decision,
        })
    executable = [x for x in items if -3 <= x["gap"] <= 3 and (x["plan"].get("rr") or 0) >= 1.5]
    executable.sort(key=lambda x: (0 if (x["plan"].get("rr") or 0) >= 1.5 else 1, abs(x["gap"]), -x["score"]))

    return f"""
<div class="card">
  <div class="section-head">
    <div class="section-label">今日可執行清單</div>
    {report_date_badge(date_str, include_next=True)}
  </div>
  <div class="strategy-note">首頁只列今天需要處理的動作：買入建議看收盤是否落在買點 ±3% 內；賣出建議看持倉或追蹤名單是否出現 MA20、損益或型態警示。</div>
  <h3 class="subsection-title">買入建議</h3>
  {build_action_rows(executable[:5], "今日沒有收盤落在買點 ±3% 內的標的。")}
  <h3 class="subsection-title">賣出建議</h3>
  {build_sell_alert_rows(stocks, limit=5, only_actionable=True)}
</div>"""


def build_b2_method_card() -> str:
    return """
<div class="card">
  <div class="section-label">M大 B2 賣壓吸收主軸</div>
  <div class="grid grid-2" style="margin-top:10px">
    <div class="info-cell"><div class="k">1. 拉長尺度</div><div class="v">長多 / 轉長多</div><div class="chip-line">先看 MA120、MA240 與240扣抵價，再看大戶、散戶、股東人數的長期動態。</div></div>
    <div class="info-cell"><div class="k">2. 跌升籌碼</div><div class="v">下跌賣壓 vs 拉升買盤</div><div class="chip-line">比較黑K/回檔段與紅K/拉升段的外資、主力、大戶短期動態。</div></div>
    <div class="info-cell"><div class="k">3. 扣抵情境</div><div class="v">20日量 / 240價</div><div class="chip-line">20日前量能與近5日量能比較，240日前價格若低於現價，長均線較有機會轉有利。</div></div>
    <div class="info-cell"><div class="k">4. 止跌品質</div><div class="v">快回均線，慢過前高</div><div class="chip-line">止跌後能否站回均線、紅K是否變多、下影線是否常出現、是否慢慢過前高。</div></div>
  </div>
</div>"""


def build_market_light_card(latest: dict, stocks: list[dict], date_str: str) -> str:
    marching, consolidation, risk = split_baskets(stocks)
    action_items = []
    for s in stocks:
        s = enrich_stock_fields(s)
        _, tech, decision = stock_trade_context(s)
        gap = tech.get("entry_gap") if tech else None
        if gap is not None and -3 <= gap <= 3:
            action_items.append((s, tech, decision))

    checks = []
    if len(risk) >= max(4, len(stocks) * 0.25):
        checks.append(("候選風險", "偏熱", "neg"))
    elif len(marching) >= len(consolidation):
        checks.append(("候選結構", f"行進籃 {len(marching)} / 盤整籃 {len(consolidation)}", "pos"))
    else:
        checks.append(("候選結構", f"盤整籃較多，行進籃 {len(marching)}", "neu"))

    checks.append(("可執行買點", f"{len(action_items)} 檔落在買點±3%", "pos" if action_items else "neu"))

    score = sum(1 for _, _, cls in checks if cls == "pos") - sum(1 for _, _, cls in checks if cls == "neg")
    if score >= 2:
        light, cls, title = "多", "pos", "可做但控部位"
    elif score <= -1:
        light, cls, title = "空", "neg", "先保守"
    else:
        light, cls, title = "中立", "neu", "挑個股，不追高"

    check_html = "".join(
        f'<div class="check-item"><div class="k">{esc(k)}</div><div class="v {cls}">{esc(v)}</div></div>'
        for k, v, cls in checks
    )
    taiex_html = coming_soon_block("大盤指數（接入中）", '<div class="strategy-note">TAIEX快取尚未接入。未來 data/taiex.csv 發布後，這個區塊會自動展開。</div>', "data/taiex.csv", False)
    overview = latest.get("market_overview", "").strip()
    overview_html = f'<div class="market-text" style="margin-top:12px">{overview.replace(chr(10), "<br>")}</div>' if overview else ""
    return f"""
<div class="card">
  <div class="section-head">
    <div class="section-label">大盤燈號</div>
    {report_date_badge(date_str)}
  </div>
  <div class="market-light">
    <div class="market-badge {cls}">{light}</div>
    <div>
      <div style="font-size:16px;font-weight:800;color:#e6edf3">{title}</div>
      <div class="strategy-note" style="margin-top:4px">大盤指數資料尚未接入時，先用候選池結構與可執行買點做風控前提；缺資料會明確顯示。</div>
      <div class="check-grid" style="margin-top:10px">{check_html}</div>
      {taiex_html}
    </div>
  </div>
  {overview_html}
</div>"""


def build_risk_watchlist(stocks: list[dict], limit: int = 6) -> list[dict]:
    candidates = []
    for s in stocks:
        s = enrich_stock_fields(s)
        rows = merge_report_close(read_price_history(s.get("id", "")), s)
        daily = aggregate_ohlcv(rows, "daily")
        weekly = aggregate_ohlcv(rows, "weekly")
        tech = technical_snapshot(daily, s)
        decision = build_trade_decision(tech, s)
        chip_series = read_chip_series(s.get("id", ""))
        sell_signal = calc_sell_signal(daily, weekly, chip_series, s, decision)
        gap = tech.get("entry_gap") if tech else None
        rr = decision.get("rr")
        volume_price = tech.get("volume_price") if tech else ""
        score = 0
        if sell_signal.get("class") == "exit":
            score += 5
        elif sell_signal.get("class") == "watch":
            score += 3
        if gap is not None and gap > 8:
            score += 2
        if rr is not None and rr < 1.5:
            score += 1
        if volume_price in {"放量下跌", "量縮下跌"}:
            score += 2
        if score:
            item = dict(s)
            item["risk_score"] = score
            candidates.append(item)
    candidates.sort(key=lambda x: (-x.get("risk_score", 0), -_to_float(x.get("score", "0"))))
    return candidates[:limit]



def build_top5_card(stocks: list[dict]) -> str:
    """首頁 Top5 快速摘要卡片"""
    top5 = stocks[:5]
    if not top5:
        return ""
    items = ""
    for i, s in enumerate(top5, 1):
        s = enrich_stock_fields(s)
        sid = s.get("id", "")
        name = s.get("name", "")
        close = s.get("price") or s.get("close", "")
        score = s.get("score", "")
        basket = basket_label(classify_basket(s))
        items += f"""
    <a href="stocks/{esc(sid)}.html" style="text-decoration:none;display:flex;align-items:center;gap:12px;padding:10px 14px;border-radius:8px;background:#161b22;transition:background .2s" onmouseover="this.style.background='#1c2128'" onmouseout="this.style.background='#161b22'">
      <div style="font-size:20px;font-weight:900;color:#484f58;min-width:28px">#{i}</div>
      <div style="flex:1;min-width:0">
        <div style="font-weight:700;color:#e6edf3;font-size:14px">{esc(sid)} {esc(name)}</div>
        <div style="font-size:12px;color:#8b949e;margin-top:2px">{esc(basket)}｜收盤 {esc(str(close))}</div>
      </div>
      <div style="font-size:18px;font-weight:900;color:#58a6ff">{esc(str(score))}</div>
    </a>"""
    return f"""
  <div class="card">
    <div class="section-label">今日 Top5 快速摘要</div>
    <div class="strategy-note" style="margin-bottom:10px">選股池前 5 名，<a href="selection.html">查看完整 Top20 →</a></div>
    <div style="display:flex;flex-direction:column;gap:6px">{items}</div>
  </div>"""


def build_sector_heat_widget(stocks: list[dict], top_n: int = 8) -> str:
    buckets: dict[str, dict] = {}
    for s in stocks:
        sid = str(s.get("id") or "")
        sector = stock_sector(sid) or "Unknown"
        item = buckets.setdefault(sector, {"count": 0, "score": 0.0})
        item["count"] += 1
        item["score"] += _to_float(s.get("score"), 0)
    if not buckets:
        return ""
    ranked = sorted(buckets.items(), key=lambda kv: (-kv[1]["count"], -kv[1]["score"]))[:top_n]
    pills = ""
    for sector, row in ranked:
        avg = row["score"] / max(1, row["count"])
        pills += f'<div class="heat-pill"><div class="k">{esc(sector)} · {row["count"]}檔</div><div class="v">{fmt_num(avg,1)}</div></div>'
    return f'<div class="card"><div class="section-label">Sector Heat</div><div class="heat-strip">{pills}</div></div>'


def build_index_page(reports: list[dict]) -> str:
    latest = latest_stock_report(reports)
    latest_stocks = with_report_date(latest.get("stocks", []), latest.get("date", ""))
    date_str = latest.get("date", "─")

    body = f"""
<div class="container">
  <div class="page-title">Stockfrom脩 量化選股站</div>
  <div class="page-sub">今日工作台：先看決策狀態、官方風險與市場流向，再回到個股證據。最新報告：{date_str}</div>
  {build_daily_decisions_panel(load_daily_decisions_payload())}
  {build_daily_market_flow_panel(load_daily_market_flow_payload())}
  {build_weekly_holder_risers_panel(load_weekly_holder_risers_payload())}
  {build_market_sentiment_panel(load_market_sentiment_payload())}
  {build_market_light_card(latest, latest_stocks, date_str)}
  {build_sector_heat_widget(latest_stocks)}
  {build_today_action_card(latest_stocks, date_str)}
  {build_top5_card(latest_stocks)}
</div>
{disclaimer_modal_html()}"""

    return html_page("首頁", "home", body)


def _m_check(text: str, cls: str = "") -> str:
    return f'<span class="m-check {cls}">{esc(text)}</span>'


def sma_at(rows: list[dict], window: int, offset: int = 0) -> float | None:
    vals = ma_values(rows, window)
    idx = len(vals) - 1 - offset
    return vals[idx] if 0 <= idx < len(vals) else None


def mda_market_regime() -> dict:
    rows = aggregate_ohlcv(read_price_history("2330"), "daily")
    if len(rows) < 60:
        return {"ok": False, "state": "資料不足", "class": "neu", "note": "2330 日線資料不足，暫不放大訊號", "ma20": None, "ma60": None}
    ma20 = sma_at(rows, 20)
    ma60 = sma_at(rows, 60)
    close = rows[-1].get("close")
    ok = bool(ma20 and ma60 and ma20 > ma60)
    state = "多頭可做" if ok else "空頭停手"
    cls = "pos" if ok else "neg"
    note = f"2330 收盤 {fmt_num(close)}｜SMA20 {fmt_num(ma20)}｜SMA60 {fmt_num(ma60)}"
    return {"ok": ok, "state": state, "class": cls, "note": note, "ma20": ma20, "ma60": ma60}


def mda_strict_entry(rows: list[dict]) -> dict:
    if len(rows) < 15:
        return {"ok": False, "items": [("資料不足", "warn")], "entry": None, "stop": None, "target1": None, "target2": None}
    close = rows[-1].get("close")
    open_ = rows[-1].get("open")
    ma5 = sma_at(rows, 5)
    ma5_prev = sma_at(rows, 5, 5)
    ma10 = sma_at(rows, 10)
    slope_positive = bool(ma5 and ma5_prev and ma5 > ma5_prev)
    pullback_ma5 = bool(close and ma5 and abs(close - ma5) / ma5 < 0.015)
    red_k = bool(close and open_ and close > open_)
    ok = slope_positive and pullback_ma5 and red_k
    checks = [
        ("SMA5斜率>0", "ok" if slope_positive else "bad"),
        ("回到SMA5±1.5%", "ok" if pullback_ma5 else "warn"),
        ("紅K", "ok" if red_k else "bad"),
    ]
    return {
        "ok": ok,
        "items": checks,
        "entry": close if ok else None,
        "stop": ma10,
        "target1": close * 1.10 if ok and close else None,
        "target2": close * 1.15 if ok and close else None,
    }


def mda_observation_checks(stock_id: str, rows: list[dict], tech: dict, chip_series: list[dict], holding: dict) -> dict:
    close = rows[-1].get("close") if rows else None
    ma120 = tech.get("ma120") if tech else None
    ma240 = tech.get("ma240") if tech else None
    slopes = tech.get("ma_slopes") or {}
    ma120_up = slopes.get("ma120") is not None and slopes.get("ma120") > 0
    ma240_up = slopes.get("ma240") is not None and slopes.get("ma240") > 0
    a_observe = ma120_up and ma240_up
    vol_ratio = tech.get("volume_ratio") if tech else None
    ma120_challenge = bool(close and ma120 and close >= ma120 * 0.97)
    ma120_vals = ma_values(rows, 120)
    ma120_stand = bool(
        len(rows) >= 122
        and all(
            rows[-1 - i].get("close") is not None
            and ma120_vals[-1 - i] is not None
            and rows[-1 - i]["close"] >= ma120_vals[-1 - i]
            for i in range(3)
        )
    )
    ma240_deduction = bool(len(rows) > 240 and close and rows[-241].get("close") and close > rows[-241]["close"])
    volume_money = bool(vol_ratio and vol_ratio >= 1.15 and ma120_challenge)

    holding_series = read_holding_series(stock_id) if stock_id else []
    if not holding_series and holding:
        latest = holding.get("latest") or {}
        prev = holding.get("prev") or {}
        holding_series = [
            {"major": prev.get("major"), "retail": prev.get("retail"), "total_people": prev.get("total_people")},
            {"major": latest.get("major"), "retail": latest.get("retail"), "total_people": latest.get("total_people")},
        ]
    major_delta = retail_delta = people_delta = None
    if len(holding_series) >= 2:
        last, prev = holding_series[-1], holding_series[-2]
        if last.get("major") is not None and prev.get("major") is not None:
            major_delta = last["major"] - prev["major"]
        if last.get("retail") is not None and prev.get("retail") is not None:
            retail_delta = last["retail"] - prev["retail"]
        if last.get("total_people") is not None and prev.get("total_people") is not None:
            people_delta = last["total_people"] - prev["total_people"]

    foreign_10d = sum(float(x.get("foreign") or 0) for x in chip_series[-10:])
    force_10d = sum(float(x.get("total") or 0) for x in chip_series[-10:])
    foreign_stopping = bool(foreign_10d >= 0 or (chip_series and chip_series[-1].get("foreign", 0) >= 0))
    main_not_back = bool((major_delta is not None and major_delta < 0) or force_10d < 0)
    retail_risk = bool((retail_delta is not None and retail_delta > 0) or (people_delta is not None and people_delta > 0))

    positives = [
        ("MA120上彎", ma120_up, "ok"),
        ("MA240上彎", ma240_up, "ok"),
        ("接近/突破120日", ma120_challenge, "ok" if ma120_stand else "warn"),
        ("120/240扣抵有利", ma240_deduction, "ok"),
        ("有量挑戰關鍵線", volume_money, "ok"),
        ("外資停止賣或偏買", foreign_stopping, "ok"),
    ]
    risks = [
        ("主力大戶未明顯回來", main_not_back),
        ("散戶/股東人數增加", retail_risk),
        ("尚未有效站上120日", not ma120_stand),
    ]
    pos_count = sum(1 for _, ok, _ in positives if ok)
    risk_count = sum(1 for _, ok in risks if ok)
    if a_observe and (foreign_stopping or not main_not_back) and risk_count <= 2:
        level = "重點觀察"
        cls = "tag-green"
    elif a_observe:
        level = "觀察中"
        cls = "tag-yellow"
    else:
        level = "暫緩觀察"
        cls = "tag"
    return {
        "level": level,
        "tag_cls": cls,
        "score": pos_count * 20 - risk_count * 8,
        "positives": positives,
        "risks": risks,
        "a_observe": a_observe,
        "line": f"MA120 {fmt_num(ma120)}（斜率 {fmt_num(slopes.get('ma120'))}）｜MA240 {fmt_num(ma240)}（斜率 {fmt_num(slopes.get('ma240'))}）｜量比 {fmt_num(vol_ratio, 2)}x｜外資10日 {fmt_num(foreign_10d, 0)} 張｜主力10日 {fmt_num(force_10d, 0)} 張｜大戶週變 {fmt_num(major_delta)}%｜散戶週變 {fmt_num(retail_delta)}%",
    }


def mda_abc_checks(s: dict, rows: list[dict], tech: dict, chip_series: list[dict], holding: dict) -> dict:
    close = rows[-1].get("close") if rows else _to_float(s.get("price"), None)
    ma20 = tech.get("ma20") if tech else None
    ma60 = tech.get("ma60") if tech else None
    ma120 = tech.get("ma120") if tech else None
    ma240 = tech.get("ma240") if tech else None
    ma120_slope = (tech.get("ma_slopes") or {}).get("ma120") if tech else None
    ma240_slope = (tech.get("ma_slopes") or {}).get("ma240") if tech else None
    detrend_240 = bool(len(rows) > 240 and close and rows[-241].get("close") and close > rows[-241]["close"])
    ma120_up = bool(ma120 and ma120_slope is not None and ma120_slope > 0)
    ma240_up = bool(ma240 and ma240_slope is not None and ma240_slope > 0)
    a_ok = ma120_up and ma240_up
    a_near = bool(close and ma20 and ma60 and close > ma20 > ma60)
    recent60 = rows[-60:] if rows else []
    low60 = min((r.get("low") for r in recent60 if r.get("low") is not None), default=None)
    high60 = max((r.get("high") for r in recent60 if r.get("high") is not None), default=None)
    recent20_low = min((r.get("low") for r in rows[-20:] if r.get("low") is not None), default=None) if rows else None
    range60 = ((high60 / low60 - 1) * 100) if high60 and low60 else None
    no_new_low = bool(recent20_low and low60 and recent20_low >= low60 * 0.98)
    a_dormant = bool(not a_ok and detrend_240 and no_new_low and (range60 is None or range60 <= 38))
    a_score = 40 if a_ok else 28 if a_near else 18 if a_dormant else 12 if close and ma20 and close > ma20 else 0

    chip = chip_trend_metrics(chip_series, holding)
    holding_series = read_holding_series(s.get("id", ""))
    major_4w_delta = holding_delta(holding_series, "major", 4)
    major_8w_delta = holding_delta(holding_series, "major", 8)
    retail_4w_delta = holding_delta(holding_series, "retail", 4)
    retail_8w_delta = holding_delta(holding_series, "retail", 8)
    people_4w_delta = holding_delta(holding_series, "total_people", 4)
    people_8w_delta = holding_delta(holding_series, "total_people", 8)
    latest_major = None
    if holding_series:
        latest_major = holding_series[-1].get("major")
    elif holding:
        latest_major = (holding.get("latest") or {}).get("major")
    major_accumulating = (
        (major_4w_delta is not None and major_4w_delta >= 0.5)
        or (major_8w_delta is not None and major_8w_delta >= 1.0)
    )
    retail_support = (
        (retail_4w_delta is not None and retail_4w_delta <= -0.3)
        or (retail_8w_delta is not None and retail_8w_delta <= -0.8)
    )
    people_support = (
        (people_4w_delta is not None and people_4w_delta < 0)
        or (people_8w_delta is not None and people_8w_delta < 0)
    )
    b1_ok = bool(major_accumulating)
    b1_score = 45 if b1_ok and (retail_support or people_support) else 38 if b1_ok else 20 if latest_major is not None else 0

    volume_price = tech.get("volume_price") if tech else "資料不足"
    pressure = pressure_absorption_analysis(s.get("id", ""), rows, chip_series, read_margin_series(s.get("id", "")), tech)
    not_break = bool(close and ma20 and close >= ma20 * 0.97)
    retail_not_hot = retail_4w_delta is None or retail_4w_delta <= 1.0
    b2_ok = pressure.get("score", 0) >= 78 and retail_not_hot
    b2_watch = pressure.get("score", 0) >= 58 or (volume_price in {"量縮價穩", "量縮價漲", "均量上彎"} and not_break)
    b2_score = 15 if b2_ok else 8 if b2_watch else 0

    items = [
        ("A：長多/空轉多", "ok" if a_ok else "warn" if ma120_up or ma240_up or a_near or a_dormant else "bad"),
        ("B1籌碼未離開", "ok" if b1_ok else "warn" if b1_score else "bad"),
        ("B2賣壓吸收", "ok" if b2_ok else "warn" if b2_score else "bad"),
    ]
    score = a_score + b1_score + b2_score
    return {
        "score": score,
        "items": items,
        "a_score": a_score,
        "b1_score": b1_score,
        "b2_score": b2_score,
        "a_phase": "已發動長多" if a_ok else "未發動空轉多觀察" if a_dormant else "轉強觀察" if a_near else "A未成立",
        "volume_line": pressure.get("summary") or (f"{volume_price}｜{tech.get('volume_price_basis', '')}" if tech else "量價資料不足"),
        "pressure": pressure,
        "chip_line": f"大戶4週 {fmt_num(major_4w_delta, 2)}%｜8週 {fmt_num(major_8w_delta, 2)}%；散戶4週 {fmt_num(retail_4w_delta, 2)}%｜8週 {fmt_num(retail_8w_delta, 2)}%；股東4週 {fmt_num(people_4w_delta, 0)}人｜8週 {fmt_num(people_8w_delta, 0)}人；主力10日 {fmt_num(chip.get('total_10d'), 0)} 張",
    }


def mda_score_stock(s: dict, market_ok: bool) -> dict:
    daily, tech, _decision = stock_trade_context(s)
    chip_series = read_chip_series(s.get("id", ""))
    holding = read_holding_summary(s.get("id", ""))
    abc = mda_abc_checks(s, daily, tech, chip_series, holding)
    strict = mda_strict_entry(daily)
    observation = mda_observation_checks(s.get("id", ""), daily, tech, chip_series, holding)
    close = daily[-1].get("close") if daily else _to_float(s.get("price"), None)
    if not market_ok:
        action = "大盤停手"
        tag_cls = "tag-red"
    else:
        action = observation["level"]
        tag_cls = observation["tag_cls"]

    score = min(100, max(0, abc["score"] * 0.55 + observation["score"] * 0.45))
    checks = [_m_check(text, cls) for text, cls in abc["items"]]
    checks += [_m_check(text, cls if ok else "bad") for text, ok, cls in observation["positives"]]
    risk_checks = [_m_check(text, "bad" if ok else "ok") for text, ok in observation["risks"]]

    return {
        "id": s.get("id", ""),
        "name": s.get("name", ""),
        "market": s.get("market", ""),
        "score": score,
        "action": action,
        "tag_cls": tag_cls,
        "close": fmt_num(close),
        "change": daily_change_text(daily),
        "abc": "ABC完整" if abc["score"] >= 78 else "ABC觀察" if abc["score"] >= 70 else "ABC未齊",
        "strict": "Strict觀察成立" if strict["ok"] else "Strict未觸發",
        "observation": observation["level"],
        "a_score": abc["a_score"],
        "b1_score": abc["b1_score"],
        "b2_score": abc["b2_score"],
        "reason": " ".join(checks),
        "risk_reason": " ".join(risk_checks),
        "chip_line": abc["chip_line"],
        "volume_line": abc.get("volume_line") or observation["line"],
        "sort": (0 if action == "重點觀察" else 1 if action == "觀察中" else 2 if action == "暫緩觀察" else 3, -score),
    }


def load_mda_universe_scan_rows() -> list[dict]:
    path = LOCAL_DATA_DIR / "mda_universe_scan.json"
    if not path.exists():
        return []
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    try:
        market_cache = json.loads(MARKET_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        market_cache = {}
    stock_info = market_cache.get("stocks") or {}
    for r in rows:
        sid = str(r.get("stock_id") or "")
        info = stock_info.get(sid) or {}
        if info.get("market"):
            r["market"] = info.get("market")
        if info.get("name") and not r.get("name"):
            r["name"] = info.get("name")
    basket_order = {"已發動籃": 0, "空轉多觀察籃": 1, "未發動觀察籃": 2, "未入籃": 3}
    rows = [
        r for r in rows
        if isinstance(r, dict) and _to_float(r.get("close"), 0) >= MDA_MIN_CLOSE
    ]
    rows.sort(key=lambda r: (
        basket_order.get(r.get("basket", ""), 9),
        -float(r.get("score") or 0),
        str(r.get("stock_id") or ""),
    ))
    return rows


def build_mda_universe_section() -> str:
    rows = load_mda_universe_scan_rows()
    if not rows:
        return ""
    summary_path = LOCAL_DATA_DIR / "mda_full_market_refresh_summary.json"
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    except Exception:
        summary = {}
    groups = ["已發動籃", "空轉多觀察籃", "未發動觀察籃"]
    counts = {g: sum(1 for r in rows if r.get("basket") == g) for g in groups}
    latest_date = max((str(r.get("date") or "") for r in rows), default="─")
    universe_count = summary.get("universe_count")
    candidate_count = summary.get("candidate_count")
    holding_dates = (summary.get("holding") or {}).get("query_dates")
    price_months = summary.get("price_months")
    core_launched = sum(1 for r in rows if mda_is_core_launched(r))
    holding_empty = not holding_dates or _to_float(holding_dates, 0) <= 0
    holding_note = "" if holding_empty else f"股權週次 {fmt_num(holding_dates, 0)}，"
    holding_column_attr = ' data-empty="true"' if holding_empty else ""
    holding_column_notice = '<div class="table-placeholder-note">股權週次欄位接入中</div>' if holding_empty else ""
    return f"""
  <div class="card">
    <div class="section-label">全市場 M大主篩</div>
    <div class="strategy-note" style="margin-bottom:12px">
      全市場掃描結果已拆到獨立籃子頁：上市櫃普通股 {fmt_num(universe_count, 0)} 檔，先用股權分散找出大戶累積候選 {fmt_num(candidate_count, 0)} 檔，再用日線判斷 MA120 / MA240 / 扣抵 / 量縮。{holding_note}日線約 {fmt_num(price_months, 0)} 個月，最新股價日 {esc(latest_date)}。
    </div>
    <div class="grid grid-3" style="margin-bottom:14px">
      <div class="metric"><div class="metric-num" style="color:#3fb950">{counts["已發動籃"]}</div><div class="metric-label">已發動籃｜核心 {core_launched}</div></div>
      <div class="metric"><div class="metric-num" style="color:#d2a520">{counts["空轉多觀察籃"]}</div><div class="metric-label">空轉多觀察籃</div></div>
      <div class="metric"><div class="metric-num" style="color:#58a6ff">{counts["未發動觀察籃"]}</div><div class="metric-label">未發動觀察籃</div></div>
    </div>
    <div class="strategy-note" style="margin-bottom:12px">本區已先排除收盤價低於 {fmt_num(MDA_MIN_CLOSE, 0)} 元的股票。</div>
    <div class="tag-row">
      <button class="tab-btn tag tag-green" data-tab="launched" style="cursor:pointer">查看 已發動籃</button>
      <button class="tab-btn tag tag-blue" data-tab="consolidation" style="cursor:pointer">查看 盤整籃</button>
    </div>
  </div>"""
    blocks = []
    for group in groups:
        group_rows = [r for r in rows if r.get("basket") == group]
        if not group_rows:
            continue
        trs = []
        for r in group_rows[:80]:
            sid = str(r.get("stock_id") or "")
            market = r.get("market") or "─"
            full_stock_page = OUTPUT_DIR / "stocks" / f"{sid}.html"
            mda_stock_page = OUTPUT_DIR / "mda_stocks" / f"{sid}.html"
            info_href = ""
            stock_link_label = ""
            if full_stock_page.exists():
                info_href = f"stocks/{esc(sid)}.html"
                stock_link_label = "個股資訊"
            elif mda_stock_page.exists():
                info_href = f"stocks/{esc(sid)}.html"
                stock_link_label = "M大解析"
            else:
                info_href = f"mda_candidates/{esc(sid)}.html"
                stock_link_label = "候選資訊"
            if info_href:
                stock_link = f'<a class="tag tag-blue" href="{info_href}">{stock_link_label}</a>'
                stock_html = f'<a class="stock-link" href="{info_href}">{esc(sid)} {esc(r.get("name", ""))}</a>'
                info_html = (
                    f'<a class="stock-link" href="{info_href}"><div class="m-checks">{esc(market)}｜{esc(r.get("date", ""))}</div></a>'
                    f'<a href="{info_href}" style="text-decoration:none"><div class="signal-dates">收盤 {fmt_num(r.get("close"), 2)}｜距高 {fmt_num(r.get("one_year_high_gap_pct"), 1)}%</div></a>'
                    f'<a href="{info_href}" style="text-decoration:none"><div class="signal-dates">MA120 {fmt_num(r.get("ma120"), 2)}｜MA240 {fmt_num(r.get("ma240"), 2)}</div></a>'
                    f'<div style="margin-top:6px">{stock_link}</div>'
                )
            trs.append(f"""
<tr>
  <td>{stock_html}<div class="signal-dates">{esc(market)}</div></td>
  <td><div class="m-score">{fmt_num(r.get("score"), 0)}</div></td>
  <td>{info_html}</td>
  <td><div class="m-checks">MA120 {fmt_num(r.get("ma120_slope_pct"), 2)}%｜MA240 {fmt_num(r.get("ma240_slope_pct"), 2)}%</div><div class="signal-dates">240扣抵 {fmt_num(r.get("deduct240_gap_pct"), 1)}%</div></td>
  <td{holding_column_attr}><div class="m-checks">大戶4週 {fmt_num(r.get("major_4w_pctpt"), 2)}pt｜8週 {fmt_num(r.get("major_8w_pctpt"), 2)}pt</div><div class="signal-dates">散戶4週 {fmt_num(r.get("retail_4w_pctpt"), 2)}pt｜股東4週 {fmt_num(r.get("people_4w"), 0)}</div></td>
  <td><div class="signal-dates">{esc(r.get("reason", ""))}</div></td>
</tr>""")
        extra = ""
        if len(group_rows) > 80:
            extra = f'<div class="strategy-note" style="margin-top:8px">此籃共 {len(group_rows)} 檔，頁面先顯示前 80 檔；完整名單在 data/mda_universe_scan.csv。</div>'
        blocks.append(f"""
<div style="margin-top:18px">
  <div class="section-label">{esc(group)}｜{len(group_rows)} 檔</div>
  {holding_column_notice}
  <div style="overflow-x:auto">
    <table class="stock-table">
      <thead><tr><th>股票</th><th>分數</th><th>個股資訊</th><th>A 長均線</th><th{holding_column_attr}>B1 股權</th><th>判讀摘要</th></tr></thead>
      <tbody>{''.join(trs)}</tbody>
    </table>
  </div>
  {extra}
</div>""")
    return f"""
  <div class="card">
    <div class="section-label">全市場 M大主篩</div>
    <div class="strategy-note" style="margin-bottom:12px">
      這裡已合併全市場掃描結果：上市櫃普通股 {fmt_num(universe_count, 0)} 檔，先用股權分散找出大戶累積候選 {fmt_num(candidate_count, 0)} 檔，再用日線判斷 MA120 / MA240 / 扣抵 / 量縮。{holding_note}日線約 {fmt_num(price_months, 0)} 個月，最新股價日 {esc(latest_date)}。
    </div>
    <div class="grid grid-3" style="margin-bottom:10px">
      <div class="metric"><div class="metric-num" style="color:#3fb950">{counts["已發動籃"]}</div><div class="metric-label">已發動籃</div></div>
      <div class="metric"><div class="metric-num" style="color:#d2a520">{counts["空轉多觀察籃"]}</div><div class="metric-label">空轉多觀察籃</div></div>
      <div class="metric"><div class="metric-num" style="color:#58a6ff">{counts["未發動觀察籃"]}</div><div class="metric-label">未發動觀察籃</div></div>
    </div>
    {''.join(blocks)}
  </div>"""


def build_mda_page(reports: list[dict]) -> str:
    """M大觀察 — 3 tabs: overview / launched / consolidation"""
    # ── Tab 1: Overview (原 build_mda_page 內容) ──
    latest = latest_stock_report(reports)
    date_str = latest.get("date", "─")
    scored = [mda_score_stock(enrich_stock_fields(dict(s)), True) for s in latest.get("stocks", [])]
    market = {"class": "", "state": "", "note": ""}
    scored.sort(key=lambda x: x["sort"])
    primary = [x for x in scored if x["action"] == "重點觀察"]
    wait = [x for x in scored if x["action"] == "觀察中"]
    avoid = [x for x in scored if x["action"] in {"暫緩觀察", "大盤停手"}]

    rows_html = ""
    for x in scored:
        change_text, change_cls = x["change"]
        rows_html += f"""
<tr>
  <td><a class="stock-link" href="stocks/{esc(x['id'])}.html">{esc(x['id'])} {esc(x['name'])}</a><div class="signal-dates">{esc(x['market'])}｜M大解析頁</div></td>
  <td><span class="tag {x['tag_cls']}">{esc(x['action'])}</span><div class="m-score">{fmt_num(x['score'], 0)}</div></td>
  <td><div class="price-main">{esc(x['close'])}</div><div class="{change_cls}">{esc(change_text)}</div></td>
  <td><div class="m-checks">{x['reason']}</div><div class="signal-dates" style="margin-top:6px">{esc(x['volume_line'])}</div></td>
  <td><div class="m-checks">{x['risk_reason']}</div><div class="signal-dates" style="margin-top:6px">{esc(x['chip_line'])}</div></td>
  <td><div class="signal-dates">{esc(x['abc'])}｜{esc(x['strict'])}<br>A {x['a_score']}｜B1 {x['b1_score']}｜B2 {x['b2_score']}</div></td>
</tr>"""
    if not rows_html:
        rows_html = '<tr><td colspan="6" style="color:#8b949e">目前沒有上市櫃候選標的。</td></tr>'
    universe_section = build_mda_universe_section()

    tab1_content = f"""
  <div class="card" style="display:none">
    <div class="section-label">M 大盤前提</div>
    <div class="market-light">
      <div class="market-badge {market['class']}">{esc(market['state'])}</div>
      <div>
        <div style="font-size:16px;font-weight:800;color:#e6edf3">{esc(market['note'])}</div>
        <div class="strategy-note" style="margin-top:8px">觀察模式：第一層先分已發動長多與未發動空轉多。已發動看 MA240 上彎與一年新高附近的 B2；未發動看 240 扣抵轉有利、低位區間不再破低、B1 是否有大錢慢慢接手。這頁不顯示買進、停損、停利。</div>
        <div class="grid grid-3" style="margin-top:12px">
          <div class="metric"><div class="metric-num" style="color:#3fb950">{len(primary)}</div><div class="metric-label">重點觀察</div></div>
          <div class="metric"><div class="metric-num" style="color:#d2a520">{len(wait)}</div><div class="metric-label">觀察中</div></div>
          <div class="metric"><div class="metric-num" style="color:#f85149">{len(avoid)}</div><div class="metric-label">暫緩觀察</div></div>
        </div>
      </div>
    </div>
  </div>
  <div class="card">
    <div class="section-label">候選清單</div>
    <div class="strategy-note" style="margin-bottom:12px">A+B+C 總綱：A 找長線多頭或即將長多，B1 找長期吸籌，B2 只在賣壓很小或消失且 B1 沒離開時考慮；B3 是不追高，耐心等每一次 B2。</div>
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>觀察等級</th><th>收盤</th><th>值得觀察的跡象</th><th>主要風險</th><th>ABC拆分</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>
  {universe_section}"""

    # ── Tab 2: Launched (原 build_mda_launched_page 內容) ──
    all_rows = load_mda_universe_scan_rows()
    launched_rows = [r for r in all_rows if r.get("basket") == "已發動籃"]
    core = [r for r in launched_rows if mda_is_core_launched(r)]
    extended = [r for r in launched_rows if r not in core]

    tab2_content = f"""
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num">{len(launched_rows)}</div><div class="metric-label">已發動總數</div></div>
    <div class="metric"><div class="metric-num" style="color:#3fb950">{len(core)}</div><div class="metric-label">核心已發動</div></div>
    <div class="metric"><div class="metric-num" style="color:#d2a520">{len(extended)}</div><div class="metric-label">延伸強勢/等回測</div></div>
  </div>
  <div class="card">
    <div class="section-label">核心已發動</div>
    <div class="strategy-note" style="margin-bottom:12px">條件：分數 >= 90、散戶或股東數有支撐、近20日不破60日低位、20日量低於120日均量、距一年高點不超過 15%、60日區間不超過 90%、收盤不超過 MA120 的 1.55 倍。</div>
    {mda_candidate_table(core)}
  </div>
  <div class="card">
    <div class="section-label">延伸強勢 / 等回測</div>
    <div class="strategy-note" style="margin-bottom:12px">這區不是不好，而是價格或波動已經比較延伸，先看主力是否未退、拉回是否量縮不破支撐。</div>
    {mda_candidate_table(extended, limit=120)}
  </div>"""

    # ── Tab 3: Consolidation (原 build_mda_consolidation_page 內容) ──
    turning = [r for r in all_rows if r.get("basket") == "空轉多觀察籃"]
    dormant = [r for r in all_rows if r.get("basket") == "未發動觀察籃"]

    tab3_content = f"""
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num">{len(turning) + len(dormant)}</div><div class="metric-label">盤整候選總數</div></div>
    <div class="metric"><div class="metric-num" style="color:#d2a520">{len(turning)}</div><div class="metric-label">空轉多觀察</div></div>
    <div class="metric"><div class="metric-num" style="color:#58a6ff">{len(dormant)}</div><div class="metric-label">未發動觀察</div></div>
  </div>
  <div class="card">
    <div class="section-label">空轉多觀察籃</div>
    {mda_candidate_table(turning, limit=120)}
  </div>
  <div class="card">
    <div class="section-label">未發動觀察籃</div>
    {mda_candidate_table(dormant, limit=120)}
  </div>"""

    body = f"""
<div class="container" id="mda-tabs">
  <div class="page-title">M大觀察</div>
  <div class="page-sub">全市場用 M大 ABC 先粗篩，再拆成已發動與盤整觀察。重點不是追高，而是看主力還在不在、賣壓有沒有變小。</div>
  <div class="tab-bar">
    <button class="tab-btn active" data-tab="overview">🌐 全市場總覽</button>
    <button class="tab-btn" data-tab="launched">🚀 已發動籃</button>
    <button class="tab-btn" data-tab="consolidation">⏳ 盤整觀察</button>
  </div>
  <div class="tab-panel active" id="overview">{tab1_content}</div>
  <div class="tab-panel" id="launched">{tab2_content}</div>
  <div class="tab-panel" id="consolidation">{tab3_content}</div>
</div>
{TAB_JS}
<script>initTabs('mda-tabs')</script>"""
    return html_page("M大觀察", "mda", body)


def build_mda_candidate_detail_page(row: dict) -> str:
    sid = str(row.get("stock_id") or "")
    name = row.get("name") or ""
    market = row.get("market") or "─"
    basket = row.get("basket") or "候選"
    reason_items = [x for x in str(row.get("reason") or "").split("、") if x]
    reason_html = "".join(f"<li>{esc(x)}</li>" for x in reason_items) or "<li>候選摘要資料不足。</li>"
    body = f"""
<div class="container">
  <div style="margin-bottom:8px"><a href="../mda.html" style="color:#6e7681;font-size:13px">&larr; 回 M大選股</a></div>
  <div class="page-title">{esc(sid)} {esc(name)}｜M大候選資訊</div>
  <div class="page-sub">{esc(market)}｜{esc(basket)}｜資料日期 {esc(row.get("date", "─"))}</div>
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num">{fmt_num(row.get("score"), 0)}</div><div class="metric-label">M大主篩分數</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(row.get("close"), 2)}</div><div class="metric-label">收盤價</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(row.get("one_year_high_gap_pct"), 1)}%</div><div class="metric-label">距一年高點</div></div>
  </div>
  <div class="card">
    <div class="section-label">A 價格結構</div>
    <div class="grid grid-3">
      <div class="info-cell"><div class="k">MA120</div><div class="v">{fmt_num(row.get("ma120"), 2)}</div><div class="chip-line">20日斜率 {fmt_num(row.get("ma120_slope_pct"), 2)}%</div></div>
      <div class="info-cell"><div class="k">MA240</div><div class="v">{fmt_num(row.get("ma240"), 2)}</div><div class="chip-line">20日斜率 {fmt_num(row.get("ma240_slope_pct"), 2)}%</div></div>
      <div class="info-cell"><div class="k">240扣抵</div><div class="v">{fmt_num(row.get("deduct240_gap_pct"), 1)}%</div><div class="chip-line">60日區間 {fmt_num(row.get("range60_pct"), 1)}%</div></div>
    </div>
  </div>
  <div class="card">
    <div class="section-label">B1 股權結構</div>
    <div class="grid grid-3">
      <div class="info-cell"><div class="k">大戶4週 / 8週</div><div class="v">{fmt_num(row.get("major_4w_pctpt"), 2)}pt / {fmt_num(row.get("major_8w_pctpt"), 2)}pt</div></div>
      <div class="info-cell"><div class="k">散戶4週 / 8週</div><div class="v">{fmt_num(row.get("retail_4w_pctpt"), 2)}pt / {fmt_num(row.get("retail_8w_pctpt"), 2)}pt</div></div>
      <div class="info-cell"><div class="k">總股東4週 / 8週</div><div class="v">{fmt_num(row.get("people_4w"), 0)} / {fmt_num(row.get("people_8w"), 0)}</div></div>
    </div>
  </div>
  <div class="card">
    <div class="section-label">B2 賣壓與位置</div>
    <div class="grid grid-3">
      <div class="info-cell"><div class="k">近20日不破低</div><div class="v">{"是" if row.get("no_new_low") else "否"}</div></div>
      <div class="info-cell"><div class="k">20日量 / 120日量</div><div class="v">{fmt_num(row.get("volume20_vs_120_pct"), 1)}%</div></div>
      <div class="info-cell"><div class="k">主篩狀態</div><div class="v">{"成立" if row.get("base_mda_watch") else "未成立"}</div></div>
    </div>
    <ul class="strategy-note" style="margin-top:14px">{reason_html}</ul>
  </div>
</div>"""
    return html_page(f"{sid} {name} M大候選資訊", "mda", body, nav_prefix="../")


def build_mda_candidate_pages() -> int:
    rows = load_mda_universe_scan_rows()
    out_dir = OUTPUT_DIR / "mda_candidates"
    out_dir.mkdir(parents=True, exist_ok=True)
    valid = {f"{r.get('stock_id')}.html" for r in rows if r.get("stock_id")}
    for old_file in out_dir.glob("*.html"):
        if old_file.name not in valid:
            old_file.unlink()
    count = 0
    for row in rows:
        sid = str(row.get("stock_id") or "")
        if not sid:
            continue
        (out_dir / f"{sid}.html").write_text(build_mda_candidate_detail_page(row), encoding="utf-8")
        count += 1
    return count


def mda_candidate_href(row: dict, prefix: str = "") -> str:
    sid = str(row.get("stock_id") or "")
    if not sid:
        return "#"
    if (OUTPUT_DIR / "stocks" / f"{sid}.html").exists():
        return f"{prefix}stocks/{esc(sid)}.html"
    if sid:
        return f"{prefix}stocks/{esc(sid)}.html"
    return f"{prefix}mda_candidates/{esc(sid)}.html"


def mda_is_core_launched(row: dict) -> bool:
    close = _to_float(row.get("close"), None)
    ma120 = _to_float(row.get("ma120"), None)
    extension = close / ma120 if close and ma120 else None
    return (
        row.get("basket") == "已發動籃"
        and _to_float(row.get("score"), 0) >= 90
        and bool(row.get("retail_or_people_support"))
        and bool(row.get("no_new_low"))
        and _to_float(row.get("volume20_vs_120_pct"), 999) <= 0
        and _to_float(row.get("one_year_high_gap_pct"), -999) >= -15
        and _to_float(row.get("range60_pct"), 999) <= 90
        and (extension is None or extension <= 1.55)
    )


def mda_holding_placeholder_active() -> bool:
    summary_path = LOCAL_DATA_DIR / "mda_full_market_refresh_summary.json"
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    except Exception:
        summary = {}
    holding_dates = (summary.get("holding") or {}).get("query_dates")
    return not holding_dates or _to_float(holding_dates, 0) <= 0


def mda_candidate_table(rows: list[dict], limit: int | None = None, prefix: str = "") -> str:
    shown = rows[:limit] if limit else rows
    holding_empty = mda_holding_placeholder_active()
    holding_column_attr = ' data-empty="true"' if holding_empty else ""
    holding_column_notice = '<div class="table-placeholder-note">股權週次欄位接入中</div>' if holding_empty else ""
    body = []
    for r in shown:
        sid = str(r.get("stock_id") or "")
        href = mda_candidate_href(r, prefix)
        market = r.get("market") or "─"
        body.append(f"""
<tr>
  <td><a class="stock-link" href="{href}">{esc(sid)} {esc(r.get("name", ""))}</a><div class="signal-dates">{esc(market)}｜{esc(r.get("date", ""))}</div></td>
  <td><div class="m-score">{fmt_num(r.get("score"), 0)}</div><span class="tag tag-blue"><a href="{href}">個股資訊</a></span></td>
  <td><div class="price-main">{fmt_num(r.get("close"), 2)}</div><div class="signal-dates">距高 {fmt_num(r.get("one_year_high_gap_pct"), 1)}%｜量 {fmt_num(r.get("volume20_vs_120_pct"), 1)}%</div></td>
  <td><div class="m-checks">MA120 {fmt_num(r.get("ma120_slope_pct"), 2)}%｜MA240 {fmt_num(r.get("ma240_slope_pct"), 2)}%</div><div class="signal-dates">MA120 {fmt_num(r.get("ma120"), 2)}｜MA240 {fmt_num(r.get("ma240"), 2)}</div></td>
  <td{holding_column_attr}><div class="m-checks">大戶4週 {fmt_num(r.get("major_4w_pctpt"), 2)}pt｜8週 {fmt_num(r.get("major_8w_pctpt"), 2)}pt</div><div class="signal-dates">散戶4週 {fmt_num(r.get("retail_4w_pctpt"), 2)}pt｜股東4週 {fmt_num(r.get("people_4w"), 0)}</div></td>
  <td><div class="signal-dates">{esc(r.get("reason", ""))}</div></td>
</tr>""")
    if not body:
        body.append('<tr><td colspan="6" style="color:#8b949e">目前沒有符合條件的候選。</td></tr>')
    extra = ""
    if limit and len(rows) > limit:
        extra = f'<div class="strategy-note" style="margin-top:8px">此區共 {len(rows)} 檔，先顯示前 {limit} 檔。</div>'
    return f"""
{holding_column_notice}
<div style="overflow-x:auto">
  <table class="stock-table">
    <thead><tr><th>股票</th><th>分數</th><th>位置/量能</th><th>A 長均線</th><th{holding_column_attr}>B1 股權</th><th>判讀摘要</th></tr></thead>
    <tbody>{''.join(body)}</tbody>
  </table>
</div>
{extra}"""


def build_mda_launched_page() -> str:
    rows = [r for r in load_mda_universe_scan_rows() if r.get("basket") == "已發動籃"]
    core = [r for r in rows if mda_is_core_launched(r)]
    extended = [r for r in rows if r not in core]
    body = f"""
<div class="container">
  <div class="page-title">M大已發動籃</div>
  <div class="page-sub">這裡放已經轉強或正在發動的股票，適合看有沒有回測買點；目前先看收盤價 >= {fmt_num(MDA_MIN_CLOSE, 0)} 元。</div>
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num">{len(rows)}</div><div class="metric-label">已發動總數</div></div>
    <div class="metric"><div class="metric-num" style="color:#3fb950">{len(core)}</div><div class="metric-label">核心已發動</div></div>
    <div class="metric"><div class="metric-num" style="color:#d2a520">{len(extended)}</div><div class="metric-label">延伸強勢/等回測</div></div>
  </div>
  <div class="card">
    <div class="section-label">核心已發動</div>
    <div class="strategy-note" style="margin-bottom:12px">條件：分數 >= 90、散戶或股東數有支撐、近20日不破60日低位、20日量低於120日均量、距一年高點不超過 15%、60日區間不超過 90%、收盤不超過 MA120 的 1.55 倍。</div>
    {mda_candidate_table(core)}
  </div>
  <div class="card">
    <div class="section-label">延伸強勢 / 等回測</div>
    <div class="strategy-note" style="margin-bottom:12px">這區不是不好，而是價格或波動已經比較延伸，先看主力是否未退、拉回是否量縮不破支撐。</div>
    {mda_candidate_table(extended, limit=120)}
  </div>
</div>"""
    return html_page("M大已發動籃", "mda_launched", body)


def build_mda_consolidation_page() -> str:
    rows = load_mda_universe_scan_rows()
    turning = [r for r in rows if r.get("basket") == "空轉多觀察籃"]
    dormant = [r for r in rows if r.get("basket") == "未發動觀察籃"]
    body = f"""
<div class="container">
  <div class="page-title">M大盤整籃</div>
  <div class="page-sub">這裡放還沒完全發動、但籌碼或型態值得等的股票，適合觀察量縮價穩與賣壓消失；目前先看收盤價 >= {fmt_num(MDA_MIN_CLOSE, 0)} 元。</div>
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num">{len(turning) + len(dormant)}</div><div class="metric-label">盤整候選總數</div></div>
    <div class="metric"><div class="metric-num" style="color:#d2a520">{len(turning)}</div><div class="metric-label">空轉多觀察</div></div>
    <div class="metric"><div class="metric-num" style="color:#58a6ff">{len(dormant)}</div><div class="metric-label">未發動觀察</div></div>
  </div>
  <div class="card">
    <div class="section-label">空轉多觀察籃</div>
    {mda_candidate_table(turning, limit=120)}
  </div>
  <div class="card">
    <div class="section-label">未發動觀察籃</div>
    {mda_candidate_table(dormant, limit=120)}
  </div>
</div>"""
    return html_page("M大盤整籃", "mda_consolidation", body)


def _mda_line(label: str, value: str, cls: str = "") -> str:
    return f'<div class="telegram-line"><div class="k">{esc(label)}</div><div class="v {cls}">{value}</div></div>'


def build_mda_auto_diagnosis(
    stock_id: str,
    stock_name: str,
    abc: dict,
    money: dict,
    pressure: dict,
    chip_answer: str,
    price_answer: str,
) -> str:
    a_phase = abc.get("a_phase", "A未成立")
    b1_score = abc.get("b1_score", 0) or 0
    b2_score = abc.get("b2_score", 0) or 0
    pressure_score = pressure.get("score", 0) or 0
    pressure_level = pressure.get("level", "B2資料不足")

    if a_phase == "已發動長多" and b1_score >= 45 and pressure_score >= 78:
        verdict = "重點觀察：A 已發動，B1 尚未明顯離開，B2 疑似出現"
        action = "不追價；等回測不破、量縮整理後再轉強，或突破後回測大量區站穩。"
        verdict_cls = "pos"
    elif a_phase in {"未發動空轉多觀察", "轉強觀察"} and b1_score >= 45:
        verdict = "觀察名單：A 還沒完整發動，但已有空轉多與 B1 線索"
        action = "先放觀察池；等 240 扣抵更有利、價格不破低、再看到 B2 才提高順位。"
        verdict_cls = "warn"
    elif b1_score < 45:
        verdict = "暫緩追蹤：B1 主力是否仍在還不夠明確"
        action = "先看大戶是否回升、散戶/股東人數是否下降，外資是否停止反彈賣。"
        verdict_cls = "neg"
    elif pressure_score < 58:
        verdict = "只觀察不動作：賣壓尚未證明變小"
        action = "等待同區間量縮推升、融資賣壓被吸收、或回檔快速站回均線。"
        verdict_cls = "neg"
    else:
        verdict = "觀察中：條件有雛形，但還缺一個清楚確認點"
        action = "繼續比對外資/融資與股價互動，避免把短線反彈誤認成 B2。"
        verdict_cls = ""

    support = [
        f"A階段：{a_phase}（A分 {fmt_num(abc.get('a_score'), 0)}）",
        f"B1：{money.get('reading', '籌碼資料不足')}（B1分 {fmt_num(b1_score, 0)}）",
        f"B2：{pressure_level}（B2壓力分 {fmt_num(pressure_score, 0)}）",
        chip_answer,
        price_answer,
    ]
    pressure_items = pressure.get("items") or []
    for name, ok, note in pressure_items:
        if ok and len(support) < 8:
            support.append(f"{name}：{note}")

    counter = []
    for name, ok, note in pressure_items:
        if not ok and len(counter) < 6:
            counter.append(f"{name}：{note}")
    if not counter:
        counter.append("目前沒有明顯反證，但仍需用圖確認是否過熱或乖離過大。")

    prompt = "\n".join([
        f"請依 M大 ABC 架構複判 {stock_id} {stock_name}：",
        f"A：{a_phase}，A分 {fmt_num(abc.get('a_score'), 0)}。",
        f"B1：{money.get('reading', '資料不足')}；主力10日 {fmt_num(money.get('force_10d'), 0)} 張，大戶4週 {fmt_num(money.get('major_4w'), 2)}%、8週 {fmt_num(money.get('major_8w'), 2)}%，散戶4週 {fmt_num(money.get('retail_4w'), 2)}%、8週 {fmt_num(money.get('retail_8w'), 2)}%。",
        f"B2：{pressure.get('summary', '資料不足')}。",
        f"程式初判：{verdict}；操作紀律：{action}",
        "請看下方日K、外資、融資、大戶/散戶/股東人數連動圖，判斷主力是否尚未離開、賣壓是否真的消失，以及目前應該買進、觀察或排除。",
    ])

    def li(items: list[str]) -> str:
        return "".join(f"<li>{esc(x)}</li>" for x in items)

    return f"""
<div class="card">
  <div class="section-label">M大自動判讀摘要</div>
  <div class="diagnosis-head">
    <div class="diagnosis-verdict">
      <div class="label">Program verdict</div>
      <div class="main {verdict_cls}">{esc(verdict)}</div>
      <div class="sub">{esc(action)}</div>
    </div>
    <div class="diagnosis-score">
      <div class="box"><div class="num">{fmt_num(abc.get('a_score'), 0)}</div><div class="k">A 趨勢分</div></div>
      <div class="box"><div class="num">{fmt_num(b1_score, 0)}</div><div class="k">B1 籌碼分</div></div>
      <div class="box"><div class="num">{fmt_num(pressure_score, 0)}</div><div class="k">B2 壓力分</div></div>
    </div>
  </div>
  <div class="diagnosis-list">
    <div class="panel"><h3>支持理由</h3><ul>{li(support)}</ul></div>
    <div class="panel"><h3>反證與待確認</h3><ul>{li(counter)}</ul></div>
  </div>
  <div class="diagnosis-prompt">{esc(prompt)}</div>
</div>"""


def mda_chip_structure(stock_id: str, chip_series: list[dict], holding: dict) -> dict:
    holding_series = read_holding_series(stock_id)
    major_4w = holding_delta(holding_series, "major", 4)
    major_8w = holding_delta(holding_series, "major", 8)
    retail_4w = holding_delta(holding_series, "retail", 4)
    retail_8w = holding_delta(holding_series, "retail", 8)
    people_4w = holding_delta(holding_series, "total_people", 4)
    people_8w = holding_delta(holding_series, "total_people", 8)
    h_latest = (holding.get("latest") or {}) if holding else {}
    foreign_10d = sum(float(x.get("foreign") or 0) for x in chip_series[-10:])
    force_10d = sum(float(x.get("total") or 0) for x in chip_series[-10:])
    major_accumulating = (
        (major_4w is not None and major_4w >= 0.5)
        or (major_8w is not None and major_8w >= 1.0)
    )
    retail_support = (
        (retail_4w is not None and retail_4w <= -0.3)
        or (retail_8w is not None and retail_8w <= -0.8)
    )
    people_support = (
        (people_4w is not None and people_4w < 0)
        or (people_8w is not None and people_8w < 0)
    )
    good = (
        major_accumulating
        and (retail_support or people_support)
    )
    bad = (
        (major_4w is not None and major_4w < -0.5)
        and (retail_4w is not None and retail_4w > 0.5)
    )
    if good:
        reading = "大戶增加、散戶減少，較接近聰明錢結構"
        cls = "pos"
    elif bad:
        reading = "大戶減少、散戶增加，避免跟散戶站一起"
        cls = "neg"
    else:
        reading = "籌碼方向尚未完全一致，列入觀察但不急著下結論"
        cls = ""
    return {
        "major_4w": major_4w,
        "major_8w": major_8w,
        "retail_4w": retail_4w,
        "retail_8w": retail_8w,
        "people_4w": people_4w,
        "people_8w": people_8w,
        "latest_major": h_latest.get("major"),
        "latest_retail": h_latest.get("retail"),
        "foreign_10d": foreign_10d,
        "force_10d": force_10d,
        "reading": reading,
        "class": cls,
    }


def mda_chart_rows(stock_id: str, daily: list[dict], holding_series: list[dict], chip_series: list[dict]) -> list[dict]:
    price_rows = daily[-CHART_LOOKBACK_BARS:]
    aligned = align_chip_to_price_dates(price_rows, holding_series, chip_series)
    aligned_by_date = {x.get("date"): x for x in aligned}
    foreign_series = read_foreign_shareholding_series(stock_id) if stock_id else []
    margin_series = read_margin_series(stock_id) if stock_id else []
    foreign_by_date = {x.get("date"): x for x in foreign_series}
    margin_by_date = {x.get("date"): x for x in margin_series}
    out = []
    foreign_cum = 0.0
    prev_close = None
    for p in price_rows:
        date = p.get("date", "")
        a = aligned_by_date.get(date, {})
        f = foreign_by_date.get(date, {})
        m = margin_by_date.get(date, {})
        close = p.get("close")
        change = None
        change_pct = None
        if close is not None and prev_close:
            try:
                change = float(close) - float(prev_close)
                change_pct = (float(close) / float(prev_close) - 1) * 100
            except Exception:
                change = None
                change_pct = None
        foreign = a.get("foreign")
        if foreign is not None:
            try:
                foreign_cum += float(foreign)
            except Exception:
                pass
        out.append({
            "date": date,
            "open": p.get("open"),
            "high": p.get("high"),
            "low": p.get("low"),
            "close": close,
            "change": change,
            "changePct": change_pct,
            "volume": (float(p.get("volume") or 0) / 1000),
            "foreign": foreign,
            "foreignCum": foreign_cum,
            "foreignShares": f.get("foreign_shares"),
            "foreignRatio": f.get("foreign_ratio"),
            "marginBalance": m.get("margin_balance"),
            "shortBalance": m.get("short_balance"),
            "major": a.get("major"),
            "middle": a.get("middle"),
            "retail": a.get("retail"),
            "totalPeople": a.get("total_people"),
            "majorPeople": a.get("major_people"),
            "middlePeople": a.get("middle_people"),
            "retailPeople": a.get("retail_people"),
            "holdingDate": a.get("holding_date", ""),
        })
        if close is not None:
            prev_close = close
    return out


def mda_metric_svg(rows: list[dict], title: str, key: str, color: str = "#58a6ff", kind: str = "line", unit: str = "") -> str:
    rows = rows[-CHART_LOOKBACK_BARS:]
    vals = []
    for r in rows:
        v = r.get(key)
        vals.append(float(v) if v is not None else None)
    real_vals = [v for v in vals if v is not None]
    if len(rows) < 2 or not real_vals:
        return f'<div class="strategy-note">{esc(title)}資料尚未接入。</div>'
    w, h = 900, 132
    pad_l, pad_r, pad_t, pad_b = 50, 18, 18, 22
    plot_h = h - pad_t - pad_b
    if kind == "bar-zero":
        max_abs = nice_number((max(abs(v) for v in real_vals) or 1) * 1.15)
        lo, hi = -max_abs, max_abs
    else:
        lo, hi = min(real_vals), max(real_vals)
        span = hi - lo
        pad = max(1.0, span * 0.12)
        if "%" in unit:
            pad = max(0.3, span * 0.12)
        lo -= pad
        hi += pad
        if hi <= lo:
            hi = lo + 1

    def x_pos(i):
        return pad_l + i * (w - pad_l - pad_r) / max(1, len(rows) - 1)

    def y_pos(v):
        return pad_t + (hi - float(v)) * plot_h / (hi - lo)

    grid = ""
    for pct in [0, .5, 1]:
        yy = pad_t + pct * plot_h
        gv = hi - pct * (hi - lo)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-pad_r}" y2="{yy:.1f}" stroke="#21262d"/>'
        grid += f'<text x="4" y="{yy+4:.1f}" fill="#6e7681" font-size="10">{compact_axis_label(gv)}{esc(unit)}</text>'
    if kind == "bar-zero":
        zy = y_pos(0)
        grid += f'<line x1="{pad_l}" y1="{zy:.1f}" x2="{w-pad_r}" y2="{zy:.1f}" stroke="#8b949e" stroke-dasharray="3 3"/>'

    marks = ""
    if kind in {"bar", "bar-zero"}:
        step = (w - pad_l - pad_r) / len(rows)
        bar_w = max(2, min(7, step * 0.56))
        zero_y = y_pos(0) if kind == "bar-zero" else y_pos(lo)
        for i, v in enumerate(vals):
            if v is None:
                continue
            x = x_pos(i)
            y = y_pos(v)
            top = min(y, zero_y)
            bh = max(abs(zero_y - y), 1.4)
            if key == "volume":
                up = (rows[i].get("close") or 0) >= (rows[i].get("open") or 0)
                bar_color = "#f85149" if up else "#3fb950"
            elif kind == "bar-zero":
                bar_color = "#f85149" if v >= 0 else "#3fb950"
            else:
                bar_color = color
            marks += f'<rect x="{x-bar_w/2:.1f}" y="{top:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="{bar_color}" opacity=".78"/>'
    else:
        pts = []
        for i, v in enumerate(vals):
            if v is None:
                continue
            pts.append(f"{x_pos(i):.1f},{y_pos(v):.1f}")
        marks = f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{" ".join(pts)}"/>'

    latest = rows[-1]
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {marks}
  <text x="{pad_l}" y="13" fill="#e6edf3" font-size="11">{esc(title)}</text>
  <text x="{pad_l}" y="{h-6}" fill="#6e7681" font-size="10">{esc(rows[0].get("date",""))}</text>
  <text x="{w-112}" y="{h-6}" fill="#6e7681" font-size="10">{esc(latest.get("date",""))}</text>
</svg>"""


def mda_synced_chart_panel(stock_id: str, daily: list[dict], holding_series: list[dict], chip_series: list[dict]) -> str:
    rows = mda_chart_rows(stock_id, daily, holding_series, chip_series)
    data = json.dumps(rows, ensure_ascii=False)
    panel_id = f"mda-sync-{stock_id}"
    def panel(kind: str, svg: str) -> str:
        return f'<div class="indicator-box indicator-hover mda-sync-panel" data-kind="{kind}">{svg}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>'
    charts = [
        panel("k", chart_svg(daily, "日K")),
        panel("volume", mda_metric_svg(rows, "成交量（張）", "volume", "#8b949e", "bar", "張")),
        panel("foreignShares", mda_metric_svg(rows, "外資持股張數", "foreignShares", "#7ee787", "line", "張")),
        panel("foreign", mda_metric_svg(rows, "外資買賣超（張）", "foreign", "#f85149", "bar-zero", "張")),
        panel("marginBalance", mda_metric_svg(rows, "融資餘額", "marginBalance", "#a78bfa", "line", "張")),
        panel("major", mda_metric_svg(rows, "大戶持股比例（400張以上）", "major", "#f85149", "line", "%")),
        panel("middlePeople", mda_metric_svg(rows, "中實戶持股人數（200-400張）", "middlePeople", "#d2a520", "line", "人")),
        panel("retail", mda_metric_svg(rows, "散戶持股比例", "retail", "#3fb950", "line", "%")),
        panel("totalPeople", mda_metric_svg(rows, "總股東人數", "totalPeople", "#58a6ff", "line", "人")),
    ]
    script = f"""
<script>
const mdaData_{stock_id} = {data};
(function(){{
  const root=document.getElementById('{panel_id}');
  if(!root) return;
  const data=mdaData_{stock_id} || [];
  const fmt=(v,d=2)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW', {{maximumFractionDigits:d, minimumFractionDigits:d}}) : '-';
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  const pct=(v)=>Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}%` : '-';
  function html(kind, x){{
    if(kind==='k') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>開</span><span>${{fmt(x.open)}}</span><span>高</span><span>${{fmt(x.high)}}</span><span>低</span><span>${{fmt(x.low)}}</span><span>收</span><span>${{fmt(x.close)}}</span><span>漲跌</span><span>${{fmt(x.change)}} / ${{pct(x.changePct)}}</span></div>`;
    if(kind==='volume') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>成交量</span><span>${{fmtInt(x.volume)}} 張</span></div>`;
    if(kind==='foreignShares') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>外資持股張數</span><span>${{fmtInt(x.foreignShares)}} 張</span><span>外資持股比例</span><span>${{pct(x.foreignRatio)}}</span></div>`;
    if(kind==='foreign') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>外資買賣超</span><span>${{fmtInt(x.foreign)}} 張</span><span>區間累積</span><span>${{fmtInt(x.foreignCum)}} 張</span></div>`;
    if(kind==='marginBalance') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>融資餘額</span><span>${{fmtInt(x.marginBalance)}} 張</span><span>融券餘額</span><span>${{fmtInt(x.shortBalance)}} 張</span></div>`;
    if(kind==='major') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>大戶比例(400張以上)</span><span>${{pct(x.major)}}</span><span>大戶人數</span><span>${{fmtInt(x.majorPeople)}} 人</span></div>`;
    if(kind==='middlePeople') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>中實戶人數(200-400張)</span><span>${{fmtInt(x.middlePeople)}} 人</span><span>中實戶比例</span><span>${{pct(x.middle)}}</span></div>`;
    if(kind==='retail') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>散戶持股</span><span>${{pct(x.retail)}}</span></div>`;
    if(kind==='totalPeople') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>總股東人數</span><span>${{fmtInt(x.totalPeople)}} 人</span></div>`;
    return `<div class="t-date">${{x.date || '-'}}</div>`;
  }}
  function position(chart, idx, htmlText){{
    const line=chart.querySelector('.chart-crosshair');
    const tip=chart.querySelector('.chart-tooltip');
    if(!line || !tip || data.length < 2) return;
    const rect=chart.getBoundingClientRect();
    const left=rect.width * 50 / 900;
    const right=rect.width * (900 - 18) / 900;
    const x=left + (right-left) * idx / Math.max(1, data.length-1);
    line.style.display='block';
    line.style.left=`${{x}}px`;
    tip.innerHTML=htmlText;
    tip.style.display='block';
    const tw=tip.offsetWidth || 210;
    let tx=x + 14;
    if(tx + tw > rect.width) tx=x - tw - 14;
    tip.style.left=`${{Math.max(6, tx)}}px`;
    tip.style.top='10px';
  }}
  function sync(idx){{
    const item=data[idx];
    root.querySelectorAll('.mda-sync-panel').forEach(chart=>position(chart, idx, html(chart.dataset.kind, item)));
  }}
  function clear(){{
    root.querySelectorAll('.mda-sync-panel').forEach(chart=>{{
      const line=chart.querySelector('.chart-crosshair');
      const tip=chart.querySelector('.chart-tooltip');
      if(line) line.style.display='none';
      if(tip) tip.style.display='none';
    }});
  }}
  root.querySelectorAll('.mda-sync-panel').forEach(chart=>{{
    chart.addEventListener('mousemove', ev=>{{
      if(data.length < 2) return;
      const rect=chart.getBoundingClientRect();
      const left=rect.width * 50 / 900;
      const right=rect.width * (900 - 18) / 900;
      const x=Math.max(left, Math.min(right, ev.clientX - rect.left));
      const idx=Math.max(0, Math.min(data.length-1, Math.round(((x-left)/Math.max(1,right-left))*(data.length-1))));
      sync(idx);
    }});
    chart.addEventListener('mouseleave', clear);
  }});
}})();
</script>"""
    return f'<div id="{panel_id}" class="chart-stack">{"".join(charts)}</div>{script}'


def mda_lightweight_chart_panel(stock_id: str, daily: list[dict], holding_series: list[dict], chip_series: list[dict]) -> str:
    rows = mda_chart_rows(stock_id, daily, holding_series, chip_series)
    data = json.dumps(rows, ensure_ascii=False)
    panel_id = f"mda-tv-{stock_id}"
    chart_defs = [
        ("k", "日K", "main"),
        ("volume", "成交量（張）", ""),
        ("foreignShares", "外資持股張數", ""),
        ("foreign", "外資買賣超（張）", ""),
        ("marginBalance", "融資餘額", ""),
        ("major", "大戶持股比例（400張以上）", ""),
        ("middlePeople", "中實戶持股人數（200-400張）", ""),
        ("retail", "散戶持股比例", ""),
        ("totalPeople", "總股東人數", ""),
    ]
    panel_html = []
    for kind, title, cls in chart_defs:
        toolbar = ""
        if kind == "k":
            toolbar = f'''<div class="tv-draw-toolbar" data-draw-toolbar="{panel_id}">
    <button type="button" class="tv-draw-btn" data-draw-tool="trend">趨勢線</button>
    <button type="button" class="tv-draw-btn" data-draw-tool="arrow">箭頭</button>
    <button type="button" class="tv-draw-btn" data-draw-tool="clear">清除畫線</button>
  </div>'''
        panel_html.append(f'''<div class="tv-chart-panel" data-kind="{kind}">
  <div class="tv-chart-title">{esc(title)}</div>
  {toolbar}
  <div id="{panel_id}-{kind}" class="tv-chart {cls}"></div>
  <div class="tv-tooltip"></div>
</div>''')
    panels = "".join(panel_html)
    script = f"""
<script src="https://unpkg.com/lightweight-charts@5.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
(function(){{
  const root=document.getElementById('{panel_id}');
  const rows={data};
  if(!root || !rows.length) return;
  const L=window.LightweightCharts;
  if(!L){{
    root.innerHTML='<div class="strategy-note">TradingView Lightweight Charts 載入失敗，請檢查網路或 CDN。</div>';
    return;
  }}
  const chartApis=[];
  let syncing=false;
  let crosshairSyncing=false;
  const gridColor='#21262d';
  const textColor='#8b949e';
  const baseOptions=(height)=>({{
    height,
    layout:{{background:{{type:'solid',color:'#0d1117'}},textColor}},
    grid:{{vertLines:{{color:gridColor}},horzLines:{{color:gridColor}}}},
    rightPriceScale:{{borderColor:'#30363d'}},
    timeScale:{{borderColor:'#30363d',timeVisible:false,secondsVisible:false,fixLeftEdge:true,fixRightEdge:true}},
    crosshair:{{mode:L.CrosshairMode.Normal}},
    localization:{{locale:'zh-TW'}},
  }});
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  const fmt=(v,d=2)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW',{{maximumFractionDigits:d,minimumFractionDigits:d}}) : '-';
  const pct=(v)=>Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}%` : '-';
  const byTime=new Map(rows.map(x=>[x.date,x]));
  const maxLogical=Math.max(0,rows.length-1);
  const drawingKey='stockDrawings:{panel_id}';
  let activeDrawTool=null;
  let mainDrawApi=null;
  function lineData(key){{ return rows.filter(x=>x[key]!=null).map(x=>({{time:x.date,value:Number(x[key])}})); }}
  function histData(key, colorFn){{ return rows.filter(x=>x[key]!=null).map(x=>({{time:x.date,value:Number(x[key]),color:colorFn ? colorFn(x) : '#58a6ff'}})); }}
  function defaultLogicalRange(){{
    if(maxLogical <= 0) return {{from:0,to:maxLogical}};
    const lastTime=Date.parse(rows[rows.length-1]?.date || '');
    let from=Math.max(0, rows.length - 22);
    if(!Number.isNaN(lastTime)){{
      const cutoff=lastTime - {CHART_DEFAULT_VISIBLE_DAYS}*24*60*60*1000;
      from=0;
      for(let i=rows.length-1;i>=0;i--){{
        const t=Date.parse(rows[i]?.date || '');
        if(!Number.isNaN(t) && t < cutoff){{ from=Math.min(maxLogical, i+1); break; }}
      }}
    }}
    return {{from:Math.max(0,from),to:maxLogical}};
  }}
  function clampLogicalRange(range){{
    if(!range) return range;
    let from=Number(range.from);
    let to=Number(range.to);
    if(!Number.isFinite(from) || !Number.isFinite(to)) return range;
    const span=to-from;
    if(span>=maxLogical) return {{from:0,to:maxLogical}};
    if(from<0){{ to-=from; from=0; }}
    if(to>maxLogical){{ from-=to-maxLogical; to=maxLogical; }}
    return {{from:Math.max(0,from),to:Math.min(maxLogical,to)}};
  }}
  function isSameRange(a,b){{
    return a && b && Math.abs(Number(a.from)-Number(b.from))<0.01 && Math.abs(Number(a.to)-Number(b.to))<0.01;
  }}
  function makeTooltip(kind,x){{
    if(!x) return '';
    if(kind==='k') return `<b>${{x.date}}</b><br>開 ${{fmt(x.open)}} 高 ${{fmt(x.high)}} 低 ${{fmt(x.low)}} 收 ${{fmt(x.close)}}<br>漲跌 ${{fmt(x.change)}} / ${{pct(x.changePct)}}`;
    if(kind==='volume') return `<b>${{x.date}}</b><br>成交量 ${{fmtInt(x.volume)}} 張`;
    if(kind==='foreignShares') return `<b>${{x.date}}</b><br>外資持股 ${{fmtInt(x.foreignShares)}} 張<br>比例 ${{pct(x.foreignRatio)}}`;
    if(kind==='foreign') return `<b>${{x.date}}</b><br>外資買賣超 ${{fmtInt(x.foreign)}} 張<br>區間累積 ${{fmtInt(x.foreignCum)}} 張`;
    if(kind==='marginBalance') return `<b>${{x.date}}</b><br>融資餘額 ${{fmtInt(x.marginBalance)}} 張<br>融券餘額 ${{fmtInt(x.shortBalance)}} 張`;
    if(kind==='major') return `<b>${{x.date}}</b><br>大戶(400張以上) ${{pct(x.major)}}<br>大戶人數 ${{fmtInt(x.majorPeople)}} 人`;
    if(kind==='middlePeople') return `<b>${{x.date}}</b><br>中實戶人數(200-400張) ${{fmtInt(x.middlePeople)}} 人<br>中實戶比例 ${{pct(x.middle)}}`;
    if(kind==='retail') return `<b>${{x.date}}</b><br>散戶持股 ${{pct(x.retail)}}`;
    if(kind==='totalPeople') return `<b>${{x.date}}</b><br>總股東人數 ${{fmtInt(x.totalPeople)}} 人`;
    return `<b>${{x.date}}</b>`;
  }}
  function valueForKind(kind,x){{
    if(!x) return 0;
    const map={{
      k:x.close,
      volume:x.volume,
      foreignShares:x.foreignShares,
      foreign:x.foreign,
      marginBalance:x.marginBalance,
      major:x.major,
      middlePeople:x.middlePeople,
      retail:x.retail,
      totalPeople:x.totalPeople,
    }};
    const v=Number(map[kind]);
    return Number.isFinite(v) ? v : 0;
  }}
  function showTip(item,x){{
    if(!item.tip) return;
    item.tip.innerHTML=makeTooltip(item.kind,x);
    item.tip.style.display='block';
  }}
  function clearAllCrosshairs(){{
    chartApis.forEach(item=>{{
      item.chart.clearCrosshairPosition();
      if(item.tip) item.tip.style.display='none';
    }});
  }}
  function syncAllCrosshairs(time){{
    const x=byTime.get(time);
    if(!x) return;
    chartApis.forEach(item=>{{
      item.chart.setCrosshairPosition(valueForKind(item.kind,x), time, item.series);
      showTip(item,x);
    }});
  }}
  function normalizeTime(t){{
    if(!t) return null;
    if(typeof t==='string') return t;
    if(typeof t==='object' && t.year) return `${{t.year}}-${{String(t.month).padStart(2,'0')}}-${{String(t.day).padStart(2,'0')}}`;
    return String(t);
  }}
  function loadDrawings(){{
    try{{ return JSON.parse(localStorage.getItem(drawingKey) || '[]').filter(x=>x && x.start && x.end); }}
    catch(e){{ return []; }}
  }}
  function saveDrawings(drawings){{
    localStorage.setItem(drawingKey, JSON.stringify(drawings.slice(-80)));
  }}
  function setDrawingButtons(mode){{
    root.querySelectorAll('.tv-draw-btn').forEach(btn=>btn.classList.toggle('active', btn.dataset.drawTool===mode));
  }}
  function updateDrawingMode(mode){{
    activeDrawTool=mode;
    setDrawingButtons(mode);
    if(mainDrawApi){{
      mainDrawApi.layer.classList.toggle('active', !!mode);
      mainDrawApi.chart.applyOptions({{handleScroll:!mode,handleScale:!mode}});
    }}
  }}
  function setupDrawing(item){{
    const layer=document.createElementNS('http://www.w3.org/2000/svg','svg');
    layer.classList.add('tv-draw-layer');
    layer.setAttribute('aria-label','畫線圖層');
    layer.innerHTML='<defs><marker id="{panel_id}-arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth"><path d="M0,0 L0,6 L9,3 z" fill="#f2cc60"></path></marker></defs>';
    item.el.appendChild(layer);
    const api={{...item,layer}};
    mainDrawApi=api;
    let draft=null;
    const color='#f2cc60';
    function pointFromEvent(ev){{
      const box=layer.getBoundingClientRect();
      const x=ev.clientX-box.left;
      const y=ev.clientY-box.top;
      const time=normalizeTime(item.chart.timeScale().coordinateToTime(x));
      const price=item.series.coordinateToPrice(y);
      if(!time || !Number.isFinite(Number(price))) return null;
      return {{x,y,time,price:Number(price)}};
    }}
    function lineNode(d, cls=''){{
      const x1=item.chart.timeScale().timeToCoordinate(d.start.time);
      const x2=item.chart.timeScale().timeToCoordinate(d.end.time);
      const y1=item.series.priceToCoordinate(Number(d.start.price));
      const y2=item.series.priceToCoordinate(Number(d.end.price));
      if(x1==null || x2==null || y1==null || y2==null) return null;
      const n=document.createElementNS('http://www.w3.org/2000/svg','line');
      n.setAttribute('x1',x1); n.setAttribute('y1',y1);
      n.setAttribute('x2',x2); n.setAttribute('y2',y2);
      n.setAttribute('stroke',color); n.setAttribute('stroke-width','2');
      n.setAttribute('fill','none');
      if(d.type==='arrow') n.setAttribute('marker-end','url(#{panel_id}-arrow)');
      if(cls) n.classList.add(cls);
      return n;
    }}
    function render(extra=null){{
      layer.setAttribute('width', item.el.clientWidth);
      layer.setAttribute('height', item.el.clientHeight);
      Array.from(layer.querySelectorAll('line')).forEach(n=>n.remove());
      loadDrawings().forEach(d=>{{ const n=lineNode(d); if(n) layer.appendChild(n); }});
      if(extra){{ const n=lineNode(extra,'draft'); if(n) layer.appendChild(n); }}
    }}
    layer.addEventListener('pointerdown',ev=>{{
      if(!activeDrawTool) return;
      const p=pointFromEvent(ev);
      if(!p) return;
      ev.preventDefault();
      layer.setPointerCapture(ev.pointerId);
      draft={{type:activeDrawTool,start:{{time:p.time,price:p.price}},end:{{time:p.time,price:p.price}}}};
      render(draft);
    }});
    layer.addEventListener('pointermove',ev=>{{
      if(!draft) return;
      const p=pointFromEvent(ev);
      if(!p) return;
      draft.end={{time:p.time,price:p.price}};
      render(draft);
    }});
    layer.addEventListener('pointerup',ev=>{{
      if(!draft) return;
      const p=pointFromEvent(ev);
      if(p) draft.end={{time:p.time,price:p.price}};
      const drawings=loadDrawings();
      drawings.push(draft);
      saveDrawings(drawings);
      draft=null;
      render();
    }});
    const toolbar=root.querySelector('[data-draw-toolbar="{panel_id}"]');
    if(toolbar){{
      toolbar.addEventListener('click',ev=>{{
        const btn=ev.target.closest('[data-draw-tool]');
        if(!btn) return;
        const tool=btn.dataset.drawTool;
        if(tool==='clear'){{ saveDrawings([]); updateDrawingMode(null); render(); return; }}
        updateDrawingMode(activeDrawTool===tool ? null : tool);
      }});
    }}
    item.chart.timeScale().subscribeVisibleLogicalRangeChange(()=>render());
    render();
  }}
  function addPanel(kind, title, height){{
    const el=document.getElementById('{panel_id}-'+kind);
    if(!el) return;
    const chart=L.createChart(el, baseOptions(height));
    let series=null;
    if(kind==='k'){{
      series=chart.addSeries(L.CandlestickSeries, {{
        upColor:'#f85149',downColor:'#3fb950',borderUpColor:'#f85149',borderDownColor:'#3fb950',wickUpColor:'#f85149',wickDownColor:'#3fb950'
      }});
      series.setData(rows.map(x=>({{time:x.date,open:Number(x.open),high:Number(x.high),low:Number(x.low),close:Number(x.close)}})));
      [[5,'#58a6ff'],[20,'#f0883e'],[60,'#3fb950'],[120,'#a78bfa'],[240,'#8b949e']].forEach(([n,c])=>{{
        const s=chart.addSeries(L.LineSeries,{{color:c,lineWidth:1,priceLineVisible:false,lastValueVisible:false}});
        const vals=[];
        for(let i=0;i<rows.length;i++){{
          if(i+1<n) continue;
          const avg=rows.slice(i+1-n,i+1).reduce((a,b)=>a+Number(b.close||0),0)/n;
          vals.push({{time:rows[i].date,value:avg}});
        }}
        s.setData(vals);
      }});
    }} else if(kind==='volume'){{
      series=chart.addSeries(L.HistogramSeries,{{priceFormat:{{type:'volume'}},priceLineVisible:false,lastValueVisible:false}});
      series.setData(histData('volume',x=>Number(x.close)>=Number(x.open)?'#f85149':'#3fb950'));
    }} else if(kind==='foreign'){{
      series=chart.addSeries(L.HistogramSeries,{{priceFormat:{{type:'volume'}},priceLineVisible:false,lastValueVisible:false}});
      series.setData(histData('foreign',x=>Number(x.foreign)>=0?'#f85149':'#3fb950'));
    }} else {{
      const key={{foreignShares:'foreignShares',marginBalance:'marginBalance',major:'major',middlePeople:'middlePeople',retail:'retail',totalPeople:'totalPeople'}}[kind];
      const color={{foreignShares:'#7ee787',marginBalance:'#a78bfa',major:'#f85149',middlePeople:'#d2a520',retail:'#3fb950',totalPeople:'#58a6ff'}}[kind] || '#58a6ff';
      series=chart.addSeries(L.LineSeries,{{color,lineWidth:2,priceLineVisible:false}});
      series.setData(lineData(key));
    }}
    chart.timeScale().setVisibleLogicalRange(defaultLogicalRange());
    chart.timeScale().subscribeVisibleLogicalRangeChange(range=>{{
      if(syncing || !range) return;
      const next=clampLogicalRange(range);
      syncing=true;
      if(!isSameRange(range,next)){{
        chart.timeScale().setVisibleLogicalRange(next);
      }}
      chartApis.forEach(item=>{{ if(item.chart!==chart) item.chart.timeScale().setVisibleLogicalRange(next); }});
      syncing=false;
    }});
    const wrapper=el.closest('.tv-chart-panel');
    const tip=wrapper ? wrapper.querySelector('.tv-tooltip') : null;
    chart.subscribeCrosshairMove(param=>{{
      if(crosshairSyncing) return;
      if(!param || !param.time){{
        crosshairSyncing=true;
        clearAllCrosshairs();
        crosshairSyncing=false;
        return;
      }}
      crosshairSyncing=true;
      syncAllCrosshairs(param.time);
      crosshairSyncing=false;
    }});
    const item={{chart,el,series,kind,tip}};
    chartApis.push(item);
    if(kind==='k') setupDrawing(item);
  }}
  addPanel('k','日K',360);
  addPanel('volume','成交量',150);
  addPanel('foreignShares','外資持股張數',150);
  addPanel('foreign','外資買賣超',150);
  addPanel('marginBalance','融資餘額',150);
  addPanel('major','大戶持股比例(400張以上)',150);
  addPanel('middlePeople','中實戶持股人數(200-400張)',150);
  addPanel('retail','散戶持股比例',150);
  addPanel('totalPeople','總股東人數',150);
  window.addEventListener('resize',()=>chartApis.forEach(item=>item.chart.applyOptions({{width:item.el.clientWidth}})));
}})();
</script>"""
    return f"""
<div id="{panel_id}" class="tv-chart-grid">
  {panels}
</div>
{script}"""


def build_mda_stock_detail_page(stock_id: str, s: dict) -> str:
    s = enrich_stock_fields(dict(s))
    daily = aggregate_ohlcv(merge_report_close(read_price_history(stock_id), s), "daily")
    tech = technical_snapshot(daily, s) if daily else {}
    chip_series = read_chip_series(stock_id)
    chip = read_chip_summary(stock_id)
    holding = read_holding_summary(stock_id)
    holding_series = read_holding_series(stock_id)
    scored = mda_score_stock(s, True)
    abc = mda_abc_checks(s, daily, tech, chip_series, holding)
    obs = mda_observation_checks(stock_id, daily, tech, chip_series, holding)
    money = mda_chip_structure(stock_id, chip_series, holding)
    close = tech.get("close")
    ma120 = tech.get("ma120")
    ma240 = tech.get("ma240")
    slopes = tech.get("ma_slopes") or {}
    detrend_120 = tech.get("detrend_120")
    ma120_gap = ((close / ma120 - 1) * 100) if close and ma120 else None
    ma240_gap = ((close / ma240 - 1) * 100) if close and ma240 else None
    detrend_gap = ((close / detrend_120 - 1) * 100) if close and detrend_120 else None
    volume_price = tech.get("volume_price", "資料不足")
    volume_basis = tech.get("volume_price_basis", "資料不足")
    pressure = abc.get("pressure") or pressure_absorption_analysis(stock_id, daily, chip_series, read_margin_series(stock_id), tech)
    synced_charts = mda_lightweight_chart_panel(stock_id, daily, holding_series, chip_series)
    ma120_slope = slopes.get("ma120")
    ma240_slope = slopes.get("ma240")
    ma120_up = ma120_slope is not None and ma120_slope > 0
    ma240_up = ma240_slope is not None and ma240_slope > 0
    if ma120_up and ma240_up:
        trend_note = "MA120、MA240 已開始向上彎，符合 M 大第一層觀察。"
        trend_cls = "pos"
    elif ma120_up or ma240_up:
        trend_note = "長均線已有一條向上彎，另一條還在等待確認。"
        trend_cls = ""
    else:
        trend_note = "MA120、MA240 尚未明確上彎，先降低觀察順位。"
        trend_cls = "neg"
    if detrend_gap is not None and detrend_gap >= 0:
        deduct_note = f"120日扣抵值 {fmt_num(detrend_120)}，收盤高於扣抵 {fmt_num(detrend_gap, 1)}%，扣抵偏低有利均線後續彎上。"
        deduct_cls = "pos"
    elif detrend_gap is not None:
        deduct_note = f"120日扣抵值 {fmt_num(detrend_120)}，收盤低於扣抵 {fmt_num(abs(detrend_gap), 1)}%，扣抵壓力還沒完全解除。"
        deduct_cls = "neg"
    else:
        deduct_note = "120日扣抵資料不足，先只看 MA120 / MA240 斜率。"
        deduct_cls = ""

    why = (
        _mda_line("觀察等級", f'<span class="tag {scored["tag_cls"]}">{esc(scored["action"])}</span>　分數 {fmt_num(scored["score"], 0)}')
        + _mda_line("長均線狀態", trend_note, trend_cls)
        + _mda_line("120日扣抵", deduct_note, deduct_cls)
    )
    a_block = (
        _mda_line("MA120", f'{fmt_num(ma120)}｜斜率 {fmt_num(slopes.get("ma120"))}｜距離 {fmt_num(ma120_gap, 1)}%')
        + _mda_line("MA240", f'{fmt_num(ma240)}｜斜率 {fmt_num(slopes.get("ma240"))}｜距離 {fmt_num(ma240_gap, 1)}%')
        + _mda_line("120日扣抵", f'{fmt_num(detrend_120)}｜收盤距扣抵 {fmt_num(detrend_gap, 1)}%')
        + _mda_line("A階段", abc.get("a_phase", "A未成立"), "pos" if abc.get("a_phase") == "已發動長多" else "warn" if abc.get("a_score", 0) >= 18 else "neg")
        + _mda_line("A判讀", "長多已發動，等 B2 不追高。" if abc.get("a_phase") == "已發動長多" else "仍未發動，但 240 扣抵與區間不破低可列入空轉多觀察。" if abc.get("a_phase") == "未發動空轉多觀察" else "長均線尚未同時上彎，觀察順位降低。", "pos" if abc.get("a_phase") == "已發動長多" else "warn" if abc.get("a_score", 0) >= 18 else "neg")
    )
    b1_block = (
        _mda_line("大戶/散戶", f'大戶4週 {fmt_num(money["major_4w"])}%｜8週 {fmt_num(money["major_8w"])}%｜散戶4週 {fmt_num(money["retail_4w"])}%｜8週 {fmt_num(money["retail_8w"])}%｜股東4週 {fmt_num(money["people_4w"], 0)} 人｜8週 {fmt_num(money["people_8w"], 0)} 人')
        + _mda_line("法人籌碼", f'外資10日 {fmt_num(money["foreign_10d"], 0)} 張｜主力10日 {fmt_num(money["force_10d"], 0)} 張')
        + _mda_line("B1判讀", esc(money["reading"]), money["class"])
    )
    b2_block = (
        _mda_line("量價", esc(volume_price))
        + _mda_line("判斷依據", esc(volume_basis))
        + _mda_line("價量籌碼關聯", esc(pressure.get("summary", "資料不足")), pressure.get("class", ""))
        + _mda_line("賣壓觀察", esc(pressure.get("line", "量價尚未證明賣壓收斂，先只觀察。")))
    )
    chip_ok = (
        (
            (money.get("major_4w") is not None and money.get("major_4w") >= 0.5)
            or (money.get("major_8w") is not None and money.get("major_8w") >= 1.0)
        )
        and (
            (money.get("retail_4w") is not None and money.get("retail_4w") <= -0.3)
            or (money.get("retail_8w") is not None and money.get("retail_8w") <= -0.8)
            or (money.get("people_4w") is not None and money.get("people_4w") < 0)
            or (money.get("people_8w") is not None and money.get("people_8w") < 0)
        )
    )
    chip_bad = (
        (money.get("major_4w") is not None and money.get("major_4w") < 0)
        or (money.get("retail_4w") is not None and money.get("retail_4w") > 0)
        or (money.get("people_4w") is not None and money.get("people_4w") > 0)
    )
    if chip_ok:
        chip_answer = "偏正向：大戶比例續增，散戶比例或股東人數沒有同步增加，籌碼較像往聰明錢集中。"
        chip_answer_cls = "pos"
    elif chip_bad:
        chip_answer = "偏保守：大戶沒有明顯續增，或散戶/股東人數同步增加，暫時不要把它當成籌碼集中。"
        chip_answer_cls = "neg"
    else:
        chip_answer = "待確認：股權結構變化不夠明確，先繼續追蹤大戶是否續增、散戶是否下降。"
        chip_answer_cls = ""

    recent_lows = [x.get("low") for x in daily[-10:] if x.get("low") is not None]
    prev_lows = [x.get("low") for x in daily[-25:-10] if x.get("low") is not None]
    recent_low = min(recent_lows) if recent_lows else None
    prev_low = min(prev_lows) if prev_lows else None
    not_break_low = recent_low is not None and prev_low is not None and recent_low >= prev_low * 0.98
    challenge_ma = bool(close and ((ma120 and close >= ma120 * 0.97) or (ma240 and close >= ma240 * 0.97)))
    volume_ok = volume_price in {"量縮價漲", "量增價漲", "量縮價穩", "均量上彎"}
    if not_break_low and challenge_ma:
        price_answer = "偏正向：量縮時價格沒有破低，目前仍能靠近或挑戰關鍵均線。"
        price_answer_cls = "pos"
    elif not_break_low and volume_ok:
        price_answer = "待突破：量價沒有轉壞，價格也沒有破低，下一步看放量時能否挑戰關鍵均線。"
        price_answer_cls = ""
    elif not_break_low:
        price_answer = "先觀察：價格暫時沒有破低，但量價訊號還不夠強，等有量攻擊再確認。"
        price_answer_cls = ""
    else:
        price_answer = "偏弱：近期價格已有破低疑慮，量縮不破低這個條件尚未成立。"
        price_answer_cls = "neg"

    next_watch = (
        _mda_line("籌碼答案", chip_answer, chip_answer_cls)
        + _mda_line("量價答案", price_answer, price_answer_cls)
        + _mda_line("B2追蹤法", "先拉長看是否長多或轉長多，再拆下跌/拉升段的主力與大戶動態；同時追20日扣抵量、240扣抵價，以及止跌後是否快速站回均線、紅K與下影線是否變多、能否慢慢過前高。")
    )
    diagnosis_html = build_mda_auto_diagnosis(
        stock_id,
        s.get("name", ""),
        abc,
        money,
        pressure,
        chip_answer,
        price_answer,
    )

    body = f"""
<div class="container">
  <div style="margin-bottom:8px"><a href="../mda.html" style="color:#6e7681;font-size:13px">&larr; 回 M大選股</a>　<a href="../stocks/{esc(stock_id)}.html" style="color:#6e7681;font-size:13px">一般個股頁 →</a></div>
  <div class="page-title">{esc(stock_id)} {esc(s.get('name',''))}｜M大觀察解析</div>
  <div class="page-sub">照 M大個股分析順序：A 長均線 → 120日扣抵 → B1 聰明錢 → B2 賣壓 → 後續追蹤</div>
  {diagnosis_html}
  <div class="grid grid-2">
    <div class="card"><div class="section-label">① 為什麼值得觀察</div><div class="telegram-phase">{why}</div></div>
    <div class="card"><div class="section-label">② A：長期趨勢</div><div class="telegram-phase">{a_block}</div></div>
    <div class="card"><div class="section-label">③ B1：聰明錢與股權結構</div><div class="telegram-phase">{b1_block}</div></div>
    <div class="card"><div class="section-label">④ B2：賣壓是否變小</div><div class="telegram-phase">{b2_block}</div></div>
  </div>
  <div class="card"><div class="section-label">⑤ 接下來觀察什麼</div><div class="telegram-phase">{next_watch}</div></div>
  <div class="card">
    <div class="section-label">日K / 外資 / 融資 / 股權結構連動圖</div>
    {synced_charts}
  </div>
</div>"""
    return html_page(f"{stock_id} M大觀察解析", "mda", body, nav_prefix="../")


def build_mda_stock_pages(reports: list[dict]) -> int:
    latest = latest_stock_report(reports)
    out_dir = OUTPUT_DIR / "mda_stocks"
    out_dir.mkdir(parents=True, exist_ok=True)
    ids = {p.stem for p in out_dir.glob("*.html") if re.fullmatch(r"\d{4,6}", p.stem)}
    ids.update(str(s.get("id", "")).strip() for s in latest.get("stocks", []) if s.get("id"))
    count = 0
    for sid in sorted(x for x in ids if x):
        (out_dir / f"{sid}.html").write_text(redirect_page(f"../stocks/{sid}.html", f"{sid} stock detail"), encoding="utf-8")
        count += 1
    return count


def build_top20_score_explainer(date_str: str) -> str:
    """Explain the Daily Top20 score without changing the screening logic."""
    return f"""
<div class="card score-note-card">
  <div class="section-head">
    <div>
      <div class="section-label">評分機制</div>
      <div class="metric-title">M大分數怎麼算</div>
    </div>
    <div class="section-date">資料日 {date_str}</div>
  </div>
  <div class="score-note-grid">
    <div class="score-note">
      <div class="k">A. 長均線與大戶基礎</div>
      <div class="v">30 分</div>
      <div class="desc">MA120 上彎、收盤站上 MA120，且大戶 4 週增加 >= 0.5% 或 8 週增加 >= 1.0%。</div>
    </div>
    <div class="score-note">
      <div class="k">B1. 籌碼支撐</div>
      <div class="v">20 分</div>
      <div class="desc">散戶 4 週下降 <= -0.3%、8 週下降 <= -0.8%，或總股東人數 4/8 週下降。</div>
    </div>
    <div class="score-note">
      <div class="k">A. MA240 結構</div>
      <div class="v">30 分</div>
      <div class="desc">收盤站上 MA240 加 15 分；MA240 近 20 日斜率 >= 0 再加 15 分。</div>
    </div>
    <div class="score-note">
      <div class="k">B2. 不破低</div>
      <div class="v">10 分</div>
      <div class="desc">近 20 日低點沒有明顯跌破近 60 日低位區，條件為 20 日低點 >= 60 日低點的 98%。</div>
    </div>
    <div class="score-note">
      <div class="k">B2. 量縮</div>
      <div class="v">10 分</div>
      <div class="desc">20 日均量比 120 日均量低至少 20%，代表賣壓或追價熱度收斂。</div>
    </div>
    <div class="score-note">
      <div class="k">Top20 排序</div>
      <div class="v">先籃位，再分數</div>
      <div class="desc">已發動籃優先，其次空轉多、未發動；同籃再依 M大分數由高到低排序。</div>
    </div>
  </div>
  <div class="score-rule">目前公式來源：`mda_universe_scan.py`。滿分 100 分，不含外資買賣超；外資欄位是另外給你判讀短線籌碼，不是這個 Score 的加分項。</div>
</div>"""


def build_daily_page(report: dict) -> str:
    """生成單日完整報告頁"""
    date_str = report.get("date", "─")
    stocks = report.get("stocks", [])
    marching, consolidation, risk = split_baskets(stocks)

    stat_row = f"""
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#3fb950">{len(marching)}</div>
    <div style="font-size:11px;color:#6e7681">行進籃</div>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#58a6ff">{len(consolidation)}</div>
    <div style="font-size:11px;color:#6e7681">盤整籃</div>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#f85149">{len(risk)}</div>
    <div style="font-size:11px;color:#6e7681">過熱/風險</div>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#58a6ff">{len(stocks)}</div>
    <div style="font-size:11px;color:#6e7681">精選總數</div>
  </div>
</div>"""

    market_section = f"""
<div class="card">
  <div class="section-label">📰 大盤市況</div>
  <div class="market-text">{report.get('market_overview','').replace(chr(10),'<br>')}</div>
</div>"""

    filter_section = f"""
<div class="card">
  <div class="section-label">🔍 篩選流程</div>
  {build_filter_steps(report.get('filter_summary', []))}
</div>"""

    table_section = (
        '<div class="card daily-top20-card">'
        '<div class="section-label">Top 20</div>'
        '<div style="font-size:13px;color:#8b949e;margin-bottom:14px;line-height:1.7">'
        '每日 Top20 視為行進籃候選清單，表格已省略行進籃欄位；外資買超為紅色、賣超為綠色；CaryBot 暫接欄先顯示現有藍點特徵，優先顯示 AI_Buy。'
        '</div>'
        + build_stock_table(stocks, compact=False, stock_link_prefix="../stocks", show_basket=False, show_status=False, show_carybot=True)
        + '</div>'
    )

    notes_text = report.get("notes", "")
    notes_section = ""
    if notes_text:
        notes_section = (
            '<div class="card">'
            '<div class="section-label">Operation Notes</div>'
            + build_notes(notes_text)
            + '</div>'
        )

    body = (
        '<div class="container">'
        '<div style="margin-bottom:8px"><a href="../index.html" style="color:#6e7681;font-size:13px">&larr; Home</a></div>'
        + f'<div class="page-title">{date_str}</div>'
        + '<div class="page-sub">每日 Top20 母名單：先看系統今天挑出哪些股票，再決定要丟進 SFZ、M大或買點雷達繼續判讀。</div>'
        + stat_row + market_section + filter_section + build_top20_score_explainer(date_str) + table_section + notes_section
        + '</div>'
    )
    return html_page(f"{date_str}", "daily", body, nav_prefix="../")


def build_sector_focus_section(stocks: list[dict], top_n: int = 8) -> str:
    try:
        import run_screener as daily_screener
        sector_scores = daily_screener.market_sector_flow(daily_screener.load_industry_map())
    except Exception:
        sector_scores = {}
    if not sector_scores:
        return ""
    top = sorted(sector_scores.values(), key=lambda r: float(r.get("rank") or 999))[:top_n]
    sector_rank = {str(row.get("sector")): row for row in top}
    hits = []
    for stock in stocks:
        sid = str(stock.get("id") or stock.get("stock_id") or "")
        sector = stock.get("sector") or stock_sector(sid)
        summary = sector_scores.get(str(sector))
        if summary and float(summary.get("rank") or 999) <= top_n:
            hits.append((stock, sector, summary))

    sector_rows = ""
    for row in top:
        avg_ret5_text = f"{fmt_num(row.get('avg_ret5'), 2)}%"
        sector_rows += f"""
<tr>
  <td>#{fmt_num(row.get('rank'), 0)}</td>
  <td>{esc(row.get('sector'))}</td>
  <td>{fmt_num(normalize_score_value(row.get('score')), 1)}</td>
  <td>{fmt_num(row.get('turnover_billion'), 1)}</td>
  <td class="{gain_color(avg_ret5_text)}">{avg_ret5_text}</td>
  <td>{fmt_num(row.get('avg_vol_ratio'), 2)}x</td>
</tr>"""

    hit_items = ""
    for stock, sector, summary in hits[:12]:
        sid = str(stock.get("id") or stock.get("stock_id") or "")
        hit_items += (
            f'<span class="tag tag-blue" style="margin:2px 4px 2px 0">'
            f'{esc(sid)} {esc(clean_stock_name(stock.get("name", "")))}｜{esc(sector)} #{fmt_num(summary.get("rank"), 0)}'
            f'</span>'
        )
    if not hit_items:
        hit_items = '<span class="muted">今日 Top20 尚未命中前排資金熱族群，先保留原 M大名單觀察。</span>'

    latest_date = next((row.get("date") for row in top if row.get("date")), "")
    return f"""
<div class="card">
  <div class="section-head">
    <div>
      <div class="section-label">市場資金族群</div>
      <div class="metric-title">資金追捧 TOP{len(top)}</div>
    </div>
    <div class="section-date">資料日期：{esc(latest_date)}</div>
  </div>
  <div class="strategy-note" style="margin-bottom:12px">以全市場價格快取計算成交金額、5/20日動能、量能比與上漲家數；每日 Top20 會優先挑前 {top_n} 名熱族群內的 M大候選，單一族群設上限避免名單過度集中。</div>
  <div style="overflow-x:auto">
    <table class="stock-table">
      <thead><tr><th>排名</th><th>族群</th><th>分數</th><th>成交金額(億)</th><th>5日動能</th><th>量能比</th></tr></thead>
      <tbody>{sector_rows}</tbody>
    </table>
  </div>
  <div class="strategy-note" style="margin-top:12px"><strong>Top20 命中熱族群：</strong><br>{hit_items}</div>
</div>"""


def build_latest_daily_page(reports, section_only=False):
    latest = latest_stock_report(reports)
    date_str = latest.get("date", "-")
    stocks = latest.get("stocks", [])

    market_card = (
        '<div class="card">'
        '<div class="section-label">Market Overview</div>'
        + '<div class="market-text">' + latest.get("market_overview", "").replace("\n", "<br>") + '</div>'
        + '</div>'
    )
    table_section = (
        '<div class="card daily-top20-card">'
        + f'<div class="section-label">Top 20 &mdash; {date_str}</div>'
        + '<div style="font-size:13px;color:#8b949e;margin-bottom:14px;line-height:1.7">'
        '每日 Top20 視為行進籃候選清單，表格已省略行進籃欄位；外資買超為紅色、賣超為綠色；CaryBot 暫接欄先顯示現有藍點特徵，優先顯示 AI_Buy。'
        '</div>'
        + build_stock_table(stocks, compact=False, show_basket=False, show_status=False, show_carybot=True)
        + '</div>'
    )
    notes_text = latest.get("notes", "")
    notes_section = ""
    if notes_text:
        notes_section = (
            '<div class="card">'
            '<div class="section-label">Notes</div>'
            + build_notes(notes_text) + '</div>'
        )

    body = (
        '<div class="container">'
        '<div class="page-title">每日 Top20</div>'
        + f'<div class="page-sub">每日 Top20 母名單：先看系統今天挑出哪些股票，再決定要丟進 SFZ、M大或買點雷達繼續判讀。資料日期：{date_str} &middot; <a href="history.html">歷史報告 &rarr;</a></div>'
        + market_card + build_sector_focus_section(stocks) + build_top20_score_explainer(date_str) + table_section + notes_section
        + '</div>'
    )
    if section_only:
        return body
    return html_page("每日Top20", "daily", body)


def build_baskets_page(reports, section_only=False):
    latest = latest_stock_report(reports)
    date_str = latest.get("date", "-")
    stocks = latest.get("stocks", [])
    marching, consolidation, risk = split_baskets(stocks)
    risk_watch = risk or build_risk_watchlist(stocks)
    ledger = build_signal_ledger(reports)
    sfz_all_payload = load_sfz_all_payload()
    market_sentiment = load_market_sentiment_payload()
    sfz_all_stocks = sfz_all_payload.get("stocks") or []
    if sfz_all_stocks:
        def payload_bucket(row: dict) -> str:
            text = f"{row.get('basket', '')} {row.get('status', '')}"
            if "風險" in text or "過熱" in text:
                return "risk"
            if "盤整" in text or "觀察" in text:
                return "consolidation"
            return "marching"

        marching_count = sum(1 for row in sfz_all_stocks if payload_bucket(row) == "marching")
        consolidation_count = sum(1 for row in sfz_all_stocks if payload_bucket(row) == "consolidation")
        risk_count = sum(1 for row in sfz_all_stocks if payload_bucket(row) == "risk")
    else:
        marching_count = len(marching)
        consolidation_count = len(consolidation)
        risk_count = len(risk)

    hero = f"""
<div class="card">
  <div class="section-label">Daily Strategy Stream</div>
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num" style="color:#3fb950">{len(marching)}</div><div class="metric-label">行進籃：SFZ 訊號日先試單，TA3 作確認/加碼</div></div>
    <div class="metric"><div class="metric-num" style="color:#58a6ff">{len(consolidation)}</div><div class="metric-label">盤整籃：M大 ABC 先觀察，等量價轉強</div></div>
    <div class="metric"><div class="metric-num" style="color:#f85149">{len(risk)}</div><div class="metric-label">過熱/風險：不追高，等 MA5/MA10/箱頂回測</div></div>
  </div>
</div>"""

    playbook = """
<div class="card">
  <div class="section-label">操作框架</div>
  <div class="grid grid-2">
    <div class="strategy-note">
      <strong style="color:#3fb950">行進籃</strong><br>
      SFZ 入籃代表波段候選已成立；不等待 TA3-Soft 才買。原訊號可小試單，TA3-Strict 或箱型強突破可加碼。漲過 +10% 後用 MA20 + 短線轉弱共振，漲過 +20% 後以 MA20 主線續抱。
    </div>
    <div class="strategy-note">
      <strong style="color:#58a6ff">盤整籃</strong><br>
      MABC 判斷是否值得等待，VPA / WR / MA5-MA10 站回負責提早找買點。未突破前只小部位；突破追不到不追，等回測 MA5/MA10/箱頂不破再處理。
    </div>
  </div>
</div>"""

    market_panel = build_market_sentiment_panel(market_sentiment, compact=True)
    full_listing = build_sfz_all_controls(sfz_all_payload, market_sentiment) if sfz_all_stocks else ""

    body = (
        '<div class="container">'
        + '<div class="page-title">SFZ 雙籃</div>'
        + f'<div class="page-sub">把每日 Top20 拆成行進籃與盤整籃：行進籃偏 SFZ 波段，盤整籃偏等待轉強。資料日期：{date_str}</div>'
        + build_daily_decisions_panel(load_daily_decisions_payload(), compact=True)
        + market_panel
        + hero
        + playbook
        + full_listing
        + '<div class="grid grid-2">'
        + build_basket_column("行進籃｜SFZ 波段", "已進入較強趨勢的候選；重點是買點可執行、MA20續抱、避免漲停追高。", marching, "marching", ledger)
        + build_basket_column("盤整籃｜MABC 觀察", "尚未完全發動但值得等待；重點是量縮價穩、籌碼不離開、早買型態浮現。", consolidation, "consolidation", ledger)
        + '</div>'
        + build_basket_column("過熱/風險觀察", "強勢但不適合追價，或已出現賣出警示；等回測、降溫或重新整理後再評估。", risk_watch, "risk", ledger)
        + '</div>'
    )
    if section_only:
        return body
    return html_page("SFZ雙籃", "basket", body)


def build_signals_page(reports, section_only=False):
    ledger = build_signal_ledger(reports)
    latest = latest_stock_report(reports)
    latest_date = latest.get("date", "-")
    latest_ids = {s.get("id") for s in latest.get("stocks", [])}
    total_events = sum(len(x["events"]) for x in ledger.values())
    pushed_events = sum(x["push_count"] for x in ledger.values())
    active_count = sum(1 for sid in ledger if sid in latest_ids)
    push_note = (
        f'<span class="push-ok">{pushed_events}</span> / {total_events}'
        if PUSH_LOG_PATH.exists()
        else '<span class="push-wait">尚未找到 signal_push_log.csv，先顯示入選歷史</span>'
    )

    query_map = build_stock_query_map(reports)
    for required_sid in ("2342", "8341"):
        if required_sid in ledger or required_sid not in query_map:
            continue
        qs = enrich_stock_fields(query_map[required_sid])
        _, _, qdecision = stock_trade_context(qs)
        price_date = qs.get("price_date") or latest_date
        ledger[required_sid] = {
            "id": required_sid,
            "name": qs.get("name", ""),
            "sector": qs.get("industry") or qs.get("sector") or "未分類",
            "query_only": True,
            "events": [{
                "date": price_date,
                "basket": classify_basket(qs),
                "entry": qdecision.get("entry_range") or qdecision.get("entry_text") or qs.get("entry", "─"),
                "raw_entry": qs.get("entry", "─"),
                "price": qs.get("price", "─"),
                "score": qs.get("score", "─"),
                "score_source": qs.get("score_source", "快取個股"),
                "pushed": False,
                "log_count": 0,
            }],
            "push_count": 0,
        }

    rows = ""
    sorted_items = sorted(
        ledger.values(),
        key=lambda x: (x["events"][-1]["date"], len(x["events"])),
        reverse=True,
    )
    for item in sorted_items:
        events = item["events"]
        latest_event = events[-1]
        dates = "、".join(e["date"] for e in events[-6:])
        if len(events) > 6:
            dates += " ..."
        latest_mark = '<span class="tag tag-green">今日仍在榜</span>' if item["id"] in latest_ids else '<span class="tag">歷史訊號</span>'
        basket = basket_label(latest_event["basket"])
        push_status = (
            f'<span class="push-ok">{item["push_count"]}/{len(events)}</span>'
            if PUSH_LOG_PATH.exists() and item["push_count"] == len(events)
            else f'<span class="push-miss">{item["push_count"]}/{len(events)}</span>'
            if PUSH_LOG_PATH.exists()
            else '<span class="push-wait">待串接</span>'
        )
        href = f"stocks/{esc(item['id'])}.html"
        count_value = 0 if item.get("query_only") else len(events)
        count_label = "查詢股" if item.get("query_only") else f"<strong>{len(events)}</strong> 次"
        first_date = events[0]["date"]
        latest_date_row = latest_event["date"]
        rows += f"""
<tr class="clickable-row" data-ledger-row data-text="{esc(item['id'] + ' ' + item['name'])}" data-code="{esc(item['id'])}" data-name="{esc(item['name'])}" data-count="{count_value}" data-first="{esc(first_date)}" data-latest="{esc(latest_date_row)}" data-entry="{esc(str(latest_event['entry']))}" data-push="{item['push_count']}" data-current="{'1' if item['id'] in latest_ids else '0'}" onclick="location.href='{href}'">
  <td>
    <div><a class="stock-link" href="{href}" onclick="event.stopPropagation()">{esc(item['id'])} {esc(item['name'])}</a></div>
    <div class="signal-dates"><a href="{href}" onclick="event.stopPropagation()">打開個股資訊卡 →</a></div>
    <div class="tag-row">{latest_mark}<span class="tag">{basket}</span></div>
  </td>
  <td>{count_label}</td>
  <td>{first_date}<br><span style="color:#8b949e">最近 {latest_date_row}</span></td>
  <td>買入區 {latest_event['entry']}<br><span style="color:#8b949e">原始買點 {latest_event.get('raw_entry','─')} ｜ 收盤 {latest_event['price']} ｜ 原始分數 {latest_event['score']}</span></td>
  <td>{push_status}</td>
  <td><div class="signal-dates">{dates}</div></td>
</tr>"""

    body = f"""
<div class="container">
  <div class="page-title">入選追蹤</div>
  <div class="page-sub">這裡不是新選股，是台帳：記錄哪些股票曾經入選、出現幾次、現在還在不在榜上。最新資料：{latest_date}</div>
  <div class="card">
    <div class="section-label">Signal Ledger</div>
    <div class="grid grid-3">
      <div class="metric"><div class="metric-num" style="color:#58a6ff">{len(ledger)}</div><div class="metric-label">歷史唯一個股</div></div>
      <div class="metric"><div class="metric-num" style="color:#3fb950">{active_count}</div><div class="metric-label">今日仍在追蹤</div></div>
      <div class="metric"><div class="metric-num" style="font-size:16px">{push_note}</div><div class="metric-label">推播覆蓋率</div></div>
    </div>
    {"" if PUSH_LOG_PATH.exists() else coming_soon_block("入選追蹤推播紀錄（signal_push_log.csv 接入中）", '<div class="strategy-note">尚未找到 signal_push_log.csv，先顯示入選歷史。</div>', "signal_push_log.csv", False)}
    <div class="strategy-note" style="margin-top:14px">
      這頁先用每日報告建立「入選台帳」。等推播流程把成功紀錄寫入 <strong>signal_push_log.csv</strong> 後，這裡就會變成查漏清單：任何 0/N 或未滿 N/N 的個股，都代表有買點需要補查。
    </div>
  </div>
  <div class="card" data-ledger>
    <div class="section-label">歷史訊號摘要</div>
    <div class="ledger-controls">
      <input type="search" data-ledger-search placeholder="搜尋代號或名稱，例如 2342" aria-label="搜尋代號或名稱">
      <label class="filter-chip"><input type="checkbox" data-ledger-current checked> 今日仍在榜</label>
      <label class="filter-chip"><input type="checkbox" data-ledger-history> 歷史訊號</label>
    </div>
    <div style="overflow-x:auto">
      <table class="stock-table signal-table">
        <thead>
          <tr><th data-ledger-sort="code">個股</th><th data-ledger-sort="count">入選</th><th data-ledger-sort="latest">首次/最近</th><th data-ledger-sort="entry">最新買入區</th><th data-ledger-sort="push">推播</th><th>出現日期</th></tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div class="pager"><button type="button" data-page-prev>前</button><span data-page-nums></span><span data-page-info></span><button type="button" data-page-next>後</button></div>
  </div>
</div>"""
    if section_only:
        return body
    return html_page("入選追蹤", "signals", body)


def build_stock_detail_page(stock_id: str, s: dict, ledger: dict[str, dict]) -> str:
    item = ledger.get(stock_id, {})
    rows = merge_report_close(read_price_history(stock_id), s)
    daily = aggregate_ohlcv(rows, "daily")
    weekly = aggregate_ohlcv(rows, "weekly")
    monthly = aggregate_ohlcv(rows, "monthly")
    latest = daily[-1] if daily else {}
    tech = technical_snapshot(daily, s)
    chip = read_chip_summary(stock_id)
    chip_series = read_chip_series(stock_id)
    holding = read_holding_summary(stock_id)
    holding_series = read_holding_series(stock_id)
    daily_chip_indicators = align_chip_to_price_dates(daily, holding_series, chip_series)
    weekly_chip_indicators = align_chip_to_price_dates(weekly, holding_series, chip_series)
    monthly_chip_indicators = align_chip_to_price_dates(monthly, holding_series, chip_series)
    decision = build_trade_decision(tech, s)
    sell_signal = calc_sell_signal(daily, weekly, chip_series, s, decision)
    s_view = dict(s)
    if latest.get("close") is not None:
        s_view["price"] = f'{latest["close"]:.2f}'
        s_view["price_date"] = latest.get("date", "")
    quick_html = f'<div class="mini-report">{esc(quick_analysis_text(s_view, item))}</div>'

    event_rows = ""
    for e in item.get("events", [])[-12:][::-1]:
        source = e.get("score_source", "原始報告 Score")
        event_rows += f"""
<tr>
  <td>{e['date']}</td><td>{basket_label(e['basket'])}</td><td>{e['entry']}<div class="signal-dates">原始 {e.get('raw_entry','─')}</div></td><td>{e['price']}</td><td>{e['score']}<div class="signal-dates">{source}</div></td>
</tr>"""
    if not event_rows:
        event_rows = '<tr><td colspan="5" style="color:#8b949e">尚無歷史訊號</td></tr>'

    h_latest = holding.get("latest", {}) if holding else {}
    holding_stat_html = (
        '<div class="holding-stats">'
        f'<div class="info-cell"><div class="k">大戶持股比例（400張以上）</div><div class="v">{fmt_num(h_latest.get("major"))}%</div></div>'
        f'<div class="info-cell"><div class="k">中實戶持股人數（200-400張）</div><div class="v">{fmt_num(h_latest.get("middle_people"),0)}人</div></div>'
        f'<div class="info-cell"><div class="k">散戶持股比例</div><div class="v">{fmt_num(h_latest.get("retail"))}%</div></div>'
        f'<div class="info-cell"><div class="k">總股東人數</div><div class="v">{fmt_num(h_latest.get("total_people"),0)}人</div></div>'
        '</div>'
    )

    chart_id = f"chart-{stock_id}"
    holding_id = f"holding-chart-{stock_id}"
    chip_flow_id = f"chip-flow-{stock_id}"
    main_force_id = f"main-force-{stock_id}"
    chart_data = json.dumps({
        "daily": chart_payload(daily),
        "weekly": chart_payload(weekly),
        "monthly": chart_payload(monthly),
    }, ensure_ascii=False)
    holding_data = json.dumps(holding_payload(holding_series), ensure_ascii=False)
    chip_flow_data = json.dumps(chip_flow_payload(chip_series), ensure_ascii=False)
    aligned_chip_data = json.dumps({
        "daily": aligned_chip_payload(daily_chip_indicators),
        "weekly": aligned_chip_payload(weekly_chip_indicators),
        "monthly": aligned_chip_payload(monthly_chip_indicators),
    }, ensure_ascii=False)
    main_force_data = json.dumps(main_force_payload(chip_series, daily), ensure_ascii=False)
    operation_card = build_operation_plan_card(s_view, tech, decision, sell_signal)
    chip_dates = f"法人 {chip.get('date') if chip else '尚無快取'}｜股權 {holding.get('date') if holding else '尚無快取'}"
    lightweight_charts = mda_lightweight_chart_panel(stock_id, daily, holding_series, chip_series)
    chip_tv_panel = chip_lightweight_flow_panel(stock_id, chip_series, daily)
    chart_script = f"""
<script>
const chartData_{stock_id} = {chart_data};
const holdingData_{stock_id} = {holding_data};
const chipFlowData_{stock_id} = {chip_flow_data};
const alignedChipData_{stock_id} = {aligned_chip_data};
const mainForceData_{stock_id} = {main_force_data};
function showChart_{stock_id}(mode){{
  const root=document.getElementById('{chart_id}');
  root.querySelectorAll('.chart-pane').forEach(x=>x.style.display='none');
  root.querySelectorAll('button').forEach(x=>x.classList.remove('active'));
  root.querySelector('[data-pane="'+mode+'"]').style.display='block';
  root.querySelector('[data-btn="'+mode+'"]').classList.add('active');
}}
function nearestByDate_{stock_id}(data, date){{
  const target=Date.parse(date || '');
  if(!data || !data.length || Number.isNaN(target)) return -1;
  let best=0;
  let bestGap=Infinity;
  data.forEach((item, idx)=>{{
    const t=Date.parse(item.date || '');
    if(Number.isNaN(t)) return;
    const gap=Math.abs(t-target);
    if(gap < bestGap){{
      best=idx;
      bestGap=gap;
    }}
  }});
  return best;
}}
function positionTooltip_{stock_id}(chart, line, tip, dataLength, idx, html, xHint, yHint){{
  if(!chart || !line || !tip || dataLength < 2 || idx < 0) return;
  const rect=chart.getBoundingClientRect();
  const left=rect.width * 50 / 900;
  const right=rect.width * (900 - 18) / 900;
  const clamped=left + (right-left) * idx / Math.max(1, dataLength-1);
  line.style.display='block';
  line.style.left=`${{clamped}}px`;
  tip.innerHTML=html;
  tip.style.display='block';
  const tipWidth=tip.offsetWidth || 210;
  const tipHeight=tip.offsetHeight || 128;
  let tx=(Number.isFinite(xHint) ? xHint : clamped) + 14;
  let ty=(Number.isFinite(yHint) ? yHint : 18);
  if(tx + tipWidth > rect.width) tx=(Number.isFinite(xHint) ? xHint : clamped) - tipWidth - 14;
  if(ty + tipHeight > rect.height) ty=rect.height - tipHeight - 8;
  tip.style.left=`${{Math.max(6, tx)}}px`;
  tip.style.top=`${{Math.max(6, ty)}}px`;
}}
function clearOverlay_{stock_id}(chart){{
  if(!chart) return;
  const tip=chart.querySelector('.chart-tooltip');
  const line=chart.querySelector('.chart-crosshair');
  if(tip) tip.style.display='none';
  if(line) line.style.display='none';
}}
function initHoverCharts_{stock_id}(){{
  const root=document.getElementById('{chart_id}');
  if(!root) return;
  const fmt=(v,d=2)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW', {{maximumFractionDigits:d, minimumFractionDigits:d}}) : '-';
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  const html=(x)=>`
    <div class="t-date">${{x.date || '-'}}</div>
    <div class="t-grid">
      <span>開 ${{fmt(x.open)}}</span><span>高 ${{fmt(x.high)}}</span>
      <span>低 ${{fmt(x.low)}}</span><span>收 ${{fmt(x.close)}}</span>
      <span>量 ${{fmtInt((x.volume || 0)/1000)}} 張</span><span></span>
    </div>
    <div class="t-ma">
      BB上 ${{fmt(x.bbUpper)}} / BB下 ${{fmt(x.bbLower)}}<br>
      MA5 ${{fmt(x.ma5)}} / MA10 ${{fmt(x.ma10)}}<br>
      MA20 ${{fmt(x.ma20)}} / MA60 ${{fmt(x.ma60)}}<br>
      KD ${{fmt(x.k,1)}} / ${{fmt(x.d,1)}}<br>
      MACD ${{fmt(x.dif,2)}} / ${{fmt(x.dea,2)}} / ${{fmt(x.macd,2)}}<br>
      W%R ${{fmt(x.wr,1)}}
    </div>`;
  root.querySelectorAll('.hover-chart').forEach(chart=>{{
    if(chart.dataset.hoverReady==='1') return;
    chart.dataset.hoverReady='1';
    const mode=chart.dataset.mode;
    const data=chartData_{stock_id}[mode] || [];
    const tip=chart.querySelector('.chart-tooltip');
    const line=chart.querySelector('.chart-crosshair');
    if(!tip || !line || data.length < 2) return;
    chart.addEventListener('mousemove', ev=>{{
      const rect=chart.getBoundingClientRect();
      const x=ev.clientX - rect.left;
      const left=rect.width * 50 / 900;
      const right=rect.width * (900 - 18) / 900;
      const clamped=Math.max(left, Math.min(right, x));
      const pct=(clamped-left) / Math.max(1, right-left);
      const idx=Math.max(0, Math.min(data.length-1, Math.round(pct*(data.length-1))));
      const item=data[idx];
      line.style.display='block';
      line.style.left=`${{clamped}}px`;
      tip.innerHTML=html(item);
      tip.style.display='block';
      const tipWidth=tip.offsetWidth || 210;
      const tipHeight=tip.offsetHeight || 128;
      let tx=x + 14;
      let ty=ev.clientY - rect.top + 14;
      if(tx + tipWidth > rect.width) tx=x - tipWidth - 14;
      if(ty + tipHeight > rect.height) ty=rect.height - tipHeight - 8;
      tip.style.left=`${{Math.max(6, tx)}}px`;
      tip.style.top=`${{Math.max(6, ty)}}px`;
      syncIndicatorPack_{stock_id}(item.date, mode);
    }});
    chart.addEventListener('mouseleave', ()=>{{
      tip.style.display='none';
      line.style.display='none';
      clearIndicatorPack_{stock_id}(mode);
    }});
  }});
}}
initHoverCharts_{stock_id}();
function holdingHtml_{stock_id}(x){{
  const fmtPct=(v)=>Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}%` : '-';
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  return `
    <div class="t-date">${{x.date || '-'}}</div>
    <div class="t-grid">
      <span>大戶(400張以上)</span><span>${{fmtPct(x.major)}}</span>
      <span>大戶人數</span><span>${{fmtInt(x.majorPeople)}}</span>
      <span>中實戶人數(200-400張)</span><span>${{fmtInt(x.middlePeople)}}</span>
      <span>中實戶比例</span><span>${{fmtPct(x.middle)}}</span>
      <span>散戶&lt;1萬股</span><span>${{fmtPct(x.retail)}}</span>
      <span>總股東數</span><span>${{fmtInt(x.totalPeople)}}</span>
    </div>`;
}}
function kHtml_{stock_id}(x){{
  const fmt=(v,d=2)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW', {{maximumFractionDigits:d, minimumFractionDigits:d}}) : '-';
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  return `
    <div class="t-date">${{x.date || '-'}}</div>
    <div class="t-grid">
      <span>開 ${{fmt(x.open)}}</span><span>高 ${{fmt(x.high)}}</span>
      <span>低 ${{fmt(x.low)}}</span><span>收 ${{fmt(x.close)}}</span>
      <span>量 ${{fmtInt((x.volume || 0)/1000)}} 張</span><span></span>
    </div>
    <div class="t-ma">
      BB上 ${{fmt(x.bbUpper)}} / BB下 ${{fmt(x.bbLower)}}<br>
      MA5 ${{fmt(x.ma5)}} / MA10 ${{fmt(x.ma10)}}<br>
      MA20 ${{fmt(x.ma20)}} / MA60 ${{fmt(x.ma60)}}<br>
      KD ${{fmt(x.k,1)}} / ${{fmt(x.d,1)}}<br>
      MACD ${{fmt(x.dif,2)}} / ${{fmt(x.dea,2)}} / ${{fmt(x.macd,2)}}<br>
      W%R ${{fmt(x.wr,1)}}
    </div>`;
}}
function indicatorData_{stock_id}(chart, mode){{
  const source=chart.dataset.source;
  if(source==='price') return chartData_{stock_id}[chart.dataset.mode || mode] || [];
  if(source==='aligned') return alignedChipData_{stock_id}[mode] || [];
  if(source==='holding') return holdingData_{stock_id} || [];
  return [];
}}
function indicatorHtml_{stock_id}(chart, x){{
  const fmt=(v,d=2)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW', {{maximumFractionDigits:d, minimumFractionDigits:d}}) : '-';
  const fmtInt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  const pct=(v)=>Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}%` : '-';
  const wrState=(v)=>!Number.isFinite(Number(v)) ? '-' : (Number(v) >= -20 ? '偏過熱，留意賣出/降溫' : (Number(v) <= -80 ? '偏超賣，留意反彈/買點' : '中性區'));
  const kdState=(k,d)=>!Number.isFinite(Number(k)) || !Number.isFinite(Number(d)) ? '-' : (Number(k) >= 80 && Number(d) >= 80 ? '偏過熱，留意賣出/降溫' : (Number(k) <= 20 && Number(d) <= 20 ? '偏超賣，留意反彈/買點' : '中性區'));
  const macdState=(x)=>!Number.isFinite(Number(x.dif)) || !Number.isFinite(Number(x.dea)) ? '-' : (Number(x.dif) > Number(x.dea) && Number(x.macd) > 0 ? '買進區' : (Number(x.dif) < Number(x.dea) && Number(x.macd) < 0 ? '賣出區' : '觀察區'));
  const kind=chart.dataset.kind;
  if(kind==='wr') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>Williams %R</span><span>${{fmt(x.wr,1)}}</span><span>區間</span><span>${{wrState(x.wr)}}</span></div>`;
  if(kind==='kd') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>K</span><span>${{fmt(x.k,1)}}</span><span>D</span><span>${{fmt(x.d,1)}}</span><span>區間</span><span>${{kdState(x.k,x.d)}}</span></div>`;
  if(kind==='macd') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>MACD</span><span>${{macdState(x)}}</span></div>`;
  if(kind==='holdingPack') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>大戶比例(400張以上)</span><span>${{pct(x.major)}}</span><span>中實戶人數(200-400張)</span><span>${{fmtInt(x.middlePeople)}} 人</span><span>中實戶比例</span><span>${{pct(x.middle)}}</span><span>散戶持股比例</span><span>${{pct(x.retail)}}</span><span>總股東人數</span><span>${{fmtInt(x.totalPeople)}} 人</span></div>`;
  if(kind==='foreignFlow') return `<div class="t-date">${{x.date || '-'}}</div><div class="t-grid"><span>外資買賣超</span><span>${{fmtInt(x.foreign)}} 張</span><span>區間累積</span><span>${{fmtInt(x.foreignCum)}} 張</span></div>`;
  return `<div class="t-date">${{x.date || '-'}}</div>`;
}}
function syncIndicatorPack_{stock_id}(date, mode){{
  const root=document.getElementById('{chart_id}');
  const pane=root ? root.querySelector('[data-pane="'+mode+'"]') : null;
  if(!pane) return;
  pane.querySelectorAll('.indicator-hover').forEach(chart=>{{
    const data=indicatorData_{stock_id}(chart, mode);
    const idx=nearestByDate_{stock_id}(data, date);
    if(idx < 0) return;
    positionTooltip_{stock_id}(chart, chart.querySelector('.chart-crosshair'), chart.querySelector('.chart-tooltip'), data.length, idx, indicatorHtml_{stock_id}(chart, data[idx]));
  }});
}}
function clearIndicatorPack_{stock_id}(mode){{
  const root=document.getElementById('{chart_id}');
  const pane=root ? root.querySelector('[data-pane="'+mode+'"]') : null;
  if(!pane) return;
  pane.querySelectorAll('.indicator-hover').forEach(chart=>clearOverlay_{stock_id}(chart));
}}
function syncMainK_{stock_id}(date, mode){{
  const root=document.getElementById('{chart_id}');
  const pane=root ? root.querySelector('[data-pane="'+mode+'"]') : null;
  const chart=pane ? pane.querySelector('.hover-chart[data-mode="'+mode+'"]') : null;
  const data=chartData_{stock_id}[mode] || [];
  const idx=nearestByDate_{stock_id}(data, date);
  if(!chart || idx < 0) return;
  positionTooltip_{stock_id}(chart, chart.querySelector('.chart-crosshair'), chart.querySelector('.chart-tooltip'), data.length, idx, kHtml_{stock_id}(data[idx]));
}}
function clearMainK_{stock_id}(mode){{
  const root=document.getElementById('{chart_id}');
  const pane=root ? root.querySelector('[data-pane="'+mode+'"]') : null;
  clearOverlay_{stock_id}(pane ? pane.querySelector('.hover-chart[data-mode="'+mode+'"]') : null);
}}
function initIndicatorHover_{stock_id}(){{
  const root=document.getElementById('{chart_id}');
  if(!root) return;
  root.querySelectorAll('.indicator-hover').forEach(chart=>{{
    if(chart.dataset.hoverReady==='1') return;
    chart.dataset.hoverReady='1';
    const tip=chart.querySelector('.chart-tooltip');
    const line=chart.querySelector('.chart-crosshair');
    if(!tip || !line) return;
    chart.addEventListener('mousemove', ev=>{{
      const pane=chart.closest('.chart-pane');
      const mode=pane ? pane.dataset.pane : 'daily';
      const data=indicatorData_{stock_id}(chart, mode);
      if(data.length < 2) return;
      const rect=chart.getBoundingClientRect();
      const x=ev.clientX - rect.left;
      const left=rect.width * 50 / 900;
      const right=rect.width * (900 - 18) / 900;
      const clamped=Math.max(left, Math.min(right, x));
      const pct=(clamped-left) / Math.max(1, right-left);
      const idx=Math.max(0, Math.min(data.length-1, Math.round(pct*(data.length-1))));
      const item=data[idx];
      syncMainK_{stock_id}(item.date, mode);
      syncIndicatorPack_{stock_id}(item.date, mode);
    }});
    chart.addEventListener('mouseleave', ()=>{{
      const pane=chart.closest('.chart-pane');
      const mode=pane ? pane.dataset.pane : 'daily';
      clearIndicatorPack_{stock_id}(mode);
      clearMainK_{stock_id}(mode);
    }});
  }});
}}
initIndicatorHover_{stock_id}();
function syncHoldingFromK_{stock_id}(date){{
  const chart=document.getElementById('{holding_id}');
  const data=holdingData_{stock_id} || [];
  const idx=nearestByDate_{stock_id}(data, date);
  if(!chart || idx < 0) return;
  positionTooltip_{stock_id}(chart, chart.querySelector('.chart-crosshair'), chart.querySelector('.chart-tooltip'), data.length, idx, holdingHtml_{stock_id}(data[idx]));
}}
function clearHoldingHover_{stock_id}(){{
  clearOverlay_{stock_id}(document.getElementById('{holding_id}'));
}}
function syncDailyFromHolding_{stock_id}(date){{
  const root=document.getElementById('{chart_id}');
  const chart=root ? root.querySelector('.hover-chart[data-mode="daily"]') : null;
  const data=chartData_{stock_id}.daily || [];
  const idx=nearestByDate_{stock_id}(data, date);
  if(!chart || idx < 0) return;
  positionTooltip_{stock_id}(chart, chart.querySelector('.chart-crosshair'), chart.querySelector('.chart-tooltip'), data.length, idx, kHtml_{stock_id}(data[idx]));
}}
function clearDailyHover_{stock_id}(){{
  const root=document.getElementById('{chart_id}');
  clearOverlay_{stock_id}(root ? root.querySelector('.hover-chart[data-mode="daily"]') : null);
}}
function initHoldingHover_{stock_id}(){{
  const chart=document.getElementById('{holding_id}');
  const data=holdingData_{stock_id} || [];
  if(!chart || data.length < 2) return;
  const tip=chart.querySelector('.chart-tooltip');
  const line=chart.querySelector('.chart-crosshair');
  if(!tip || !line) return;
  chart.addEventListener('mousemove', ev=>{{
    const rect=chart.getBoundingClientRect();
    const x=ev.clientX - rect.left;
    const left=rect.width * 50 / 900;
    const right=rect.width * (900 - 18) / 900;
    const clamped=Math.max(left, Math.min(right, x));
    const pct=(clamped-left) / Math.max(1, right-left);
    const idx=Math.max(0, Math.min(data.length-1, Math.round(pct*(data.length-1))));
    const item=data[idx];
    line.style.display='block';
    line.style.left=`${{clamped}}px`;
    tip.innerHTML=holdingHtml_{stock_id}(item);
    tip.style.display='block';
    const tipWidth=tip.offsetWidth || 210;
    const tipHeight=tip.offsetHeight || 112;
    let tx=x + 14;
    let ty=ev.clientY - rect.top + 14;
    if(tx + tipWidth > rect.width) tx=x - tipWidth - 14;
    if(ty + tipHeight > rect.height) ty=rect.height - tipHeight - 8;
    tip.style.left=`${{Math.max(6, tx)}}px`;
    tip.style.top=`${{Math.max(6, ty)}}px`;
    syncDailyFromHolding_{stock_id}(item.date);
  }});
  chart.addEventListener('mouseleave', ()=>{{
    tip.style.display='none';
    line.style.display='none';
    clearDailyHover_{stock_id}();
  }});
}}
initHoldingHover_{stock_id}();
function initChipFlowHover_{stock_id}(){{
  const chart=document.getElementById('{chip_flow_id}');
  const data=chipFlowData_{stock_id} || [];
  if(!chart || data.length < 2) return;
  const tip=chart.querySelector('.chart-tooltip');
  const line=chart.querySelector('.chart-crosshair');
  if(!tip || !line) return;
  const fmt=(v)=>Number.isFinite(Number(v)) ? Math.round(Number(v)).toLocaleString('zh-TW') : '-';
  const html=(x)=>`
    <div class="t-date">${{x.date || '-'}}</div>
    <div class="t-grid">
      <span>外資</span><span>${{fmt(x.foreign)}} 張</span>
      <span>投信</span><span>${{fmt(x.trust)}} 張</span>
      <span>自營商</span><span>${{fmt(x.dealer)}} 張</span>
      <span>三大合計</span><span>${{fmt(x.total)}} 張</span>
    </div>`;
  chart.addEventListener('mousemove', ev=>{{
    const rect=chart.getBoundingClientRect();
    const x=ev.clientX - rect.left;
    const left=rect.width * 54 / 900;
    const right=rect.width * (900 - 18) / 900;
    const clamped=Math.max(left, Math.min(right, x));
    const pct=(clamped-left) / Math.max(1, right-left);
    const idx=Math.max(0, Math.min(data.length-1, Math.round(pct*(data.length-1))));
    positionTooltip_{stock_id}(chart, line, tip, data.length, idx, html(data[idx]), x, ev.clientY - rect.top);
  }});
  chart.addEventListener('mouseleave', ()=>clearOverlay_{stock_id}(chart));
}}
function initMainForceHover_{stock_id}(){{
  const chart=document.getElementById('{main_force_id}');
  const data=mainForceData_{stock_id} || [];
  if(!chart || data.length < 2) return;
  const tip=chart.querySelector('.chart-tooltip');
  const line=chart.querySelector('.chart-crosshair');
  if(!tip || !line) return;
  const fmt=(v,d=0)=>Number.isFinite(Number(v)) ? Number(v).toLocaleString('zh-TW', {{maximumFractionDigits:d, minimumFractionDigits:d}}) : '-';
  const pct=(v)=>Number.isFinite(Number(v)) ? `${{Number(v).toFixed(2)}}%` : '-';
  const html=(x)=>`
    <div class="t-date">${{x.date || '-'}}</div>
    <div class="t-grid">
      <span>主力合計</span><span>${{fmt(x.total,0)}} 張</span>
      <span>收盤價</span><span>${{fmt(x.close,2)}}</span>
      <span>漲幅</span><span>${{pct(x.changePct)}}</span>
    </div>`;
  chart.addEventListener('mousemove', ev=>{{
    const rect=chart.getBoundingClientRect();
    const x=ev.clientX - rect.left;
    const left=rect.width * 54 / 900;
    const right=rect.width * (900 - 54) / 900;
    const clamped=Math.max(left, Math.min(right, x));
    const pct=(clamped-left) / Math.max(1, right-left);
    const idx=Math.max(0, Math.min(data.length-1, Math.round(pct*(data.length-1))));
    positionTooltip_{stock_id}(chart, line, tip, data.length, idx, html(data[idx]), x, ev.clientY - rect.top);
  }});
  chart.addEventListener('mouseleave', ()=>clearOverlay_{stock_id}(chart));
}}
initChipFlowHover_{stock_id}();
initMainForceHover_{stock_id}();
</script>"""
    telegram_card = build_telegram_info_card(stock_id, s_view, tech, chip, holding, decision, item, sell_signal)
    traffic_light = stock_traffic_light(stock_id, s_view, tech, decision, daily, chip_series)
    rr_warning = rr_warning_bar(decision)
    mda_abc_card = build_stock_mda_abc_block(stock_id, s_view, daily, tech, chip_series, holding)
    carybot_history_card = build_carybot_signal_history_panel(stock_id)
    daily_decision_payload = load_daily_decisions_payload()
    decision_badge_html = build_daily_decision_badge(stock_id, daily_decision_payload)
    report_date = str(s.get("report_date") or "").strip()
    price_date = str(s_view.get("price_date") or latest.get("date") or "").strip()
    if not is_blank(report_date):
        page_sub_html = f'<div class="page-sub">個股研究頁 · 報告日期 {esc(report_date)}</div>'
    elif not is_blank(price_date):
        page_sub_html = f'<div class="page-sub">個股查詢頁 · 最新收盤 {esc(price_date)}</div>'
    else:
        page_sub_html = '<div class="page-sub">個股查詢頁</div>'
    body = f"""
<div class="container">
  <div style="margin-bottom:8px"><a href="../selection.html#sfz-baskets" style="color:#6e7681;font-size:13px">&larr; 回雙籃儀表板</a></div>
  <div class="page-title">{esc(stock_id)} {esc(s.get('name',''))}</div>
  {page_sub_html}
  <div class="tag-row" data-stock-decision-badge="{esc(stock_id)}" style="margin:10px 0 12px">{decision_badge_html}</div>
  {rr_warning}
  <div class="detail-hero">
    <div class="card">
      {traffic_light}
      <div class="section-label">資訊卡</div>
      {telegram_card}
    </div>
    <div>
      <div class="card">
        <div class="section-label">操作規劃</div>
        {operation_card}
      </div>
      <div class="card" style="margin-top:12px">
        <div class="section-label">Quick 分析</div>
        {quick_html}
      </div>
    </div>
  </div>

  {carybot_history_card}

  {mda_abc_card}

  <div class="card">
    <div class="section-label">v44 技術 / 買點雷達</div>
    {build_tech_panel(tech)}
    <div class="strategy-note" style="margin-top:12px">
      行進籃以 SFZ 訊號與 MA20 續抱為主；盤整籃以 MABC 值得等待、量價轉強買點浮現為主。若距建議買點已明顯過高，視為不追價，等待 MA5/MA10/箱頂回測。
    </div>
  </div>

  <div class="card">
    <div class="section-label">日K / 週K / 月K</div>
    {lightweight_charts}
    <div id="{chart_id}" class="chart-box" style="display:none">
      <div class="chart-tabs">
        <button type="button" class="active" data-btn="daily" onclick="showChart_{stock_id}('daily')">日K</button>
        <button type="button" data-btn="weekly" onclick="showChart_{stock_id}('weekly')">週K</button>
        <button type="button" data-btn="monthly" onclick="showChart_{stock_id}('monthly')">月K</button>
      </div>
      <div class="chart-pane" data-pane="daily"><div class="hover-chart" data-mode="daily">{chart_svg(daily, '日K')}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>{indicator_chart_panel(daily, '日K', 'daily')}{chip_indicator_panel(daily_chip_indicators)}</div>
      <div class="chart-pane" data-pane="weekly" style="display:none"><div class="hover-chart" data-mode="weekly">{chart_svg(weekly, '週K')}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>{indicator_chart_panel(weekly, '週K', 'weekly')}{chip_indicator_panel(weekly_chip_indicators)}</div>
      <div class="chart-pane" data-pane="monthly" style="display:none"><div class="hover-chart" data-mode="monthly">{chart_svg(monthly, '月K')}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>{indicator_chart_panel(monthly, '月K', 'monthly')}{chip_indicator_panel(monthly_chip_indicators)}</div>
    </div>
  </div>

  <div class="card">
    <div class="section-label">10 日籌碼動向折線圖｜{esc(chip_dates)}</div>
    {build_chip_panel(chip, holding)}
    {chip_tv_panel}
    <div class="strategy-note" style="margin-top:12px">
      外資、投信、自營商以 FinMind 法人買賣超換算為張數；主力增減張數先以三大法人合計近似。柱狀圖向上為買超，向下為賣超。
    </div>
  </div>

  <div class="card">
    <div class="section-label">歷史訊號</div>
    <div style="overflow-x:auto">
      <table class="stock-table"><thead><tr><th>日期</th><th>籃別</th><th>買入區</th><th>收盤</th><th>原始分數</th></tr></thead><tbody>{event_rows}</tbody></table>
    </div>
    <div class="strategy-note" style="margin-top:12px">買入區以該次報告日期以前的 14 日高低價反推 Williams -65~-85，並用 MA20 作為濾網；下方「原始」保留當天報告寫入的買點。原始分數來自報告 Score；舊格式沒有 Score 時，才用 0-100 的排名遞減補值。</div>
  </div>
</div>
{chart_script}"""
    return html_page(f"{stock_id} {s.get('name','')}", "stocks", body, nav_prefix="../")


def build_stocks_index_page(reports: list[dict]) -> str:
    stock_map = build_stock_query_map(reports)
    ledger = build_signal_ledger(reports)
    items = []
    def signed_lots(value) -> str:
        try:
            if value is None:
                return "─"
            return f"{float(value):+,.0f}張"
        except Exception:
            return "─"

    for sid, s in sorted(stock_map.items()):
        rows = merge_report_close(read_price_history(sid), s)
        latest = rows[-1] if rows else {}
        daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily")
        tech = technical_snapshot(daily, s)
        decision = build_trade_decision(tech, s)
        chip = read_chip_summary(sid)
        holding = read_holding_summary(sid)
        h_latest = (holding.get("latest") or {}) if holding else {}
        chip_parts = []
        if chip:
            sum5 = chip.get("sum5", {})
            chip_parts.append(f"法人 {chip.get('date')}")
            chip_parts.append(f"外資5日 {signed_lots(sum5.get('foreign'))}")
            chip_parts.append(f"主力5日 {signed_lots(sum5.get('total'))}")
        else:
            chip_parts.append("法人尚無快取")
        if holding:
            chip_parts.append(f"股權 {holding.get('date')}")
            chip_parts.append(f"大戶 {fmt_num(h_latest.get('major'))}%")
            chip_parts.append(f"中實戶 {fmt_num(h_latest.get('middle_people'), 0)}人")
        else:
            chip_parts.append("股權尚無快取")
        chip_text = "｜".join(chip_parts)
        price = latest.get("close")
        date = latest.get("date", "")
        item = {
            "id": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "basket": "未入籃" if s.get("query_only") else basket_label(classify_basket(s)),
            "price": fmt_num(price),
            "price_date": date,
            "entry": decision.get("entry_text", "─"),
            "target": decision.get("target_text", "─"),
            "stop": decision.get("initial_stop_text", "─"),
            "support": decision.get("reference_support_text", "─"),
            "rr": decision.get("rr_text", "─"),
            "rr_num": decision.get("rr") if decision.get("rr") is not None else 0,
            "rr_class": decision.get("rr_class", ""),
            "score": s.get("score", "─"),
            "events": len(ledger.get(sid, {}).get("events", [])),
            "query_only": bool(s.get("query_only")),
            "chip_text": chip_text,
        }
        items.append(item)
    basket_count = sum(1 for x in items if not x["query_only"])
    query_count = sum(1 for x in items if x["query_only"])

    rows_html = ""
    for x in items:
        search_extra = "個股查詢 未入籃" if x["query_only"] else "個股查詢"
        search = f"{x['id']} {x['name']} {x['basket']} {x['chip_text']} {search_extra}".lower()
        tag_cls = "tag" if x["query_only"] else "tag-green"
        rows_html += f"""
<tr data-search="{esc(search)}">
  <td><a class="stock-link" href="stocks/{x['id']}.html">{x['id']} {esc(x['name'])}</a><div class="signal-dates">{esc(x['price_date'])}</div></td>
  <td><span class="{tag_cls}">{esc(x['basket'])}</span></td>
  <td class="price-main">{esc(x['price'])}</td>
  <td><div class="price-entry">進 {esc(x['entry'])}</div><div class="price-target">目 {esc(x['target'])}</div><div class="price-stop">初停 {esc(x['stop'])}</div><div class="price-support">支撐 {esc(x['support'])}</div><div class="price-rr {x['rr_class']}">R:R {esc(x['rr'])}</div></td>
  <td><div class="chip-line">{esc(x['chip_text'])}</div></td>
  <td>{esc(x['score'])}</td>
  <td>{x['events']} 次</td>
</tr>"""

    script = """
<script>
function filterStocks(){
  const q=document.getElementById('stockSearch').value.trim().toLowerCase();
  document.querySelectorAll('#stockRows tr').forEach(tr=>{
    tr.style.display=tr.dataset.search.includes(q)?'':'none';
  });
}
</script>"""
    body = f"""
<div class="container">
  <div class="page-title">個股查詢</div>
  <div class="page-sub">只收錄價格快取已更新到近期的個股；每列都顯示法人與股權籌碼狀態，尚無快取會直接標示。</div>
  <div class="card">
    <div class="section-label">Stock Query</div>
    <div class="grid grid-3" style="margin-bottom:14px">
      <div class="metric"><div class="metric-num">{len(items)}</div><div class="metric-label">可查詢個股</div></div>
      <div class="metric"><div class="metric-num" style="color:#3fb950">{basket_count}</div><div class="metric-label">目前在籃中</div></div>
      <div class="metric"><div class="metric-num" style="color:#d2a520">{query_count}</div><div class="metric-label">未入籃但有快取</div></div>
    </div>
    <input id="stockSearch" class="searchbar" placeholder="搜尋股票代號、名稱、未入籃、行進籃、盤整籃..." oninput="filterStocks()">
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>分類</th><th>FinMind收盤</th><th>買點/目標/初停/R:R</th><th>籌碼狀態</th><th>分數</th><th>訊號</th></tr></thead>
        <tbody id="stockRows">{rows_html}</tbody>
      </table>
    </div>
  </div>
</div>
{script}"""
    return html_page("個股查詢", "stocks", body)


def radar_bucket(gap) -> tuple[str, str, str]:
    if gap is None:
        return "資料不足", "tag", "買點價格無法解析"
    if -2 <= gap <= 3:
        return "接近買點", "tag-green", "可優先打開資訊卡確認量價"
    if 3 < gap <= 8:
        return "稍高等回測", "tag-yellow", "等 MA5/MA10/箱頂回測"
    if gap > 8:
        return "離買點過遠", "tag-red", "不追高，等整理"
    return "跌破買點", "tag", "等重新站回或出現轉強"


def radar_filter_key(gap) -> str:
    if gap is None:
        return "far"
    if -3 <= gap <= 3:
        return "near"
    if 3 < gap <= 8:
        return "pullback"
    if gap < -3:
        return "broken"
    return "far"


def build_buy_radar_page(reports: list[dict], section_only: bool = False) -> str:
    stock_map = find_latest_stock_map(reports)
    rows = []
    for sid, s in stock_map.items():
        daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily")
        tech = technical_snapshot(daily, s)
        decision = build_trade_decision(tech, s)
        gap = tech.get("entry_gap") if tech else None
        bucket, cls, note = radar_bucket(gap)
        rows.append({
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "basket": basket_label(classify_basket(s)),
            "bucket": bucket,
            "status_key": radar_filter_key(gap),
            "cls": cls,
            "note": note,
            "gap": gap,
            "close": tech.get("close") if tech else None,
            "entry": decision.get("entry_text", "─"),
            "target": decision.get("target_text", "─"),
            "stop": decision.get("initial_stop_text", "─"),
            "support": decision.get("reference_support_text", "─"),
            "rr": decision.get("rr_text", "─"),
            "rr_num": decision.get("rr") if decision.get("rr") is not None else 0,
            "rr_class": decision.get("rr_class", ""),
            "trend": tech.get("trend", "─") if tech else "─",
            "score": _to_float(s.get("score", "0")),
        })
    rows.sort(key=lambda x: (999 if x["gap"] is None else abs(x["gap"]), -x["score"]))
    near = sum(1 for x in rows if x["bucket"] == "接近買點")
    pullback = sum(1 for x in rows if x["bucket"] == "稍高等回測")
    extended = sum(1 for x in rows if x["bucket"] == "離買點過遠")

    baskets = sorted({x["basket"] for x in rows if x.get("basket")})
    sectors = sorted({x["sector"] for x in rows if x.get("sector")})
    basket_options = '<option value="all">全部</option>' + ''.join(f'<option value="{esc(x)}">{esc(x)}</option>' for x in baskets)
    sector_options = '<option value="all">全部</option>' + ''.join(f'<option value="{esc(x)}">{esc(x)}</option>' for x in sectors)
    filter_bar = f"""
    <div class="radar-filter-bar radar-filter sticky-top">
      <fieldset>
        <legend>狀態</legend>
        <label class="filter-chip"><input type="checkbox" data-filter="status" data-radar-status value="near" checked> 接近買點</label>
        <label class="filter-chip"><input type="checkbox" data-filter="status" data-radar-status value="pullback" checked> 稍高等回測</label>
        <label class="filter-chip"><input type="checkbox" data-filter="status" data-radar-status value="broken"> 跌破買點</label>
        <label class="filter-chip"><input type="checkbox" data-filter="status" data-radar-status value="far"> 離買點過遠</label>
      </fieldset>
      <label class="filter-chip">籃別 <select data-filter="basket" data-radar-basket>{basket_options}</select></label>
      <label class="filter-chip">最低 R:R <input type="number" data-filter="min-rr" data-radar-min-rr value="2.0" step="0.1" min="0"></label>
      <label class="filter-chip">產業 <select data-filter="industry" data-radar-sector>{sector_options}</select></label>
      <button type="button" id="reset-filter" class="filter-reset" data-radar-reset>重置</button>
      <span class="filter-count" data-radar-count></span>
    </div>"""
    table = ""
    for x in rows:
        gap_txt = "─" if x["gap"] is None else f'{x["gap"]:+.1f}%'
        table += f"""
<tr data-radar-row data-status="{x['status_key']}" data-basket="{esc(x['basket'])}" data-sector="{esc(x['sector'])}" data-rr="{x['rr_num']}">
  <td><a class="stock-link" href="stocks/{x['sid']}.html">{x['sid']} {esc(x['name'])}</a><div class="signal-dates">{esc(x['basket'])} ｜ {esc(x['trend'])}</div></td>
  <td><span class="tag {x['cls']}">{esc(x['bucket'])}</span><div class="signal-dates">{esc(x['note'])}</div></td>
  <td class="price-main">{fmt_num(x['close'])}</td>
  <td>{gap_txt}</td>
  <td><div class="price-entry">進 {esc(x['entry'])}</div><div class="price-target">目 {esc(x['target'])}</div><div class="price-stop">初停 {esc(x['stop'])}</div><div class="price-support">支撐 {esc(x['support'])}</div><div class="price-rr {x['rr_class']}">R:R {esc(x['rr'])}</div></td>
</tr>"""

    body = f"""
<div class="container">
  <div class="page-title">買點雷達</div>
  <div class="page-sub">買點雷達只看已經在名單裡的股票，幫你找現在離買入區近不近；重點是能不能掛單，不是重新選股。</div>
  <div class="card">
    <div class="section-label">Buy Radar</div>
    <div class="grid grid-3">
      <div class="metric"><div class="metric-num" style="color:#3fb950">{near}</div><div class="metric-label">接近買點：優先確認</div></div>
      <div class="metric"><div class="metric-num" style="color:#d2a520">{pullback}</div><div class="metric-label">稍高：等回測</div></div>
      <div class="metric"><div class="metric-num" style="color:#f85149">{extended}</div><div class="metric-label">過遠：不追高</div></div>
    </div>
    <div class="strategy-note" style="margin-top:14px">這頁以 Williams -65~-85 反推價格帶，並搭配 MA20 濾網建立網站版雷達。後續可再把 MABC A/B/C、量價共振分數接進同一張表。</div>
  </div>
  <div class="card" data-radar>
    <div class="section-label">候選排序</div>
    {filter_bar}
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>狀態</th><th>收盤</th><th>距買點</th><th>買點/目標/初停/R:R</th></tr></thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  </div>
</div>"""
    if section_only:
        return body
    return html_page("買點雷達", "radar", body)


def read_carybot_marker_features() -> list[dict]:
    path = V44_BACKTEST_OUTPUT_DIR / "carybot_buy_markers_v42_features.csv"
    if not path.exists():
        return []
    rows = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                rows.append(row)
    except Exception:
        return []
    return rows


def _first_existing_carybot_path(names: list[str]) -> Path | None:
    for name in names:
        path = V44_BACKTEST_OUTPUT_DIR / name
        if path.exists():
            return path
    return None


def _read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def read_carybot_signal_master() -> list[dict]:
    path = _first_existing_carybot_path([
        "carybot_signal_master_v50.csv",
        "carybot_signal_master_v44.csv",
    ])
    if path:
        return _read_csv_rows(path)
    return read_carybot_marker_features()


def read_carybot_signal_summary() -> list[dict]:
    path = _first_existing_carybot_path([
        "carybot_signal_master_v50_summary.csv",
        "carybot_signal_master_v44_summary.csv",
    ])
    return _read_csv_rows(path) if path else []


def read_carybot_phase_summary() -> list[dict]:
    return _read_csv_rows(V44_BACKTEST_OUTPUT_DIR / "carybot_signal_master_v50_phase_summary.csv")


def read_carybot_transition_summary() -> list[dict]:
    return _read_csv_rows(V44_BACKTEST_OUTPUT_DIR / "carybot_signal_master_v50_transition_summary.csv")


def read_carybot_daily_ai_buy_v51() -> list[dict]:
    return _read_csv_rows(V44_BACKTEST_OUTPUT_DIR / "carybot_daily_ai_buy_v51.csv")


def read_carybot_daily_ai_buy_v51_summary() -> dict:
    rows = _read_csv_rows(V44_BACKTEST_OUTPUT_DIR / "carybot_daily_ai_buy_v51_summary.csv")
    return rows[0] if rows else {}


def carybot_master_source_path() -> Path | None:
    return _first_existing_carybot_path([
        "carybot_signal_master_v50.csv",
        "carybot_signal_master_v44.csv",
        "carybot_buy_markers_v42_features.csv",
    ])


def read_carybot_indicator_confidence() -> list[dict]:
    return _read_csv_rows(V44_BACKTEST_OUTPUT_DIR / "carybot_indicator_confidence_v44.csv")


def _median(values: list[float]) -> float | None:
    vals = sorted(v for v in values if v is not None and not math.isnan(v))
    if not vals:
        return None
    mid = len(vals) // 2
    if len(vals) % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def build_carybot_validation_page(reports: list[dict], section_only: bool = False) -> str:
    master_rows = read_carybot_signal_master()
    rows = master_rows or read_carybot_marker_features()
    summary_rows = read_carybot_signal_summary()
    phase_rows = read_carybot_phase_summary()
    transition_rows = read_carybot_transition_summary()
    confidence_rows = read_carybot_indicator_confidence()
    daily_ai_buy_rows = read_carybot_daily_ai_buy_v51()
    daily_ai_buy_summary = read_carybot_daily_ai_buy_v51_summary()
    stock_map = find_latest_stock_map(reports)
    source_path = carybot_master_source_path()
    source_name = source_path.name if source_path else "尚未找到 CaryBot 輸出"
    using_v50 = bool(source_path and source_path.name == "carybot_signal_master_v50.csv")

    def signal_type(row: dict) -> str:
        return str(row.get("signal_type") or row.get("marker_type") or "").strip()

    def marker_side(row: dict) -> str:
        side = str(row.get("marker_side") or "").strip()
        if side:
            return side
        return "buy" if signal_type(row) in {"PreBuy", "AI_Buy"} else "sell" if signal_type(row) in {"PreSell", "AI_Sell"} else ""

    prebuy = [r for r in rows if signal_type(r) == "PreBuy"]
    ai_buy = [r for r in rows if signal_type(r) == "AI_Buy"]
    presell = [r for r in rows if signal_type(r) == "PreSell"]
    ai_sell = [r for r in rows if signal_type(r) == "AI_Sell"]
    buy_rows = [r for r in rows if marker_side(r) == "buy"]
    sell_rows = [r for r in rows if marker_side(r) == "sell"]

    def metric_card(label: str, value: str, note: str, color: str) -> str:
        return f"""<div class="metric">
  <div class="metric-num" style="color:{color}">{esc(value)}</div>
  <div class="metric-label">{esc(label)}</div>
  <div class="chip-line">{esc(note)}</div>
</div>"""

    def pct_text(value) -> str:
        v = _to_float(value, None)
        if v is None or math.isnan(v):
            return "─"
        return f"{v * 100:.1f}%"

    def plain_num(value, digits: int = 2) -> str:
        v = _to_float(value, None)
        if v is None or math.isnan(v):
            return "─"
        return f"{v:.{digits}f}"

    def daily_ai_buy_v51_section() -> str:
        if not daily_ai_buy_rows:
            return """
  <div class="card">
    <div class="section-label">v51 全市場收盤後 AI_Buy 雷達</div>
    <div class="strategy-note">尚未找到 <code>carybot_daily_ai_buy_v51.csv</code>；請先執行 v51 收盤後掃描腳本。</div>
  </div>"""

        def truthy(value) -> bool:
            return str(value).strip().lower() in {"1", "true", "yes", "y"}

        def stock_link(row: dict) -> str:
            sid = str(row.get("stock", "")).strip()
            name = str(row.get("stock_name", "")).strip()
            if not name and sid in stock_map:
                name = stock_map.get(sid, {}).get("name", "")
            label = f"{sid} {name}".strip()
            return f'<a class="stock-link" href="stocks/{esc(sid)}.html">{esc(label)}</a>' if sid else "?"

        rows_html = ""
        for r in daily_ai_buy_rows[:20]:
            score = _to_float(r.get("quality_score"), None)
            tag_cls = "tag-green" if truthy(r.get("candidate_pass")) else "tag-blue"
            outside = "清單外" if truthy(r.get("outside_latest_site_report")) else "站內清單"
            rows_html += f"""
<tr>
  <td>{esc(r.get("recommendation_rank", ""))}</td>
  <td>{stock_link(r)}</td>
  <td><span class="tag {tag_cls}">{esc(r.get("quality_grade", ""))}</span><div class="signal-dates">{fmt_num(score, 1)}</div></td>
  <td>{esc(r.get("carybot_phase", ""))}<div class="signal-dates">{esc(r.get("transition_5d", ""))}</div></td>
  <td class="price-main">{fmt_num(_to_float(r.get("Close"), None))}</td>
  <td>{fmt_num(_to_float(r.get("entry_watch_price"), None))}</td>
  <td>{fmt_num(_to_float(r.get("stop_price"), None))}</td>
  <td>{fmt_num(_to_float(r.get("target_price"), None))}</td>
  <td>{pct_text(r.get("risk_pct"))}</td>
  <td>{esc(outside)}</td>
  <td>{esc(r.get("reason", ""))}</td>
</tr>"""

        top = daily_ai_buy_rows[0]
        top_label = f"{top.get('stock', '')} {top.get('stock_name', '')}".strip()
        source_note = (
            "這是 AI_Buy-like 收盤後雷達：用 v50 已驗證的顏色狀態與 5D 轉折做每日排序，"
            "不是宣稱 CaryBot 原始 AI_Buy 公式已完全破解。"
        )
        return f"""
  <div class="card">
    <div class="section-label">v51 全市場收盤後 AI_Buy 雷達</div>
    <div class="strategy-note">{esc(source_note)}</div>
    <div class="grid grid-4" style="margin-top:12px">
      {metric_card("今日主推", top_label, f"分數 {fmt_num(_to_float(top.get('quality_score'), None), 1)} / {esc(top.get('carybot_phase', ''))}", "#3fb950")}
      {metric_card("資料日", str(daily_ai_buy_summary.get("global_data_date", "")), f"掃描 {daily_ai_buy_summary.get('price_cache_stock_n', '')} 檔快取", "#58a6ff")}
      {metric_card("通過候選", str(daily_ai_buy_summary.get("candidate_pass_n", "")), f"可評分 {daily_ai_buy_summary.get('scored_stock_n', '')} 檔", "#d2a520")}
      {metric_card("清單外命中", str(daily_ai_buy_summary.get("outside_latest_site_report_n", "")), f"發布 {daily_ai_buy_summary.get('published_stock_n', '')} 檔", "#a371f7")}
    </div>
    <div style="overflow-x:auto;margin-top:12px">
      <table class="stock-table">
        <thead><tr><th>Rank</th><th>股票</th><th>等級</th><th>狀態 / 5D</th><th>收盤</th><th>觀察價</th><th>停損</th><th>目標</th><th>風險</th><th>來源</th><th>理由</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>"""

    def summary_for_type(kind: str) -> dict:
        for r in summary_rows:
            if str(r.get("signal_type") or r.get("marker_type") or "").strip() == kind:
                return r
        return {}

    def summary_metric(kind: str, key: str, fallback: str = "─") -> str:
        r = summary_for_type(kind)
        value = r.get(key)
        if value in (None, ""):
            return fallback
        return str(value)

    def summary_table() -> str:
        if not summary_rows:
            return '<tr><td colspan="11">尚未找到 CaryBot 勝敗統計；請先產出 carybot_signal_master_v50_summary.csv。</td></tr>'
        html = ""
        for r in summary_rows:
            kind = r.get("signal_type") or r.get("marker_type", "")
            side = r.get("marker_side") or ("buy" if kind in {"PreBuy", "AI_Buy"} else "sell" if kind in {"PreSell", "AI_Sell"} else "")
            valid = r.get("valid_n") or r.get("labeled_n") or ""
            success_rate = r.get("success_rate_20d") if r.get("success_rate_20d") not in (None, "") else r.get("win_rate")
            html += f"""
<tr>
  <td>{esc(side)}</td>
  <td>{esc(kind)}</td>
  <td>{esc(r.get("n", ""))}</td>
  <td>{esc(valid)}</td>
  <td>{esc(r.get("success_n", ""))}</td>
  <td>{pct_text(success_rate)}</td>
  <td>{pct_text(r.get("sell_risk_success_60d"))}</td>
  <td>{pct_text(r.get("sell_drawdown_ge8_60d_rate"))}</td>
  <td>{pct_text(r.get("future_return_20d_median") or r.get("future_return_20d_avg"))}</td>
  <td>{pct_text(r.get("max_gain_60d_median") or r.get("max_gain_60d_avg"))}</td>
  <td>{pct_text(r.get("max_drawdown_60d_median") or r.get("max_drawdown_60d_avg"))}</td>
</tr>"""
        return html

    def confidence_table() -> str:
        if not confidence_rows:
            return '<tr><td colspan="2">尚未找到指標可信度表。</td></tr>'
        html = ""
        for r in confidence_rows:
            html += f'<tr><td>{esc(r.get("indicator", ""))}</td><td>{esc(r.get("status", ""))}</td></tr>'
        return html

    def indicator_median_card(title: str, data: list[dict]) -> str:
        cols = ["QZ", "QTYR", "VAM", "VAM5", "VAM20", "VAM60", "VPA480"]
        cells = ""
        for col in cols:
            med = _median([_to_float(r.get(col), math.nan) for r in data])
            val = "─" if med is None else f"{med:.2f}"
            cells += f'<div class="info-cell"><div class="k">{esc(col)}</div><div class="v">{val}</div></div>'
        return f"""
<div class="card">
  <div class="section-label">{esc(title)}</div>
  <div class="grid grid-3" style="margin-top:10px">{cells}</div>
</div>"""

    def compact_summary_cards() -> str:
        ai_buy_row = summary_for_type("AI_Buy")
        pre_buy_row = summary_for_type("PreBuy")
        ai_sell_row = summary_for_type("AI_Sell")
        pre_sell_row = summary_for_type("PreSell")
        return f"""
<div class="grid grid-4">
  {metric_card("AI_Buy 20日勝率", pct_text(ai_buy_row.get("success_rate_20d")), f"{summary_metric('AI_Buy', 'success_n')} / {summary_metric('AI_Buy', 'valid_n')}", "#3fb950")}
  {metric_card("PreBuy 20日勝率", pct_text(pre_buy_row.get("success_rate_20d")), f"{summary_metric('PreBuy', 'success_n')} / {summary_metric('PreBuy', 'valid_n')}", "#58a6ff")}
  {metric_card("AI_Sell 60日風險", pct_text(ai_sell_row.get("sell_risk_success_60d")), f"{summary_metric('AI_Sell', 'sell_risk_success_n')} / {summary_metric('AI_Sell', 'sell_risk_valid_n')}", "#f85149")}
  {metric_card("PreSell 60日風險", pct_text(pre_sell_row.get("sell_risk_success_60d")), f"{summary_metric('PreSell', 'sell_risk_success_n')} / {summary_metric('PreSell', 'sell_risk_valid_n')}", "#d2a520")}
</div>"""

    def phase_table() -> str:
        if not phase_rows:
            return '<tr><td colspan="9">尚未找到 v50 顏色狀態統計。</td></tr>'
        ordered = sorted(phase_rows, key=lambda r: (r.get("marker_side", ""), r.get("signal_type", ""), -int(_to_float(r.get("n"), 0))))
        html = ""
        for r in ordered[:36]:
            html += f"""
<tr>
  <td>{esc(r.get("marker_side", ""))}</td>
  <td>{esc(r.get("signal_type", ""))}</td>
  <td>{esc(r.get("carybot_phase", ""))}</td>
  <td>{esc(r.get("n", ""))}</td>
  <td>{esc(r.get("valid_n", ""))}</td>
  <td>{pct_text(r.get("success_rate_20d"))}</td>
  <td>{pct_text(r.get("sell_risk_success_60d"))}</td>
  <td>{pct_text(r.get("future_return_20d_median"))}</td>
  <td>{pct_text(r.get("max_drawdown_60d_median"))}</td>
</tr>"""
        return html

    def transition_table() -> str:
        if not transition_rows:
            return '<tr><td colspan="9">尚未找到 v50 五日顏色反轉統計。</td></tr>'
        ordered = sorted(transition_rows, key=lambda r: (r.get("marker_side", ""), r.get("signal_type", ""), -int(_to_float(r.get("n"), 0))))
        html = ""
        for r in ordered[:42]:
            html += f"""
<tr>
  <td>{esc(r.get("marker_side", ""))}</td>
  <td>{esc(r.get("signal_type", ""))}</td>
  <td>{esc(r.get("transition_5d", ""))}</td>
  <td>{esc(r.get("n", ""))}</td>
  <td>{esc(r.get("valid_n", ""))}</td>
  <td>{pct_text(r.get("success_rate_20d"))}</td>
  <td>{pct_text(r.get("sell_risk_success_60d"))}</td>
  <td>{pct_text(r.get("future_return_20d_median"))}</td>
  <td>{pct_text(r.get("max_drawdown_60d_median"))}</td>
</tr>"""
        return html

    sample_rows = []
    for r in rows:
        stock_id = str(r.get("stock", "")).strip()
        s = stock_map.get(stock_id, {})
        sample_rows.append({
            "stock": stock_id,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "marker_side": marker_side(r),
            "marker_type": signal_type(r),
            "marker_date": r.get("date") or r.get("marker_date", ""),
            "close": _to_float(r.get("Price") or r.get("Close"), None),
            "qz": _to_float(r.get("QZ"), None),
            "qtyr": _to_float(r.get("QTYR"), None),
            "vam": _to_float(r.get("VAM"), None),
            "vam20": _to_float(r.get("VAM20"), None),
            "vam60": _to_float(r.get("VAM60"), None),
            "vpa480": _to_float(r.get("VPA480"), None),
            "phase": r.get("carybot_phase", ""),
            "transition": r.get("transition_5d", ""),
            "future20": _to_float(r.get("future_return_20d"), None),
            "mfe60": _to_float(r.get("max_gain_60d"), None),
            "mae60": _to_float(r.get("max_drawdown_60d"), None),
            "label": r.get("win_loss_label", ""),
        })
    sample_rows.sort(key=lambda x: (0 if x["marker_side"] == "buy" else 1, 0 if x["marker_type"] in {"AI_Buy", "AI_Sell"} else 1, x["stock"], x["marker_date"]))

    table = ""
    for x in sample_rows[:100]:
        tag_cls = "tag-green" if x["marker_type"] == "AI_Buy" else "tag-blue" if x["marker_type"] == "PreBuy" else "tag-red" if x["marker_type"] == "AI_Sell" else "tag-yellow"
        stock_label = f'{x["stock"]} {x["name"]}'.strip()
        stock_html = f'<a class="stock-link" href="stocks/{x["stock"]}.html">{esc(stock_label)}</a>' if x["stock"] else "─"
        table += f"""
<tr>
  <td>{stock_html}</td>
  <td><span class="tag {tag_cls}">{esc(x["marker_type"])}</span><div class="signal-dates">{esc(x["marker_date"])}</div></td>
  <td class="price-main">{fmt_num(x["close"])}</td>
  <td>{fmt_num(x["qz"])}</td>
  <td>{fmt_num(x["qtyr"])}</td>
  <td>{fmt_num(x["vam"])}</td>
  <td>{fmt_num(x["vam20"])}</td>
  <td>{fmt_num(x["vam60"])}</td>
  <td>{esc(x["phase"])}</td>
  <td>{esc(x["transition"])}</td>
  <td>{pct_text(x["future20"])}</td>
  <td>{pct_text(x["mfe60"])}</td>
  <td>{pct_text(x["mae60"])}</td>
  <td>{esc(x["label"])}</td>
</tr>"""

    if not table:
        table = '<tr><td colspan="14">尚未找到 CaryBot v50 買賣點資料；請先產出 carybot_signal_master_v50.csv。</td></tr>'

    data_note = f"資料來源：{source_path}" if source_path else "資料來源：尚未找到 CaryBot 輸出"
    stale_note = "" if using_v50 else '<div class="strategy-note" style="margin-top:10px;color:#d2a520">目前使用舊版 CaryBot 資料 fallback；產出 v50 後會顯示買賣點、顏色狀態與 5D transition。</div>'
    body = f"""
<div class="container">
  <div class="page-title">CaryBot 驗證</div>
  <div class="page-sub">這頁把 CaryBot 標點當成 timing / confirmation layer，不取代 SFZ 趨勢選股與 M大觀察池；重點是分辨健康買點、過熱追價與賣點風險警示。</div>

  <details class="placeholder-block" data-source="data/carybot_signal_master_v50.csv">
    <summary>CaryBot 訊號驗證層（v50 / v51 開發中，預計接入後展開）</summary>
    <div class="placeholder-body">
  <div class="card">
    <div class="section-label">目前定位</div>
    <div class="grid grid-4">
      {metric_card("PreBuy 樣本", str(len(prebuy)), "偏觀察 / 等確認", "#58a6ff")}
      {metric_card("AI_Buy 樣本", str(len(ai_buy)), "偏正式買點標記", "#3fb950")}
      {metric_card("PreSell 樣本", str(len(presell)), "偏提前風險提醒", "#d2a520")}
      {metric_card("AI_Sell 樣本", str(len(ai_sell)), "偏正式賣點警示", "#f85149")}
    </div>
    <div class="strategy-note" style="margin-top:14px">CaryBot 現階段最適合接在買點雷達後面：SFZ 負責趨勢股、M大負責未發動觀察股，CaryBot 用來檢查 VPA / VAM / QTYR 與顏色狀態是否支持進場或降風險。</div>
{stale_note}
  </div>

  {daily_ai_buy_v51_section().strip()}

  <div class="card">
    <div class="section-label">v50 買賣點勝敗速覽</div>
    {compact_summary_cards()}
  </div>

  <div class="card">
    <div class="section-label">使用方式</div>
    <div class="grid grid-2" style="margin-top:10px">
      <div class="info-cell"><div class="k">SFZ 行進籃</div><div class="v">等 timing</div><div class="chip-line">SFZ 已選出趨勢後，用 CaryBot 藍點與 ATRB/VAM 強勢確認，避免太早或太晚進。</div></div>
      <div class="info-cell"><div class="k">風險警示</div><div class="v">看賣點</div><div class="chip-line">PreSell / AI_Sell 不直接等於做空，先視為過熱後的減碼、停利或重新檢查持股訊號。</div></div>
    </div>
  </div>

  {indicator_median_card("買點指標中位數", buy_rows)}
  {indicator_median_card("賣點指標中位數", sell_rows)}

  <div class="card">
    <div class="section-label">買點參考與賣點風險統計</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="stock-table">
        <thead><tr><th>方向</th><th>訊號</th><th>樣本</th><th>可評估</th><th>成功</th><th>20日成功率</th><th>60日風險釋放</th><th>8%回撤率</th><th>20日中位</th><th>MFE60中位</th><th>MAE60中位</th></tr></thead>
        <tbody>{summary_table()}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <div class="section-label">顏色狀態勝敗</div>
    <div class="strategy-note">健康回拉與觀察轉強通常比過熱追價更適合當買點參考；賣點則看是否真的引出後續風險釋放。</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="stock-table">
        <thead><tr><th>方向</th><th>訊號</th><th>顏色狀態</th><th>樣本</th><th>可評估</th><th>20日成功率</th><th>60日風險釋放</th><th>20日中位</th><th>MAE60中位</th></tr></thead>
        <tbody>{phase_table()}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <div class="section-label">5D 顏色反轉追蹤</div>
    <div class="strategy-note">這裡看的是 5 個交易日前狀態到標點當日狀態的變化，用來分辨回拉轉穩、過熱追價、以及賣點是否從強勢/過熱後釋放。</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="stock-table">
        <thead><tr><th>方向</th><th>訊號</th><th>5D transition</th><th>樣本</th><th>可評估</th><th>20日成功率</th><th>60日風險釋放</th><th>20日中位</th><th>MAE60中位</th></tr></thead>
        <tbody>{transition_table()}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <div class="section-label">指標可信度</div>
    <div class="strategy-note">ATRB / QTYR / VPA 是目前較穩定的核心；VAM5 / VAM20 / VAM60 仍以 proxy/research 標示，不宣稱完全破解。</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="stock-table">
        <thead><tr><th>指標</th><th>狀態</th></tr></thead>
        <tbody>{confidence_table()}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <div class="section-label">買賣點樣本與指標</div>
    <div class="strategy-note">{esc(data_note)}</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>標記</th><th>收盤</th><th>QZ</th><th>QTYR</th><th>VAM</th><th>VAM20</th><th>VAM60</th><th>顏色狀態</th><th>5D變化</th><th>20日</th><th>MFE60</th><th>MAE60</th><th>標籤</th></tr></thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <div class="section-label">下一步：紅色 CaryBot 箭頭</div>
    <div class="strategy-note">紅色 CaryBot 箭頭這版仍未納入，因為它與紅 K 棒太像，需要獨立形狀分類器；目前 v50 先以 PreBuy / AI_Buy / PreSell / AI_Sell 建立乾淨母表。</div>
  </div>
    </div>
  </details>
</div>"""
    if section_only:
        return body
    return html_page("CaryBot驗證", "carybot", body)


def parse_range_values(text: str) -> tuple[float | None, float | None]:
    nums = [_to_float(x, None) for x in re.findall(r"\d+(?:\.\d+)?", str(text or ""))]
    vals = [x for x in nums if x is not None]
    if not vals:
        return None, None
    return min(vals), max(vals)


def _price_zone_text(low, high) -> str:
    if low is None or high is None:
        return "資料不足"
    return f"{fmt_num(low)} ~ {fmt_num(high)}"


def williams_price_zone(rows: list[dict], low_wr: float, high_wr: float, lookback: int = 14) -> tuple[float | None, float | None]:
    if len(rows) < lookback:
        return None, None
    recent = rows[-lookback:]
    hi = max(float(r.get("high", 0) or 0) for r in recent)
    lo = min(float(r.get("low", 0) or 0) for r in recent)
    if hi <= lo:
        return None, None
    prices = [hi + (wr / 100.0) * (hi - lo) for wr in [low_wr, high_wr]]
    return min(prices), max(prices)


def formal_williams_entry_zone(rows: list[dict], ma20: float | None = None) -> dict:
    low, high = williams_price_zone(rows, -85, -65, 14)
    close = rows[-1].get("close") if rows else None
    filter_ok = bool(close and ma20 and close >= ma20)
    return {
        "low": low,
        "high": high,
        "mid": ((low + high) / 2) if low is not None and high is not None else None,
        "filter_ok": filter_ok if ma20 is not None else None,
        "basis": "Williams -65~-85 / 14日高低區間 + MA20濾網",
    }


def kd_rsv_price_zone(rows: list[dict], low_rsv: float, high_rsv: float, lookback: int = 9) -> tuple[float | None, float | None]:
    if len(rows) < lookback:
        return None, None
    recent = rows[-lookback:]
    hi = max(float(r.get("high", 0) or 0) for r in recent)
    lo = min(float(r.get("low", 0) or 0) for r in recent)
    if hi <= lo:
        return None, None
    prices = [lo + (rsv / 100.0) * (hi - lo) for rsv in [low_rsv, high_rsv]]
    return min(prices), max(prices)


def indicator_entry_zone(method: str, past_rows: list[dict], decision: dict) -> dict:
    if method == "original":
        low, high = parse_range_values(decision.get("entry_range"))
        return {"low": low, "high": high, "label": decision.get("entry_range", "資料不足")}
    if method in {"wr_65_85", "wr_65_85_ma20", "wr_65_85_no_vol_down"}:
        low, high = williams_price_zone(past_rows, -85, -65, 14)
        return {"low": low, "high": high, "label": _price_zone_text(low, high)}
    if method == "wr_60_80":
        low, high = williams_price_zone(past_rows, -80, -60, 14)
        return {"low": low, "high": high, "label": _price_zone_text(low, high)}
    if method == "wr_80_90":
        low, high = williams_price_zone(past_rows, -90, -80, 14)
        return {"low": low, "high": high, "label": _price_zone_text(low, high)}
    if method in {"wr_70_85", "wr_70_85_ma20", "wr_70_85_no_vol_down", "wr_70_85_b1"}:
        low, high = williams_price_zone(past_rows, -85, -70, 14)
        return {"low": low, "high": high, "label": _price_zone_text(low, high)}
    if method == "kd_20_35":
        low, high = kd_rsv_price_zone(past_rows, 20, 35, 9)
        return {"low": low, "high": high, "label": _price_zone_text(low, high)}
    if method == "wr_kd_overlap":
        wr_low, wr_high = williams_price_zone(past_rows, -90, -80, 14)
        kd_low, kd_high = kd_rsv_price_zone(past_rows, 20, 35, 9)
        if None in {wr_low, wr_high, kd_low, kd_high}:
            return {"low": None, "high": None, "label": "資料不足"}
        low, high = max(wr_low, kd_low), min(wr_high, kd_high)
        if low > high:
            return {"low": None, "high": None, "label": "無重疊區"}
        return {"low": low, "high": high, "label": _price_zone_text(low, high)}
    return {"low": None, "high": None, "label": "資料不足"}


def variant_initial_stop(entry_price: float, tech: dict, decision: dict) -> float | None:
    ma10 = tech.get("ma10") if tech else None
    ma20 = tech.get("ma20") if tech else None
    large_low = (tech.get("large_volume_event") or {}).get("low") if tech else None
    report_stop = decision.get("reference_support") or decision.get("initial_stop")
    candidates = []
    for value in [report_stop, large_low, ma20, ma10]:
        if not value or value >= entry_price:
            continue
        risk_pct = (1 - value / entry_price) * 100
        if 3 <= risk_pct <= 12:
            candidates.append(value)
    if candidates:
        return max(candidates)
    return entry_price * 0.94 if entry_price else None


def trade_path_metrics(rows: list[dict], entry_date: str, exit_date: str, entry_price: float | None) -> dict:
    if not rows or not entry_price:
        return {"max_return": None, "max_drawdown": None}
    path = [r for r in rows if entry_date <= str(r.get("date", "")) <= exit_date]
    if not path:
        return {"max_return": None, "max_drawdown": None}
    max_high = max((float(r.get("high") or entry_price) for r in path), default=entry_price)
    min_low = min((float(r.get("low") or entry_price) for r in path), default=entry_price)
    return {
        "max_return": (max_high / entry_price - 1) * 100,
        "max_drawdown": (min_low / entry_price - 1) * 100,
    }


ENTRY_VARIANTS = [
    ("original", "正式買入區", "Williams -65~-85 + MA20濾網"),
    ("wr_80_90", "Williams -80~-90", "14日 Williams 低接區"),
    ("wr_70_85", "Williams -70~-85", "較寬鬆 Williams 低接區"),
    ("wr_65_85", "Williams -65~-85", "放寬上緣，提高成交機會"),
    ("wr_60_80", "Williams -60~-80", "更寬鬆 Williams 觀察區"),
    ("wr_70_85_ma20", "WR -70~-85 + MA20", "低接區且訊號日不跌破 MA20"),
    ("wr_70_85_no_vol_down", "WR -70~-85 + 非放量下跌", "低接區且排除放量下跌"),
    ("wr_65_85_ma20", "WR -65~-85 + MA20", "放寬低接區且訊號日不跌破 MA20"),
    ("wr_65_85_no_vol_down", "WR -65~-85 + 非放量下跌", "放寬低接區且排除放量下跌"),
    ("wr_70_85_b1", "WR -70~-85 + B1未離開", "低接區且籌碼未明顯離開"),
    ("kd_20_35", "KD RSV 20~35", "9日 KD 低檔價格區"),
    ("wr_kd_overlap", "WR/KD 重疊", "Williams 與 KD 低接區交集"),
]


def backtest_entry_variant(report_date: str, s: dict, method: str, max_wait_bars: int = 20) -> dict | None:
    sid = s.get("id", "")
    if not sid:
        return None
    s = enrich_stock_fields(dict(s))
    s["report_date"] = report_date
    all_rows = sorted(merge_report_close(read_price_history(sid), s), key=lambda r: r.get("date", ""))
    past_rows = [r for r in all_rows if r.get("date", "") <= report_date]
    future_rows = [r for r in all_rows if r.get("date", "") > report_date]
    if not past_rows or not future_rows:
        return None

    tech = technical_snapshot(past_rows, s)
    decision = build_trade_decision(tech, s)
    close = tech.get("close")
    ma20 = tech.get("ma20")
    volume_price = tech.get("volume_price")
    if (method == "original" or method.endswith("_ma20")) and close and ma20 and close < ma20:
        return {
            "method": method,
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "report_date": report_date,
            "status": "濾網排除",
            "entry_range": "MA20濾網排除",
            "entry": None,
            "ret": None,
            "wait_days": None,
            "entry_vs_signal_ret": None,
            "exit_reason": "訊號日收盤跌破MA20",
        }
    if method.endswith("_no_vol_down") and volume_price == "放量下跌":
        return {
            "method": method,
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "report_date": report_date,
            "status": "濾網排除",
            "entry_range": "量價濾網排除",
            "entry": None,
            "ret": None,
            "wait_days": None,
            "entry_vs_signal_ret": None,
            "exit_reason": "訊號日放量下跌",
        }
    if method.endswith("_b1"):
        force_status = b1_force_status(s, read_chip_series(sid), read_holding_summary(sid))
        if force_status == "B1主力已離開":
            return {
                "method": method,
                "sid": sid,
                "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
                "report_date": report_date,
                "status": "濾網排除",
                "entry_range": "B1濾網排除",
                "entry": None,
                "ret": None,
                "wait_days": None,
                "entry_vs_signal_ret": None,
                "exit_reason": "B1主力已離開",
            }
    zone = indicator_entry_zone(method, past_rows, decision)
    entry_low, entry_high = zone.get("low"), zone.get("high")
    target = decision.get("target")
    if entry_low is None or entry_high is None:
        return {
            "method": method,
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "report_date": report_date,
            "status": "無買區",
            "entry_range": zone.get("label", "資料不足"),
            "entry": None,
            "ret": None,
            "wait_days": None,
            "entry_vs_signal_ret": None,
            "exit_reason": "買入區無法計算",
        }

    fill = None
    wait_rows = future_rows[:max_wait_bars]
    center = (entry_low + entry_high) / 2
    for idx, row in enumerate(wait_rows, start=1):
        low, high = row.get("low"), row.get("high")
        if low is None or high is None:
            continue
        if low <= entry_high and high >= entry_low:
            open_price = row.get("open") or center
            fill_price = center if low <= center <= high else min(max(open_price, entry_low), entry_high)
            fill = {"date": row.get("date", ""), "price": fill_price, "wait_bars": idx}
            break
    if not fill:
        return {
            "method": method,
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "report_date": report_date,
            "status": "未成交",
            "entry_range": zone.get("label", "資料不足"),
            "entry": None,
            "ret": None,
            "wait_days": None,
            "entry_vs_signal_ret": None,
            "exit_reason": f"{max_wait_bars}日內未觸及",
        }

    entry_price = fill["price"]
    stop = variant_initial_stop(entry_price, tech, decision)
    exit_date = ""
    exit_price = None
    exit_reason = ""
    fill_seen = False
    for row in future_rows:
        if row.get("date") == fill["date"]:
            fill_seen = True
        if not fill_seen:
            continue
        low, high = row.get("low"), row.get("high")
        if low is None or high is None:
            continue
        if stop and low <= stop:
            exit_date, exit_price, exit_reason = row.get("date", ""), stop, "初始停損"
            break
        if target and high >= target:
            exit_date, exit_price, exit_reason = row.get("date", ""), target, "停利"
            break
    if exit_price is None:
        last = all_rows[-1]
        exit_date, exit_price, exit_reason = last.get("date", ""), last.get("close"), "持有中"
    ret = ((exit_price / entry_price - 1) * 100) if entry_price and exit_price else None
    signal_close = past_rows[-1].get("close")
    entry_vs_signal_ret = ((entry_price / signal_close - 1) * 100) if entry_price and signal_close else None
    path = trade_path_metrics(all_rows, fill["date"], exit_date, entry_price)
    return {
        "method": method,
        "sid": sid,
        "name": s.get("name", ""),
        "report_date": report_date,
        "status": "持有中" if exit_reason == "持有中" else "停利" if exit_reason == "停利" else "停損/出場",
        "entry_range": zone.get("label", "資料不足"),
        "entry_date": fill["date"],
        "entry": entry_price,
        "exit_date": exit_date,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "ret": ret,
        "wait_days": fill.get("wait_bars"),
        "entry_vs_signal_ret": entry_vs_signal_ret,
        "max_return": path.get("max_return"),
        "max_drawdown": path.get("max_drawdown"),
        "target": target,
        "stop": stop,
    }


def build_entry_variant_results(reports: list[dict]) -> list[dict]:
    results = []
    for report in sorted(reports, key=lambda r: r.get("date", "")):
        report_date = report.get("date", "")
        for s in report.get("stocks", []):
            for method, _, _ in ENTRY_VARIANTS:
                result = backtest_entry_variant(report_date, s, method)
                if result:
                    results.append(result)
    return results


def summarize_entry_variants(results: list[dict]) -> list[dict]:
    summary = []
    by_method = {method: [x for x in results if x.get("method") == method] for method, _, _ in ENTRY_VARIANTS}
    for method, label, note in ENTRY_VARIANTS:
        rows = by_method.get(method, [])
        filled = [x for x in rows if x.get("entry") is not None]
        closed = [x for x in filled if x.get("exit_reason") != "持有中"]
        wins = [x for x in closed if (x.get("ret") or 0) > 0]
        stops = [x for x in filled if x.get("exit_reason") == "初始停損"]
        targets = [x for x in filled if x.get("exit_reason") == "停利"]
        losses = [x for x in filled if (x.get("ret") or 0) < 0]
        current_wins = [x for x in filled if (x.get("ret") or 0) > 0]
        avg_ret = sum(x.get("ret") or 0 for x in filled) / len(filled) if filled else None
        avg_closed = sum(x.get("ret") or 0 for x in closed) / len(closed) if closed else None
        avg_loss = sum(x.get("ret") or 0 for x in losses) / len(losses) if losses else None
        avg_wait = sum(x.get("wait_days") or 0 for x in filled) / len(filled) if filled else None
        avg_entry_gap = sum(x.get("entry_vs_signal_ret") or 0 for x in filled) / len(filled) if filled else None
        worst = min((x.get("ret") for x in filled if x.get("ret") is not None), default=None)
        best = max((x.get("ret") for x in filled if x.get("ret") is not None), default=None)
        fill_rate = len(filled) / len(rows) * 100 if rows else None
        win_rate = len(wins) / len(closed) * 100 if closed else None
        current_win_rate = len(current_wins) / len(filled) * 100 if filled else None
        loss_rate = len(losses) / len(filled) * 100 if filled else None
        stop_rate = len(stops) / len(filled) * 100 if filled else None
        if not filled:
            score = -999
        else:
            fill = fill_rate or 0
            # Prefer a usable fill rate, not a one-off perfect-looking sample.
            fill_score = max(0, 10 - abs(fill - 18) / 2)
            risk_score = (current_win_rate or 0) / 10 - (loss_rate or 0) / 8 - (stop_rate or 0) / 10 + (worst or 0) / 2
            cheap_score = min(3, max(-3, -(avg_entry_gap or 0) / 1.5))
            small_sample_penalty = 16 if len(filled) < 5 else 6 if len(filled) < 8 else 0
            score = fill_score + risk_score + cheap_score - small_sample_penalty
        summary.append({
            "method": method,
            "label": label,
            "note": note,
            "signals": len(rows),
            "filled": len(filled),
            "closed": len(closed),
            "fill_rate": fill_rate,
            "win_rate": win_rate,
            "current_win_rate": current_win_rate,
            "loss_rate": loss_rate,
            "stop_rate": stop_rate,
            "avg_ret": avg_ret,
            "avg_closed": avg_closed,
            "avg_loss": avg_loss,
            "avg_wait": avg_wait,
            "avg_entry_gap": avg_entry_gap,
            "best": best,
            "worst": worst,
            "targets": len(targets),
            "stops": len(stops),
            "score": score,
        })
    summary.sort(key=lambda x: x.get("score") or -999, reverse=True)
    return summary


def build_entry_variant_comparison_html(reports: list[dict]) -> str:
    results = build_entry_variant_results(reports)
    summary = summarize_entry_variants(results)
    rows_html = ""
    for x in summary:
        rows_html += f"""
<tr>
  <td><strong>{esc(x['label'])}</strong><div class="signal-dates">{esc(x['note'])}</div></td>
  <td>{x['filled']} / {x['signals']}<div class="signal-dates">{fmt_num(x['fill_rate'],1)}%</div></td>
  <td>{fmt_num(x.get('current_win_rate'),1)}%<div class="signal-dates">已出場 {fmt_num(x.get('win_rate'),1)}%</div></td>
  <td class="{('neg' if (x.get('loss_rate') or 0) > 35 else 'pos')}">{fmt_num(x.get('loss_rate'),1)}%</td>
  <td class="{('neg' if (x.get('stop_rate') or 0) > 20 else '')}">{fmt_num(x.get('stop_rate'),1)}%</td>
  <td class="{('pos' if (x.get('avg_entry_gap') or 0) <= 0 else 'neg')}">{fmt_num(x.get('avg_entry_gap'),1)}%</td>
  <td><span class="neg">{fmt_num(x.get('worst'),1)}%</span><div class="signal-dates">均虧 {fmt_num(x.get('avg_loss'),1)}%</div></td>
  <td>{fmt_num(x.get('avg_wait'),1)}</td>
</tr>"""
    sample_rows = ""
    for x in sorted([r for r in results if r.get("entry") is not None], key=lambda r: (r.get("method") != summary[0]["method"], r.get("report_date", ""), r.get("sid", "")), reverse=True)[:18]:
        ret = x.get("ret")
        ret_cls = "pos" if ret is not None and ret > 0 else "neg" if ret is not None and ret < 0 else ""
        label = next((label for method, label, _ in ENTRY_VARIANTS if method == x.get("method")), x.get("method", ""))
        sample_rows += f"""
<tr>
  <td><a class="stock-link" href="stocks/{x['sid']}.html">{esc(x['sid'])} {esc(x['name'])}</a><div class="signal-dates">{esc(label)}｜報告 {esc(x['report_date'])}</div></td>
  <td>{esc(x.get('entry_range','─'))}<div class="signal-dates">成交 {esc(x.get('entry_date','─'))}｜{fmt_num(x.get('entry'))}</div></td>
  <td>{fmt_num(x.get('entry_vs_signal_ret'),1)}%</td>
  <td>{esc(x.get('exit_reason',''))}<div class="signal-dates">{esc(x.get('exit_date','─'))}</div></td>
  <td class="{ret_cls}" style="font-weight:800">{fmt_num(ret,1)}%</td>
</tr>"""
    return f"""
<div class="card">
  <div class="section-label">買點版本比較</div>
  <div class="strategy-note">同一批歷史訊號，訊號日後最多等待 20 個交易日。這裡只替換買入區，停利沿用原報告目標價，初始停損用該買入價下方最近可執行支撐或買點 -6%。</div>
  <div style="overflow-x:auto;margin-top:12px">
    <table class="stock-table">
      <thead><tr><th>買點版本</th><th>成交數</th><th>目前勝率</th><th>虧損率</th><th>停損率</th><th>買貴/便宜</th><th>最差風險</th><th>平均等待日</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
  <div class="strategy-note" style="margin-top:12px">目前勝率把持有中的未實現損益也納入；已出場勝率只看已停利/停損的單。買貴/便宜為成交價相對報告日收盤價，負值代表買得比較低。這張表重點看成交數是否夠、虧損率與停損率是否可接受、最差風險是否太深。</div>
  <div style="overflow-x:auto;margin-top:14px">
    <table class="stock-table">
      <thead><tr><th>近期成交樣本</th><th>買入區/成交</th><th>買貴/便宜</th><th>出場</th><th>報酬</th></tr></thead>
      <tbody>{sample_rows}</tbody>
    </table>
  </div>
</div>"""


def backtest_one_signal(report_date: str, s: dict) -> dict | None:
    sid = s.get("id", "")
    if not sid:
        return None
    s = enrich_stock_fields(dict(s))
    s["report_date"] = report_date
    all_rows = merge_report_close(read_price_history(sid), s)
    all_rows = sorted(all_rows, key=lambda r: r.get("date", ""))
    past_rows = [r for r in all_rows if r.get("date", "") <= report_date]
    future_rows = [r for r in all_rows if r.get("date", "") > report_date]
    if not past_rows or not future_rows:
        return None

    tech = technical_snapshot(past_rows, s)
    decision = build_trade_decision(tech, s)
    if tech.get("formal_entry_filter_ok") is False:
        return {
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "report_date": report_date,
            "basket": basket_label(classify_basket(s)),
            "status": "濾網排除",
            "entry_range": "MA20濾網排除",
            "entry": None,
            "exit_date": "─",
            "exit_price": None,
            "exit_reason": "訊號日收盤跌破MA20",
            "ret": None,
            "hold_days": None,
            "latest_close": all_rows[-1].get("close") if all_rows else None,
            "target": decision.get("target"),
            "stop": decision.get("initial_stop"),
        }
    entry_low, entry_high = parse_range_values(decision.get("entry_range"))
    entry = decision.get("entry")
    target = decision.get("target")
    stop = decision.get("initial_stop")
    if entry_low is None or entry_high is None or entry is None or stop is None:
        return None

    fill = None
    for row in future_rows:
        low, high = row.get("low"), row.get("high")
        if low is None or high is None:
            continue
        if low <= entry_high and high >= entry_low:
            open_price = row.get("open") or entry
            fill_price = entry if low <= entry <= high else min(max(open_price, entry_low), entry_high)
            if stop and fill_price <= stop:
                continue
            fill = {"date": row.get("date", ""), "price": fill_price, "row": row}
            break

    if not fill:
        last = all_rows[-1] if all_rows else {}
        return {
            "sid": sid,
            "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
            "report_date": report_date,
            "basket": basket_label(classify_basket(s)),
            "status": "未成交",
            "entry_range": decision.get("entry_range", "─"),
            "entry": None,
            "exit_date": "─",
            "exit_price": None,
            "exit_reason": "未觸及買入區",
            "ret": None,
            "hold_days": None,
            "latest_close": last.get("close"),
            "target": target,
            "stop": stop,
        }

    entry_price = fill["price"]
    exit_date = ""
    exit_price = None
    exit_reason = ""
    fill_seen = False
    for row in future_rows:
        if row.get("date") == fill["date"]:
            fill_seen = True
        if not fill_seen:
            continue
        low, high = row.get("low"), row.get("high")
        if low is None or high is None:
            continue
        if stop and low <= stop:
            exit_date, exit_price, exit_reason = row.get("date", ""), stop, "初始停損"
            break
        if target and high >= target:
            exit_date, exit_price, exit_reason = row.get("date", ""), target, "停利"
            break

    if exit_price is None:
        last = all_rows[-1]
        exit_date = last.get("date", "")
        exit_price = last.get("close")
        exit_reason = "持有中"

    ret = ((exit_price / entry_price - 1) * 100) if entry_price and exit_price else None
    path = trade_path_metrics(all_rows, fill["date"], exit_date, entry_price)
    hold_days = None
    try:
        hold_days = max(0, (datetime.fromisoformat(exit_date) - datetime.fromisoformat(fill["date"])).days)
    except Exception:
        pass
    status = "持有中" if exit_reason == "持有中" else "停利" if exit_reason == "停利" else "停損/出場"
    return {
        "sid": sid,
        "name": s.get("name", ""),
        "report_date": report_date,
        "basket": basket_label(classify_basket(s)),
        "status": status,
        "entry_range": decision.get("entry_range", "─"),
        "entry_date": fill["date"],
        "entry": entry_price,
        "exit_date": exit_date,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "ret": ret,
        "max_return": path.get("max_return"),
        "max_drawdown": path.get("max_drawdown"),
        "hold_days": hold_days,
        "latest_close": all_rows[-1].get("close") if all_rows else None,
        "target": target,
        "stop": stop,
    }


def build_backtest_results(reports: list[dict]) -> list[dict]:
    results = []
    for report in sorted(reports, key=lambda r: r.get("date", "")):
        report_date = report.get("date", "")
        for s in report.get("stocks", []):
            result = backtest_one_signal(report_date, s)
            if result:
                results.append(result)
    return results


def historical_scan_universe(reports: list[dict]) -> list[dict]:
    stock_map = find_latest_stock_map(reports)
    out = []
    for sid, s in sorted(stock_map.items()):
        rows = sorted(read_price_history(sid), key=lambda r: r.get("date", ""))
        if len(rows) >= 80:
            out.append({"sid": sid, "stock": enrich_stock_fields(dict(s)), "rows": rows})
    return out


def attack_wave_confirmed(rows: list[dict], lookback: int = 12) -> bool:
    if len(rows) < max(lookback, 25):
        return False
    recent = rows[-lookback:]
    start_close = recent[0].get("close")
    high = max((r.get("high") or 0) for r in recent)
    ma20 = ma_values(rows, 20)[-1]
    vol20 = sum(r.get("volume", 0) for r in rows[-20:]) / 20
    vol5 = sum(r.get("volume", 0) for r in rows[-5:]) / 5
    return bool(start_close and high and ma20 and rows[-1].get("close") and high / start_close >= 1.08 and rows[-1]["close"] >= ma20 and vol5 >= vol20 * 0.9)


def historical_entry_signal(method: str, rows: list[dict], tech: dict, decision: dict) -> dict:
    if method == "sfz_ta3":
        strict = mda_strict_entry(rows)
        if not strict.get("ok"):
            return {"ok": False}
        close = rows[-1].get("close")
        return {
            "ok": True,
            "low": close,
            "high": close,
            "entry": close,
            "label": "波段初始買點",
            "title": "SFZ_TA3 初始買點",
            "rule": "尚未完全發動時找波段趨勢的一開始：SMA5斜率>0 / 回到SMA5±1.5% / 紅K確認",
        }
    if method == "wr_after_attack":
        if not attack_wave_confirmed(rows):
            return {"ok": False}
        zone = indicator_entry_zone("wr_65_85_ma20", rows, decision)
        if zone.get("low") is None or zone.get("high") is None:
            return {"ok": False}
        return {
            "ok": True,
            "low": zone.get("low"),
            "high": zone.get("high"),
            "entry": None,
            "label": zone.get("label", "資料不足"),
            "title": "行進籃 Williams 回落買點",
            "rule": "已走一段攻擊波、進入行進籃後，等回落到 Williams -65~-85 反推區找再進場/加碼點",
        }
    return {"ok": False}


def _run_backtest_scan(reports: list[dict], start_date: str = "2024-01-01", method: str = "sfz_ta3", use_pit: bool = False) -> list[dict]:
    trades = []
    for item in historical_scan_universe(reports):
        sid = item["sid"]
        s = item["stock"]
        rows = item["rows"]
        chip_series = read_chip_series(sid)
        i = 60
        while i < len(rows) - 1:
            row = rows[i]
            signal_date = row.get("date", "")
            if signal_date < start_date:
                i += 1
                continue
            if use_pit and not is_in_universe(sid, signal_date):
                i += 1
                continue
            past_rows = rows[: i + 1]
            tech = technical_snapshot(past_rows, {**s, "report_date": signal_date, "price": str(row.get("close", ""))})
            close = tech.get("close")
            ma20 = tech.get("ma20")
            if not close or not ma20 or close < ma20:
                i += 1
                continue
            if tech.get("volume_price") == "放量下跌":
                i += 1
                continue
            decision = build_trade_decision(tech, {**s, "report_date": signal_date, "price": str(close)})
            signal = historical_entry_signal(method, past_rows, tech, decision)
            if not signal.get("ok"):
                i += 1
                continue
            entry_low, entry_high = signal.get("low"), signal.get("high")
            if entry_low is None or entry_high is None:
                i += 1
                continue
            low, high = row.get("low"), row.get("high")
            if low is None or high is None or not (low <= entry_high and high >= entry_low):
                i += 1
                continue

            center = (entry_low + entry_high) / 2
            open_price = row.get("open") or center
            entry_price = signal.get("entry") or (center if low <= center <= high else min(max(open_price, entry_low), entry_high))
            stop = variant_initial_stop(entry_price, tech, decision)
            target = None
            exit_date = rows[-1].get("date", "")
            exit_price = rows[-1].get("close")
            exit_reason = "持有中"
            exit_idx = len(rows) - 1
            for j in range(i, len(rows)):
                r = rows[j]
                r_low, r_high = r.get("low"), r.get("high")
                if r_low is None or r_high is None:
                    continue
                if stop and r_low <= stop:
                    exit_date, exit_price, exit_reason, exit_idx = r.get("date", ""), stop, "初始停損", j
                    break
                if j > i:
                    sell_price, sell_reason = historical_sell_exit(rows[: j + 1], chip_series, s, entry_price, i, 20)
                    if sell_price is not None and sell_reason:
                        exit_date, exit_price, exit_reason, exit_idx = r.get("date", ""), sell_price, sell_reason, j
                        break
            ret = ((exit_price / entry_price - 1) * 100) if entry_price and exit_price else None
            path = trade_path_metrics(rows, signal_date, exit_date, entry_price)
            activated_20 = bool(path.get("max_return") is not None and path["max_return"] >= 20)
            stopped_before_activation = bool(exit_reason == "初始停損" and not activated_20)
            hold_days = None
            try:
                hold_days = max(0, (datetime.fromisoformat(exit_date) - datetime.fromisoformat(signal_date)).days)
            except Exception:
                pass
            trades.append({
                "sid": sid,
                "name": s.get("name", ""),
            "sector": s.get("industry") or s.get("sector") or "未分類",
                "signal_date": signal_date,
                "entry_title": signal.get("title", "買點"),
                "entry_rule": signal.get("rule", ""),
                "entry_range": signal.get("label", "資料不足"),
                "entry_date": signal_date,
                "entry": entry_price,
                "exit_date": exit_date,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "ret": ret,
                "max_return": path.get("max_return"),
                "max_drawdown": path.get("max_drawdown"),
                "activated_20": activated_20,
                "stopped_before_activation": stopped_before_activation,
                "hold_days": hold_days,
                "target": target,
                "stop": stop,
            })
            i = max(exit_idx + 1, i + 20)
    return trades


def _run_backtest_legacy(reports: list[dict], start_date: str = "2024-01-01", method: str = "sfz_ta3") -> list[dict]:
    return _run_backtest_scan(reports, start_date, method, use_pit=False)


def _run_backtest_pit(reports: list[dict], start_date: str = "2024-01-01", method: str = "sfz_ta3") -> list[dict]:
    return _run_backtest_scan(reports, start_date, method, use_pit=True)


def summarize_trade_rows(rows: list[dict]) -> dict:
    filled = [x for x in rows if x.get("entry") is not None]
    closed = [x for x in filled if x.get("exit_reason") != "持有中"]
    wins = [x for x in closed if (x.get("ret") or 0) > 0]
    losses = [x for x in closed if (x.get("ret") or 0) <= 0]
    activated = [x for x in filled if x.get("activated_20") or ((x.get("max_return") or 0) >= 20)]
    activated_closed = [x for x in activated if x.get("exit_reason") != "持有中"]
    activated_wins = [x for x in activated_closed if (x.get("ret") or 0) > 0]
    pre_activation_stops = [
        x for x in filled
        if x.get("stopped_before_activation")
        or (x.get("exit_reason") == "初始停損" and (x.get("max_return") or 0) < 20)
    ]
    hold_count = len([x for x in filled if x.get("hold_days") is not None])
    return {
        "signals": len(rows),
        "filled": len(filled),
        "closed": len(closed),
        "open": len([x for x in filled if x.get("exit_reason") == "持有中"]),
        "win_rate": len(wins) / len(closed) * 100 if closed else None,
        "avg_ret": sum(x.get("ret") or 0 for x in filled) / len(filled) if filled else None,
        "best": max((x.get("ret") for x in filled if x.get("ret") is not None), default=None),
        "worst": min((x.get("ret") for x in filled if x.get("ret") is not None), default=None),
        "max_return": max((x.get("max_return") for x in filled if x.get("max_return") is not None), default=None),
        "max_drawdown": min((x.get("max_drawdown") for x in filled if x.get("max_drawdown") is not None), default=None),
        "avg_drawdown": sum(x.get("max_drawdown") or 0 for x in filled) / len(filled) if filled else None,
        "avg_hold": sum(x.get("hold_days") or 0 for x in filled if x.get("hold_days") is not None) / hold_count if hold_count else None,
        "activated": len(activated),
        "activated_closed": len(activated_closed),
        "activated_wins": len(activated_wins),
        "activation_rate": len(activated) / len(filled) * 100 if filled else None,
        "activated_win_rate": len(activated_wins) / len(activated_closed) * 100 if activated_closed else None,
        "activated_avg_ret": sum(x.get("ret") or 0 for x in activated) / len(activated) if activated else None,
        "pre_activation_stops": len(pre_activation_stops),
        "pre_activation_stop_rate": len(pre_activation_stops) / len(filled) * 100 if filled else None,
        "wins": len(wins),
        "losses": len(losses),
    }


def historical_sell_exit(
    daily_rows: list[dict],
    chip_series: list[dict],
    stock: dict,
    entry_price: float,
    entry_index: int,
    activation_pct: float = 20,
) -> tuple[float | None, str | None]:
    if len(daily_rows) < 20 or not entry_price:
        return None, None
    if entry_index < 0 or entry_index >= len(daily_rows):
        return None, None
    current_date = str(daily_rows[-1].get("date", ""))
    current_chip = [x for x in chip_series if str(x.get("date", "")) <= current_date]
    weekly_rows = aggregate_ohlcv(daily_rows, "weekly")
    signal = calc_sell_signal(daily_rows, weekly_rows, current_chip, stock, {"entry": entry_price})
    close = daily_rows[-1].get("close")
    level = signal.get("level", "")
    reason = signal.get("reason", "")
    current_ma20 = ma_values(daily_rows, 20)[-1] if len(daily_rows) >= 20 else None
    trade_rows = daily_rows[entry_index:]
    high_water = max((r.get("high") or entry_price) for r in trade_rows) if trade_rows else entry_price
    high_ret = ((high_water / entry_price - 1) * 100) if high_water and entry_price else None

    if level == "立即檢查" and close:
        return close, f"{level}｜{reason}"
    if high_ret is not None and high_ret >= activation_pct and close and current_ma20 and close < current_ma20:
        return close, f"跌破MA20｜曾漲過{activation_pct:.0f}%後啟動MA20續抱"
    return None, None


def build_historical_scan_block(trades: list[dict], title: str, note: str, variant_label: str = "") -> str:
    summary = summarize_trade_rows(trades)
    first_date = min((x.get("signal_date") for x in trades if x.get("signal_date")), default="─")
    last_date = max((x.get("signal_date") for x in trades if x.get("signal_date")), default="─")
    rows_html = ""
    for x in sorted(trades, key=lambda r: (r.get("signal_date", ""), r.get("sid", "")), reverse=True)[:80]:
        ret = x.get("ret")
        ret_cls = "pos" if ret is not None and ret > 0 else "neg" if ret is not None and ret < 0 else ""
        entry_heading = f"{esc(x.get('entry_title','買點'))}｜成交 {esc(x.get('entry_date','─'))}｜{fmt_num(x.get('entry'))}"
        activated = bool(x.get("activated_20") or ((x.get("max_return") or 0) >= 20))
        activation_badge = "+20%已啟動" if activated else "未啟動"
        activation_cls = "price-target" if activated else "signal-dates"
        rows_html += f"""
<tr>
  <td><a class="stock-link" href="stocks/{x['sid']}.html">{esc(x['sid'])} {esc(x['name'])}</a><div class="signal-dates">訊號 {esc(x.get('signal_date','─'))}</div></td>
  <td><strong>{entry_heading}</strong><div class="signal-dates">{esc(x.get('entry_range','─'))}<br>{esc(x.get('entry_rule',''))}</div></td>
  <td>{esc(x.get('exit_reason',''))}<div class="signal-dates">{esc(x.get('exit_date','─'))}｜出場 {fmt_num(x.get('exit_price'))}</div></td>
  <td class="{ret_cls}" style="font-weight:800">{fmt_num(ret,1)}%</td>
  <td><span class="pos">{fmt_num(x.get('max_return'),1)}%</span><div class="signal-dates">最大回撤 <span class="neg">{fmt_num(x.get('max_drawdown'),1)}%</span></div></td>
  <td><div class="{activation_cls}">{activation_badge}</div><div class="price-stop">初停 {fmt_num(x.get('stop'))}</div></td>
</tr>"""
    if not rows_html:
        rows_html = '<tr><td colspan="6" style="color:#8b949e">目前資料不足，還無法形成 2024 起掃描回測。</td></tr>'
    return f"""
<div class="card">
  <div class="section-label">{esc(title)}</div>
  {f'<div class="strategy-note" style="font-weight:800">{variant_label}</div>' if variant_label else ''}
  <div class="strategy-note">資料範圍 {esc(first_date)} ~ {esc(last_date)}。前提是 SFZ 選股池，不是全市場掃描。{esc(note)} 出場沿用回測資料夾的 high-water activation：先守初始停損；交易期間最高價曾漲過 +20% 後才啟動 MA20 主線續抱，啟動後收盤跌破 MA20 出場；量大長黑且外資連賣則立即檢查；不設固定 +10% 停利。</div>
  <div class="grid grid-3" style="margin-top:12px">
    <div class="metric"><div class="metric-num">{summary['filled']}</div><div class="metric-label">成交筆數</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(summary.get('win_rate'),1)}%</div><div class="metric-label">全體已出場勝率</div></div>
    <div class="metric"><div class="metric-num {('pos' if (summary.get('avg_ret') or 0) >= 0 else 'neg')}">{fmt_num(summary.get('avg_ret'),1)}%</div><div class="metric-label">全體平均報酬</div></div>
    <div class="metric"><div class="metric-num pos">{fmt_num(summary.get('activation_rate'),1)}%</div><div class="metric-label">+20%啟動率</div></div>
    <div class="metric"><div class="metric-num pos">{fmt_num(summary.get('activated_win_rate'),1)}%</div><div class="metric-label">+20%啟動後勝率</div></div>
    <div class="metric"><div class="metric-num {('pos' if (summary.get('activated_avg_ret') or 0) >= 0 else 'neg')}">{fmt_num(summary.get('activated_avg_ret'),1)}%</div><div class="metric-label">+20%啟動後均報酬</div></div>
    <div class="metric"><div class="metric-num neg">{fmt_num(summary.get('pre_activation_stop_rate'),1)}%</div><div class="metric-label">啟動前初停率</div></div>
    <div class="metric"><div class="metric-num pos">{fmt_num(summary.get('max_return'),1)}%</div><div class="metric-label">最大曾有報酬</div></div>
    <div class="metric"><div class="metric-num neg">{fmt_num(summary.get('max_drawdown'),1)}%</div><div class="metric-label">最大回撤</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(summary.get('avg_hold'),1)}</div><div class="metric-label">平均持有天數</div></div>
  </div>
  <div class="chip-line">全體勝率＝所有成交中已出場獲利 / 已出場；+20%啟動後勝率＝只看曾漲過20%的子集合。已出場 {summary['closed']} 筆｜持有中 {summary['open']} 筆｜全體獲利 {summary['wins']} 筆｜全體虧損 {summary['losses']} 筆｜+20%啟動 {summary['activated']} 筆｜啟動後已出場 {summary['activated_closed']} 筆｜啟動後獲利 {summary['activated_wins']} 筆｜啟動前初停 {summary['pre_activation_stops']} 筆｜最佳實現 {fmt_num(summary.get('best'),1)}%｜最差實現 {fmt_num(summary.get('worst'),1)}%｜平均回撤 {fmt_num(summary.get('avg_drawdown'),1)}%</div>
  <div style="overflow-x:auto;margin-top:14px">
    <table class="stock-table">
      <thead><tr><th>個股/訊號日</th><th>買點與成交</th><th>出場</th><th>實現報酬</th><th>最大報酬/回撤</th><th>MA20啟動/初停</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>"""


def _metric_change(new_value, old_value, pct: bool = False) -> str:
    if new_value is None or old_value is None:
        return "─"
    diff = new_value - old_value
    suffix = "%" if pct else ""
    sign = "+" if diff > 0 else ""
    return f"{sign}{fmt_num(diff, 1)}{suffix}"


def _historical_scan_compare_table(title: str, legacy_summary: dict, pit_summary: dict) -> str:
    metrics = [
        ("成交筆數", "filled", False),
        ("全體勝率", "win_rate", True),
        ("全體平均報酬", "avg_ret", True),
        ("+20%啟動率", "activation_rate", True),
    ]
    rows = ""
    for label, key, is_pct in metrics:
        legacy_value = legacy_summary.get(key)
        pit_value = pit_summary.get(key)
        rows += f"""
<tr>
  <td>{esc(label)}</td>
  <td>{fmt_num(legacy_value, 1)}{'%' if is_pct else ''}</td>
  <td>{fmt_num(pit_value, 1)}{'%' if is_pct else ''}</td>
  <td>{esc(_metric_change(pit_value, legacy_value, is_pct))}</td>
</tr>"""
    return f"""
<div class="card">
  <div class="section-label">{esc(title)}｜Legacy vs PIT 對照</div>
  <div style="overflow-x:auto;margin-top:10px">
    <table class="stock-table">
      <thead><tr><th>指標</th><th>Legacy（舊）</th><th>PIT（新）</th><th>變化</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>"""


def _build_historical_scan_pair(reports: list[dict], method: str, title: str, note: str) -> str:
    legacy_trades = _run_backtest_legacy(reports, "2024-01-01", method)
    pit_trades = _run_backtest_pit(reports, "2024-01-01", method)
    legacy_summary = summarize_trade_rows(legacy_trades)
    pit_summary = summarize_trade_rows(pit_trades)
    warning = '<span style="color:#f85149">⚠️ 已套用 point-in-time universe 過濾。預期勝率與平均報酬會低於 Legacy 版本，這是正確修正方向。</span>'
    return (
        _historical_scan_compare_table(title, legacy_summary, pit_summary)
        + '<div class="grid grid-2">'
        + build_historical_scan_block(
            legacy_trades,
            f"{title}｜Legacy（含倖存者偏差）",
            note,
            "Legacy（含倖存者偏差）：沿用今天回頭看的候選池。",
        )
        + build_historical_scan_block(
            pit_trades,
            f"{title}｜PIT（point-in-time universe）",
            note,
            warning,
        )
        + "</div>"
    )


def build_historical_scan_html(reports: list[dict]) -> str:
    return (
        _build_historical_scan_pair(
            reports,
            "sfz_ta3",
            "網站壓力測試｜逐日掃描版 SFZ_TA3",
            "這段不是原 168 筆回測；它是用目前網站候選池逐日掃描 2024 起所有符合點，條件較簡化，所以會重複觸發並放大啟動前停損風險。",
        )
        + _build_historical_scan_pair(
            reports,
            "wr_after_attack",
            "網站壓力測試｜行進籃 Williams 回落",
            "這段同樣是網站逐日掃描版；先確認攻擊波已出現，再用 Williams -65~-85 反推回測區，不等同原 SFZ 168 訊號母體。",
        )
    )


def _read_backtest_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _find_backtest_row(path: Path, **criteria) -> dict:
    for row in _read_backtest_csv(path):
        if all(str(row.get(k, "")) == str(v) for k, v in criteria.items()):
            return row
    return {}


def _metric_from_row(row: dict, key: str, suffix: str = "%") -> str:
    if not row or row.get(key) in {None, ""}:
        return "─"
    try:
        return f"{float(row[key]):.1f}{suffix}"
    except Exception:
        return esc(str(row.get(key)))


def _num_from_row(row: dict, key: str) -> str:
    if not row or row.get(key) in {None, ""}:
        return "─"
    try:
        return str(int(float(row[key])))
    except Exception:
        return esc(str(row.get(key)))


def load_backtest_dashboard_payload(path: Path = BACKTEST_DASHBOARD_PATH) -> dict:
    if not path.exists():
        return {
            "schema_version": 1,
            "updated_at": "",
            "cost_model": {"round_trip_rate": 0.0044},
            "strategies": [],
            "notes": ["data/backtest_results.json is not generated yet."],
            "freshness": {"status": "missing", "data_date": None, "expected_data_date": None},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "schema_version": 1,
            "updated_at": "",
            "cost_model": {"round_trip_rate": 0.0044},
            "strategies": [],
            "notes": ["data/backtest_results.json could not be parsed."],
            "freshness": {"status": "schema_error", "data_date": None, "expected_data_date": None},
        }
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("schema_version", 1)
    payload.setdefault("cost_model", {"round_trip_rate": 0.0044})
    payload.setdefault("strategies", [])
    payload.setdefault("notes", [])
    payload.setdefault("freshness", {})
    return payload


def _dash_pct(value, digits: int = 1) -> str:
    try:
        if value is None:
            return "N/A"
        return f"{float(value) * 100:.{digits}f}%"
    except Exception:
        return "N/A"


def _dash_num(value, digits: int = 2) -> str:
    try:
        if value is None:
            return "N/A"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "N/A"


def artifact_freshness_warning(payload: dict, label: str) -> str:
    freshness = payload.get("freshness") if isinstance(payload.get("freshness"), dict) else {}
    status = str(freshness.get("status") or "")
    if status not in {"stale", "fallback_fresh", "fallback_stale", "missing", "schema_error"}:
        return ""
    data_date = freshness.get("data_date") or payload.get("date") or "未知"
    expected_date = freshness.get("expected_data_date") or "未知"
    if status == "fallback_fresh":
        message = f"{label}目前使用保留的備援資料（資料日 {data_date}）；尚在 freshness SLA 內。"
    elif status == "fallback_stale":
        message = f"{label}目前使用已過期的保留資料（資料日 {data_date}，應更新至 {expected_date}），請勿視為最新結果。"
    elif status == "stale":
        message = f"{label}資料已過期（資料日 {data_date}，應更新至 {expected_date}）。"
    elif status == "schema_error":
        message = f"{label}資料欄位未通過 schema 驗證。"
    else:
        message = f"{label}目前沒有可用資料。"
    return f'<div class="warning-banner" data-artifact-freshness="{esc(status)}">⚠ {esc(message)}</div>'


def build_backtest_dashboard_page(payload: dict | None = None, section_only: bool = False) -> str:
    payload = payload or load_backtest_dashboard_payload()
    strategies = payload.get("strategies") if isinstance(payload.get("strategies"), list) else []
    cost = payload.get("cost_model") if isinstance(payload.get("cost_model"), dict) else {}
    round_trip = cost.get("round_trip_rate", 0.0044)
    updated_at = payload.get("updated_at") or payload.get("date") or "N/A"
    period = payload.get("period") if isinstance(payload.get("period"), dict) else {}
    period_text = f"{period.get('start') or 'N/A'} ~ {period.get('end') or 'N/A'}"
    top = strategies[0] if strategies else {}
    top_metrics = top.get("metrics") if isinstance(top.get("metrics"), dict) else {}
    best_sharpe = max(
        (s.get("metrics", {}).get("sharpe_ratio") for s in strategies if isinstance(s.get("metrics"), dict) and s.get("metrics", {}).get("sharpe_ratio") is not None),
        default=None,
    )
    row_html = ""
    for idx, strategy in enumerate(strategies):
        metrics = strategy.get("metrics") if isinstance(strategy.get("metrics"), dict) else {}
        period_row = strategy.get("period") if isinstance(strategy.get("period"), dict) else {}
        name = strategy.get("strategy_name", f"Strategy {idx + 1}")
        annual = metrics.get("annual_return")
        sharpe = metrics.get("sharpe_ratio")
        drawdown = metrics.get("max_drawdown")
        win_rate = metrics.get("win_rate")
        trades = metrics.get("total_trades")
        pf = metrics.get("profit_factor")
        row_html += f"""
<tr data-strategy-row data-index="{idx}">
  <td><strong>{esc(name)}</strong><div class="signal-dates">{esc(strategy.get('category', ''))} · {esc(period_row.get('start', ''))} ~ {esc(period_row.get('end', ''))}</div></td>
  <td data-value="{'' if sharpe is None else sharpe}">{_dash_num(sharpe, 2)}</td>
  <td data-value="{'' if annual is None else annual}" class="{('pos' if (annual or 0) >= 0 else 'neg') if annual is not None else ''}">{_dash_pct(annual, 1)}</td>
  <td data-value="{'' if drawdown is None else drawdown}" class="neg">{_dash_pct(drawdown, 1)}</td>
  <td data-value="{'' if win_rate is None else win_rate}">{_dash_pct(win_rate, 1)}</td>
  <td data-value="{'' if trades is None else trades}">{esc(str(trades if trades is not None else 'N/A'))}</td>
  <td data-value="{'' if pf is None else pf}">{_dash_num(pf, 2)}</td>
</tr>"""
    if not row_html:
        row_html = '<tr><td colspan="7" style="color:#8b949e">No standardized backtest strategies are available yet.</td></tr>'

    notes = payload.get("notes") if isinstance(payload.get("notes"), list) else []
    notes_html = "".join(f"<div class=\"chip-line\">{esc(str(note))}</div>" for note in notes[:4])
    data_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    first_name = top.get("strategy_name", "N/A")
    freshness_warning = artifact_freshness_warning(payload, "回測 Dashboard")
    body = f"""
<div class="container" data-backtest-dashboard>
  <div class="page-title">回測 Dashboard</div>
  <div class="page-sub">標準化比較 SFZ、TA3、CaryBot timing sidecar 與既有分散回測輸出。資料更新：{esc(str(updated_at))}</div>
  {freshness_warning}
  <div class="grid grid-4">
    <div class="metric"><div class="metric-num">{len(strategies)}</div><div class="metric-label">策略數</div></div>
    <div class="metric"><div class="metric-num">{_dash_pct(round_trip, 2)}</div><div class="metric-label">台股單趟交易成本</div></div>
    <div class="metric"><div class="metric-num">{_dash_num(best_sharpe, 2)}</div><div class="metric-label">最佳 Sharpe</div></div>
    <div class="metric"><div class="metric-num">{_dash_pct(top_metrics.get('annual_return'), 1)}</div><div class="metric-label">預設策略年化</div></div>
  </div>
  <div class="card">
    <div class="section-label">策略比較表</div>
    <div class="chip-line">期間：{esc(period_text)} · 成本：買方手續費 0.6‰ + 賣方手續費 0.6‰ + 賣方證交稅 3‰ + slippage 0.2‰ = {_dash_pct(round_trip, 2)}</div>
    <div class="backtest-table-wrap">
      <table class="stock-table" data-strategy-table>
        <thead><tr><th>策略</th><th data-sort-key="sharpe_ratio">Sharpe</th><th data-sort-key="annual_return">年化報酬</th><th data-sort-key="max_drawdown">最大回撤</th><th data-sort-key="win_rate">勝率</th><th data-sort-key="total_trades">交易數</th><th data-sort-key="profit_factor">Profit Factor</th></tr></thead>
        <tbody data-strategy-body>{row_html}</tbody>
      </table>
    </div>
  </div>
  <div class="backtest-layout">
    <div class="card">
      <div class="section-label">Equity Curve</div>
      <div class="backtest-toolbar">
        <select data-strategy-select aria-label="Strategy selector"></select>
        <span class="tag tag-blue" data-lib="Chart.js">Chart.js</span>
      </div>
      <div class="chart-box backtest-chart-box"><canvas id="equityCurveChart" height="130"></canvas></div>
    </div>
    <div class="card">
      <div class="section-label">策略參數</div>
      <div class="chip-line" data-selected-strategy>{esc(str(first_name))}</div>
      <pre class="strategy-params" data-strategy-params>{{}}</pre>
    </div>
  </div>
  <div class="card">
    <div class="section-label">月報酬 Heatmap</div>
    <div class="backtest-heatmap" data-monthly-heatmap></div>
  </div>
  <div class="card">
    <div class="section-label">資料備註</div>
    {notes_html}
    <div class="chip-line">Public JSON: <code>data/backtest_results.json</code></div>
  </div>
</div>
<script id="backtestDashboardData" type="application/json">{data_json}</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.8/dist/chart.umd.min.js"></script>
<script>
(function(){{
  const dataNode=document.getElementById('backtestDashboardData');
  if(!dataNode) return;
  const payload=JSON.parse(dataNode.textContent||'{{}}');
  const strategies=payload.strategies||[];
  const select=document.querySelector('[data-strategy-select]');
  const label=document.querySelector('[data-selected-strategy]');
  const params=document.querySelector('[data-strategy-params]');
  const heat=document.querySelector('[data-monthly-heatmap]');
  const tbody=document.querySelector('[data-strategy-body]');
  let chart=null;
  function pct(v){{ if(v===null||v===undefined||v==='') return 'N/A'; return (Number(v)*100).toFixed(1)+'%'; }}
  function choose(index){{
    const s=strategies[index]||strategies[0];
    if(!s) return;
    const selectedIndex=strategies.indexOf(s);
    if(label) label.textContent=s.strategy_name||'Strategy';
    if(params) params.textContent=JSON.stringify(s.parameters||{{}}, null, 2);
    if(select) select.value=String(selectedIndex);
    document.querySelectorAll('[data-strategy-row]').forEach(row=>row.classList.toggle('active', row.dataset.index===String(selectedIndex)));
    renderHeatmap(s);
    renderChart(s);
  }}
  function renderChart(s){{
    const canvas=document.getElementById('equityCurveChart');
    if(!canvas || !window.Chart) return;
    const curve=s.equity_curve||[];
    const labels=curve.map(p=>p[0]);
    const values=curve.map(p=>p[1]);
    if(chart) chart.destroy();
    chart=new Chart(canvas, {{
      type:'line',
      data:{{labels:labels,datasets:[{{label:s.strategy_name||'Equity',data:values,borderColor:'#58a6ff',backgroundColor:'rgba(88,166,255,.14)',borderWidth:2,tension:.18,pointRadius:0,fill:true}}]}},
      options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},tooltip:{{callbacks:{{label:function(ctx){{return 'Equity '+Number(ctx.parsed.y).toFixed(3);}}}}}}}},scales:{{x:{{ticks:{{color:'#8b949e',maxTicksLimit:8}},grid:{{color:'rgba(48,54,61,.35)'}}}},y:{{ticks:{{color:'#8b949e'}},grid:{{color:'rgba(48,54,61,.35)'}}}}}}}}
    }});
  }}
  function renderHeatmap(s){{
    if(!heat) return;
    const monthly=s.monthly_returns||{{}};
    const months=Object.keys(monthly).sort();
    heat.innerHTML='';
    months.forEach(month=>{{
      const value=Number(monthly[month]);
      const cell=document.createElement('div');
      cell.className='heat-cell '+(value>0?'heat-pos':value<0?'heat-neg':'heat-flat');
      cell.innerHTML='<div class="m">'+month+'</div><div class="r">'+pct(value)+'</div>';
      heat.appendChild(cell);
    }});
    if(!months.length) heat.innerHTML='<div class="chip-line">No monthly returns.</div>';
  }}
  if(select){{
    strategies.forEach((s,i)=>{{ const opt=document.createElement('option'); opt.value=String(i); opt.textContent=s.strategy_name||('Strategy '+(i+1)); select.appendChild(opt); }});
    select.addEventListener('change',()=>choose(Number(select.value||0)));
  }}
  document.querySelectorAll('[data-strategy-row]').forEach(row=>row.addEventListener('click',()=>choose(Number(row.dataset.index||0))));
  document.querySelectorAll('[data-sort-key]').forEach(th=>{{
    th.addEventListener('click',()=>{{
      const key=th.dataset.sortKey;
      const asc=!th.classList.contains('sort-asc');
      document.querySelectorAll('[data-sort-key]').forEach(x=>x.classList.remove('sort-asc','sort-desc'));
      th.classList.add(asc?'sort-asc':'sort-desc');
      const metricKeys=['sharpe_ratio','annual_return','max_drawdown','win_rate','total_trades','profit_factor'];
      const rows=Array.from(document.querySelectorAll('[data-strategy-row]'));
      rows.sort((a,b)=>{{
        const sa=strategies[Number(a.dataset.index)]||{{}};
        const sb=strategies[Number(b.dataset.index)]||{{}};
        const av=metricKeys.includes(key)?Number((sa.metrics||{{}})[key] ?? -999999):0;
        const bv=metricKeys.includes(key)?Number((sb.metrics||{{}})[key] ?? -999999):0;
        return asc ? av-bv : bv-av;
      }});
      rows.forEach(row=>tbody&&tbody.appendChild(row));
    }});
  }});
  choose(0);
}})();
</script>"""
    if section_only:
        return body
    return html_page("回測 Dashboard", "backtest", body)


def build_original_sfz_backtest_reference_html() -> str:
    ma_path = V44_BACKTEST_OUTPUT_DIR / "sfz_ma_trailing_after_activation_summary.csv"
    wait_path = V44_BACKTEST_OUTPUT_DIR / "sfz_signal_wait_ta3_entry_summary.csv"
    unique_path = V44_BACKTEST_OUTPUT_DIR / "sfz_ta3_unique_entry_exit_compare.csv"
    ma10 = _find_backtest_row(ma_path, year="ALL", basket_type="ALL", ma_line="MA20", activation_pct="10")
    ma20 = _find_backtest_row(ma_path, year="ALL", basket_type="ALL", ma_line="MA20", activation_pct="20")
    strict20 = _find_backtest_row(wait_path, mode="Strict+漲過20%後MA20")
    unique20 = _find_backtest_row(unique_path, mode="TA3唯一進場+漲過20%後MA20")
    rows = [
        ("原 168 訊號｜漲過10%後 MA20", ma10, "signals", "原 SFZ/v42 入籃訊號，只有已觸發 +10% 的子集合。"),
        ("原 168 訊號｜漲過20%後 MA20", ma20, "signals", "原 SFZ/v42 入籃訊號，只有已觸發 +20% 的子集合。"),
        ("原訊號後等 20 日｜Strict+20% MA20", strict20, "filled", "訊號日後最多等 20 個交易日，等到 Strict 才進場。"),
        ("TA3 唯一進場｜+20% MA20", unique20, "signals", "去除同日同檔重複 setup 後的 TA3 進場。"),
    ]
    trs = ""
    for label, row, count_key, desc in rows:
        trs += f"""
<tr>
  <td><strong>{esc(label)}</strong><div class="signal-dates">{esc(desc)}</div></td>
  <td>{_num_from_row(row, count_key)}</td>
  <td>{_metric_from_row(row, "win_rate")}</td>
  <td class="{('pos' if float(row.get('avg_ret') or 0) >= 0 else 'neg') if row else ''}">{_metric_from_row(row, "avg_ret")}</td>
  <td>{_metric_from_row(row, "median_ret")}</td>
  <td>{_metric_from_row(row, "loss_over_5pct")}</td>
  <td>{_metric_from_row(row, "avg_days", "")}</td>
</tr>"""
    return f"""
<div class="card">
  <div class="section-label">原始回測基準｜不是網站逐日掃描</div>
  <div class="strategy-note">這裡直接讀取 <code>{esc(str(V44_BACKTEST_OUTPUT_DIR))}</code> 內先前產出的回測摘要。之前漂亮的數字主要來自「原 168 筆 SFZ/v42 訊號」與「啟動後 MA20」；不是把目前候選股每天重複掃描。</div>
  <div class="chip-line">停損差異：原始 MA20 trailing 沒有固定初始停損，未觸發 +10%/+20% 則以 90 日到期計算；TA3 結構停損則看破近 5 日結構低點、跌破 MA33 且未脫離成本、或箱型假突破。下方網站壓力測試才是用 3%~12% 支撐停損，找不到就預設 6%。</div>
  <div style="overflow-x:auto;margin-top:14px">
    <table class="stock-table">
      <thead><tr><th>策略母體</th><th>樣本</th><th>勝率</th><th>平均報酬</th><th>中位數</th><th>-5%以上虧損</th><th>平均天數</th></tr></thead>
      <tbody>{trs}</tbody>
    </table>
  </div>
</div>"""


def build_ta3_box_split_reference_html() -> str:
    summary_path = V44_BACKTEST_OUTPUT_DIR / "sfz_signal_wait_ta3_entry_summary.csv"
    setup_path = V44_BACKTEST_OUTPUT_DIR / "sfz_signal_wait_ta3_entry_by_setup.csv"
    strict = _find_backtest_row(summary_path, mode="Strict+漲過20%後MA20")
    soft = _find_backtest_row(summary_path, mode="Soft+漲過20%後MA20")
    setup_rows = _read_backtest_csv(setup_path)
    wanted = [
        ("Strict:箱型突破直接買", "Strict｜箱型突破直接買", "樣本少，但目前表現最好；比較適合當強加碼。"),
        ("Strict:箱型突破後回測買", "Strict｜箱型突破後回測買", "目前沒有樣本，不下結論。"),
        ("Soft:TA3-Soft短箱突破直接買", "Soft｜短箱突破直接買", "攻擊性較高，報酬漂亮但 -5% 虧損仍有 25%。"),
        ("Soft:TA3-Soft短箱突破後回測", "Soft｜短箱突破後回測", "較穩，但目前只有 2 筆，不能定案。"),
    ]
    setup_by_mode = {r.get("mode", ""): r for r in setup_rows}
    setup_html = ""
    for key, label, note in wanted:
        row = setup_by_mode.get(key, {})
        setup_html += f"""
<tr>
  <td><strong>{esc(label)}</strong><div class="signal-dates">{esc(note)}</div></td>
  <td>{_num_from_row(row, "filled")}</td>
  <td>{_metric_from_row(row, "win_rate")}</td>
  <td class="{('pos' if float(row.get('avg_ret') or 0) >= 0 else 'neg') if row else ''}">{_metric_from_row(row, "avg_ret")}</td>
  <td>{_metric_from_row(row, "median_ret")}</td>
  <td>{_metric_from_row(row, "loss_over_5pct")}</td>
</tr>"""
    fill_html = f"""
<tr><td>TA3-Strict</td><td>168</td><td>{_num_from_row(strict, "filled")}</td><td>{_metric_from_row(strict, "fill_rate")}</td><td>{_metric_from_row(strict, "win_rate")}</td><td>{_metric_from_row(strict, "avg_ret")}</td></tr>
<tr><td>TA3-Soft</td><td>168</td><td>{_num_from_row(soft, "filled")}</td><td>{_metric_from_row(soft, "fill_rate")}</td><td>{_metric_from_row(soft, "win_rate")}</td><td>{_metric_from_row(soft, "avg_ret")}</td></tr>"""
    return f"""
<div class="card">
  <div class="section-label">TA3 拆分基準｜原 168 訊號後等待 1-20 日</div>
  <div class="strategy-note">這段對應你貼的拆分結果：先用原 168 筆 SFZ/v42 訊號當母體，訊號日後 1-20 日等待 TA3 買點；箱型突破再拆成「直接買」與「突破後回測」。出場統一看「漲過20%後 MA20」。</div>
  <div class="grid grid-2">
    <div class="strategy-note">
      <strong>等到買點比例</strong>
      <div style="overflow-x:auto;margin-top:10px">
        <table class="stock-table">
          <thead><tr><th>買點層級</th><th>原訊號數</th><th>等到買點</th><th>Fill rate</th><th>勝率</th><th>平均報酬</th></tr></thead>
          <tbody>{fill_html}</tbody>
        </table>
      </div>
    </div>
    <div class="strategy-note">
      <strong>操作解讀</strong>
      <div class="chip-line">最佳用法仍是：原 SFZ 訊號日先試單；TA3-Soft 當續抱或小加碼；TA3-Strict 當強加碼。Soft 等買點不適合完全取代原訊號日買進，因為同批比較下略差。</div>
    </div>
  </div>
  <div style="overflow-x:auto;margin-top:14px">
    <table class="stock-table">
      <thead><tr><th>箱型拆分買點</th><th>筆數</th><th>勝率</th><th>平均報酬</th><th>中位數</th><th>-5%以上虧損</th></tr></thead>
      <tbody>{setup_html}</tbody>
    </table>
  </div>
</div>"""


def build_backtest_page(reports: list[dict], section_only: bool = False) -> str:
    results = build_backtest_results(reports)
    filled = [x for x in results if x.get("entry") is not None]
    closed = [x for x in filled if x.get("exit_reason") != "持有中"]
    open_positions = [x for x in filled if x.get("exit_reason") == "持有中"]
    wins = [x for x in closed if (x.get("ret") or 0) > 0]
    losses = [x for x in closed if (x.get("ret") or 0) <= 0]
    avg_ret = sum(x.get("ret") or 0 for x in filled) / len(filled) if filled else None
    avg_closed = sum(x.get("ret") or 0 for x in closed) / len(closed) if closed else None
    win_rate = len(wins) / len(closed) * 100 if closed else None
    best = max((x.get("ret") for x in filled if x.get("ret") is not None), default=None)
    worst = min((x.get("ret") for x in filled if x.get("ret") is not None), default=None)
    avg_hold = sum(x.get("hold_days") or 0 for x in filled if x.get("hold_days") is not None) / len([x for x in filled if x.get("hold_days") is not None]) if filled else None

    rows_html = ""
    for x in sorted(results, key=lambda r: (r.get("report_date", ""), r.get("sid", "")), reverse=True):
        ret = x.get("ret")
        ret_cls = "pos" if ret is not None and ret > 0 else "neg" if ret is not None and ret < 0 else ""
        status_cls = "tag-green" if x.get("status") == "停利" else "tag-red" if x.get("status") == "停損/出場" else "tag-yellow" if x.get("status") == "持有中" else "tag"
        href = f"stocks/{x['sid']}.html"
        rows_html += f"""
<tr>
  <td><a class="stock-link" href="{href}">{esc(x['sid'])} {esc(x['name'])}</a><div class="signal-dates">{esc(x['basket'])}｜報告 {esc(x['report_date'])}</div></td>
  <td><span class="tag {status_cls}">{esc(x['status'])}</span><div class="signal-dates">{esc(x.get('exit_reason',''))}</div></td>
  <td>{esc(x.get('entry_range','─'))}<div class="signal-dates">成交 {esc(x.get('entry_date','─'))}｜{fmt_num(x.get('entry'))}</div></td>
  <td>{esc(x.get('exit_date','─'))}<div class="signal-dates">出場 {fmt_num(x.get('exit_price'))}</div></td>
  <td class="{ret_cls}" style="font-weight:800">{'─' if ret is None else f'{ret:+.1f}%'}</td>
  <td>{'─' if x.get('hold_days') is None else str(x.get('hold_days'))}</td>
  <td><div class="price-target">目 {fmt_num(x.get('target'))}</div><div class="price-stop">初停 {fmt_num(x.get('stop'))}</div></td>
</tr>"""

    if not rows_html:
        rows_html = '<tr><td colspan="7" style="color:#8b949e">目前資料不足，還無法形成回測結果。</td></tr>'

    body = f"""
<div class="container">
  <div class="page-title">歷史回測</div>
  <div class="page-sub">先看原始 168 筆 SFZ 回測基準，再看網站逐日掃描壓力測試；兩者分母不同，不能直接混成同一個勝率。</div>
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num">{len(results)}</div><div class="metric-label">歷史訊號</div></div>
    <div class="metric"><div class="metric-num">{len(filled)}</div><div class="metric-label">已觸及買入區</div></div>
    <div class="metric"><div class="metric-num">{len(open_positions)}</div><div class="metric-label">持有中</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(win_rate,1)}%</div><div class="metric-label">已出場勝率</div></div>
    <div class="metric"><div class="metric-num {('pos' if (avg_ret or 0) >= 0 else 'neg')}">{fmt_num(avg_ret,1)}%</div><div class="metric-label">平均報酬，含持有中</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(avg_hold,1)}</div><div class="metric-label">平均持有天數</div></div>
  </div>
  <div class="card">
    <div class="section-label">回測規則</div>
    <div class="strategy-note">用報告當日以前的資料計算買入區與初始停損；報告日後若日K區間碰到買入區視為成交。成交後同一天同時碰停損/停利時採保守停損優先；尚未碰停利或停損者以最新收盤列為持有中。</div>
    <div class="chip-line">勝率＝已出場且實現報酬 &gt; 0 的筆數 / 已出場筆數，不含持有中。已出場：{len(closed)} 筆｜停利/獲利：{len(wins)} 筆｜停損/虧損：{len(losses)} 筆｜平均已實現：{fmt_num(avg_closed,1)}%｜最佳：{fmt_num(best,1)}%｜最差：{fmt_num(worst,1)}%</div>
  </div>
{build_historical_scan_html(reports)}
{build_original_sfz_backtest_reference_html()}
{build_ta3_box_split_reference_html()}
{build_entry_variant_comparison_html(reports)}
  <div class="card">
    <div class="section-label">逐筆追蹤</div>
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>狀態</th><th>買入區/成交</th><th>出場</th><th>報酬</th><th>天數</th><th>目標/初停</th></tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>
</div>"""
    if section_only:
        return body
    return html_page("歷史回測", "backtest", body)


def build_stock_pages(reports: list[dict]) -> int:
    stock_map = build_stock_query_map(reports)
    ledger = build_signal_ledger(reports)
    out_dir = OUTPUT_DIR / "stocks"
    out_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for stock_id, s in sorted(stock_map.items()):
        (out_dir / f"{stock_id}.html").write_text(build_stock_detail_page(stock_id, s, ledger), encoding="utf-8")
        count += 1
    return count


def build_history_page(reports, section_only=False):
    items = ""
    for r in reports:
        cnt   = len(r.get("stocks", []))
        green  = sum(1 for s in r.get("stocks", []) if s["icon"] == "\U0001f7e2")
        yellow = sum(1 for s in r.get("stocks", []) if s["icon"] == "\U0001f7e1")
        red    = sum(1 for s in r.get("stocks", []) if s["icon"] == "\U0001f534")
        items += (
            '<div class="history-item">'
            '<div>'
            + f'<div class="history-date">{r["date"]}</div>'
            + f'<div class="history-meta">'
            + f'<span style="color:#3fb950">G:{green}</span>&nbsp;'
            + f'<span style="color:#d2a520">Y:{yellow}</span>&nbsp;'
            + f'<span style="color:#f85149">R:{red}</span>&nbsp;&middot;&nbsp;{cnt} stocks'
            + '</div></div>'
            + f'<a href="daily/{r["date"]}.html" class="history-link">View &rarr;</a>'
            + '</div>'
        )
    body = (
        '<div class="container">'
        + f'<div class="page-title">History ({len(reports)} reports)</div>'
        + '<div class="page-sub"><a href="history.html#backtest">查看歷史回測 →</a></div>'
        + '<div class="card">' + items + '</div>'
        + '</div>'
    )
    if section_only:
        return body
    return html_page("History", "history", body)




def build_history_combined_page(reports: list[dict]) -> str:
    """歷史分析 — 2 tabs: backtest / reports"""
    if os.environ.get("SITE_FULL_BACKTEST") == "1":
        tab1 = build_backtest_page(reports, section_only=True)
    else:
        payload = load_backtest_dashboard_payload()
        tab1 = build_backtest_dashboard_page(payload, section_only=True)
    tab2 = build_history_page(reports, section_only=True)

    body = f"""
<div class="container" id="history-tabs">
  <div class="page-title">歷史分析</div>
  <div class="page-sub">同一個分析入口：先看回測策略績效與風險，再切到每日歷史報告；回測與報告不再分散在主導覽。</div>
  <div class="tab-bar">
    <button class="tab-btn active" data-tab="backtest">📊 歷史回測</button>
    <button class="tab-btn" data-tab="reports">📄 歷史報告</button>
  </div>
  <div class="tab-panel active" id="backtest">{tab1}</div>
  <div class="tab-panel" id="reports">{tab2}</div>
</div>
{TAB_JS}
<script>initTabs('history-tabs')</script>"""
    return html_page("歷史分析", "history", body)


def build_selection_page(reports: list[dict]) -> str:
    """選股池 — 3 tabs: top20 / sfz / tracking"""
    tab1 = build_latest_daily_page(reports, section_only=True)
    tab2 = build_baskets_page(reports, section_only=True)
    tab3 = build_signals_page(reports, section_only=True)

    body = f"""
<div class="container" id="selection-tabs">
  <div class="page-title">選股池</div>
  <div class="page-sub">從每日 Top20 出發，拆成 SFZ 雙籃，追蹤歷史入選紀錄。</div>
  <div class="tab-bar">
    <button class="tab-btn tab-link active" data-tab="daily-top20">🏆 每日 Top20</button>
    <button class="tab-btn tab-link" data-tab="sfz-baskets">🧺 SFZ 雙籃</button>
    <button class="tab-btn tab-link" data-tab="signal-ledger">📡 入選追蹤</button>
  </div>
  <div class="tab-panel active" id="daily-top20">{tab1}</div>
  <div class="tab-panel" id="sfz-baskets">{tab2}</div>
  <div class="tab-panel" id="signal-ledger">{tab3}</div>
</div>
{TAB_JS}
<script>initTabs('selection-tabs')</script>"""
    return html_page("選股池", "selection", body)


def build_timing_page(reports: list[dict]) -> str:
    """買賣時機 — 2 tabs: radar / carybot"""
    tab1 = build_buy_radar_page(reports, section_only=True)
    tab2 = build_carybot_validation_page(reports, section_only=True)

    body = f"""
<div class="container" id="timing-tabs">
  <div class="page-title">買賣時機</div>
  <div class="page-sub">選完股後，用買點雷達看離買入區多遠，用 CaryBot 驗證買賣信號強度。</div>
  <div class="tab-bar">
    <button class="tab-btn active" data-tab="buy-radar">🎯 買點雷達</button>
    <button class="tab-btn" data-tab="carybot">🤖 CaryBot 驗證</button>
  </div>
  <div class="tab-panel active" id="buy-radar">{tab1}</div>
  <div class="tab-panel" id="carybot">{tab2}</div>
</div>
{TAB_JS}
<script>initTabs('timing-tabs')</script>"""
    return html_page("買賣時機", "timing", body)


def main():
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print("[Stockfrom] Site Generator v1.0", flush=True)
    print(f"   Reports: {REPORTS_DIR}", flush=True)
    print(f"   Output:  {OUTPUT_DIR}", flush=True)

    (OUTPUT_DIR / "daily").mkdir(parents=True, exist_ok=True)
    write_static_assets()
    print("   [OK] css/components.css, js/auto-expand-placeholder.js", flush=True)
    public_data = publish_data_assets()
    if public_data:
        print(f"   [OK] data/{', data/'.join(public_data)}", flush=True)
    reports = load_reports()

    if not reports:
        print("[ERROR] No reports parsed or cached.", flush=True)
        return
    set_site_latest_report_date(reports)

    print("\n[Build] Generating pages...", flush=True)
    (OUTPUT_DIR / "index.html").write_text(build_index_page(reports), encoding="utf-8")
    print("   [OK] index.html", flush=True)
    (OUTPUT_DIR / "institutional-flow.html").write_text(build_institutional_flow_page(), encoding="utf-8")
    print("   [OK] institutional-flow.html", flush=True)
    (OUTPUT_DIR / "holder-risers.html").write_text(build_weekly_holder_risers_page(), encoding="utf-8")
    print("   [OK] holder-risers.html", flush=True)
    (OUTPUT_DIR / "selection.html").write_text(build_selection_page(reports), encoding="utf-8")
    print("   [OK] selection.html", flush=True)
    (OUTPUT_DIR / "daily.html").write_text(redirect_page("selection.html#daily-top20", "每日Top20"), encoding="utf-8")
    print("   [OK] daily.html (redirect)", flush=True)
    (OUTPUT_DIR / "mda.html").write_text(build_mda_page(reports), encoding="utf-8")
    print("   [OK] mda.html", flush=True)
    (OUTPUT_DIR / "mda_launched.html").write_text(redirect_page("mda.html#launched", "M大已發動"), encoding="utf-8")
    print("   [OK] mda_launched.html (redirect)", flush=True)
    (OUTPUT_DIR / "mda_consolidation.html").write_text(redirect_page("mda.html#consolidation", "M大盤整"), encoding="utf-8")
    print("   [OK] mda_consolidation.html (redirect)", flush=True)
    (OUTPUT_DIR / "baskets.html").write_text(redirect_page("selection.html#sfz-baskets", "SFZ雙籃"), encoding="utf-8")
    print("   [OK] baskets.html (redirect)", flush=True)
    (OUTPUT_DIR / "signals.html").write_text(redirect_page("selection.html#signal-ledger", "入選追蹤"), encoding="utf-8")
    print("   [OK] signals.html (redirect)", flush=True)
    (OUTPUT_DIR / "stocks.html").write_text(build_stocks_index_page(reports), encoding="utf-8")
    print("   [OK] stocks.html", flush=True)
    (OUTPUT_DIR / "timing.html").write_text(build_timing_page(reports), encoding="utf-8")
    print("   [OK] timing.html", flush=True)
    (OUTPUT_DIR / "radar.html").write_text(redirect_page("timing.html#buy-radar", "買點雷達"), encoding="utf-8")
    print("   [OK] radar.html (redirect)", flush=True)
    (OUTPUT_DIR / "carybot.html").write_text(redirect_page("timing.html#carybot", "CaryBot驗證"), encoding="utf-8")
    print("   [OK] carybot.html (redirect)", flush=True)
    (OUTPUT_DIR / "backtest_dashboard.html").write_text(build_backtest_dashboard_page(), encoding="utf-8")
    print("   [OK] backtest_dashboard.html", flush=True)
    (OUTPUT_DIR / "backtest.html").write_text(redirect_page("backtest_dashboard.html", "歷史回測"), encoding="utf-8")
    print("   [OK] backtest.html (redirect)", flush=True)
    (OUTPUT_DIR / "history.html").write_text(build_history_combined_page(reports), encoding="utf-8")
    print("   [OK] history.html", flush=True)
    stock_page_count = build_stock_pages(reports)
    print(f"   [OK] stocks/*.html ({stock_page_count})", flush=True)
    mda_stock_page_count = build_mda_stock_pages(reports)
    print(f"   [OK] mda_stocks/*.html ({mda_stock_page_count})", flush=True)
    mda_candidate_page_count = build_mda_candidate_pages()
    print(f"   [OK] mda_candidates/*.html ({mda_candidate_page_count})", flush=True)

    for r in reports:
        html = build_daily_page(r)
        out = OUTPUT_DIR / "daily" / f"{r['date']}.html"
        out.write_text(html, encoding="utf-8")
        print(f"   [OK] daily/{r['date']}.html", flush=True)

    sitemap_urls = ["index.html", "institutional-flow.html", "holder-risers.html", "selection.html", "mda.html", "timing.html", "stocks.html", "backtest_dashboard.html", "history.html"]
    sitemap_urls += [f"stocks/{p.name}" for p in sorted((OUTPUT_DIR / "stocks").glob("*.html"))]
    sitemap = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + ''.join(
        f"  <url><loc>https://tcfsh010778.github.io/stock-from-Hsiu/{u}</loc></url>\n" for u in sitemap_urls
    ) + '</urlset>\n'
    (OUTPUT_DIR / "sitemap.xml").write_text(sitemap, encoding="utf-8")
    (OUTPUT_DIR / "robots.txt").write_text("User-agent: *\nAllow: /\nSitemap: https://tcfsh010778.github.io/stock-from-Hsiu/sitemap.xml\n", encoding="utf-8")
    print("   [OK] sitemap.xml / robots.txt", flush=True)

    print(f"\n[Done] {len(reports)+10+stock_page_count+mda_stock_page_count+mda_candidate_page_count} files -> {OUTPUT_DIR}", flush=True)
    print("[Next] git init && git add . && git commit && push to GitHub Pages", flush=True)


if __name__ == "__main__":
    main()
