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
LOCAL_PRICE_DIR = LOCAL_DATA_DIR / "prices"
LOCAL_CHIP_DIR = LOCAL_DATA_DIR / "chips"
LOCAL_HOLDING_DIR = LOCAL_DATA_DIR / "holding_shares"
LOCAL_FOREIGN_SHAREHOLDING_DIR = LOCAL_DATA_DIR / "foreign_shareholding"
LOCAL_MARGIN_DIR = LOCAL_DATA_DIR / "margin"
REPORTS_CACHE_PATH = LOCAL_DATA_DIR / "site_reports.json"
MARKET_CACHE_PATH = LOCAL_DATA_DIR / "stock_markets.json"
V44_PRICE_DIR = V44_ROOT / "回測" / "v6_outputs" / "prices"
V44_CHIP_DIR = V44_ROOT / "回測" / "v6_outputs" / "chips"
V44_HOLDING_DIR = V44_ROOT / "回測" / "v6_outputs" / "holding_shares"
V44_FOREIGN_SHAREHOLDING_DIR = V44_ROOT / "回測" / "v6_outputs" / "foreign_shareholding"
V44_MARGIN_DIR = V44_ROOT / "回測" / "v6_outputs" / "margin"
V44_DB_PATH = V44_ROOT / "v9_reports" / "stockfromshu_records.sqlite"
_V44_FETCHER = None

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
    ### 1. 🟢 6213 聯茂 ｜健康整理 ｜ Score: 200.9
    | 收盤價 | **253.5 元** |
    """
    stock_pattern = re.compile(
        r"### \d+\. ([🟢🟡🔴]) (\d{4}) (.+?) ｜(.+?)｜ Score: ([\d.]+)(.*?)(?=### \d+\.|---|\Z)",
        re.DOTALL
    )
    for m in stock_pattern.finditer(text):
        status_icon = m.group(1)
        stock_id    = m.group(2)
        stock_name  = m.group(3).strip()
        score       = float(m.group(5))
        block       = m.group(6)

        def ext(label, blk=block):
            p = re.search(rf"\| {label} \| \*?\*?(.*?)\*?\*? \|", blk)
            return _clean_cell(p.group(1)) if p else "─"

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
            "score":         score,
            "price":         ext("收盤價"),
            "gain_6w":       ext("近6週漲幅"),
            "rsi":           ext("RSI\\(14\\)"),
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
                "score":         round((21 - stock_no) * 10.0, 1),
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
                "score_source":   "排名換算（第1名200，每名-10）",
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


def load_stock_market_map() -> dict[str, str]:
    """Load listed/OTC market map from official daily quote APIs, with a local cache fallback."""
    if MARKET_CACHE_PATH.exists():
        try:
            cache = json.loads(MARKET_CACHE_PATH.read_text(encoding="utf-8"))
            updated_at = datetime.fromisoformat(cache.get("updated_at", "1970-01-01T00:00:00"))
            if datetime.now() - updated_at < timedelta(days=1):
                return cache.get("markets", {})
        except Exception:
            pass

    markets: dict[str, str] = {}
    errors: list[str] = []
    try:
        for row in _fetch_json(TWSE_STOCK_DAY_ALL_URL):
            code = str(row.get("Code", "")).strip()
            if re.fullmatch(r"\d{4}", code):
                markets[code] = "上市"
    except Exception as exc:
        errors.append(f"TWSE {exc}")

    try:
        for row in _fetch_json(TPEX_DAILY_CLOSE_URL):
            code = str(row.get("SecuritiesCompanyCode", "")).strip()
            if re.fullmatch(r"\d{4}", code):
                markets[code] = "上櫃"
    except Exception as exc:
        errors.append(f"TPEX {exc}")

    if markets:
        LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
        MARKET_CACHE_PATH.write_text(
            json.dumps({"updated_at": datetime.now().isoformat(timespec="seconds"), "markets": markets}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"   [Market] loaded {len(markets)} listed/OTC codes", flush=True)
        return markets

    if MARKET_CACHE_PATH.exists():
        cache = json.loads(MARKET_CACHE_PATH.read_text(encoding="utf-8"))
        print(f"   [Market] using cached market map after fetch failure: {'; '.join(errors)}", flush=True)
        return cache.get("markets", {})

    print(f"   [Market][WARN] market map unavailable, skip listed/OTC filter: {'; '.join(errors)}", flush=True)
    return {}


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
            LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
            REPORTS_CACHE_PATH.write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
            return filter_listed_otc_reports(reports)

    if REPORTS_CACHE_PATH.exists():
        print(f"\n[Read] No MD reports found; using cache {REPORTS_CACHE_PATH}", flush=True)
        return filter_listed_otc_reports(json.loads(REPORTS_CACHE_PATH.read_text(encoding="utf-8")))

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
.metric{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px}
.metric-num{font-size:26px;font-weight:800;color:#e6edf3}
.metric-label{font-size:12px;color:#6e7681;margin-top:2px}
.action-list{display:grid;gap:10px;margin-top:12px}
.action-row{display:grid;grid-template-columns:1.2fr repeat(5,minmax(76px,1fr));gap:10px;align-items:center;background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:10px}
.action-row .label{font-size:11px;color:#6e7681}
.action-row .value{font-size:13px;color:#e6edf3;font-weight:800;margin-top:2px}
.action-row .note{font-size:12px;color:#8b949e;line-height:1.5}
.market-light{display:grid;grid-template-columns:180px 1fr;gap:14px;align-items:stretch}
.market-badge{display:flex;align-items:center;justify-content:center;border-radius:10px;border:1px solid #30363d;background:#0d1117;font-size:28px;font-weight:900}
.market-badge.pos{border-color:rgba(63,185,80,.45);background:rgba(63,185,80,.09)}
.market-badge.neu{border-color:rgba(210,153,34,.45);background:rgba(210,153,34,.09);color:#d2a520}
.market-badge.neg{border-color:rgba(248,81,73,.45);background:rgba(248,81,73,.09)}
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
.pos{color:#3fb950}
.neg{color:#f85149}

/* Entry/Target/Stop */
.price-row{display:flex;flex-direction:column;gap:2px}
.price-entry{color:#58a6ff;font-size:12px}
.price-target{color:#3fb950;font-size:12px}
.price-stop{color:#f85149;font-size:12px}
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

/* Footer */
footer{text-align:center;padding:24px 20px;color:#484f58;font-size:12px;border-top:1px solid #30363d;margin-top:48px}
footer .disclaimer{color:#e74c3c;margin-top:6px;font-size:11px}

/* Responsive */
@media(max-width:768px){
  .stock-table{font-size:12px}
  .stock-table .hide-mobile{display:none}
  .filter-steps{flex-direction:column}
  .grid-2,.grid-3{grid-template-columns:1fr}
  .ma-strip{grid-template-columns:repeat(2,minmax(0,1fr))}
  .detail-hero,.info-grid,.tech-panel,.tech-summary-grid,.telegram-head,.telegram-line,.action-row,.market-light,.check-grid,.alert-row{grid-template-columns:1fr}
  .telegram-head{display:grid}
  .telegram-rating{text-align:left}
}
"""

def nav_html(active: str = "home", prefix: str = "") -> str:
    tabs = [
        ("home",    "index.html",   "首頁"),
        ("daily",   "daily.html",   "今日選股"),
        ("mda",     "mda.html",     "M大選股"),
        ("basket",  "baskets.html", "雙籃儀表板"),
        ("signals", "signals.html", "訊號追蹤"),
        ("stocks",  "stocks.html",  "個股總覽"),
        ("radar",   "radar.html",   "買點雷達"),
        ("backtest", "backtest.html", "歷史回測"),
        ("history", "history.html", "歷史報告"),
    ]
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
    return f"""
<footer>
  <p>資料來源：FinMind 付費版 · TWSE · Yahoo Finance</p>
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


def stock_href(stock_id: str, prefix: str = "stocks") -> str:
    return f"{prefix}/{esc(stock_id)}.html"


def html_page(title: str, nav_key: str, body: str, nav_prefix: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title} — Stockfrom脩 選股站</title>
<meta name="description" content="量化選股 · 每日精選 Top 20 · ABC籌碼分析 · 台股研究">
<style>{CSS}</style>
</head>
<body>
{nav_html(nav_key, nav_prefix)}
{body}
{footer_html()}
</body>
</html>"""


# ──────────────────────────────────────────────
#  各頁面生成
# ──────────────────────────────────────────────

def build_stock_table(stocks: list[dict], compact: bool = False, stock_link_prefix: str = "stocks") -> str:
    """生成股票表格 HTML"""
    rows = ""
    for i, s in enumerate(stocks, 1):
        s = enrich_stock_fields(s)
        _, tech, decision = stock_trade_context(s)
        plan = decision
        badge = basket_badge(s)
        gain_cls = gain_color(s["gain_6w"])

        if compact:
            rows += f"""
<tr>
  <td><span style="color:#6e7681;font-size:11px">#{i}</span></td>
  <td>
    <div><a class="stock-link" href="{stock_href(s['id'], stock_link_prefix)}">{s['id']} {s['name']}</a></div>
    <div style="margin-top:3px">{badge}</div>
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
  </td>
  <td>{badge}</td>
  <td class="price-main">{s['price']}</td>
  <td class="{gain_cls}" style="font-weight:600">{s['gain_6w']}</td>
  <td style="color:#8b949e">{s['rsi']}</td>
  <td style="color:#8b949e">{s['bband_pct']}</td>
  <td class="hide-mobile" style="color:#8b949e">{s['vol_5d']}</td>
  <td class="hide-mobile" style="color:{'#3fb950' if s['foreign_5d'].startswith('+') else '#f85149' if s['foreign_5d'].startswith('-') else '#8b949e'}">{s['foreign_5d']}</td>
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
        header = """<tr>
  <th>#</th><th>代號/名稱</th><th>狀態</th><th>收盤</th>
  <th>近6週漲幅</th><th>RSI</th><th>%B</th>
  <th class="hide-mobile">近5日量</th><th class="hide-mobile">外資近5日</th>
  <th>評分</th><th>進場/目標/初停/R:R</th>
</tr>"""

    return f"""<div style="overflow-x:auto">
<table class="stock-table">
<thead>{header}</thead>
<tbody>{rows}</tbody>
</table>
</div>"""


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


def classify_basket(s: dict) -> str:
    gain = _to_float(s.get("gain_6w", "0"))
    score = _to_float(s.get("score", "0"))
    icon = s.get("icon", "")
    status = s.get("status", "")
    if icon == "🔴" or "超買" in status:
        return "risk"
    if icon == "🟡" or gain >= 18 or score >= 170:
        return "marching"
    return "consolidation"


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
        return "B1主力已離開"
    if structure_ok:
        return "B1主力未離開"
    if flow_bad:
        return "B1短線轉弱"
    return "B1資料不足"


def basket_reason(s: dict, tech: dict | None = None, chip_series: list[dict] | None = None, holding: dict | None = None) -> str:
    basket = classify_basket(s)
    gain = _to_float(s.get("gain_6w", "0"))
    score = _to_float(s.get("score", "0"))
    status = s.get("status", "")
    icon = s.get("icon", "")
    tech = tech or {}
    checks = []
    trend = tech.get("trend") or tech.get("trend_pattern")
    volume_price = tech.get("volume_price") or "量價資料不足"
    force_status = b1_force_status(s, chip_series, holding)

    if basket == "marching":
        checks.append("行進籃")
        if score >= 170:
            checks.append("評分>=170")
        if gain >= 18:
            checks.append("近6週漲幅>=18%")
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
    else:
        if icon == "🔴" or "超買" in status:
            checks.append("原報告風險/超買")
        if gain >= 18:
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
    event_stock = enrich_stock_fields(dict(s))
    event_stock["report_date"] = report_date
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
                item = enrich_stock_fields(s)
                item["report_date"] = report.get("date", "")
                stocks[sid] = item
    return stocks


def read_price_history(stock_id: str, limit: int = 760) -> list[dict]:
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
    if not rows:
        months = int(os.environ.get("V44_FETCH_MONTHS", "24"))
        rows = fetch_v44_price_history(stock_id, months=months)
    return rows[-limit:]


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
    rows = []
    last_close = None
    for item in chip_series[-30:]:
        close = close_by_date.get(item.get("date"))
        if close is not None:
            last_close = close
        rows.append({
            "date": item.get("date", ""),
            "total": item.get("total"),
            "close": last_close,
        })
    return [r for r in rows if r.get("close") is not None]


def _holding_group(level: str) -> str:
    text = str(level)
    if text in {"total", "差異數調整（說明4）"}:
        return "other"
    nums = [int(x.replace(",", "")) for x in re.findall(r"\d[\d,]*", text)]
    if "more than" in text or (nums and max(nums) >= 1000001):
        return "major"
    if nums and max(nums) >= 400001:
        return "large"
    if nums and max(nums) <= 10000:
        return "retail"
    return "middle"


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
        result = {"major": 0.0, "large": 0.0, "retail": 0.0, "middle": 0.0, "total_people": None}
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
        item = {"date": date, "major": 0.0, "large": 0.0, "retail": 0.0, "total_people": None}
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
            if group in {"major", "large", "retail"}:
                item[group] += pct
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


def holding_payload(series: list[dict]) -> list[dict]:
    return [
        {
            "date": item.get("date", ""),
            "major": item.get("major"),
            "large": item.get("large"),
            "retail": item.get("retail"),
            "totalPeople": item.get("total_people"),
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
            "retail": item.get("retail"),
            "totalPeople": item.get("total_people"),
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
            "retail": latest_h.get("retail") if latest_h else None,
            "total_people": latest_h.get("total_people") if latest_h else None,
            "holding_date": latest_h.get("date") if latest_h else "",
            "foreign": chip.get("foreign"),
        })
    return out


def get_v44_fetcher():
    global _V44_FETCHER
    if _V44_FETCHER is not None:
        return _V44_FETCHER
    if os.environ.get("V44_LIVE_FETCH", "0") == "0":
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
    keys = [("major", "大戶>1000張", "#f85149"), ("large", "400~1000張", "#d2a520"), ("retail", "散戶<1萬股", "#3fb950")]
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
        })
    rows = [r for r in rows if r.get("date")]
    if len(rows) < 2:
        return '<div class="strategy-note">籌碼資料不足，暫時無法形成 TradingView 籌碼圖。</div>'
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
  function wireRange(chart){{
    chart.timeScale().fitContent();
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
    return `<b>${{x.date}}</b><br>主力合計 ${{fmtInt(x.total)}} 張<br>收盤價 ${{fmt(x.close)}}`;
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
    <div class="chip-line">柱狀圖為三大法人合計；折線分別為外資、投信、自營商。可縮放、拖曳，並限制在資料範圍內。</div>
    <div id="{panel_id}-flow" class="tv-chip-chart"></div>
    <div class="tv-tooltip"></div>
  </div>
  <div class="tv-chart-panel">
    <div class="tv-chart-title">主力增減張數與收盤價關係</div>
    <div class="chip-line">左軸為主力合計張數，右軸為收盤價；紅柱買超、綠柱賣超。</div>
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
    force_line = (
        f"外資5日 {_value_or_dash(foreign5)}；"
        f"三大法人當日 {fmt_num(chip_latest.get('total'), 0)} 張；"
        f"大戶 {fmt_num(h_latest.get('major'))}% / 散戶 {fmt_num(h_latest.get('retail'))}%"
    )
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
      <div class="telegram-meta">收盤日期 {esc(price_date)}｜報告日期 {esc(s.get('report_date','─'))}</div>
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
    basket = basket_label(classify_basket(s))
    events = ledger_item.get("events", []) if ledger_item else []
    repeat_note = f"歷史入選 {len(events)} 次，最近 {events[-1]['date']}。" if events else "首次或尚未建立歷史台帳。"
    sid = s.get("id", "")
    daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily") if sid else []
    decision = build_trade_decision(technical_snapshot(daily, s), s) if daily else {
        "rating": "觀望",
        "entry_range": "資料不足",
        "defense": "資料不足",
        "reason": "等待價格快取更新",
    }
    if basket == "行進籃":
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
    elif profit is not None and profit > 20 and weekly_turn and last2_drop:
        level, cls = "波段轉弱", "exit"
        reasons.append("漲幅>20%，週K轉折且日K連兩根跌逾3%")
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
    rows = [x for x in series[-CHART_LOOKBACK_BARS:] if x.get("major") is not None or x.get("retail") is not None or x.get("total_people") is not None]
    if len(rows) < 2:
        return '<div class="strategy-note">股權分配資料不足。</div>'
    w, h = 900, 150
    pad_l, pad_r, pad_t, pad_b = 50, 56, 22, 24
    plot_h = h - pad_t - pad_b
    pct_series = [
        ("major", "大戶", "#f85149"),
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
    legend = f'<text x="{w-218}" y="14" fill="#f85149" font-size="10">大戶</text><text x="{w-158}" y="14" fill="#3fb950" font-size="10">散戶</text><text x="{w-98}" y="14" fill="#58a6ff" font-size="10">股東數</text>'
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
    daily = aggregate_ohlcv(read_price_history(sid), "daily")
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
    if not chip and not holding:
        return '<div class="strategy-note">尚未找到籌碼/股權分配快取；刷新 FinMind 後會顯示法人買賣超與大戶比例。</div>'
    chip_latest = chip.get("latest", {})
    chip_sum5 = chip.get("sum5", {})
    chip_sum10 = chip.get("sum10", {})
    h_latest = holding.get("latest", {}) if holding else {}
    h_prev = holding.get("prev", {}) if holding else {}
    major_delta = h_latest.get("major", 0) - h_prev.get("major", 0) if h_latest and h_prev else None
    retail_delta = h_latest.get("retail", 0) - h_prev.get("retail", 0) if h_latest and h_prev else None
    chip_note = (
        f"外資10日 {fmt_num(chip_sum10.get('foreign'),0)} 張｜"
        f"主力10日 {fmt_num(chip_sum10.get('total'),0)} 張｜"
        f"大戶週變化 {fmt_num(major_delta)}%｜散戶週變化 {fmt_num(retail_delta)}%"
    )
    return f"""<div class="info-grid">
  <div class="info-cell"><div class="k">外資買賣超</div><div class="v {('pos' if chip_latest.get('foreign',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('foreign'),0)}張</div></div>
  <div class="info-cell"><div class="k">投信買賣超</div><div class="v {('pos' if chip_latest.get('trust',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('trust'),0)}張</div></div>
  <div class="info-cell"><div class="k">自營商買賣超</div><div class="v {('pos' if chip_latest.get('dealer',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('dealer'),0)}張</div></div>
  <div class="info-cell"><div class="k">主力當日合計</div><div class="v {('pos' if chip_latest.get('total',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('total'),0)}張</div></div>
  <div class="info-cell"><div class="k">外資5日</div><div class="v {('pos' if chip_sum5.get('foreign',0)>=0 else 'neg')}">{fmt_num(chip_sum5.get('foreign'),0)}張</div></div>
  <div class="info-cell"><div class="k">投信5日</div><div class="v {('pos' if chip_sum5.get('trust',0)>=0 else 'neg')}">{fmt_num(chip_sum5.get('trust'),0)}張</div></div>
  <div class="info-cell"><div class="k">主力5日合計</div><div class="v {('pos' if chip_sum5.get('total',0)>=0 else 'neg')}">{fmt_num(chip_sum5.get('total'),0)}張</div></div>
</div>
<div class="holding-info-grid">
  <div class="info-cell"><div class="k">大戶比例</div><div class="v">{fmt_num(h_latest.get('major'))}%</div></div>
  <div class="info-cell"><div class="k">散戶比例</div><div class="v">{fmt_num(h_latest.get('retail'))}%</div></div>
  <div class="info-cell"><div class="k">總股東人數</div><div class="v">{fmt_num(h_latest.get('total_people'),0)}</div></div>
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
  <div style="font-size:12px;color:#c9d1d9">買點 {plan['entry_text']} ｜ 目標 {plan['target_text']} ｜ 初始停損 {plan['initial_stop_text']} ｜ <span class="price-rr {plan['rr_class']}">R:R {plan['rr_text']}</span></div>
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


def build_today_action_card(stocks: list[dict]) -> str:
    items = []
    for s in stocks:
        s = enrich_stock_fields(s)
        _, tech, decision = stock_trade_context(s)
        gap = tech.get("entry_gap") if tech else None
        if gap is None:
            continue
        items.append({
            "sid": s.get("id", ""),
            "name": s.get("name", ""),
            "basket": basket_label(classify_basket(s)),
            "reason": basket_reason(s, tech),
            "gap": gap,
            "close": tech.get("close"),
            "score": _to_float(s.get("score", "0")),
            "plan": decision,
        })
    executable = [x for x in items if -3 <= x["gap"] <= 3]
    waiting = [x for x in items if 3 < x["gap"] <= 8 or -8 <= x["gap"] < -3]
    executable.sort(key=lambda x: (0 if (x["plan"].get("rr") or 0) >= 1.5 else 1, abs(x["gap"]), -x["score"]))
    waiting.sort(key=lambda x: (abs(x["gap"]), -x["score"]))

    return f"""
<div class="card">
  <div class="section-label">今日可執行清單</div>
  <div class="strategy-note">收盤落在買點 ±3% 內列為「明天開盤可掛單」；距離較近但還沒到位的放在等待區。</div>
  <h3 style="font-size:15px;margin:14px 0 0;color:#e6edf3">明天開盤可掛單</h3>
  {build_action_rows(executable[:5], "今日沒有收盤落在買點 ±3% 內的標的。")}
  <h3 style="font-size:15px;margin:16px 0 0;color:#e6edf3">繼續等待</h3>
  {build_action_rows(waiting[:5], "目前沒有接近但尚未到位的候選。")}
</div>"""


def build_market_light_card(latest: dict, stocks: list[dict]) -> str:
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
    checks.append(("大盤指數", "TAIEX快取尚未接入", "neu"))

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
    overview = latest.get("market_overview", "").strip()
    overview_html = f'<div class="market-text" style="margin-top:12px">{overview.replace(chr(10), "<br>")}</div>' if overview else ""
    return f"""
<div class="card">
  <div class="section-label">大盤燈號</div>
  <div class="market-light">
    <div class="market-badge {cls}">{light}</div>
    <div>
      <div style="font-size:16px;font-weight:800;color:#e6edf3">{title}</div>
      <div class="strategy-note" style="margin-top:4px">大盤指數資料尚未接入時，先用候選池結構與可執行買點做風控前提；缺資料會明確顯示。</div>
      <div class="check-grid" style="margin-top:10px">{check_html}</div>
    </div>
  </div>
  {overview_html}
</div>"""


def build_sell_alert_card(stocks: list[dict], limit: int = 6) -> str:
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
        alerts.append({
            "id": s.get("id", ""),
            "name": s.get("name", ""),
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
    return f"""
<div class="card">
  <div class="section-label">持倉 / 追蹤賣出警示</div>
  <div class="strategy-note">目前網站還沒有券商持股清單，這區先用候選與訊號追蹤名單做賣出風險掃描；之後接入實際庫存後可改成只看持有中標的。</div>
  {rows}
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


def build_index_page(reports: list[dict]) -> str:
    latest = latest_stock_report(reports)
    date_str = latest.get("date", "─")
    marching, consolidation, risk = split_baskets(latest.get("stocks", []))

    # 篩選摘要
    filter_card = f"""
<div class="card">
  <div class="section-label">🔍 篩選漏斗</div>
  {build_filter_steps(latest.get('filter_summary', []))}
  <div style="margin-top:12px;font-size:12px;color:#6e7681">從全市場 2,000+ 檔，六大技術條件篩選至最終 <strong style="color:#58a6ff">20 檔</strong></div>
</div>"""

    # Top 20 精選（簡潔版）
    stocks = latest.get("stocks", [])[:20]
    basket_summary = f"""
<div class="grid grid-3" style="margin-bottom:16px">
  <div class="metric"><div class="metric-num" style="color:#3fb950">{len(marching)}</div><div class="metric-label">行進籃：SFZ 波段候選</div></div>
  <div class="metric"><div class="metric-num" style="color:#58a6ff">{len(consolidation)}</div><div class="metric-label">盤整籃：MABC 觀察</div></div>
  <div class="metric"><div class="metric-num" style="color:#f85149">{len(risk)}</div><div class="metric-label">過熱/風險：不追高</div></div>
</div>"""
    table_card = f"""
<div class="card">
  <div class="section-label">🏆 今日精選 Top 20</div>
  <p style="font-size:12px;color:#6e7681;margin-bottom:14px">資料日期：{date_str} · 評分公式：週排名 × 外資籌碼 · <a href="baskets.html">查看雙籃儀表板 →</a></p>
  {basket_summary}
  {build_stock_table(stocks, compact=True)}
</div>"""

    # 最近報告卡片
    recent_items = ""
    for r in reports[:5]:
        cnt = len(r.get("stocks", []))
        recent_items += f"""
<div class="history-item">
  <div>
    <div class="history-date">{r['date']}</div>
    <div class="history-meta">精選 {cnt} 檔</div>
  </div>
  <a href="daily/{r['date']}.html" class="history-link">查看報告 →</a>
</div>"""

    history_card = f"""
<div class="card">
  <div class="section-label">🗂 最近報告</div>
  {recent_items}
  <div style="margin-top:12px"><a href="history.html" style="font-size:13px;font-weight:600">查看所有報告 →</a></div>
</div>"""

    body = f"""
<div class="container">
  <div class="page-title">Stockfrom脩 量化選股站</div>
  <div class="page-sub">每個交易日自動更新 · 最新報告：{date_str}</div>
  {build_market_light_card(latest, latest.get("stocks", []))}
  {build_today_action_card(latest.get("stocks", []))}
  {build_sell_alert_card(latest.get("stocks", []))}
  {filter_card}
  {table_card}
  {history_card}
</div>"""

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
    a_score = 40 if a_ok else 28 if a_near else 12 if close and ma20 and close > ma20 else 0

    chip = chip_trend_metrics(chip_series, holding)
    holding_series = read_holding_series(s.get("id", ""))
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
    b1_ok = (
        (major_4w_delta is not None and major_4w_delta > 0)
        or (latest_major is not None and latest_major >= 45)
        or (chip.get("total_10d") is not None and chip.get("total_10d") > 0 and chip.get("foreign_10d", 0) > 0)
    )
    b1_score = 45 if b1_ok else 20 if latest_major is not None or chip_series else 0

    volume_price = tech.get("volume_price") if tech else "資料不足"
    not_break = bool(close and ma20 and close >= ma20 * 0.97)
    retail_not_hot = retail_4w_delta is None or retail_4w_delta <= 1.0
    b2_ok = volume_price in {"量縮價穩", "量縮價漲", "均量上彎"} and not_break and retail_not_hot
    b2_score = 15 if b2_ok else 8 if volume_price in {"量價未表態", "量能資料不足"} and not_break else 0

    items = [
        ("A：MA120/MA240上彎", "ok" if a_ok else "warn" if ma120_up or ma240_up or a_near else "bad"),
        ("B1籌碼未離開", "ok" if b1_ok else "warn" if b1_score else "bad"),
        ("B2賣壓小", "ok" if b2_ok else "warn" if b2_score else "bad"),
    ]
    score = a_score + b1_score + b2_score
    return {
        "score": score,
        "items": items,
        "a_score": a_score,
        "b1_score": b1_score,
        "b2_score": b2_score,
        "volume_line": f"{volume_price}｜{tech.get('volume_price_basis', '')}" if tech else "量價資料不足",
        "chip_line": f"外資10日 {fmt_num(chip.get('foreign_10d'), 0)} 張｜主力10日 {fmt_num(chip.get('total_10d'), 0)} 張｜大戶4週 {fmt_num(major_4w_delta, 2)}%｜散戶4週 {fmt_num(retail_4w_delta, 2)}%",
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
        "volume_line": observation["line"],
        "sort": (0 if action == "重點觀察" else 1 if action == "觀察中" else 2 if action == "暫緩觀察" else 3, -score),
    }


def build_mda_page(reports: list[dict]) -> str:
    latest = latest_stock_report(reports)
    date_str = latest.get("date", "─")
    scored = [mda_score_stock(enrich_stock_fields(dict(s)), True) for s in latest.get("stocks", [])]
    market = {"class": "", "state": "", "note": ""}
    scored.sort(key=lambda x: x["sort"])
    primary = [x for x in scored if x["action"] == "重點觀察"]
    wait = [x for x in scored if x["action"] == "觀察中"]
    avoid = [x for x in scored if x["action"] in {"暫緩觀察", "大盤停手"}]

    rows = ""
    for x in scored:
        change_text, change_cls = x["change"]
        rows += f"""
<tr>
  <td><a class="stock-link" href="mda_stocks/{esc(x['id'])}.html">{esc(x['id'])} {esc(x['name'])}</a><div class="signal-dates">{esc(x['market'])}｜M大解析頁</div></td>
  <td><span class="tag {x['tag_cls']}">{esc(x['action'])}</span><div class="m-score">{fmt_num(x['score'], 0)}</div></td>
  <td><div class="price-main">{esc(x['close'])}</div><div class="{change_cls}">{esc(change_text)}</div></td>
  <td><div class="m-checks">{x['reason']}</div><div class="signal-dates" style="margin-top:6px">{esc(x['volume_line'])}</div></td>
  <td><div class="m-checks">{x['risk_reason']}</div><div class="signal-dates" style="margin-top:6px">{esc(x['chip_line'])}</div></td>
  <td><div class="signal-dates">{esc(x['abc'])}｜{esc(x['strict'])}<br>A {x['a_score']}｜B1 {x['b1_score']}｜B2 {x['b2_score']}</div></td>
</tr>"""
    if not rows:
        rows = '<tr><td colspan="6" style="color:#8b949e">目前沒有上市櫃候選標的。</td></tr>'

    body = f"""
<div class="container">
  <div class="page-title">M大選股</div>
  <div class="page-sub">最新報告：{esc(date_str)} · 只做 A / B1 聰明錢觀察清單，不給買進訊號</div>
  <div class="card" style="display:none">
    <div class="section-label">M 大盤前提</div>
    <div class="market-light">
      <div class="market-badge {market['class']}">{esc(market['state'])}</div>
      <div>
        <div style="font-size:16px;font-weight:800;color:#e6edf3">{esc(market['note'])}</div>
        <div class="strategy-note" style="margin-top:8px">觀察模式：第一層只看 A，MA120 與 MA240 同時上彎就納入觀察池；接著才看這些尚未發動的股票，股權結構與籌碼是否有聰明錢慢慢接手。輔助看 120 日線是否被反覆挑戰或有效站上、扣抵是否轉有利、量能是否代表資金開始集中。這頁不顯示買進、停損、停利。</div>
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
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>觀察等級</th><th>收盤</th><th>值得觀察的跡象</th><th>主要風險</th><th>ABC拆分</th></tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  </div>
</div>"""
    return html_page("M大選股", "mda", body)


def _mda_line(label: str, value: str, cls: str = "") -> str:
    return f'<div class="telegram-line"><div class="k">{esc(label)}</div><div class="v {cls}">{value}</div></div>'


def mda_chip_structure(stock_id: str, chip_series: list[dict], holding: dict) -> dict:
    holding_series = read_holding_series(stock_id)
    major_4w = retail_4w = people_4w = None
    if len(holding_series) >= 5:
        last, base = holding_series[-1], holding_series[-5]
        if last.get("major") is not None and base.get("major") is not None:
            major_4w = last["major"] - base["major"]
        if last.get("retail") is not None and base.get("retail") is not None:
            retail_4w = last["retail"] - base["retail"]
        if last.get("total_people") is not None and base.get("total_people") is not None:
            people_4w = last["total_people"] - base["total_people"]
    h_latest = (holding.get("latest") or {}) if holding else {}
    foreign_10d = sum(float(x.get("foreign") or 0) for x in chip_series[-10:])
    force_10d = sum(float(x.get("total") or 0) for x in chip_series[-10:])
    good = (
        (major_4w is not None and major_4w > 0)
        and (retail_4w is None or retail_4w <= 0)
    )
    bad = (
        (major_4w is not None and major_4w < 0)
        and (retail_4w is not None and retail_4w > 0)
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
        "retail_4w": retail_4w,
        "people_4w": people_4w,
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
            "retail": a.get("retail"),
            "totalPeople": a.get("total_people"),
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
        panel("major", mda_metric_svg(rows, "千張大戶持股比例", "major", "#f85149", "line", "%")),
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
    if(kind==='major') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>千張大戶</span><span>${{pct(x.major)}}</span></div>`;
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
        ("major", "千張大戶持股比例", ""),
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
    if(kind==='major') return `<b>${{x.date}}</b><br>千張大戶 ${{pct(x.major)}}`;
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
      const key={{foreignShares:'foreignShares',marginBalance:'marginBalance',major:'major',retail:'retail',totalPeople:'totalPeople'}}[kind];
      const color={{foreignShares:'#7ee787',marginBalance:'#a78bfa',major:'#f85149',retail:'#3fb950',totalPeople:'#58a6ff'}}[kind] || '#58a6ff';
      series=chart.addSeries(L.LineSeries,{{color,lineWidth:2,priceLineVisible:false}});
      series.setData(lineData(key));
    }}
    chart.timeScale().fitContent();
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
  addPanel('major','千張大戶持股比例',150);
  addPanel('retail','散戶持股比例',150);
  addPanel('totalPeople','總股東人數',150);
  window.addEventListener('resize',()=>chartApis.forEach(item=>item.chart.applyOptions({{width:item.el.clientWidth}})));
}})();
</script>"""
    return f"""
<div id="{panel_id}" class="tv-chart-grid">
  {panels}
  <div class="tv-chart-note">圖表使用 TradingView Lightweight Charts；K 線與台股籌碼資料仍由本站 FinMind 快取提供。</div>
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
        + _mda_line("A判讀", "MA120 與 MA240 同時上彎，先納入觀察池。" if abc.get("a_score", 0) >= 40 else "長均線尚未同時上彎，觀察順位降低。", "pos" if abc.get("a_score", 0) >= 40 else "neg")
    )
    b1_block = (
        _mda_line("大戶/散戶", f'大戶4週 {fmt_num(money["major_4w"])}%｜散戶4週 {fmt_num(money["retail_4w"])}%｜股東4週 {fmt_num(money["people_4w"], 0)} 人')
        + _mda_line("法人籌碼", f'外資10日 {fmt_num(money["foreign_10d"], 0)} 張｜主力10日 {fmt_num(money["force_10d"], 0)} 張')
        + _mda_line("B1判讀", esc(money["reading"]), money["class"])
    )
    b2_block = (
        _mda_line("量價", esc(volume_price))
        + _mda_line("判斷依據", esc(volume_basis))
        + _mda_line("賣壓觀察", "量縮價穩或縮量不破線，代表賣壓有機會變小。" if abc.get("b2_score", 0) >= 8 else "量價尚未證明賣壓收斂，先只觀察。")
    )
    chip_ok = (
        (money.get("major_4w") is not None and money.get("major_4w") > 0)
        and (money.get("retail_4w") is None or money.get("retail_4w") <= 0)
        and (money.get("people_4w") is None or money.get("people_4w") <= 0)
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
    )

    body = f"""
<div class="container">
  <div style="margin-bottom:8px"><a href="../mda.html" style="color:#6e7681;font-size:13px">&larr; 回 M大選股</a>　<a href="../stocks/{esc(stock_id)}.html" style="color:#6e7681;font-size:13px">一般個股頁 →</a></div>
  <div class="page-title">{esc(stock_id)} {esc(s.get('name',''))}｜M大觀察解析</div>
  <div class="page-sub">照 M大個股分析順序：A 長均線 → 120日扣抵 → B1 聰明錢 → B2 賣壓 → 後續追蹤</div>
  <div class="grid grid-2">
    <div class="card"><div class="section-label">① 為什麼值得觀察</div><div class="telegram-phase">{why}</div></div>
    <div class="card"><div class="section-label">② A：長期趨勢</div><div class="telegram-phase">{a_block}</div></div>
    <div class="card"><div class="section-label">③ B1：聰明錢與股權結構</div><div class="telegram-phase">{b1_block}</div></div>
    <div class="card"><div class="section-label">④ B2：賣壓是否變小</div><div class="telegram-phase">{b2_block}</div></div>
  </div>
  <div class="card"><div class="section-label">⑤ 接下來觀察什麼</div><div class="telegram-phase">{next_watch}</div></div>
  <div class="card">
    <div class="section-label">日K / 籌碼 / 股權結構連動圖</div>
    {synced_charts}
  </div>
</div>"""
    return html_page(f"{stock_id} M大觀察解析", "mda", body, nav_prefix="../")


def build_mda_stock_pages(reports: list[dict]) -> int:
    latest = latest_stock_report(reports)
    out_dir = OUTPUT_DIR / "mda_stocks"
    out_dir.mkdir(parents=True, exist_ok=True)
    valid = {f"{s.get('id')}.html" for s in latest.get("stocks", []) if s.get("id")}
    for old_file in out_dir.glob("*.html"):
        if old_file.name not in valid:
            old_file.unlink()
    count = 0
    for s in latest.get("stocks", []):
        sid = s.get("id", "")
        if not sid:
            continue
        (out_dir / f"{sid}.html").write_text(build_mda_stock_detail_page(sid, s), encoding="utf-8")
        count += 1
    return count


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
        '<div class="card">'
        '<div class="section-label">Top 20</div>'
        '<div style="font-size:12px;color:#6e7681;margin-bottom:14px">'
        '綠色＝行進籃｜藍色＝盤整籃｜紅色＝過熱/風險'
        '</div>'
        + build_stock_table(stocks, compact=False, stock_link_prefix="../stocks")
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
        + '<div class="page-sub">Quant Screener &middot; FinMind &middot; Foreign Capital Weighted Score</div>'
        + stat_row + market_section + filter_section + table_section + notes_section
        + '</div>'
    )
    return html_page(f"{date_str}", "daily", body, nav_prefix="../")


def build_latest_daily_page(reports):
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
        '<div class="card">'
        + f'<div class="section-label">Top 20 &mdash; {date_str}</div>'
        + '<div style="font-size:12px;color:#6e7681;margin-bottom:14px">'
        '&#x1F7E2; Healthy | &#x1F7E1; Strong | &#x1F534; Overbought'
        '</div>'
        + build_stock_table(stocks, compact=False)
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
        '<div class="page-title">Daily Top 20</div>'
        + f'<div class="page-sub">Date: {date_str} &middot; <a href="history.html">History &rarr;</a></div>'
        + market_card + table_section + notes_section
        + '</div>'
    )
    return html_page("Today", "daily", body)


def build_baskets_page(reports):
    latest = latest_stock_report(reports)
    date_str = latest.get("date", "-")
    stocks = latest.get("stocks", [])
    marching, consolidation, risk = split_baskets(stocks)
    risk_watch = risk or build_risk_watchlist(stocks)
    ledger = build_signal_ledger(reports)

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

    body = (
        '<div class="container">'
        + '<div class="page-title">雙籃選股儀表板</div>'
        + f'<div class="page-sub">資料日期：{date_str} · 網站負責完整巡檢，重要提醒另由推播流程處理</div>'
        + hero
        + playbook
        + '<div class="grid grid-2">'
        + build_basket_column("行進籃｜SFZ 波段", "已進入較強趨勢的候選；重點是買點可執行、MA20續抱、避免漲停追高。", marching, "marching", ledger)
        + build_basket_column("盤整籃｜MABC 觀察", "尚未完全發動但值得等待；重點是量縮價穩、籌碼不離開、早買型態浮現。", consolidation, "consolidation", ledger)
        + '</div>'
        + build_basket_column("過熱/風險觀察", "強勢但不適合追價，或已出現賣出警示；等回測、降溫或重新整理後再評估。", risk_watch, "risk", ledger)
        + '</div>'
    )
    return html_page("雙籃儀表板", "basket", body)


def build_signals_page(reports):
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
        rows += f"""
<tr class="clickable-row" onclick="location.href='{href}'">
  <td>
    <div><a class="stock-link" href="{href}" onclick="event.stopPropagation()">{esc(item['id'])} {esc(item['name'])}</a></div>
    <div class="signal-dates"><a href="{href}" onclick="event.stopPropagation()">打開個股資訊卡 →</a></div>
    <div class="tag-row">{latest_mark}<span class="tag">{basket}</span></div>
  </td>
  <td><strong>{len(events)}</strong> 次</td>
  <td>{events[0]['date']}<br><span style="color:#8b949e">最近 {latest_event['date']}</span></td>
  <td>買入區 {latest_event['entry']}<br><span style="color:#8b949e">原始買點 {latest_event.get('raw_entry','─')} ｜ 收盤 {latest_event['price']} ｜ 原始分數 {latest_event['score']}</span></td>
  <td>{push_status}</td>
  <td><div class="signal-dates">{dates}</div></td>
</tr>"""

    body = f"""
<div class="container">
  <div class="page-title">訊號追蹤</div>
  <div class="page-sub">最新資料：{latest_date} · 目標是確認每個買點都有被記錄、追蹤、推播</div>
  <div class="card">
    <div class="section-label">Signal Ledger</div>
    <div class="grid grid-3">
      <div class="metric"><div class="metric-num" style="color:#58a6ff">{len(ledger)}</div><div class="metric-label">歷史唯一個股</div></div>
      <div class="metric"><div class="metric-num" style="color:#3fb950">{active_count}</div><div class="metric-label">今日仍在追蹤</div></div>
      <div class="metric"><div class="metric-num" style="font-size:16px">{push_note}</div><div class="metric-label">推播覆蓋率</div></div>
    </div>
    <div class="strategy-note" style="margin-top:14px">
      這頁先用每日報告建立「入選台帳」。等推播流程把成功紀錄寫入 <strong>signal_push_log.csv</strong> 後，這裡就會變成查漏清單：任何 0/N 或未滿 N/N 的個股，都代表有買點需要補查。
    </div>
  </div>
  <div class="card">
    <div class="section-label">歷史訊號摘要</div>
    <div style="overflow-x:auto">
      <table class="stock-table signal-table">
        <thead>
          <tr><th>個股</th><th>入選</th><th>首次/最近</th><th>最新買入區</th><th>推播</th><th>出現日期</th></tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  </div>
</div>"""
    return html_page("訊號追蹤", "signals", body)


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
        f'<div class="info-cell"><div class="k">大戶持股比例</div><div class="v">{fmt_num(h_latest.get("major"))}%</div></div>'
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
    chip_dates = f"法人 {chip.get('date','─')}｜股權 {holding.get('date','─') if holding else '─'}"
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
      <span>大戶&gt;1000張</span><span>${{fmtPct(x.major)}}</span>
      <span>400~1000張</span><span>${{fmtPct(x.large)}}</span>
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
  if(kind==='holdingPack') return `<div class="t-date">${{x.date || '-'}}${{x.holdingDate ? '｜股權 '+x.holdingDate : ''}}</div><div class="t-grid"><span>大戶持股比例</span><span>${{pct(x.major)}}</span><span>散戶持股比例</span><span>${{pct(x.retail)}}</span><span>總股東人數</span><span>${{fmtInt(x.totalPeople)}} 人</span></div>`;
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
  const html=(x)=>`
    <div class="t-date">${{x.date || '-'}}</div>
    <div class="t-grid">
      <span>主力合計</span><span>${{fmt(x.total,0)}} 張</span>
      <span>收盤價</span><span>${{fmt(x.close,2)}}</span>
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
    body = f"""
<div class="container">
  <div style="margin-bottom:8px"><a href="../baskets.html" style="color:#6e7681;font-size:13px">&larr; 回雙籃儀表板</a></div>
  <div class="page-title">{esc(stock_id)} {esc(s.get('name',''))}</div>
  <div class="page-sub">v44 個股研究頁 · 報告日期 {esc(s.get('report_date','─'))}</div>
  <div class="detail-hero">
    <div class="card">
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
    <div class="strategy-note" style="margin-top:12px">買入區以該次報告日期以前的 14 日高低價反推 Williams -65~-85，並用 MA20 作為濾網；下方「原始」保留當天報告寫入的買點。原始分數來自報告 Score；舊格式沒有 Score 時，才用排名換算（第1名200，每名-10）。</div>
  </div>
</div>
{chart_script}"""
    return html_page(f"{stock_id} {s.get('name','')}", "basket", body, nav_prefix="../")


def build_stocks_index_page(reports: list[dict]) -> str:
    stock_map = find_latest_stock_map(reports)
    ledger = build_signal_ledger(reports)
    items = []
    for sid, s in sorted(stock_map.items()):
        rows = merge_report_close(read_price_history(sid), s)
        latest = rows[-1] if rows else {}
        daily = aggregate_ohlcv(merge_report_close(read_price_history(sid), s), "daily")
        tech = technical_snapshot(daily, s)
        decision = build_trade_decision(tech, s)
        price = latest.get("close")
        date = latest.get("date", "")
        item = {
            "id": sid,
            "name": s.get("name", ""),
            "basket": basket_label(classify_basket(s)),
            "price": fmt_num(price),
            "price_date": date,
            "entry": decision.get("entry_text", "─"),
            "target": decision.get("target_text", "─"),
            "stop": decision.get("initial_stop_text", "─"),
            "support": decision.get("reference_support_text", "─"),
            "rr": decision.get("rr_text", "─"),
            "rr_class": decision.get("rr_class", ""),
            "score": s.get("score", "─"),
            "events": len(ledger.get(sid, {}).get("events", [])),
        }
        items.append(item)

    rows_html = ""
    for x in items:
        search = f"{x['id']} {x['name']} {x['basket']}".lower()
        rows_html += f"""
<tr data-search="{esc(search)}">
  <td><a class="stock-link" href="stocks/{x['id']}.html">{x['id']} {esc(x['name'])}</a><div class="signal-dates">{esc(x['price_date'])}</div></td>
  <td>{esc(x['basket'])}</td>
  <td class="price-main">{esc(x['price'])}</td>
  <td><div class="price-entry">進 {esc(x['entry'])}</div><div class="price-target">目 {esc(x['target'])}</div><div class="price-stop">初停 {esc(x['stop'])}</div><div class="price-support">支撐 {esc(x['support'])}</div><div class="price-rr {x['rr_class']}">R:R {esc(x['rr'])}</div></td>
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
  <div class="page-title">個股總覽</div>
  <div class="page-sub">FinMind 收盤價 · SFZ 買點 · MABC 分類 · 點股票進資訊卡</div>
  <div class="card">
    <div class="section-label">Stock Browser</div>
    <input id="stockSearch" class="searchbar" placeholder="搜尋股票代號、名稱、行進籃、盤整籃..." oninput="filterStocks()">
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>分類</th><th>FinMind收盤</th><th>買點/目標/初停/R:R</th><th>分數</th><th>訊號</th></tr></thead>
        <tbody id="stockRows">{rows_html}</tbody>
      </table>
    </div>
  </div>
</div>
{script}"""
    return html_page("個股總覽", "stocks", body)


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


def build_buy_radar_page(reports: list[dict]) -> str:
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
            "basket": basket_label(classify_basket(s)),
            "bucket": bucket,
            "cls": cls,
            "note": note,
            "gap": gap,
            "close": tech.get("close") if tech else None,
            "entry": decision.get("entry_text", "─"),
            "target": decision.get("target_text", "─"),
            "stop": decision.get("initial_stop_text", "─"),
            "support": decision.get("reference_support_text", "─"),
            "rr": decision.get("rr_text", "─"),
            "rr_class": decision.get("rr_class", ""),
            "trend": tech.get("trend", "─") if tech else "─",
            "score": _to_float(s.get("score", "0")),
        })
    rows.sort(key=lambda x: (999 if x["gap"] is None else abs(x["gap"]), -x["score"]))
    near = sum(1 for x in rows if x["bucket"] == "接近買點")
    pullback = sum(1 for x in rows if x["bucket"] == "稍高等回測")
    extended = sum(1 for x in rows if x["bucket"] == "離買點過遠")

    table = ""
    for x in rows:
        gap_txt = "─" if x["gap"] is None else f'{x["gap"]:+.1f}%'
        table += f"""
<tr>
  <td><a class="stock-link" href="stocks/{x['sid']}.html">{x['sid']} {esc(x['name'])}</a><div class="signal-dates">{esc(x['basket'])} ｜ {esc(x['trend'])}</div></td>
  <td><span class="tag {x['cls']}">{esc(x['bucket'])}</span><div class="signal-dates">{esc(x['note'])}</div></td>
  <td class="price-main">{fmt_num(x['close'])}</td>
  <td>{gap_txt}</td>
  <td><div class="price-entry">進 {esc(x['entry'])}</div><div class="price-target">目 {esc(x['target'])}</div><div class="price-stop">初停 {esc(x['stop'])}</div><div class="price-support">支撐 {esc(x['support'])}</div><div class="price-rr {x['rr_class']}">R:R {esc(x['rr'])}</div></td>
</tr>"""

    body = f"""
<div class="container">
  <div class="page-title">買點雷達</div>
  <div class="page-sub">用 FinMind 最新收盤比對 Williams -65~-85 買入區，優先找「能執行」而不是「已經追遠」的標的</div>
  <div class="card">
    <div class="section-label">Buy Radar</div>
    <div class="grid grid-3">
      <div class="metric"><div class="metric-num" style="color:#3fb950">{near}</div><div class="metric-label">接近買點：優先確認</div></div>
      <div class="metric"><div class="metric-num" style="color:#d2a520">{pullback}</div><div class="metric-label">稍高：等回測</div></div>
      <div class="metric"><div class="metric-num" style="color:#f85149">{extended}</div><div class="metric-label">過遠：不追高</div></div>
    </div>
    <div class="strategy-note" style="margin-top:14px">這頁以 Williams -65~-85 反推價格帶，並搭配 MA20 濾網建立網站版雷達。後續可再把 MABC A/B/C、量價共振分數接進同一張表。</div>
  </div>
  <div class="card">
    <div class="section-label">候選排序</div>
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>狀態</th><th>收盤</th><th>距買點</th><th>買點/目標/初停/R:R</th></tr></thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  </div>
</div>"""
    return html_page("買點雷達", "radar", body)


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


def backtest_historical_scan(reports: list[dict], start_date: str = "2024-01-01", method: str = "wr_65_85_ma20") -> list[dict]:
    trades = []
    for item in historical_scan_universe(reports):
        sid = item["sid"]
        s = item["stock"]
        rows = item["rows"]
        i = 60
        while i < len(rows) - 1:
            row = rows[i]
            signal_date = row.get("date", "")
            if signal_date < start_date:
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
            zone = indicator_entry_zone(method, past_rows, decision)
            entry_low, entry_high = zone.get("low"), zone.get("high")
            if entry_low is None or entry_high is None:
                i += 1
                continue
            low, high = row.get("low"), row.get("high")
            if low is None or high is None or not (low <= entry_high and high >= entry_low):
                i += 1
                continue

            center = (entry_low + entry_high) / 2
            open_price = row.get("open") or center
            entry_price = center if low <= center <= high else min(max(open_price, entry_low), entry_high)
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
                if j > i and len(rows[: j + 1]) >= 20:
                    ma20_now = ma_values(rows[: j + 1], 20)[-1]
                    close_now = r.get("close")
                    if ma20_now and close_now and close_now < ma20_now:
                        exit_date, exit_price, exit_reason, exit_idx = r.get("date", ""), close_now, "跌破MA20出場", j
                        break
                    break
            ret = ((exit_price / entry_price - 1) * 100) if entry_price and exit_price else None
            path = trade_path_metrics(rows, signal_date, exit_date, entry_price)
            hold_days = None
            try:
                hold_days = max(0, (datetime.fromisoformat(exit_date) - datetime.fromisoformat(signal_date)).days)
            except Exception:
                pass
            trades.append({
                "sid": sid,
                "name": s.get("name", ""),
                "signal_date": signal_date,
                "entry_rule": "Williams -65~-85 反推買入區 + MA20濾網；日K碰到區間成交",
                "entry_range": zone.get("label", "資料不足"),
                "entry_date": signal_date,
                "entry": entry_price,
                "exit_date": exit_date,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "ret": ret,
                "max_return": path.get("max_return"),
                "max_drawdown": path.get("max_drawdown"),
                "hold_days": hold_days,
                "target": target,
                "stop": stop,
            })
            i = max(exit_idx + 1, i + 20)
    return trades


def summarize_trade_rows(rows: list[dict]) -> dict:
    filled = [x for x in rows if x.get("entry") is not None]
    closed = [x for x in filled if x.get("exit_reason") != "持有中"]
    wins = [x for x in closed if (x.get("ret") or 0) > 0]
    losses = [x for x in closed if (x.get("ret") or 0) <= 0]
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
        "avg_hold": sum(x.get("hold_days") or 0 for x in filled if x.get("hold_days") is not None) / len([x for x in filled if x.get("hold_days") is not None]) if filled else None,
        "wins": len(wins),
        "losses": len(losses),
    }


def build_historical_scan_html(reports: list[dict]) -> str:
    trades = backtest_historical_scan(reports, "2024-01-01")
    summary = summarize_trade_rows(trades)
    first_date = min((x.get("signal_date") for x in trades if x.get("signal_date")), default="─")
    last_date = max((x.get("signal_date") for x in trades if x.get("signal_date")), default="─")
    rows_html = ""
    for x in sorted(trades, key=lambda r: (r.get("signal_date", ""), r.get("sid", "")), reverse=True)[:80]:
        ret = x.get("ret")
        ret_cls = "pos" if ret is not None and ret > 0 else "neg" if ret is not None and ret < 0 else ""
        rows_html += f"""
<tr>
  <td><a class="stock-link" href="stocks/{x['sid']}.html">{esc(x['sid'])} {esc(x['name'])}</a><div class="signal-dates">訊號 {esc(x.get('signal_date','─'))}</div></td>
  <td>{esc(x.get('entry_range','─'))}<div class="signal-dates">{esc(x.get('entry_rule',''))}<br>成交 {esc(x.get('entry_date','─'))}｜{fmt_num(x.get('entry'))}</div></td>
  <td>{esc(x.get('exit_reason',''))}<div class="signal-dates">{esc(x.get('exit_date','─'))}｜出場 {fmt_num(x.get('exit_price'))}</div></td>
  <td class="{ret_cls}" style="font-weight:800">{fmt_num(ret,1)}%</td>
  <td><span class="pos">{fmt_num(x.get('max_return'),1)}%</span><div class="signal-dates">最大回撤 <span class="neg">{fmt_num(x.get('max_drawdown'),1)}%</span></div></td>
  <td><div class="price-target">續抱 MA20</div><div class="price-stop">初停 {fmt_num(x.get('stop'))}</div></td>
</tr>"""
    if not rows_html:
        rows_html = '<tr><td colspan="6" style="color:#8b949e">目前資料不足，還無法形成 2024 起掃描回測。</td></tr>'
    return f"""
<div class="card">
  <div class="section-label">2024 起歷史掃描回測</div>
  <div class="strategy-note">資料範圍 {esc(first_date)} ~ {esc(last_date)}。這不是人工報告訊號，而是用目前上市櫃候選池逐日掃描：買點為 Williams -65~-85 反推價格區，且訊號日收盤需站上 MA20；日 K 碰到買入區視為成交。出場改用波段邏輯：成交後先守初始停損，未停損則沿 MA20 續抱，收盤跌破 MA20 才出場；不設固定 +10% 停利。</div>
  <div class="grid grid-3" style="margin-top:12px">
    <div class="metric"><div class="metric-num">{summary['filled']}</div><div class="metric-label">成交筆數</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(summary.get('win_rate'),1)}%</div><div class="metric-label">已出場勝率</div></div>
    <div class="metric"><div class="metric-num {('pos' if (summary.get('avg_ret') or 0) >= 0 else 'neg')}">{fmt_num(summary.get('avg_ret'),1)}%</div><div class="metric-label">平均報酬</div></div>
    <div class="metric"><div class="metric-num pos">{fmt_num(summary.get('max_return'),1)}%</div><div class="metric-label">最大曾有報酬</div></div>
    <div class="metric"><div class="metric-num neg">{fmt_num(summary.get('max_drawdown'),1)}%</div><div class="metric-label">最大回撤</div></div>
    <div class="metric"><div class="metric-num">{fmt_num(summary.get('avg_hold'),1)}</div><div class="metric-label">平均持有天數</div></div>
  </div>
  <div class="chip-line">已出場 {summary['closed']} 筆｜持有中 {summary['open']} 筆｜獲利 {summary['wins']} 筆｜虧損 {summary['losses']} 筆｜最佳實現 {fmt_num(summary.get('best'),1)}%｜最差實現 {fmt_num(summary.get('worst'),1)}%｜平均回撤 {fmt_num(summary.get('avg_drawdown'),1)}%</div>
  <div style="overflow-x:auto;margin-top:14px">
    <table class="stock-table">
      <thead><tr><th>個股/訊號日</th><th>買點與成交</th><th>出場</th><th>實現報酬</th><th>最大報酬/回撤</th><th>續抱/初停</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>"""


def build_backtest_page(reports: list[dict]) -> str:
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
  <div class="page-sub">依 Williams -65~-85 買入區、MA20 濾網、停利與初始停損追蹤訊號後績效。成交從報告日後一個交易日開始計算。</div>
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
    <div class="chip-line">已出場：{len(closed)} 筆｜停利/獲利：{len(wins)} 筆｜停損/虧損：{len(losses)} 筆｜平均已實現：{fmt_num(avg_closed,1)}%｜最佳：{fmt_num(best,1)}%｜最差：{fmt_num(worst,1)}%</div>
  </div>
  {build_historical_scan_html(reports)}
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
    return html_page("歷史回測", "backtest", body)


def build_stock_pages(reports: list[dict]) -> int:
    stock_map = find_latest_stock_map(reports)
    ledger = build_signal_ledger(reports)
    out_dir = OUTPUT_DIR / "stocks"
    out_dir.mkdir(parents=True, exist_ok=True)
    valid_files = {f"{stock_id}.html" for stock_id in stock_map}
    for old_file in out_dir.glob("*.html"):
        if old_file.name not in valid_files:
            old_file.unlink()
    count = 0
    for stock_id, s in sorted(stock_map.items()):
        (out_dir / f"{stock_id}.html").write_text(build_stock_detail_page(stock_id, s, ledger), encoding="utf-8")
        count += 1
    return count


def build_history_page(reports):
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
        + '<div class="page-sub"><a href="backtest.html">查看歷史回測 →</a></div>'
        + '<div class="card">' + items + '</div>'
        + '</div>'
    )
    return html_page("History", "history", body)


def main():
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    print("[Stockfrom] Site Generator v1.0", flush=True)
    print(f"   Reports: {REPORTS_DIR}", flush=True)
    print(f"   Output:  {OUTPUT_DIR}", flush=True)

    (OUTPUT_DIR / "daily").mkdir(parents=True, exist_ok=True)
    reports = load_reports()

    if not reports:
        print("[ERROR] No reports parsed or cached.", flush=True)
        return

    print("\n[Build] Generating pages...", flush=True)
    (OUTPUT_DIR / "index.html").write_text(build_index_page(reports), encoding="utf-8")
    print("   [OK] index.html", flush=True)
    (OUTPUT_DIR / "daily.html").write_text(build_latest_daily_page(reports), encoding="utf-8")
    print("   [OK] daily.html", flush=True)
    (OUTPUT_DIR / "mda.html").write_text(build_mda_page(reports), encoding="utf-8")
    print("   [OK] mda.html", flush=True)
    (OUTPUT_DIR / "baskets.html").write_text(build_baskets_page(reports), encoding="utf-8")
    print("   [OK] baskets.html", flush=True)
    (OUTPUT_DIR / "signals.html").write_text(build_signals_page(reports), encoding="utf-8")
    print("   [OK] signals.html", flush=True)
    (OUTPUT_DIR / "stocks.html").write_text(build_stocks_index_page(reports), encoding="utf-8")
    print("   [OK] stocks.html", flush=True)
    (OUTPUT_DIR / "radar.html").write_text(build_buy_radar_page(reports), encoding="utf-8")
    print("   [OK] radar.html", flush=True)
    (OUTPUT_DIR / "backtest.html").write_text(build_backtest_page(reports), encoding="utf-8")
    print("   [OK] backtest.html", flush=True)
    (OUTPUT_DIR / "history.html").write_text(build_history_page(reports), encoding="utf-8")
    print("   [OK] history.html", flush=True)
    stock_page_count = build_stock_pages(reports)
    print(f"   [OK] stocks/*.html ({stock_page_count})", flush=True)
    mda_stock_page_count = build_mda_stock_pages(reports)
    print(f"   [OK] mda_stocks/*.html ({mda_stock_page_count})", flush=True)

    for r in reports:
        html = build_daily_page(r)
        out = OUTPUT_DIR / "daily" / f"{r['date']}.html"
        out.write_text(html, encoding="utf-8")
        print(f"   [OK] daily/{r['date']}.html", flush=True)

    print(f"\n[Done] {len(reports)+8+stock_page_count+mda_stock_page_count} files -> {OUTPUT_DIR}", flush=True)
    print("[Next] git init && git add . && git commit && push to GitHub Pages", flush=True)


if __name__ == "__main__":
    main()
