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
from pathlib import Path
from datetime import datetime

# ──────────────────────────────────────────────
#  路徑設定（Windows / Linux 自動切換）
# ──────────────────────────────────────────────
_WIN_REPORTS  = Path(r"C:\Users\USER\OneDrive\文件\Claude\Projects\Stock from Zero")
_LINUX_REPORTS = Path("/sessions/adoring-amazing-mayer/mnt/Stock from Zero")
_REPO_REPORTS = Path(__file__).parent / "reports"
PUSH_LOG_PATH = Path(__file__).parent / "signal_push_log.csv"
V44_ROOT = Path(os.environ.get("V44_ROOT", r"C:\Users\USER\OneDrive\桌面\股票\自動交易程式"))
LOCAL_DATA_DIR = Path(__file__).parent / "data"
LOCAL_PRICE_DIR = LOCAL_DATA_DIR / "prices"
LOCAL_CHIP_DIR = LOCAL_DATA_DIR / "chips"
LOCAL_HOLDING_DIR = LOCAL_DATA_DIR / "holding_shares"
REPORTS_CACHE_PATH = LOCAL_DATA_DIR / "site_reports.json"
V44_PRICE_DIR = V44_ROOT / "回測" / "v6_outputs" / "prices"
V44_CHIP_DIR = V44_ROOT / "回測" / "v6_outputs" / "chips"
V44_HOLDING_DIR = V44_ROOT / "回測" / "v6_outputs" / "holding_shares"
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
            return reports

    if REPORTS_CACHE_PATH.exists():
        print(f"\n[Read] No MD reports found; using cache {REPORTS_CACHE_PATH}", flush=True)
        return json.loads(REPORTS_CACHE_PATH.read_text(encoding="utf-8"))

    return []


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
.basket-card{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:14px;margin-bottom:10px}
.basket-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:8px}
.basket-code{font-size:17px;font-weight:800;color:#e6edf3}
.basket-name{font-size:12px;color:#8b949e;margin-top:2px}
.basket-action{font-size:12px;font-weight:700;padding:3px 8px;border-radius:999px;background:#1a1a2e;color:#58a6ff;white-space:nowrap}
.tag-row{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
.tag{font-size:11px;color:#8b949e;background:#161b22;border:1px solid #30363d;border-radius:999px;padding:3px 7px}
.tag-green{color:#3fb950;border-color:rgba(63,185,80,.35);background:rgba(63,185,80,.08)}
.tag-yellow{color:#d2a520;border-color:rgba(210,153,34,.35);background:rgba(210,153,34,.08)}
.tag-red{color:#f85149;border-color:rgba(248,81,73,.35);background:rgba(248,81,73,.08)}
.strategy-note{font-size:13px;color:#c9d1d9;line-height:1.75}
.signal-foot{font-size:12px;color:#8b949e;margin-top:8px;border-top:1px solid #21262d;padding-top:8px}
.signal-foot strong{color:#e6edf3}
.signal-table td{vertical-align:top}
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
.chart-box{background:#0d1117;border:1px solid #30363d;border-radius:10px;padding:12px;margin-top:10px}
.chart-tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.chart-tabs button{background:#161b22;color:#c9d1d9;border:1px solid #30363d;border-radius:8px;padding:6px 10px;cursor:pointer}
.chart-tabs button.active{background:#1a6bc4;color:#fff;border-color:#1a6bc4}
.hover-chart{position:relative}
.chart-crosshair{position:absolute;top:0;bottom:0;width:1px;background:rgba(88,166,255,.85);display:none;pointer-events:none}
.chart-tooltip{position:absolute;z-index:5;display:none;min-width:190px;max-width:240px;background:rgba(13,17,23,.96);border:1px solid #30363d;border-radius:8px;padding:9px 10px;color:#c9d1d9;font-size:12px;line-height:1.55;box-shadow:0 10px 28px rgba(0,0,0,.35);pointer-events:none}
.chart-tooltip .t-date{color:#e6edf3;font-weight:800;margin-bottom:4px}
.chart-tooltip .t-grid{display:grid;grid-template-columns:1fr 1fr;gap:2px 10px}
.chart-tooltip .t-ma{margin-top:5px;padding-top:5px;border-top:1px solid #30363d;color:#8b949e}
.linked-holding-panel{margin-top:14px;padding-top:12px;border-top:1px solid #30363d}
.linked-holding-title{font-size:12px;color:#8b949e;margin-bottom:6px;font-weight:700}
.chart-stack{display:grid;grid-template-columns:1fr;gap:12px;margin-top:12px}
.holding-stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-bottom:8px}
.tech-panel{display:grid;grid-template-columns:280px 1fr;gap:14px;align-items:start}
.tech-summary-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:10px}
.indicator-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:12px}
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
  .detail-hero,.info-grid,.tech-panel,.tech-summary-grid,.indicator-grid{grid-template-columns:1fr}
}
"""

def nav_html(active: str = "home", prefix: str = "") -> str:
    tabs = [
        ("home",    "index.html",   "首頁"),
        ("daily",   "daily.html",   "今日選股"),
        ("basket",  "baskets.html", "雙籃儀表板"),
        ("signals", "signals.html", "訊號追蹤"),
        ("stocks",  "stocks.html",  "個股總覽"),
        ("radar",   "radar.html",   "買點雷達"),
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
        return f'<span class="badge badge-green">健康整理</span>'
    elif icon == "🟡":
        return f'<span class="badge badge-yellow">強勢追漲</span>'
    else:
        return f'<span class="badge badge-red">超買</span>'


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
        badge = status_badge(s["icon"], s["status"])
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
    <div class="price-entry">進 {s['entry']}</div>
    <div class="price-target">目 {s['target']}</div>
    <div class="price-stop">損 {s['stop']}</div>
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
    <div class="price-entry">📌 {s['entry']}</div>
    <div class="price-target">🎯 {s['target']}</div>
    <div class="price-stop">🛑 {s['stop']}</div>
  </td>
</tr>"""

    if compact:
        header = """<tr>
  <th>#</th><th>個股</th><th>收盤</th><th>近6週漲幅</th><th>評分</th><th>進場/目標/停損</th>
</tr>"""
    else:
        header = """<tr>
  <th>#</th><th>代號/名稱</th><th>狀態</th><th>收盤</th>
  <th>近6週漲幅</th><th>RSI</th><th>%B</th>
  <th class="hide-mobile">近5日量</th><th class="hide-mobile">外資近5日</th>
  <th>評分</th><th>進場/目標/停損</th>
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
    items = re.findall(r"\d+\.\s+\*\*(.+?)\*\*[：:]\s*(.*?)(?=\n\d+\.|\Z)", notes_text, re.DOTALL)
    if not items:
        # fallback: split by numbered lines
        lines = [l.strip() for l in notes_text.split("\n") if l.strip() and re.match(r"\d+\.", l.strip())]
        def clean_line(l):
            return re.sub(r'^\d+\.\s*', '', l).replace('**', '')
        items_html = "\n".join(f"<li>{clean_line(l)}</li>" for l in lines)
    else:
        items_html = "\n".join(f"<li><strong>{t}：</strong>{d.strip().replace(chr(10), ' ')}</li>" for t, d in items)
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
            item["events"].append({
                "date": date,
                "basket": classify_basket(s),
                "entry": s.get("entry", "─"),
                "price": s.get("price", "─"),
                "score": s.get("score", "─"),
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


def read_price_history(stock_id: str, limit: int = 420) -> list[dict]:
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
        months = int(os.environ.get("V44_FETCH_MONTHS", "12"))
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
    sum5 = {k: sum(by_date[d].get(k, 0.0) for d in last5) for k in ["foreign", "trust", "dealer", "total"]}
    return {"date": dates[-1], "latest": latest, "sum5": sum5}


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


def volume_price_relation(row: dict, volume_ratio: float | None) -> str:
    close = row.get("close")
    open_ = row.get("open")
    if close is None or open_ is None:
        return "資料不足"
    up = close >= open_
    if volume_ratio is None:
        return "量能資料不足"
    if volume_ratio >= 1.8 and up:
        return "放量上漲"
    if volume_ratio >= 1.8 and not up:
        return "放量下跌"
    if volume_ratio <= 0.7 and up:
        return "量縮上漲"
    if volume_ratio <= 0.7 and not up:
        return "量縮下跌"
    return "量價平穩"


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
    rows = rows[-160:]
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
    rows = rows[-160:]
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
    max_vol_lot = max_vol / 1000 if max_vol else 0
    last = rows[-1]
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  <line x1="{pad_l}" y1="{vol_top:.1f}" x2="{w-pad_r}" y2="{vol_top:.1f}" stroke="#30363d"/>
  <text x="4" y="{vol_top+12:.1f}" fill="#6e7681" font-size="11">量</text>
  <text x="4" y="{vol_top+28:.1f}" fill="#6e7681" font-size="11">{max_vol_lot:.0f}張</text>
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
    max_abs = max(abs(float(item.get(k, 0) or 0)) for item in series for k, _, _ in keys) or 1
    max_abs *= 1.15
    zero_y = pad_t + plot_h / 2

    def y(v):
        return zero_y - float(v) * (plot_h / 2) / max_abs

    grid = f'<line x1="{pad_l}" y1="{zero_y:.1f}" x2="{w-pad_r}" y2="{zero_y:.1f}" stroke="#8b949e" stroke-width="1"/>'
    for v in [max_abs, max_abs / 2, -max_abs / 2, -max_abs]:
        yy = y(v)
        grid += f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{w-pad_r}" y2="{yy:.1f}" stroke="#21262d"/><text x="4" y="{yy+4:.1f}" fill="#6e7681" font-size="11">{v:.0f}</text>'

    group_w = (w - pad_l - pad_r) / len(series)
    bar_w = max(6, min(18, group_w / 5))
    bars = ""
    labels = ""
    for i, item in enumerate(series):
        cx = pad_l + group_w * (i + 0.5)
        for j, (key, _, color) in enumerate(keys):
            v = float(item.get(key, 0) or 0)
            x = cx + (j - 1) * (bar_w + 2) - bar_w / 2
            yy = y(v)
            top = min(yy, zero_y)
            bh = max(abs(zero_y - yy), 1.5)
            bars += f'<rect x="{x:.1f}" y="{top:.1f}" width="{bar_w:.1f}" height="{bh:.1f}" fill="{color}" opacity=".82"/>'
        if i in {0, len(series) - 1}:
            labels += f'<text x="{cx-32:.1f}" y="{h-10}" fill="#6e7681" font-size="11">{esc(item.get("date",""))}</text>'

    legend = ""
    for i, (_, label, color) in enumerate(keys):
        legend += f'<text x="{w-210+i*65}" y="16" fill="{color}" font-size="11">{label}</text>'
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {bars}
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
        action = "偏向盤整觀察：重點看 MABC 是否維持 A/B，CaryBot 或量縮價穩轉強時才處理早買點。"
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
    ma_trends = {f"ma{n}": ma_trend_direction(rows, n) for n in [5, 10, 20, 60]}
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
    entry = _to_float(s.get("entry", ""), None)
    entry_gap = ((close / entry - 1) * 100) if entry else None
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
        "open": rows[-1].get("open"),
        "high": rows[-1].get("high"),
        "low": rows[-1].get("low"),
        "volume": latest_vol,
        "avg_vol20": avg_vol20,
        "volume_ratio": volume_ratio,
        "volume_price": volume_price_relation(rows[-1], volume_ratio),
        "trend_pattern": trend_pattern(rows, ma5, ma10, ma20, ma60),
        "candle_pattern": candle_pattern(rows),
        "large_volume": large_volume,
        "large_volume_event": large_volume_event,
        "support": support,
        "resistance": resistance,
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


def build_trade_decision(tech: dict, s: dict) -> dict:
    if not tech:
        return {"rating": "觀望", "rating_class": "", "entry_range": "資料不足", "defense": "資料不足", "reason": "等待價格快取更新"}
    close = tech.get("close")
    ma5 = tech.get("ma5")
    ma10 = tech.get("ma10")
    ma20 = tech.get("ma20")
    ma60 = tech.get("ma60")
    entry = _to_float(s.get("entry", ""), None)
    large_event = tech.get("large_volume_event") or {}
    large_low = large_event.get("low")

    entry_candidates = [x for x in [entry, ma5, ma10] if x]
    if entry_candidates:
        low = min(entry_candidates) * 0.99
        high = max(entry_candidates) * 1.01
        entry_range = f"{fmt_num(low)} ~ {fmt_num(high)}"
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

    entry_high = None
    if entry_range != "資料不足":
        nums = [_to_float(x, None) for x in re.findall(r"\d+(?:\.\d+)?", entry_range)]
        entry_high = max([x for x in nums if x is not None], default=None)
    defense_value = _to_float(defense, None)
    gap = ((close / entry_high - 1) * 100) if close and entry_high else None

    if close and defense_value and close < defense_value:
        rating, cls, reason = "賣出/避開", "neg", "跌破關鍵防守價位"
    elif tech.get("volume_price") == "放量下跌":
        rating, cls, reason = "觀望", "", "放量下跌，先等賣壓消化"
    elif gap is not None and gap <= 2:
        rating, cls, reason = "可買進", "pos", "收盤仍在買進區間附近"
    elif gap is not None and gap <= 8:
        rating, cls, reason = "觀望", "", "略高於買進區間，等回測"
    else:
        rating, cls, reason = "觀望", "", "距買進區間偏遠，不追價"

    return {
        "rating": rating,
        "rating_class": cls,
        "entry_range": entry_range,
        "defense": defense,
        "reason": reason,
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
            macd_state = "多方動能"
        elif dif[-1] < dea_series[-1] and (macd_hist or 0) < 0:
            macd_state = "空方動能"
        else:
            macd_state = "收斂觀察"

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


def mini_line_svg(title: str, series_defs: list[tuple[str, list[float | None], str]], height: int = 150, fixed_range: tuple[float, float] | None = None, zero_line: bool = False) -> str:
    w, h = 300, height
    pad_l, pad_r, pad_t, pad_b = 34, 10, 20, 22
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
        legend += f'<text x="{w-92+idx*44}" y="14" fill="{color}" font-size="10">{esc(label)}</text>'
    return f"""
<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{esc(title)}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="#0d1117"/>
  {grid}
  {lines}
  <text x="{pad_l}" y="14" fill="#e6edf3" font-size="11">{esc(title)}</text>
  {legend}
</svg>"""


def indicator_chart_panel(rows: list[dict], label: str) -> str:
    data = indicator_series(rows)
    if not data:
        return '<div class="strategy-note" style="margin-top:10px">指標資料不足。</div>'
    kd = mini_line_svg(f"{label} KD", [("K", data["k"], "#58a6ff"), ("D", data["d"], "#d2a520")], fixed_range=(0, 100))
    macd = mini_line_svg(f"{label} MACD", [("DIF", data["dif"], "#58a6ff"), ("DEA", data["dea"], "#d2a520"), ("M", data["hist"], "#f85149")], zero_line=True)
    wr = mini_line_svg(f"{label} Williams %R", [("%R", data["wr"], "#a78bfa")], fixed_range=(-100, 0))
    return f"""
<div class="indicator-grid">
  <div class="indicator-box">{kd}</div>
  <div class="indicator-box">{macd}</div>
  <div class="indicator-box">{wr}</div>
</div>"""


def enrich_stock_fields(s: dict) -> dict:
    out = dict(s)
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
        if is_blank(out.get("entry")) and tech.get("ma5"):
            out["entry"] = f"{tech['ma5'] * 0.985:.2f} (MA5×98.5%)"
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
    ma_strip = ""
    for n in [5, 10, 20, 60]:
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
    return f"""
<div class="tech-panel">
  <div class="ma-strip">{ma_strip}</div>
  <div class="tech-summary-grid">
    <div class="info-cell"><div class="k">量價關係</div><div class="v">{esc(tech.get('volume_price','─'))}</div></div>
    <div class="info-cell"><div class="k">趨勢型態</div><div class="v">{esc(tech.get('trend_pattern','─'))}</div></div>
    <div class="info-cell"><div class="k">K線型態</div><div class="v">{esc(tech.get('candle_pattern','─'))}</div></div>
  </div>
</div>"""


def build_chip_panel(chip: dict, holding: dict) -> str:
    if not chip and not holding:
        return '<div class="strategy-note">尚未找到籌碼/股權分配快取；刷新 FinMind 後會顯示法人買賣超與大戶比例。</div>'
    chip_latest = chip.get("latest", {})
    chip_sum5 = chip.get("sum5", {})
    return f"""<div class="info-grid">
  <div class="info-cell"><div class="k">法人日期</div><div class="v">{esc(chip.get('date','─'))}</div></div>
  <div class="info-cell"><div class="k">外資買賣超</div><div class="v {('pos' if chip_latest.get('foreign',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('foreign'),0)}張</div></div>
  <div class="info-cell"><div class="k">投信買賣超</div><div class="v {('pos' if chip_latest.get('trust',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('trust'),0)}張</div></div>
  <div class="info-cell"><div class="k">自營商買賣超</div><div class="v {('pos' if chip_latest.get('dealer',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('dealer'),0)}張</div></div>
  <div class="info-cell"><div class="k">主力當日合計</div><div class="v {('pos' if chip_latest.get('total',0)>=0 else 'neg')}">{fmt_num(chip_latest.get('total'),0)}張</div></div>
  <div class="info-cell"><div class="k">外資5日</div><div class="v {('pos' if chip_sum5.get('foreign',0)>=0 else 'neg')}">{fmt_num(chip_sum5.get('foreign'),0)}張</div></div>
  <div class="info-cell"><div class="k">投信5日</div><div class="v {('pos' if chip_sum5.get('trust',0)>=0 else 'neg')}">{fmt_num(chip_sum5.get('trust'),0)}張</div></div>
  <div class="info-cell"><div class="k">主力5日合計</div><div class="v {('pos' if chip_sum5.get('total',0)>=0 else 'neg')}">{fmt_num(chip_sum5.get('total'),0)}張</div></div>
</div>"""


def basket_card(s: dict, basket: str, ledger: dict[str, dict] | None = None) -> str:
    gain_cls = gain_color(s.get("gain_6w", ""))
    if basket == "marching":
        action = "SFZ試單/續抱"
        tags = [
            ("行進籃", "tag-green"),
            ("TA3加碼觀察", "tag-yellow"),
            ("MA20主線", "tag"),
        ]
    elif basket == "consolidation":
        action = "MABC+CaryBot觀察"
        tags = [
            ("盤整籃", "tag-yellow"),
            ("早買雷達", "tag"),
            ("等轉強", "tag"),
        ]
    else:
        action = "過熱不追"
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
      <div class="basket-code">{s.get('id','')} <span class="basket-name">{s.get('name','')}</span></div>
      <div style="font-size:12px;color:#8b949e;margin-top:4px">收盤 {s.get('price','─')} ｜ 近6週 <span class="{gain_cls}">{s.get('gain_6w','─')}</span> ｜ 分數 {s.get('score','─')}</div>
    </div>
    <div class="basket-action">{action}</div>
  </div>
  <div style="font-size:12px;color:#c9d1d9">買點 {s.get('entry','─')} ｜ 目標 {s.get('target','─')} ｜ 防守 {s.get('stop','─')}</div>
  <div class="tag-row">{tag_html}</div>
  {signal_summary_html(s.get('id',''), ledger or {})}
  <div style="margin-top:10px"><a class="history-link" href="{stock_href(s.get('id',''))}">打開個股頁 →</a></div>
</div>"""


def build_basket_column(title: str, subtitle: str, stocks: list[dict], basket: str, ledger: dict[str, dict] | None = None) -> str:
    cards = "\n".join(basket_card(s, basket, ledger) for s in stocks[:12])
    if not cards:
        cards = '<div class="basket-card" style="color:#6e7681">今日沒有符合此籃條件的標的。</div>'
    return f"""
<div class="card">
  <div class="section-label">{title}</div>
  <div class="strategy-note" style="margin-bottom:12px">{subtitle}</div>
  {cards}
</div>"""


def build_index_page(reports: list[dict]) -> str:
    latest = reports[0] if reports else {}
    date_str = latest.get("date", "─")
    marching, consolidation, risk = split_baskets(latest.get("stocks", []))

    # 市場概況 card
    market_card = f"""
<div class="card">
  <div class="section-label">📰 大盤市況</div>
  <div class="market-text">{latest.get('market_overview','').replace(chr(10), '<br>')}</div>
</div>"""

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
  <div class="metric"><div class="metric-num" style="color:#d2a520">{len(consolidation)}</div><div class="metric-label">盤整籃：MABC/CaryBot 觀察</div></div>
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
  {market_card}
  {filter_card}
  {table_card}
  {history_card}
</div>"""

    return html_page("首頁", "home", body)


def build_daily_page(report: dict) -> str:
    """生成單日完整報告頁"""
    date_str = report.get("date", "─")
    stocks = report.get("stocks", [])
    green  = sum(1 for s in stocks if s["icon"] == "🟢")
    yellow = sum(1 for s in stocks if s["icon"] == "🟡")
    red    = sum(1 for s in stocks if s["icon"] == "🔴")

    stat_row = f"""
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#3fb950">{green}</div>
    <div style="font-size:11px;color:#6e7681">健康整理 🟢</div>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#d2a520">{yellow}</div>
    <div style="font-size:11px;color:#6e7681">強勢追漲 🟡</div>
  </div>
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 20px;text-align:center">
    <div style="font-size:24px;font-weight:800;color:#f85149">{red}</div>
    <div style="font-size:11px;color:#6e7681">短線超買 🔴</div>
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
        '&#x1F7E2; Health | &#x1F7E1; Strong | &#x1F534; Overbought'
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
    latest = reports[0] if reports else {}
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
    latest = reports[0] if reports else {}
    date_str = latest.get("date", "-")
    stocks = latest.get("stocks", [])
    marching, consolidation, risk = split_baskets(stocks)
    ledger = build_signal_ledger(reports)

    hero = f"""
<div class="card">
  <div class="section-label">Daily Strategy Stream</div>
  <div class="grid grid-3">
    <div class="metric"><div class="metric-num" style="color:#3fb950">{len(marching)}</div><div class="metric-label">行進籃：SFZ 訊號日先試單，TA3 作確認/加碼</div></div>
    <div class="metric"><div class="metric-num" style="color:#d2a520">{len(consolidation)}</div><div class="metric-label">盤整籃：M大 ABC 先觀察，CaryBot 找早買</div></div>
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
      <strong style="color:#d2a520">盤整籃</strong><br>
      MABC 判斷是否值得等待，CaryBot / VPA / WR / MA5-MA10 站回負責提早找買點。未突破前只小部位；突破追不到不追，等回測 MA5/MA10/箱頂不破再處理。
    </div>
  </div>
</div>"""

    body = (
        '<div class="container">'
        + '<div class="page-title">雙籃選股儀表板</div>'
        + f'<div class="page-sub">資料日期：{date_str} · 網站負責完整巡檢，Telegram 只負責重要提醒</div>'
        + hero
        + playbook
        + '<div class="grid grid-2">'
        + build_basket_column("行進籃｜SFZ 波段", "已進入較強趨勢的候選；重點是買點可執行、MA20續抱、避免漲停追高。", marching, "marching", ledger)
        + build_basket_column("盤整籃｜MABC + CaryBot", "尚未完全發動但值得等待；重點是量縮價穩、籌碼不離開、早買型態浮現。", consolidation, "consolidation", ledger)
        + '</div>'
        + build_basket_column("過熱/風險觀察", "強勢但不適合追價；等回測、降溫或重新整理後再評估。", risk, "risk", ledger)
        + '</div>'
    )
    return html_page("雙籃儀表板", "basket", body)


def build_signals_page(reports):
    ledger = build_signal_ledger(reports)
    latest = reports[0] if reports else {}
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
        rows += f"""
<tr>
  <td>
    <div style="font-weight:700">{item['id']} {item['name']}</div>
    <div class="tag-row">{latest_mark}<span class="tag">{basket}</span></div>
  </td>
  <td><strong>{len(events)}</strong> 次</td>
  <td>{events[0]['date']}<br><span style="color:#8b949e">最近 {latest_event['date']}</span></td>
  <td>買點 {latest_event['entry']}<br><span style="color:#8b949e">收盤 {latest_event['price']} ｜ 分數 {latest_event['score']}</span></td>
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
      這頁先用每日報告建立「入選台帳」。等 Telegram 發送程式把成功推播寫入 <strong>signal_push_log.csv</strong> 後，這裡就會變成查漏清單：任何 0/N 或未滿 N/N 的個股，都代表有買點需要補查。
    </div>
  </div>
  <div class="card">
    <div class="section-label">歷史訊號摘要</div>
    <div style="overflow-x:auto">
      <table class="stock-table signal-table">
        <thead>
          <tr><th>個股</th><th>入選</th><th>首次/最近</th><th>最新買點</th><th>推播</th><th>出現日期</th></tr>
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
    decision = build_trade_decision(tech, s)
    s_view = dict(s)
    if latest.get("close") is not None:
        s_view["price"] = f'{latest["close"]:.2f}'
        s_view["price_date"] = latest.get("date", "")
    ai_logs = read_ai_logs(stock_id)
    ai_html = ""
    if ai_logs:
        for log in ai_logs:
            headline = f"{log['created_at']}｜{log['kind']}｜收盤 {log.get('close') or '─'}｜買點 {log.get('buy_zone') or '─'}"
            body = (log.get("report") or "")[:1800]
            ai_html += f'<div class="mini-report"><strong>{esc(headline)}</strong>\n\n{esc(body)}</div>'
    else:
        ai_html = '<div class="strategy-note">目前沒有讀到 v44 AI 分析紀錄。之後只要 v44 的快速分析或 AI深度分析寫入 SQLite，這裡會自動帶出最近紀錄。</div>'

    event_rows = ""
    for e in item.get("events", [])[-12:][::-1]:
        event_rows += f"""
<tr>
  <td>{e['date']}</td><td>{basket_label(e['basket'])}</td><td>{e['entry']}</td><td>{e['price']}</td><td>{e['score']}</td>
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
    main_force_data = json.dumps(main_force_payload(chip_series, daily), ensure_ascii=False)
    chart_script = f"""
<script>
const chartData_{stock_id} = {chart_data};
const holdingData_{stock_id} = {holding_data};
const chipFlowData_{stock_id} = {chip_flow_data};
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
      syncHoldingFromK_{stock_id}(item.date);
    }});
    chart.addEventListener('mouseleave', ()=>{{
      tip.style.display='none';
      line.style.display='none';
      clearHoldingHover_{stock_id}();
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

    body = f"""
<div class="container">
  <div style="margin-bottom:8px"><a href="../baskets.html" style="color:#6e7681;font-size:13px">&larr; 回雙籃儀表板</a></div>
  <div class="page-title">{esc(stock_id)} {esc(s.get('name',''))}</div>
  <div class="page-sub">v44 個股研究頁 · 報告日期 {esc(s.get('report_date','─'))}</div>
  <div class="detail-hero">
    <div class="card">
      <div class="section-label">資訊卡</div>
      <div class="info-grid">
        <div class="info-cell"><div class="k">FinMind收盤 {esc(s_view.get('price_date',''))}</div><div class="v">{esc(s_view.get('price','─'))}</div></div>
        <div class="info-cell"><div class="k">操作評價</div><div class="v {decision['rating_class']}">{esc(decision['rating'])}</div></div>
        <div class="info-cell"><div class="k">分類</div><div class="v">{basket_label(classify_basket(s))}</div></div>
        <div class="info-cell"><div class="k">買進區間</div><div class="v">{esc(decision['entry_range'])}</div></div>
        <div class="info-cell"><div class="k">關鍵防守價位</div><div class="v">{esc(decision['defense'])}</div></div>
        <div class="info-cell"><div class="k">判斷理由</div><div class="v">{esc(decision['reason'])}</div></div>
        <div class="info-cell"><div class="k">外資近5日</div><div class="v">{esc(s.get('foreign_5d','─'))}</div></div>
      </div>
      <div class="pill-row">
        <span class="tag tag-green">SFZ</span><span class="tag tag-yellow">MABC/CaryBot</span><span class="tag">v44資料</span>
      </div>
    </div>
    <div class="card">
      <div class="section-label">快速分析</div>
      <div class="mini-report">{esc(quick_analysis_text(s_view, item))}</div>
    </div>
  </div>

  <div class="card">
    <div class="section-label">v44 技術 / 買點雷達</div>
    {build_tech_panel(tech)}
    <div class="strategy-note" style="margin-top:12px">
      行進籃以 SFZ 訊號與 MA20 續抱為主；盤整籃以 MABC 值得等待、CaryBot 買點浮現為主。若距建議買點已明顯過高，視為不追價，等待 MA5/MA10/箱頂回測。
    </div>
  </div>

  <div class="card">
    <div class="section-label">日K / 週K / 月K</div>
    <div id="{chart_id}" class="chart-box">
      <div class="chart-tabs">
        <button type="button" class="active" data-btn="daily" onclick="showChart_{stock_id}('daily')">日K</button>
        <button type="button" data-btn="weekly" onclick="showChart_{stock_id}('weekly')">週K</button>
        <button type="button" data-btn="monthly" onclick="showChart_{stock_id}('monthly')">月K</button>
      </div>
      <div class="chart-pane" data-pane="daily"><div class="hover-chart" data-mode="daily">{chart_svg(daily, '日K')}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>{indicator_chart_panel(daily, '日K')}</div>
      <div class="chart-pane" data-pane="weekly" style="display:none"><div class="hover-chart" data-mode="weekly">{chart_svg(weekly, '週K')}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>{indicator_chart_panel(weekly, '週K')}</div>
      <div class="chart-pane" data-pane="monthly" style="display:none"><div class="hover-chart" data-mode="monthly">{chart_svg(monthly, '月K')}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>{indicator_chart_panel(monthly, '月K')}</div>
      <div class="linked-holding-panel">
        <div class="linked-holding-title">股權分配連動</div>
        {holding_stat_html}
        <div id="{holding_id}" class="hover-chart">{holding_line_svg(holding_series, "股權分配折線圖")}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="section-label">10 日籌碼動向折射圖</div>
    {build_chip_panel(chip, holding)}
    <div class="chart-stack">
      <div id="{chip_flow_id}" class="chart-box hover-chart">{chip_flow_svg(chip_series, "10日籌碼動向折射圖")}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>
      <div id="{main_force_id}" class="chart-box hover-chart">{main_force_price_svg(chip_series, daily, "主力增減張數與收盤價關係")}<div class="chart-crosshair"></div><div class="chart-tooltip"></div></div>
    </div>
    <div class="strategy-note" style="margin-top:12px">
      外資、投信、自營商以 FinMind 法人買賣超換算為張數；主力增減張數先以三大法人合計近似。柱狀圖向上為買超，向下為賣超。
    </div>
  </div>

  <div class="grid grid-2">
    <div class="card">
      <div class="section-label">歷史訊號</div>
      <div style="overflow-x:auto">
        <table class="stock-table"><thead><tr><th>日期</th><th>籃別</th><th>買點</th><th>收盤</th><th>分數</th></tr></thead><tbody>{event_rows}</tbody></table>
      </div>
    </div>
    <div class="card">
      <div class="section-label">v44 AI / 快速分析紀錄</div>
      {ai_html}
    </div>
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
        price = latest.get("close")
        date = latest.get("date", "")
        item = {
            "id": sid,
            "name": s.get("name", ""),
            "basket": basket_label(classify_basket(s)),
            "price": fmt_num(price),
            "price_date": date,
            "entry": s.get("entry", "─"),
            "target": s.get("target", "─"),
            "stop": s.get("stop", "─"),
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
  <td><div class="price-entry">進 {esc(x['entry'])}</div><div class="price-target">目 {esc(x['target'])}</div><div class="price-stop">守 {esc(x['stop'])}</div></td>
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
  <div class="page-sub">FinMind 收盤價 · SFZ 買點 · MABC/CaryBot 分類 · 點股票進資訊卡</div>
  <div class="card">
    <div class="section-label">Stock Browser</div>
    <input id="stockSearch" class="searchbar" placeholder="搜尋股票代號、名稱、行進籃、盤整籃..." oninput="filterStocks()">
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>分類</th><th>FinMind收盤</th><th>買點/目標/防守</th><th>分數</th><th>訊號</th></tr></thead>
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
            "entry": s.get("entry", "─"),
            "target": s.get("target", "─"),
            "stop": s.get("stop", "─"),
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
  <td><div class="price-entry">進 {esc(x['entry'])}</div><div class="price-target">目 {esc(x['target'])}</div><div class="price-stop">守 {esc(x['stop'])}</div></td>
</tr>"""

    body = f"""
<div class="container">
  <div class="page-title">買點雷達</div>
  <div class="page-sub">用 FinMind 最新收盤比對 SFZ 建議買點，優先找「能執行」而不是「已經追遠」的標的</div>
  <div class="card">
    <div class="section-label">Buy Radar</div>
    <div class="grid grid-3">
      <div class="metric"><div class="metric-num" style="color:#3fb950">{near}</div><div class="metric-label">接近買點：優先確認</div></div>
      <div class="metric"><div class="metric-num" style="color:#d2a520">{pullback}</div><div class="metric-label">稍高：等回測</div></div>
      <div class="metric"><div class="metric-num" style="color:#f85149">{extended}</div><div class="metric-label">過遠：不追高</div></div>
    </div>
    <div class="strategy-note" style="margin-top:14px">這頁先用 SFZ 報告買點 + FinMind 收盤價建立網站版雷達。完整 v44 的 rule-first 雷達可再把 CaryBot PreBuy、MABC A/B/C、量價共振分數接進同一張表。</div>
  </div>
  <div class="card">
    <div class="section-label">候選排序</div>
    <div style="overflow-x:auto">
      <table class="stock-table">
        <thead><tr><th>個股</th><th>狀態</th><th>收盤</th><th>距買點</th><th>買點/目標/防守</th></tr></thead>
        <tbody>{table}</tbody>
      </table>
    </div>
  </div>
</div>"""
    return html_page("買點雷達", "radar", body)


def build_stock_pages(reports: list[dict]) -> int:
    stock_map = find_latest_stock_map(reports)
    ledger = build_signal_ledger(reports)
    out_dir = OUTPUT_DIR / "stocks"
    out_dir.mkdir(parents=True, exist_ok=True)
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
    (OUTPUT_DIR / "baskets.html").write_text(build_baskets_page(reports), encoding="utf-8")
    print("   [OK] baskets.html", flush=True)
    (OUTPUT_DIR / "signals.html").write_text(build_signals_page(reports), encoding="utf-8")
    print("   [OK] signals.html", flush=True)
    (OUTPUT_DIR / "stocks.html").write_text(build_stocks_index_page(reports), encoding="utf-8")
    print("   [OK] stocks.html", flush=True)
    (OUTPUT_DIR / "radar.html").write_text(build_buy_radar_page(reports), encoding="utf-8")
    print("   [OK] radar.html", flush=True)
    (OUTPUT_DIR / "history.html").write_text(build_history_page(reports), encoding="utf-8")
    print("   [OK] history.html", flush=True)
    stock_page_count = build_stock_pages(reports)
    print(f"   [OK] stocks/*.html ({stock_page_count})", flush=True)

    for r in reports:
        html = build_daily_page(r)
        out = OUTPUT_DIR / "daily" / f"{r['date']}.html"
        out.write_text(html, encoding="utf-8")
        print(f"   [OK] daily/{r['date']}.html", flush=True)

    print(f"\n[Done] {len(reports)+6+stock_page_count} files -> {OUTPUT_DIR}", flush=True)
    print("[Next] git init && git add . && git commit && push to GitHub Pages", flush=True)


if __name__ == "__main__":
    main()
