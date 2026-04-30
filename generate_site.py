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
from pathlib import Path
from datetime import datetime

# ──────────────────────────────────────────────
#  路徑設定（Windows / Linux 自動切換）
# ──────────────────────────────────────────────
_WIN_REPORTS  = Path(r"C:\Users\USER\OneDrive\文件\Claude\Projects\Stock from Zero")
_LINUX_REPORTS = Path("/sessions/adoring-amazing-mayer/mnt/Stock from Zero")
_REPO_REPORTS = Path(__file__).parent / "reports"

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
}
"""

def nav_html(active: str = "home") -> str:
    tabs = [
        ("home",    "index.html",   "首頁"),
        ("daily",   "daily.html",   "今日選股"),
        ("basket",  "baskets.html", "雙籃儀表板"),
        ("history", "history.html", "歷史報告"),
    ]
    items = ""
    for key, href, label in tabs:
        cls = "tab active" if key == active else "tab"
        items += f'<a href="{href}" class="{cls}">{label}</a>\n'
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


def html_page(title: str, nav_key: str, body: str) -> str:
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
{nav_html(nav_key)}
{body}
{footer_html()}
</body>
</html>"""


# ──────────────────────────────────────────────
#  各頁面生成
# ──────────────────────────────────────────────

def build_stock_table(stocks: list[dict], compact: bool = False) -> str:
    """生成股票表格 HTML"""
    rows = ""
    for i, s in enumerate(stocks, 1):
        badge = status_badge(s["icon"], s["status"])
        gain_cls = gain_color(s["gain_6w"])

        if compact:
            rows += f"""
<tr>
  <td><span style="color:#6e7681;font-size:11px">#{i}</span></td>
  <td>
    <div style="font-weight:700">{s['id']} {s['name']}</div>
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
    <div style="font-weight:700;font-size:14px">{s['id']}</div>
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
        return float(s)
    except Exception:
        return default


def split_baskets(stocks: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """用現有每日報告欄位先做網站層分籃；正式版可改讀 JSON。"""
    marching, consolidation, risk = [], [], []
    for s in stocks:
        gain = _to_float(s.get("gain_6w", "0"))
        score = _to_float(s.get("score", "0"))
        icon = s.get("icon", "")
        status = s.get("status", "")
        if icon == "🔴" or "超買" in status:
            risk.append(s)
        elif icon == "🟡" or gain >= 18 or score >= 170:
            marching.append(s)
        else:
            consolidation.append(s)
    return marching, consolidation, risk


def basket_card(s: dict, basket: str) -> str:
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
</div>"""


def build_basket_column(title: str, subtitle: str, stocks: list[dict], basket: str) -> str:
    cards = "\n".join(basket_card(s, basket) for s in stocks[:12])
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
        + build_stock_table(stocks, compact=False)
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
    return html_page(f"{date_str}", "daily", body)


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
        + build_basket_column("行進籃｜SFZ 波段", "已進入較強趨勢的候選；重點是買點可執行、MA20續抱、避免漲停追高。", marching, "marching")
        + build_basket_column("盤整籃｜MABC + CaryBot", "尚未完全發動但值得等待；重點是量縮價穩、籌碼不離開、早買型態浮現。", consolidation, "consolidation")
        + '</div>'
        + build_basket_column("過熱/風險觀察", "強勢但不適合追價；等回測、降溫或重新整理後再評估。", risk, "risk")
        + '</div>'
    )
    return html_page("雙籃儀表板", "basket", body)


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
    md_files = find_all_reports()
    if not md_files:
        print("[ERROR] No report MD files found.", flush=True)
        return

    print(f"\n[Read] Found {len(md_files)} reports...", flush=True)
    reports = []
    for f in md_files:
        try:
            r = parse_report(f)
            reports.append(r)
            print(f"   [OK] {r['date']} - {len(r['stocks'])} stocks", flush=True)
        except Exception as e:
            print(f"   [WARN] {f.name}: {e}", flush=True)

    if not reports:
        print("[ERROR] No reports parsed.", flush=True)
        return

    print("\n[Build] Generating pages...", flush=True)
    (OUTPUT_DIR / "index.html").write_text(build_index_page(reports), encoding="utf-8")
    print("   [OK] index.html", flush=True)
    (OUTPUT_DIR / "daily.html").write_text(build_latest_daily_page(reports), encoding="utf-8")
    print("   [OK] daily.html", flush=True)
    (OUTPUT_DIR / "baskets.html").write_text(build_baskets_page(reports), encoding="utf-8")
    print("   [OK] baskets.html", flush=True)
    (OUTPUT_DIR / "history.html").write_text(build_history_page(reports), encoding="utf-8")
    print("   [OK] history.html", flush=True)

    for r in reports:
        html = build_daily_page(r)
        out = OUTPUT_DIR / "daily" / f"{r['date']}.html"
        out.write_text(html, encoding="utf-8")
        print(f"   [OK] daily/{r['date']}.html", flush=True)

    print(f"\n[Done] {len(reports)+3} files -> {OUTPUT_DIR}", flush=True)
    print("[Next] git init && git add . && git commit && push to GitHub Pages", flush=True)


if __name__ == "__main__":
    main()
