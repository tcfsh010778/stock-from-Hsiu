from __future__ import annotations

import argparse
import concurrent.futures
import html
import json
import os
import re
import warnings
from pathlib import Path

import pandas as pd
from jsonschema import Draft202012Validator

from stock_v2_public.analysis.engine import ENGINE_VERSION, analyze_multi_timeframe, stable_json
from stock_v2_public.site import STOCK_PAGE_HTML, V2_CSS, V2_JS, stock_redirect_html

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
SCHEMA_PATH = ROOT / "schemas" / "technical_pattern_packet.schema.json"
PRIVATE_SOURCE_SHA = "a88c54258cf29f0d898e6ef68d8edbdba3e83ab2"


def load_stock_map(docs_dir: Path, data_dir: Path) -> dict[str, dict]:
    """Read the stock/name map from the freshly generated search page.

    This avoids running the legacy report/query pipeline a second time. Price
    files remain the coverage fallback so every cached stock gets a V2 route.
    """
    stocks: dict[str, dict] = {}
    search_page = docs_dir / "stocks.html"
    if search_page.exists():
        text = search_page.read_text(encoding="utf-8")
        pattern = r'href="(?:v2/)?stocks/([0-9A-Za-z]+)\.html"[^>]*>\s*\1\s+([^<]+)</a>'
        for stock_id, name in re.findall(pattern, text):
            stocks[stock_id] = {"name": html.unescape(name).strip()}
    for price_path in sorted((data_dir / "prices").glob("*.csv")):
        stocks.setdefault(price_path.stem, {"name": ""})
    return stocks


def load_decisions(path: Path) -> tuple[dict[str, dict], dict]:
    if not path.exists():
        return {}, {"data_quality": {"state": "missing", "warnings": ["daily_decisions.json is missing"]}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    decisions = {str(row.get("stock_id") or row.get("security_id")): row for row in payload.get("decisions", [])}
    return decisions, payload


def safe_decision(stock_id: str, decision: dict | None) -> dict:
    if decision:
        return decision
    return {
        "stock_id": stock_id,
        "action_state": "UNRATED",
        "rule_version": "daily_decisions_uncovered",
        "reasons": ["此股票尚未納入當日規則決策集合"],
        "blockers": ["缺少 daily_decisions 規則結果；不得由 AI 補寫"],
        "warnings": [],
    }


def trim_packet(packet: dict) -> dict:
    limit = {"daily": 120, "weekly": 60, "monthly": 36}.get(packet.get("timeframe"), 90)
    packet["series"] = packet.get("series", [])[-limit:]
    packet.pop("swings", None)
    packet["patterns"] = packet.get("patterns", [])[:24]
    packet["trendlines"] = packet.get("trendlines", [])[:8]
    packet["support_resistance"] = packet.get("support_resistance", [])[:12]
    return packet


def switch_navigation(path: Path, available_ids: set[str] | None = None) -> int:
    if not path.exists():
        return 0
    text = path.read_text(encoding="utf-8")
    def replace(match: re.Match) -> str:
        stock_id = match.group(1)
        if available_ids is not None and stock_id not in available_ids:
            return match.group(0)
        return f'href="v2/stocks/{stock_id}.html"'

    changed, count = re.subn(r'href="stocks/([0-9A-Za-z]+)\.html"', replace, text)
    if changed != text:
        path.write_text(changed, encoding="utf-8")
    return count


_WORKER_VALIDATOR: Draft202012Validator | None = None


def analyze_stock_task(args: tuple) -> tuple[str, str, list[dict] | None, str | None]:
    stock_id, name, price_path, decision, freshness_status, global_warnings, validate = args
    try:
        warnings.filterwarnings("ignore", message="some peaks have a prominence of 0")
        frame = pd.read_csv(price_path)
        if len(frame) < 30:
            raise ValueError("fewer than 30 OHLCV rows")
        latest_date = str(frame.iloc[-1]["date"])
        packets = analyze_multi_timeframe(
            frame,
            stock_id=stock_id,
            price_adjustment={"mode": "none", "source": None, "verified": False},
            decision=decision,
            freshness={"status": freshness_status, "data_date": latest_date, "warnings": global_warnings},
        )
        global _WORKER_VALIDATOR
        if validate and _WORKER_VALIDATOR is None:
            _WORKER_VALIDATOR = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))
        for packet in packets:
            trim_packet(packet)
            packet["warnings"] = sorted(set(packet.get("warnings", []) + global_warnings))
            if _WORKER_VALIDATOR:
                _WORKER_VALIDATOR.validate(packet)
        return stock_id, name, packets, None
    except Exception as exc:
        return stock_id, name, None, str(exc)


def build_v2(*, docs_dir: Path = DOCS_DIR, data_dir: Path = DATA_DIR, validate: bool = False, switch_links: bool = False, only: set[str] | None = None, all_stocks: bool = False, workers: int | None = None) -> dict:
    stock_map = load_stock_map(docs_dir, data_dir)
    decisions, decision_payload = load_decisions(data_dir / "daily_decisions.json")
    quality = decision_payload.get("data_quality") or {}
    global_warnings = list(quality.get("warnings") or [])
    freshness_status = str(quality.get("state") or "unknown")

    root = docs_dir / "v2"
    packet_dir = root / "data"
    redirect_dir = root / "stocks"
    asset_dir = root / "assets"
    for directory in (packet_dir, redirect_dir, asset_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (root / "stock.html").write_text(STOCK_PAGE_HTML, encoding="utf-8")
    (asset_dir / "v2.css").write_text(V2_CSS + "\n", encoding="utf-8")
    (asset_dir / "v2.js").write_text(V2_JS + "\n", encoding="utf-8")

    target_ids = set(stock_map) if all_stocks else set(decisions)
    if only:
        target_ids = set(only)
    target_ids &= set(stock_map)
    if not only:
        for old in packet_dir.glob("*.json"):
            old.unlink()
        for old in redirect_dir.glob("*.html"):
            old.unlink()

    index: dict[str, dict] = {}
    failures: list[dict] = []
    exclusions: list[dict] = []
    tasks = []
    for stock_id in sorted(target_ids):
        stock = stock_map[stock_id]
        price_path = data_dir / "prices" / f"{stock_id}.csv"
        if not price_path.exists():
            failures.append({"stock_id": stock_id, "reason": "price file missing"})
            continue
        tasks.append((stock_id, str(stock.get("name") or ""), str(price_path), safe_decision(stock_id, decisions.get(stock_id)), freshness_status, global_warnings, validate))

    worker_count = workers or min(4, os.cpu_count() or 1)
    with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as executor:
        for stock_id, name, packets, error in executor.map(analyze_stock_task, tasks, chunksize=1):
            if not error and packets:
                (packet_dir / f"{stock_id}.json").write_text(stable_json(packets) + "\n", encoding="utf-8")
                (redirect_dir / f"{stock_id}.html").write_text(stock_redirect_html(stock_id), encoding="utf-8")
                index[stock_id] = {
                    "name": name,
                    "action_state": packets[0].get("decision", {}).get("action_state", "UNRATED"),
                    "data_date": packets[0]["data_date"],
                }
            else:
                reason = error or "no packets generated"
                if "invalid high/low/volume" in reason or "fewer than 30 OHLCV rows" in reason:
                    exclusions.append({"stock_id": stock_id, "reason": reason})
                else:
                    failures.append({"stock_id": stock_id, "reason": reason})

    manifest = {
        "schema_version": "1.0.0",
        "engine_version": ENGINE_VERSION,
        "private_source_sha": PRIVATE_SOURCE_SHA,
        "stock_count": len(index),
        "coverage": "all_prices" if all_stocks else "daily_decisions",
        "failure_count": len(failures),
        "excluded_count": len(exclusions),
        "stocks": index,
        "failures": failures,
        "exclusions": exclusions,
    }
    (packet_dir / "index.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    switched = 0
    if switch_links:
        available_ids = set(index)
        switched += switch_navigation(docs_dir / "index.html", available_ids)
        switched += switch_navigation(docs_dir / "stocks.html", available_ids)

    sitemap_path = docs_dir / "sitemap.xml"
    if sitemap_path.exists():
        sitemap = sitemap_path.read_text(encoding="utf-8")
        marker = "</urlset>"
        urls = ["v2/stock.html"] + [f"v2/stocks/{sid}.html" for sid in index]
        additions = "".join(f"  <url><loc>https://tcfsh010778.github.io/stock-from-Hsiu/{url}</loc></url>\n" for url in urls if url not in sitemap)
        if additions:
            sitemap_path.write_text(sitemap.replace(marker, additions + marker), encoding="utf-8")

    return {"stock_count": len(index), "excluded_count": len(exclusions), "failure_count": len(failures), "switched_links": switched, "failures": failures}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate public-safe Stock from Hsiu V2 pages")
    parser.add_argument("--validate", action="store_true", help="validate every generated packet")
    parser.add_argument("--switch-navigation", action="store_true", help="point home/search stock links at V2")
    parser.add_argument("--only", action="append", help="generate selected stock id (repeatable)")
    parser.add_argument("--all-stocks", action="store_true", help="generate V2 for every cached price file instead of the daily decision universe")
    parser.add_argument("--workers", type=int, help="parallel analysis processes (default: up to 4)")
    args = parser.parse_args()
    result = build_v2(validate=args.validate, switch_links=args.switch_navigation, only=set(args.only or []) or None, all_stocks=args.all_stocks, workers=args.workers)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["failure_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
