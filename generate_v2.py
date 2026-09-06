from __future__ import annotations

import argparse
import concurrent.futures
from copy import deepcopy
import csv
import html
import math
from functools import lru_cache
from datetime import date as date_type
import json
import os
import re
import warnings
from pathlib import Path

import pandas as pd
from jsonschema import Draft202012Validator

from stock_v2_public.analysis.engine import ENGINE_VERSION, analyze_multi_timeframe, stable_json
from stock_v2_public.site import STOCK_PAGE_HTML, V2_CSS, V2_JS, stock_redirect_html
from stock_rules import holding_group
from stock_v2_public.profile_page import profile_page_html

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
SCHEMA_PATH = ROOT / "schemas" / "technical_pattern_packet.schema.json"
CANDLE_SCHEMA_PATH = ROOT / "schemas" / "candlestick_pattern_event.schema.json"
PRIVATE_SOURCE_SHA = "2b3ef38ca47f50132556dbd3c469a48c8810ab3e"
FIXED_STOP_PCT = 15.0


def _float(value: object) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


@lru_cache(maxsize=8)
def _official_sidecar(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _validated_official_records(path: Path, *, dataset_id: str, unit: str, as_of: str | None) -> list[dict]:
    payload = _official_sidecar(path)
    if (
        payload.get("dataset_id") != dataset_id
        or payload.get("schema_version") != "1.0.0"
        or payload.get("unit") != unit
    ):
        raise ValueError(f"official sidecar metadata mismatch: {path.name}")
    data_date = str(payload.get("date") or "")
    try:
        data_date = date_type.fromisoformat(data_date).isoformat()
        if as_of is not None:
            as_of = date_type.fromisoformat(as_of).isoformat()
    except ValueError as exc:
        raise ValueError(f"official sidecar date mismatch: {path.name}") from exc
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError(f"official sidecar rows mismatch: {path.name}")
    validated: list[dict] = []
    for raw in rows:
        if not isinstance(raw, dict) or not raw.get("security_id"):
            raise ValueError(f"official sidecar row mismatch: {path.name}")
        row = dict(raw)
        try:
            row_date = date_type.fromisoformat(str(row.get("date") or "")).isoformat()
        except ValueError as exc:
            raise ValueError(f"official sidecar row date mismatch: {path.name}") from exc
        if row_date > data_date:
            raise ValueError(f"official sidecar row is newer than dataset date: {path.name}")
        row["date"] = row_date
        if as_of is None or row_date <= as_of:
            validated.append(row)
    return validated


def load_market_evidence(data_dir: Path, stock_id: str, *, as_of: str | None = None) -> dict:
    """Build the public-safe synchronized chip panels without importing v44.

    Missing datasets stay missing and are disclosed in ``gaps``.  This keeps a
    sparse public cache from silently turning zeroes into fabricated evidence.
    """

    if as_of is not None:
        try:
            as_of = date_type.fromisoformat(as_of).isoformat()
        except ValueError as exc:
            raise ValueError("market evidence as-of date is invalid") from exc
    gaps: list[str] = []
    source_dates: dict[str, str] = {}

    chip_rows = _csv_rows(data_dir / "chips" / f"{stock_id}.csv")
    institutional_by_date: dict[str, dict] = {}
    for row in chip_rows:
        date = str(row.get("date") or "")
        buy, sell = _float(row.get("buy")), _float(row.get("sell"))
        if not date or buy is None or sell is None:
            continue
        item = institutional_by_date.setdefault(
            date, {"date": date, "foreign": 0.0, "trust": 0.0, "dealer": 0.0, "total": 0.0}
        )
        net_lots = (buy - sell) / 1000.0
        name = str(row.get("name") or "")
        if "Foreign" in name:
            item["foreign"] += net_lots
        elif "Investment_Trust" in name:
            item["trust"] += net_lots
        elif "Dealer" in name:
            item["dealer"] += net_lots
        item["total"] += net_lots
    institutional = [institutional_by_date[key] for key in sorted(institutional_by_date)][-260:]
    if institutional:
        source_dates["institutional"] = institutional[-1]["date"]
    else:
        gaps.append("institutional")

    foreign_ownership = []
    for row in _csv_rows(data_dir / "foreign_shareholding" / f"{stock_id}.csv"):
        date = str(row.get("date") or "")
        shares = _float(row.get("foreign_shares_lot"))
        if shares is None:
            raw = _float(row.get("foreign_shares") or row.get("ForeignInvestmentShares"))
            shares = raw / 1000.0 if raw is not None else None
        ratio = _float(row.get("foreign_ratio") or row.get("ForeignInvestmentSharesRatio"))
        if date and (shares is not None or ratio is not None):
            foreign_ownership.append({"date": date, "foreign_shares": shares, "foreign_ratio": ratio})
    foreign_ownership = sorted(foreign_ownership, key=lambda item: item["date"])[-260:]
    if foreign_ownership:
        source_dates["foreign_ownership"] = foreign_ownership[-1]["date"]
    else:
        gaps.append("foreign_ownership")

    margin = []
    for row in _csv_rows(data_dir / "margin" / f"{stock_id}.csv"):
        date = str(row.get("date") or "")
        margin_balance = _float(row.get("margin_balance") or row.get("MarginPurchaseTodayBalance"))
        short_balance = _float(row.get("short_balance") or row.get("ShortSaleTodayBalance"))
        if date and (margin_balance is not None or short_balance is not None):
            margin.append({"date": date, "margin_balance": margin_balance, "short_balance": short_balance})
    margin = sorted(margin, key=lambda item: item["date"])[-260:]
    if margin:
        source_dates["margin"] = margin[-1]["date"]
    else:
        gaps.append("margin")

    holding_by_date: dict[str, list[dict]] = {}
    for row in _csv_rows(data_dir / "holding_shares" / f"{stock_id}.csv"):
        date = str(row.get("date") or "")
        if date:
            holding_by_date.setdefault(date, []).append(row)
    holdings = []
    for date in sorted(holding_by_date):
        item = {
            "date": date,
            "major": 0.0,
            "middle": 0.0,
            "retail": 0.0,
            "total_people": None,
        }
        for row in holding_by_date[date]:
            level = str(row.get("HoldingSharesLevel") or "")
            people, percent = _float(row.get("people")), _float(row.get("percent"))
            if level == "total":
                item["total_people"] = int(people) if people is not None else None
                continue
            group = holding_group(level)
            if group in {"major", "middle", "retail"} and percent is not None:
                item[group] += percent
        for key in ("major", "middle", "retail"):
            item[key] = round(item[key], 4)
        holdings.append(item)
    holdings = holdings[-104:]
    if holdings:
        source_dates["holdings"] = holdings[-1]["date"]
    else:
        gaps.append("holdings")

    # Verified official net values override the corresponding date without inventing buy/sell legs.
    for filename, unit in (("official_chip_history.json", "shares"), ("official_margin_snapshot.json", "lots")):
        path = data_dir / filename
        if not path.exists():
            continue
        dataset_id = "official_chip_history" if unit == "shares" else "official_margin_snapshot"
        records = [
            row for row in _validated_official_records(path, dataset_id=dataset_id, unit=unit, as_of=as_of)
            if str(row.get("security_id")) == stock_id
        ]
        if unit == "shares":
            merged = {row["date"]: row for row in institutional}
            for row in records:
                item = {"date": row["date"]}
                for key, source_key in (("foreign", "foreign_net_shares"), ("trust", "investment_trust_net_shares"), ("dealer", "dealer_net_shares")):
                    value = _float(row.get(source_key))
                    item[key] = value / 1000 if value is not None else merged.get(row["date"], {}).get(key)
                item["total"] = sum(item[key] for key in ("foreign", "trust", "dealer")) if all(item[key] is not None for key in ("foreign", "trust", "dealer")) else None
                merged[row["date"]] = item
            institutional = [merged[key] for key in sorted(merged)][-260:]
            if institutional:
                source_dates["institutional"] = institutional[-1]["date"]
                gaps = [gap for gap in gaps if gap != "institutional"]
        else:
            merged = {row["date"]: row for row in margin}
            for row in records:
                merged[row["date"]] = {
                    "date": row["date"],
                    "margin_balance": _float(row.get("margin_balance_lots")),
                    "short_balance": _float(row.get("short_balance_lots")),
                }
            margin = [merged[key] for key in sorted(merged)][-260:]
            if margin:
                source_dates["margin"] = margin[-1]["date"]
                gaps = [gap for gap in gaps if gap != "margin"]

    if as_of is not None:
        for values in (institutional, foreign_ownership, margin, holdings):
            values[:] = [row for row in values if str(row.get("date") or "") <= as_of]
        source_dates = {
            key: values[-1]["date"]
            for key, values in (
                ("institutional", institutional),
                ("foreign_ownership", foreign_ownership),
                ("margin", margin),
                ("holdings", holdings),
            )
            if values
        }
        gaps = [key for key in ("institutional", "foreign_ownership", "margin", "holdings") if key not in source_dates]
    return {
        "institutional": institutional,
        "foreign_ownership": foreign_ownership,
        "margin": margin,
        "holdings": holdings,
        "source_dates": source_dates,
        "gaps": gaps,
    }


def add_public_workbench(packet: dict, market_evidence: dict | None = None) -> dict:
    rows = packet.get("series") or []
    if rows:
        reference_price = float(rows[-1]["close"])
        packet["risk_control"] = {
            "method": "fixed_percent_from_latest_close",
            "reference_date": rows[-1]["date"],
            "reference_price": round(reference_price, 4),
            "stop_loss_pct": FIXED_STOP_PCT,
            "stop_price": round(reference_price * (1.0 - FIXED_STOP_PCT / 100.0), 4),
        }
    if packet.get("timeframe") == "daily":
        packet["market_evidence"] = market_evidence or {
            "institutional": [], "foreign_ownership": [], "margin": [], "holdings": [],
            "source_dates": {}, "gaps": ["institutional", "foreign_ownership", "margin", "holdings"],
        }
    return packet


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


def load_price_refresh_summary(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def load_price_basis(data_dir: Path, stock_id: str, frame: pd.DataFrame, expected_date: str) -> dict:
    path = data_dir / "price_basis" / f"{stock_id}.json"
    if not path.exists():
        raise ValueError("unverified price basis: official history rebuild required")
    metadata = json.loads(path.read_text(encoding="utf-8"))
    if metadata.get("mode") != "official_reference_ratio_back_adjusted_v1" or metadata.get("verified") is not True or metadata.get("volume_basis") != "official_raw_shares":
        raise ValueError("unverified price basis metadata")
    if str(metadata.get("stock_id") or "") != stock_id:
        raise ValueError("price basis stock identity mismatch")
    dates = [str(value) for value in frame["date"]]
    if not dates or dates != sorted(set(dates)) or dates[-1] != expected_date or any(date_type.fromisoformat(value).isoformat() != value or value > expected_date for value in dates):
        raise ValueError("price basis dates must be unique, ordered and within as-of")
    if metadata.get("adjustment_as_of") != expected_date:
        raise ValueError("price adjustment as-of differs from latest official date")
    required = ["raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "adjustment_factor"]
    if any(key not in frame for key in required):
        raise ValueError("unverified price basis: missing raw columns")
    for row in frame.to_dict("records"):
        raw = {key: float(row[f"raw_{key}"]) for key in ("open", "high", "low", "close", "volume")}
        if not all(math.isfinite(value) for value in raw.values()) or min(raw[key] for key in ("open","high","low","close")) <= 0 or raw["volume"] < 0 or raw["high"] < max(raw["open"],raw["close"],raw["low"]) or raw["low"] > min(raw["open"],raw["close"]):
            raise ValueError("invalid raw OHLCV in adjusted price cache")
        factor = float(row["adjustment_factor"])
        if not math.isfinite(factor) or factor <= 0:
            raise ValueError("invalid adjustment factor")
        for key in ("open", "high", "low", "close"):
            if not math.isclose(float(row[key]), float(row[f"raw_{key}"])*factor, rel_tol=1e-9, abs_tol=1e-7):
                raise ValueError("adjusted/raw OHLC mismatch")
        if not math.isclose(float(row["volume"]), float(row["raw_volume"]), rel_tol=0, abs_tol=1e-8):
            raise ValueError("volume is not official raw shares")
    if float(frame.iloc[-1]["adjustment_factor"]) != 1.0:
        raise ValueError("latest price factor must equal one")
    return metadata


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
    limit = {"daily": 240, "weekly": 60, "monthly": 36}.get(packet.get("timeframe"), 90)
    packet["series"] = packet.get("series", [])[-limit:]
    if "series_coverage" in packet:
        packet["series_coverage"]["returned_bars"] = len(packet["series"])
        packet["series_coverage"]["requested_bars"] = limit
    visible_dates = {row.get("date") for row in packet["series"]}
    annotations = packet.get("candlestick_annotations")
    if annotations:
        annotations["events"] = [
            event for event in annotations.get("events", []) if event.get("bar_date") in visible_dates
        ]
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
_WORKER_CANDLE_VALIDATOR: Draft202012Validator | None = None


def analyze_stock_task(args: tuple) -> tuple[str, str, list[dict] | None, str | None]:
    stock_id, name, price_path, data_dir, decision, freshness_status, expected_price_date, global_warnings, validate = args
    try:
        warnings.filterwarnings("ignore", message="some peaks have a prominence of 0")
        frame = pd.read_csv(price_path)
        if frame.empty:
            raise ValueError("empty OHLCV")
        latest_date = str(frame.iloc[-1]["date"])
        if expected_price_date and latest_date != expected_price_date:
            raise ValueError(f"stale OHLCV: latest={latest_date}, expected={expected_price_date}")
        market = str((((decision.get("evidence") or {}).get("market_risk") or {}).get("market") or "listed"))
        if market not in {"listed", "otc", "emerging"}:
            market = "listed"
        basis = load_price_basis(Path(data_dir), stock_id, frame, expected_price_date or latest_date)
        packets = analyze_multi_timeframe(
            frame,
            stock_id=stock_id,
            price_adjustment=basis,
            decision=decision,
            freshness={"status": freshness_status, "data_date": latest_date, "warnings": global_warnings},
            market=market,
        )
        market_evidence = load_market_evidence(Path(data_dir), stock_id, as_of=latest_date)
        global _WORKER_VALIDATOR, _WORKER_CANDLE_VALIDATOR
        if validate and _WORKER_VALIDATOR is None:
            _WORKER_VALIDATOR = Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8")))
            _WORKER_CANDLE_VALIDATOR = Draft202012Validator(
                json.loads(CANDLE_SCHEMA_PATH.read_text(encoding="utf-8"))
            )
        for packet in packets:
            trim_packet(packet)
            # The public V2 is an evidence workbench.  Daily decision semantics
            # remain in their source dataset but are intentionally not copied
            # into the public technical packet.
            packet.pop("decision", None)
            add_public_workbench(packet, deepcopy(market_evidence))
            packet["warnings"] = sorted(set(packet.get("warnings", []) + global_warnings))
            if _WORKER_VALIDATOR:
                _WORKER_VALIDATOR.validate(packet)
                if packet.get("candlestick_annotations"):
                    _WORKER_CANDLE_VALIDATOR.validate(packet["candlestick_annotations"])
        return stock_id, name, packets, None
    except Exception as exc:
        return stock_id, name, None, str(exc)


def build_v2(*, docs_dir: Path = DOCS_DIR, data_dir: Path = DATA_DIR, validate: bool = False, switch_links: bool = False, only: set[str] | None = None, all_stocks: bool = False, workers: int | None = None) -> dict:
    stock_map = load_stock_map(docs_dir, data_dir)
    decisions, decision_payload = load_decisions(data_dir / "daily_decisions.json")
    quality = decision_payload.get("data_quality") or {}
    global_warnings = list(quality.get("warnings") or [])
    freshness_status = str(quality.get("state") or "unknown")
    price_summary = load_price_refresh_summary(data_dir / "price_refresh_summary.json")
    expected_price_date = str(price_summary.get("latest_data_date") or "")
    price_refresh_status = str(price_summary.get("status") or "missing")
    if price_refresh_status != "fresh":
        global_warnings.append(f"official price refresh status is {price_refresh_status}")

    root = docs_dir / "v2"
    packet_dir = root / "data"
    redirect_dir = root / "stocks"
    asset_dir = root / "assets"
    for directory in (packet_dir, redirect_dir, asset_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (root / "stock.html").write_text(STOCK_PAGE_HTML, encoding="utf-8")
    (root / "volume-profile.html").write_text(profile_page_html(), encoding="utf-8")
    (asset_dir / "v2.css").write_text(V2_CSS + "\n", encoding="utf-8")
    (asset_dir / "v2.js").write_text(V2_JS + "\n", encoding="utf-8")
    for asset in ("volume_profile.js", "volume_profile_ui.js"):
        (asset_dir / asset).write_text((ROOT / "stock_v2_public" / asset).read_text(encoding="utf-8"), encoding="utf-8")

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
        tasks.append((stock_id, str(stock.get("name") or ""), str(price_path), str(data_dir), safe_decision(stock_id, decisions.get(stock_id)), freshness_status, expected_price_date, global_warnings, validate))

    worker_count = workers or min(4, os.cpu_count() or 1)
    with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as executor:
        for stock_id, name, packets, error in executor.map(analyze_stock_task, tasks, chunksize=1):
            if not error and packets:
                (packet_dir / f"{stock_id}.json").write_text(stable_json(packets) + "\n", encoding="utf-8")
                (redirect_dir / f"{stock_id}.html").write_text(stock_redirect_html(stock_id), encoding="utf-8")
                index[stock_id] = {
                    "name": name,
                    "data_date": packets[0]["data_date"],
                }
            else:
                reason = error or "no packets generated"
                if "invalid high/low/volume" in reason or "fewer than 30 OHLCV rows" in reason or "stale OHLCV" in reason:
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
        "price_data_date": expected_price_date or None,
        "price_refresh_status": price_refresh_status,
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
