from __future__ import annotations

import argparse
import concurrent.futures
import csv
import html
import json
import os
import re
import warnings
import shutil
from pathlib import Path
from functools import lru_cache

import pandas as pd
from jsonschema import Draft202012Validator

from stock_v2_public.analysis.engine import ENGINE_VERSION, analyze_multi_timeframe, stable_json
from stock_v2_public.site import STOCK_PAGE_HTML, V2_CSS, V2_JS, stock_redirect_html
from stock_rules import holding_group
from tools.official_workbench import merge_official_evidence, prepare_official_evidence

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DOCS_DIR = ROOT / "docs"
SCHEMA_PATH = ROOT / "schemas" / "technical_pattern_packet.schema.json"
CANDLE_SCHEMA_PATH = ROOT / "schemas" / "candlestick_pattern_event.schema.json"
PRIVATE_SOURCE_SHA = '332cf6013059c47224a07ef36e75c4c3dbc0cd31'
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


@lru_cache(maxsize=4)
def _prepared_market_evidence(data_dir: str, as_of: str, source_versions: tuple) -> dict:
    """Index shared official sources once per worker and source revision."""
    root = Path(data_dir)
    payloads = []
    for name in ("daily_market_flow.json", "tdcc_compact_weekly_snapshots.json"):
        path = root / name
        payloads.append(json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else {})
    return prepare_official_evidence(as_of, *payloads)


def load_market_evidence(data_dir: Path, stock_id: str, *, as_of: str | None = None) -> dict:
    """Build the public-safe synchronized chip panels without importing v44.

    Missing datasets stay missing and are disclosed in ``gaps``.  This keeps a
    sparse public cache from silently turning zeroes into fabricated evidence.
    """

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
            date, {"date": date, "foreign": None, "trust": None, "dealer": None, "total": None}
        )
        net_lots = (buy - sell) / 1000.0
        name = str(row.get("name") or "")
        if "Foreign" in name:
            item["foreign"] = (item["foreign"] or 0.0) + net_lots
        elif "Investment_Trust" in name:
            item["trust"] = (item["trust"] or 0.0) + net_lots
        elif "Dealer" in name:
            item["dealer"] = (item["dealer"] or 0.0) + net_lots
    for item in institutional_by_date.values():
        if all(item[key] is not None for key in ("foreign", "trust", "dealer")):
            item["total"] = sum(item[key] for key in ("foreign", "trust", "dealer"))
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
            if level.isdigit():
                band = int(level)
                group = "major" if 12 <= band <= 15 else "middle" if band == 11 else "retail" if 1 <= band <= 3 else "other"
            else:
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

    pool_path = data_dir / 'mda_weekly_top50.json'
    if as_of and pool_path.exists():
        from mda_weekly_pipeline import validate_pool
        pool = json.loads(pool_path.read_text(encoding='utf-8'))
        try:
            rows = validate_pool(pool, as_of)
        except (ValueError, KeyError, TypeError):
            rows = []
        for row in rows:
            if row['security_id'] != stock_id:
                continue
            by_date = {r['date']: r for r in holdings}
            for day, key in ((pool['previous_date'], 'prior_major_400_percent'), (pool['data_date'], 'major_400_percent')):
                by_date[day] = {'date': day, 'major': row[key], 'middle': None,
                                'retail': None, 'total_people': None}
            holdings = [by_date[d] for d in sorted(by_date)]
    if as_of:
        for values in (institutional, foreign_ownership, margin, holdings):
            values[:] = [r for r in values if str(r.get('date') or '') <= as_of]
    source_dates = {key: values[-1]['date'] for key, values in (
        ('institutional', institutional), ('foreign_ownership', foreign_ownership),
        ('margin', margin), ('holdings', holdings)) if values}
    gaps = [key for key in ('institutional', 'foreign_ownership', 'margin', 'holdings') if key not in source_dates]
    evidence = {
        "institutional": institutional,
        "foreign_ownership": foreign_ownership,
        "margin": margin,
        "holdings": holdings,
        "source_dates": source_dates,
        "gaps": gaps,
    }
    if as_of:
        paths = [data_dir / name for name in ("daily_market_flow.json", "tdcc_compact_weekly_snapshots.json")]
        versions = tuple((path.stat().st_mtime_ns, path.stat().st_size) if path.exists() else None for path in paths)
        prepared = _prepared_market_evidence(str(data_dir.resolve()), as_of, versions)
        evidence = merge_official_evidence(evidence, stock_id, as_of, {}, {}, prepared=prepared)
    return evidence


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
    for filename, key in (("stock_industries.json", "stock_name"), ("stock_markets.json", "name")):
        path = data_dir / filename
        if path.exists():
            references = json.loads(path.read_text(encoding="utf-8-sig")).get("stocks", {})
            for sid, row in references.items():
                if row.get(key) and sid in stocks:
                    stocks[sid]["name"] = row[key]
    return stocks


VERIFIED_PRICE_MODES = {
    "official_reference_ratio_back_adjusted_v1": "official_raw_shares",
    "finmind_raw_reconciled_reference_ratio_back_adjusted_v1": "finmind_raw_shares",
    "reference_ratio_back_adjusted_mixed_sources_v1": "raw_shares",
}


def load_verified_universe(data_dir: Path) -> tuple[set[str], list[dict]]:
    """Enumerate claimed verified pairs; content corruption is checked by workers."""
    ids, rejected = set(), []
    for path in sorted((data_dir / "price_basis").glob("*.json")):
        try:
            meta = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            rejected.append({"stock_id": path.stem, "reason_code": "basis_metadata_corrupt", "reason": str(exc)})
            continue
        sid, mode = str(meta.get("stock_id") or ""), str(meta.get("mode") or "")
        if meta.get("verified") is not True:
            continue
        if sid != path.stem or mode not in VERIFIED_PRICE_MODES or meta.get("volume_basis") != VERIFIED_PRICE_MODES[mode]:
            rejected.append({"stock_id": path.stem, "reason_code": "basis_claim_invalid", "reason": "unsupported or inconsistent verified metadata"})
            continue
        ids.add(sid)
    return ids, rejected


def _managed_dir(path: Path, docs_dir: Path, expected_name: str) -> Path:
    if path.name != expected_name or path.parent.resolve() != docs_dir.resolve() or path.is_symlink():
        raise RuntimeError(f"unsafe managed V2 path: {path}")
    return path


def _remove_managed(path: Path, docs_dir: Path, expected_name: str) -> None:
    checked = _managed_dir(path, docs_dir, expected_name)
    if checked.exists():
        shutil.rmtree(checked)


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


def load_price_basis(data_dir: Path, stock_id: str, frame: pd.DataFrame, expected_date: str) -> dict:
    from build_review_data import verified_frame, read_json
    checked = verified_frame(data_dir, stock_id, expected_date)
    if len(checked) != len(frame) or not checked['date'].astype(str).equals(frame['date'].astype(str)):
        raise ValueError('price frame differs from verified file')
    for field in ('open', 'high', 'low', 'close', 'volume', 'adjustment_factor'):
        if not pd.to_numeric(frame[field]).equals(checked[field]):
            raise ValueError('price frame differs from verified file')
    meta = read_json(data_dir / 'price_basis' / f'{stock_id}.json')
    if meta['mode'].startswith('finmind_') and (
            meta.get('data_start') != checked.iloc[0]['date']
            or meta.get('data_end') != expected_date or meta.get('row_count') != len(checked)):
        raise ValueError('price metadata coverage mismatch')
    return {'mode': meta['mode'], 'verified': True,
            'source': meta.get('source') or '; '.join([meta.get('price_source', ''), *meta.get('action_sources', [])]),
            'adjustment_as_of': expected_date, 'volume_basis': meta['volume_basis'],
            'event_count': meta.get('event_count', 0)}


def trim_packet(packet: dict) -> dict:
    limit = {"daily": 240, "weekly": 60, "monthly": 36}.get(packet.get("timeframe"), 90)
    packet["series"] = packet.get("series", [])[-limit:]
    if 'series_coverage' in packet:
        packet['series_coverage'].update(returned_bars=len(packet['series']), requested_bars=limit)
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
        if expected_price_date and latest_date > expected_price_date:
            raise ValueError(f"future OHLCV: latest={latest_date}, expected={expected_price_date}")
        price_stale = bool(expected_price_date and latest_date < expected_price_date)
        market = str((((decision.get("evidence") or {}).get("market_risk") or {}).get("market") or "listed"))
        if market not in {"listed", "otc", "emerging"}:
            market = "listed"
        try:
            basis = load_price_basis(Path(data_dir), stock_id, frame, latest_date)
        except ValueError as exc:
            if price_stale:
                raise ValueError(f"stale OHLCV without verified historical basis: {exc}") from exc
            raise
        packets = analyze_multi_timeframe(
            frame,
            stock_id=stock_id,
            price_adjustment=basis,
            decision=decision,
            freshness={"status": "stale" if price_stale else "fresh", "data_date": latest_date,
                       "warnings": [f"行情尚未更新：資料 {latest_date}，預期 {expected_price_date}；僅供歷史核對。"] if price_stale else []},
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
            add_public_workbench(packet, market_evidence)
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
    summary_price_date = str(price_summary.get("latest_data_date") or "")
    from build_review_data import expected_session
    expected_price_date = expected_session(data_dir=data_dir)
    price_refresh_status = str(price_summary.get("status") or "missing")
    if price_refresh_status != "fresh":
        global_warnings.append(f"official price refresh status is {price_refresh_status}")

    published_root = docs_dir / "v2"
    root = docs_dir / ".v2-staging"
    backup = docs_dir / ".v2-previous"
    _managed_dir(root, docs_dir, ".v2-staging")
    _managed_dir(backup, docs_dir, ".v2-previous")
    if backup.exists():
        if published_root.exists():
            raise RuntimeError("previous V2 backup still exists; refusing to discard uncertain recovery data")
        backup.rename(published_root)
    _remove_managed(root, docs_dir, ".v2-staging")
    if only and published_root.exists():
        shutil.copytree(published_root, root)
    packet_dir = root / "data"
    redirect_dir = root / "stocks"
    asset_dir = root / "assets"
    for directory in (packet_dir, redirect_dir, asset_dir):
        directory.mkdir(parents=True, exist_ok=True)
    (root / "stock.html").write_text(STOCK_PAGE_HTML, encoding="utf-8")
    (asset_dir / "v2.css").write_text(V2_CSS + "\n", encoding="utf-8")
    (asset_dir / "v2.js").write_text(V2_JS + "\n", encoding="utf-8")

    verified_ids, metadata_failures = load_verified_universe(data_dir)
    for sid in verified_ids:
        stock_map.setdefault(sid, {"name": ""})
    target_ids = set(verified_ids)
    review_path = data_dir / "sfz_technical_candidates.json"
    if review_path.exists():
        review = json.loads(review_path.read_text(encoding="utf-8"))
        target_ids.update(str(row["stock_id"]) for row in review.get("stocks", [])
                          if row.get("candidate") or row.get("stage") in {"box_forming", "breakout_wait_retest", "retest_confirmed"})
    mda_path = data_dir / 'mda_weekly_top50.json'
    if mda_path.exists():
        from mda_weekly_pipeline import validate_pool
        pool = json.loads(mda_path.read_text(encoding='utf-8'))
        try:
            for row in validate_pool(pool, expected_price_date):
                sid = str(row['security_id'])
                target_ids.add(sid)
                stock_map.setdefault(sid, {'name': row.get('name', '')})
                if not stock_map[sid].get('name'):
                    stock_map[sid]['name'] = row.get('name', '')
        except (ValueError, KeyError, TypeError):
            pass  # Invalid pool does not create chart eligibility.
    if only:
        target_ids &= set(only)
    target_ids &= verified_ids

    old_index_path = packet_dir / "index.json"
    old_manifest = json.loads(old_index_path.read_text(encoding="utf-8")) if only and old_index_path.exists() else {}
    index: dict[str, dict] = dict(old_manifest.get("stocks") or {})
    failures: list[dict] = list(metadata_failures)
    exclusions: list[dict] = [
        {"stock_id": sid, "reason_code": "verified_basis_missing", "reason": "not in verified price universe"}
        for sid in sorted(set(stock_map) - verified_ids)
    ]
    tasks = []
    for stock_id in sorted(target_ids):
        stock = stock_map[stock_id]
        price_path = data_dir / "prices" / f"{stock_id}.csv"
        if not price_path.exists():
            failures.append({"stock_id": stock_id, "reason_code": "verified_price_file_missing", "reason": "price file missing"})
            continue
        tasks.append((stock_id, str(stock.get("name") or ""), str(price_path), str(data_dir), safe_decision(stock_id, decisions.get(stock_id)), freshness_status, expected_price_date, global_warnings, validate))

    worker_count = workers or min(4, os.cpu_count() or 1)
    with concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as executor:
        for completed_count, (stock_id, name, packets, error) in enumerate(executor.map(analyze_stock_task, tasks, chunksize=1), 1):
            if not error and packets:
                (packet_dir / f"{stock_id}.json").write_text(stable_json(packets) + "\n", encoding="utf-8")
                (redirect_dir / f"{stock_id}.html").write_text(stock_redirect_html(stock_id), encoding="utf-8")
                index[stock_id] = {
                    "name": name,
                    "data_date": packets[0]["data_date"],
                }
            else:
                reason = error or "no packets generated"
                failures.append({"stock_id": stock_id, "reason_code": "verified_pair_corrupt", "reason": reason})
                print(f"[V2] failed {stock_id}: {reason[:240]}", flush=True)
            if completed_count % 50 == 0 or completed_count == len(tasks):
                print(f"[V2] analyzed {completed_count}/{len(tasks)}; published candidates={len(index)}; failures={len(failures)}", flush=True)

    fresh_count = sum(item["data_date"] == expected_price_date for item in index.values())

    manifest = {
        "schema_version": "1.0.0",
        "engine_version": ENGINE_VERSION,
        "private_source_sha": PRIVATE_SOURCE_SHA,
        "stock_count": len(index),
        "coverage": "verified_price_universe",
        "verified_universe_count": len(verified_ids),
        "target_count": len(index) if only else len(target_ids),
        "generated_count": len(index),
        "fresh_count": fresh_count,
        "failure_count": len(failures),
        "excluded_count": len(exclusions),
        "price_data_date": expected_price_date or None,
        "price_refresh_status": price_refresh_status,
        "stocks": index,
        "failures": failures,
        "exclusions": exclusions,
    }
    (packet_dir / "index.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    freshness_ready = price_refresh_status == "fresh" and summary_price_date == expected_price_date
    release_ready = not failures and freshness_ready and fresh_count >= 400
    blocker = None if release_ready else ("corruption_failures" if failures else ("price_refresh_mismatch" if not freshness_ready else "fresh_coverage_below_400"))
    status = {"as_of": expected_price_date, "summary_price_date": summary_price_date, "price_refresh_status": price_refresh_status, "verified_universe_count": len(verified_ids), "generated_count": len(index), "fresh_count": fresh_count, "failure_count": len(failures), "excluded_count": len(exclusions), "release_ready": release_ready, "release_blocker": blocker, "failures": failures}
    (data_dir / "v2_build_status.json").write_text(stable_json(status) + "\n", encoding="utf-8")
    if not release_ready:
        _remove_managed(root, docs_dir, ".v2-staging")
        return {"stock_count": len(index), "excluded_count": len(exclusions), "failure_count": len(failures), "release_ready": False, "switched_links": 0, "failures": failures}

    if published_root.exists():
        published_root.rename(backup)
    try:
        root.rename(published_root)
    except Exception:
        if backup.exists() and not published_root.exists():
            backup.rename(published_root)
        raise
    _remove_managed(backup, docs_dir, ".v2-previous")

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

    return {"stock_count": len(index), "excluded_count": len(exclusions), "failure_count": len(failures), "release_ready": True, "switched_links": switched, "failures": failures}


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
    if result["failure_count"] or not result.get("release_ready"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
