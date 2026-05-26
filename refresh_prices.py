# -*- coding: utf-8 -*-
"""
Refresh FinMind/v44 price cache for the static site.

This keeps GitHub Pages static and fast:
1. fetch price data once
2. save CSV under data/prices/
3. generate_site.py reads the local cache
"""

import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from datetime import date, timedelta

import requests

os.environ.setdefault("V44_LIVE_FETCH", "1")
os.environ.setdefault("V44_FETCH_MONTHS", "24")

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from generate_site import (  # noqa: E402
    LOCAL_CHIP_DIR,
    LOCAL_FOREIGN_SHAREHOLDING_DIR,
    LOCAL_HOLDING_DIR,
    LOCAL_MARGIN_DIR,
    LOCAL_PRICE_DIR,
    INDUSTRY_CACHE_PATH,
    MARKET_CACHE_PATH,
    V44_ROOT,
    find_all_reports,
    load_reports,
    parse_report,
)


def _report_stock_ids(scope: str) -> set[str]:
    ids = set()
    md_files = find_all_reports()
    if md_files:
        reports = []
        for md in md_files:
            try:
                reports.append(parse_report(md))
            except Exception:
                continue
    else:
        reports = load_reports()
    reports = sorted(reports, key=lambda r: r.get("date", ""), reverse=True)
    if scope not in {"all", "history"} and reports:
        reports = reports[:1]
    for report in reports:
        for s in report.get("stocks", []):
            sid = str(s.get("id", "")).strip()
            if sid:
                ids.add(sid)
    return ids


def _cache_stock_ids_from_json(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    ids: set[str] = set()
    if isinstance(payload, dict):
        if isinstance(payload.get("stocks"), dict):
            ids.update(str(sid).strip() for sid in payload["stocks"])
        else:
            ids.update(str(sid).strip() for sid in payload)
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                ids.add(str(item.get("stock_id") or item.get("id") or item.get("code") or "").strip())
    return {sid for sid in ids if re.fullmatch(r"\d{4}", sid)}


def _cache_stock_ids_from_dirs() -> set[str]:
    ids: set[str] = set()
    for folder in [LOCAL_PRICE_DIR, LOCAL_CHIP_DIR, LOCAL_HOLDING_DIR, LOCAL_FOREIGN_SHAREHOLDING_DIR, LOCAL_MARGIN_DIR]:
        if not folder.exists():
            continue
        ids.update(path.stem for path in folder.glob("*.csv") if re.fullmatch(r"\d{4}", path.stem))
    return ids


def collect_cached_stock_ids() -> set[str]:
    ids = set()
    market_ids = _cache_stock_ids_from_json(MARKET_CACHE_PATH)
    dir_ids = _cache_stock_ids_from_dirs()
    ids.update(market_ids)
    ids.update(dir_ids)
    if not ids:
        ids.update(_cache_stock_ids_from_json(INDUSTRY_CACHE_PATH))
    return ids


def collect_stock_ids() -> list[str]:
    scope = os.environ.get("V44_REFRESH_SCOPE", "latest").strip().lower()
    ids = _report_stock_ids(scope)
    if scope == "all":
        ids.update(collect_cached_stock_ids())
    return sorted(ids)


def write_price_csv(stock_id: str, rows: list[dict]) -> None:
    LOCAL_PRICE_DIR.mkdir(parents=True, exist_ok=True)
    out = LOCAL_PRICE_DIR / f"{stock_id}.csv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)


def write_bulk_price_rows(stock_ids: set[str], rows: list[dict]) -> int:
    LOCAL_PRICE_DIR.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        sid = str(row.get("stock_id") or "").strip()
        if sid not in stock_ids:
            continue
        try:
            grouped[sid].append(
                {
                    "date": row["date"],
                    "open": float(row["open"]),
                    "high": float(row["max"]),
                    "low": float(row["min"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("Trading_Volume") or 0),
                }
            )
        except Exception:
            continue
    written = 0
    for sid, new_rows in grouped.items():
        out = LOCAL_PRICE_DIR / f"{sid}.csv"
        existing: list[dict] = []
        if out.exists():
            try:
                with out.open("r", encoding="utf-8-sig", newline="") as fh:
                    existing = list(csv.DictReader(fh))
            except Exception:
                existing = []
        by_date = {}
        for item in existing + new_rows:
            key = str(item.get("date") or "")
            if key:
                by_date[key] = {
                    "date": key,
                    "open": item.get("open", ""),
                    "high": item.get("high", ""),
                    "low": item.get("low", ""),
                    "close": item.get("close", ""),
                    "volume": item.get("volume", ""),
                }
        merged = [by_date[key] for key in sorted(by_date)]
        write_price_csv(sid, merged)
        written += 1
    return written


def load_finmind_token() -> str:
    if os.environ.get("FINMIND_TOKEN"):
        return os.environ["FINMIND_TOKEN"].strip()
    env_path = V44_ROOT / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if line.strip().startswith("FINMIND_TOKEN="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def fetch_finmind_prices(stock_id: str, months: int) -> list[dict]:
    token = load_finmind_token()
    start = (date.today() - timedelta(days=int(months * 31))).strftime("%Y-%m-%d")
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": start,
    }
    if token:
        params["token"] = token
    try:
        resp = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        print(f"  {stock_id} FinMind error: {exc}")
        return []
    data = payload.get("data") or []
    rows = []
    for r in data:
        try:
            rows.append({
                "date": r["date"],
                "open": float(r["open"]),
                "high": float(r["max"]),
                "low": float(r["min"]),
                "close": float(r["close"]),
                "volume": float(r.get("Trading_Volume") or 0),
            })
        except Exception:
            continue
    return rows


def fetch_finmind_bulk_price_snapshot(start_date: str) -> list[dict]:
    token = load_finmind_token()
    params = {"dataset": "TaiwanStockPrice", "start_date": start_date}
    if token:
        params["token"] = token
    try:
        resp = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if str(payload.get("status")) != "200":
            print(f"  bulk TaiwanStockPrice {start_date} status={payload.get('status')} msg={payload.get('msg')}")
            return []
        return payload.get("data") or []
    except Exception as exc:
        print(f"  bulk TaiwanStockPrice {start_date} error: {exc}")
        return []


def refresh_bulk_market_prices(stock_ids: list[str], days: int) -> dict:
    wanted = set(stock_ids)
    written_total = 0
    dataset_rows = 0
    dates_with_data = []
    start = date.today() - timedelta(days=max(0, days - 1))
    for offset in range(days):
        query_date = (start + timedelta(days=offset)).isoformat()
        rows = fetch_finmind_bulk_price_snapshot(query_date)
        if not rows:
            continue
        dataset_rows += len(rows)
        written = write_bulk_price_rows(wanted, rows)
        written_total += written
        dates_with_data.append(query_date)
        print(f"  bulk price {query_date} rows={len(rows)} written_files={written}")
    return {
        "scope_stocks": len(stock_ids),
        "query_days": days,
        "dates_with_data": dates_with_data,
        "dataset_rows": dataset_rows,
        "written_files": written_total,
    }


def fetch_finmind_dataset(stock_id: str, dataset: str, months: int) -> list[dict]:
    token = load_finmind_token()
    start = (date.today() - timedelta(days=int(months * 31))).strftime("%Y-%m-%d")
    params = {"dataset": dataset, "data_id": stock_id, "start_date": start}
    if token:
        params["token"] = token
    try:
        resp = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=20)
        resp.raise_for_status()
        return resp.json().get("data") or []
    except Exception as exc:
        print(f"  {stock_id} {dataset} error: {exc}")
        return []


def write_generic_csv(out_dir: Path, stock_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{stock_id}.csv"
    fields = list(rows[0].keys())
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_foreign_shareholding_csv(stock_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    LOCAL_FOREIGN_SHAREHOLDING_DIR.mkdir(parents=True, exist_ok=True)
    out = LOCAL_FOREIGN_SHAREHOLDING_DIR / f"{stock_id}.csv"
    fields = ["date", "stock_id", "foreign_shares", "foreign_shares_lot", "foreign_ratio", "issued_shares"]
    normalized = []
    for r in rows:
        try:
            shares = float(r.get("ForeignInvestmentShares") or 0)
        except Exception:
            shares = 0.0
        normalized.append({
            "date": r.get("date", ""),
            "stock_id": r.get("stock_id", stock_id),
            "foreign_shares": int(shares),
            "foreign_shares_lot": shares / 1000,
            "foreign_ratio": r.get("ForeignInvestmentSharesRatio", ""),
            "issued_shares": r.get("NumberOfSharesIssued", ""),
        })
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(normalized)


def write_margin_csv(stock_id: str, rows: list[dict]) -> None:
    if not rows:
        return
    LOCAL_MARGIN_DIR.mkdir(parents=True, exist_ok=True)
    out = LOCAL_MARGIN_DIR / f"{stock_id}.csv"
    fields = [
        "date",
        "stock_id",
        "margin_balance",
        "margin_buy",
        "margin_sell",
        "short_balance",
        "short_buy",
        "short_sell",
    ]
    normalized = []
    for r in rows:
        normalized.append({
            "date": r.get("date", ""),
            "stock_id": r.get("stock_id", stock_id),
            "margin_balance": r.get("MarginPurchaseTodayBalance", ""),
            "margin_buy": r.get("MarginPurchaseBuy", ""),
            "margin_sell": r.get("MarginPurchaseSell", ""),
            "short_balance": r.get("ShortSaleTodayBalance", ""),
            "short_buy": r.get("ShortSaleBuy", ""),
            "short_sell": r.get("ShortSaleSell", ""),
        })
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(normalized)


def main() -> None:
    months = int(os.environ.get("V44_FETCH_MONTHS", "24"))
    scope = os.environ.get("V44_REFRESH_SCOPE", "latest").strip().lower()
    stock_ids = collect_stock_ids()
    print(f"[refresh_prices] scope={scope} stocks={len(stock_ids)} months={months}")
    ok = 0
    if scope == "all":
        days = int(os.environ.get("V44_BULK_PRICE_DAYS", "14"))
        summary = refresh_bulk_market_prices(stock_ids, days)
        print(f"[refresh_prices] bulk prices {summary}")
        price_ids: list[str] = []
        aux_scope = os.environ.get("V44_REFRESH_AUX_SCOPE", "latest").strip().lower()
        aux_ids = stock_ids if aux_scope == "all" else sorted(_report_stock_ids("latest"))
    else:
        price_ids = stock_ids
        aux_ids = stock_ids

    for i, sid in enumerate(price_ids, 1):
        rows = fetch_finmind_prices(sid, months=months)
        if rows:
            write_price_csv(sid, rows)
            ok += 1
            print(f"  price [{i:02d}/{len(price_ids)}] {sid} rows={len(rows)}")
        else:
            print(f"  price [{i:02d}/{len(price_ids)}] {sid} no data")

    print(f"[refresh_prices] refreshing auxiliary datasets for stocks={len(aux_ids)}")
    for i, sid in enumerate(aux_ids, 1):
        chip_rows = fetch_finmind_dataset(sid, "TaiwanStockInstitutionalInvestorsBuySell", months)
        if chip_rows:
            write_generic_csv(LOCAL_CHIP_DIR, sid, chip_rows)
        holding_rows = fetch_finmind_dataset(sid, "TaiwanStockHoldingSharesPer", months)
        if holding_rows:
            write_generic_csv(LOCAL_HOLDING_DIR, sid, holding_rows)
        foreign_shareholding_rows = fetch_finmind_dataset(sid, "TaiwanStockShareholding", months)
        if foreign_shareholding_rows:
            write_foreign_shareholding_csv(sid, foreign_shareholding_rows)
        margin_rows = fetch_finmind_dataset(sid, "TaiwanStockMarginPurchaseShortSale", months)
        if margin_rows:
            write_margin_csv(sid, margin_rows)
        print(f"  aux [{i:02d}/{len(aux_ids)}] {sid}")
    print(f"[refresh_prices] done individual_price_ok={ok}/{len(price_ids)} -> {LOCAL_PRICE_DIR}")


if __name__ == "__main__":
    main()
