from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from refresh_prices import fetch_finmind_prices, load_finmind_token, write_price_csv
from mda_universe_scan import delta, read_holding_series


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
PRICE_DIR = DATA_DIR / "prices"
HOLDING_DIR = DATA_DIR / "holding_shares"
MARKETS_PATH = DATA_DIR / "stock_markets.json"
SUMMARY_PATH = DATA_DIR / "mda_full_market_refresh_summary.json"


try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def load_stock_universe(include_etf: bool = False, limit: int | None = None) -> dict[str, dict]:
    cache = json.loads(MARKETS_PATH.read_text(encoding="utf-8"))
    stocks = cache.get("stocks") or {}
    out: dict[str, dict] = {}
    for sid, item in sorted(stocks.items()):
        sid = str(sid).strip()
        if not re.fullmatch(r"\d{4}", sid):
            continue
        if not include_etf and sid.startswith("0"):
            continue
        market = (item or {}).get("market", "")
        if market not in {"上市", "上櫃"}:
            continue
        out[sid] = item or {}
        if limit and len(out) >= limit:
            break
    return out


def fetch_finmind_bulk(dataset: str, start_date: str) -> list[dict]:
    token = load_finmind_token()
    if not token:
        raise RuntimeError("FINMIND_TOKEN not found. Put it in env or v44 .env before full-market refresh.")
    params = {"dataset": dataset, "start_date": start_date, "token": token}
    resp = requests.get("https://api.finmindtrade.com/api/v4/data", params=params, timeout=180)
    resp.raise_for_status()
    payload = resp.json()
    if str(payload.get("status")) != "200":
        raise RuntimeError(f"{dataset} failed: {payload.get('status')} {payload.get('msg')}")
    return payload.get("data") or []


def write_grouped_csv(rows_by_stock: dict[str, list[dict]], out_dir: Path, fields: list[str], merge_existing: bool = False) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for sid, rows in rows_by_stock.items():
        if not rows:
            continue
        out = out_dir / f"{sid}.csv"
        if merge_existing and out.exists():
            try:
                with out.open("r", encoding="utf-8-sig", newline="") as fh:
                    existing = list(csv.DictReader(fh))
            except Exception:
                existing = []
            by_key = {}
            for row in existing + rows:
                key = tuple(str(row.get(field, "")) for field in fields[:3])
                by_key[key] = {field: row.get(field, "") for field in fields}
            rows = sorted(by_key.values(), key=lambda x: (x.get("date", ""), x.get("HoldingSharesLevel", "")))
        with out.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        written += 1
    return written


def refresh_one_day_prices(universe: dict[str, dict], start_date: str) -> dict:
    rows = fetch_finmind_bulk("TaiwanStockPrice", start_date)
    wanted = set(universe)
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        sid = str(row.get("stock_id") or "").strip()
        if sid not in wanted:
            continue
        try:
            grouped[sid].append({
                "date": row["date"],
                "open": float(row["open"]),
                "high": float(row["max"]),
                "low": float(row["min"]),
                "close": float(row["close"]),
                "volume": float(row.get("Trading_Volume") or 0),
            })
        except Exception:
            continue
    for sid in grouped:
        grouped[sid].sort(key=lambda x: x["date"])
    written = write_grouped_csv(grouped, PRICE_DIR, ["date", "open", "high", "low", "close", "volume"])
    return {"dataset_rows": len(rows), "matched_stocks": len(grouped), "written_files": written}


def refresh_holdings(universe: dict[str, dict], start_date: str) -> dict:
    rows = fetch_finmind_bulk("TaiwanStockHoldingSharesPer", start_date)
    wanted = set(universe)
    grouped: dict[str, list[dict]] = defaultdict(list)
    fields = ["date", "stock_id", "HoldingSharesLevel", "people", "percent", "unit"]
    for row in rows:
        sid = str(row.get("stock_id") or "").strip()
        if sid not in wanted:
            continue
        grouped[sid].append({field: row.get(field, "") for field in fields})
    for sid in grouped:
        grouped[sid].sort(key=lambda x: (x.get("date", ""), x.get("HoldingSharesLevel", "")))
    written = write_grouped_csv(grouped, HOLDING_DIR, fields, merge_existing=True)
    return {"dataset_rows": len(rows), "matched_stocks": len(grouped), "written_files": written}


def friday_dates(start_date: str, end: date | None = None) -> list[str]:
    end = end or date.today()
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    while current.weekday() != 4:
        current += timedelta(days=1)
    out = []
    while current <= end:
        out.append(current.isoformat())
        current += timedelta(days=7)
    return out


def refresh_weekly_holdings(universe: dict[str, dict], start_date: str) -> dict:
    wanted = set(universe)
    fields = ["date", "stock_id", "HoldingSharesLevel", "people", "percent", "unit"]
    grouped: dict[str, dict[tuple[str, str], dict]] = defaultdict(dict)
    dataset_rows = 0
    query_dates = friday_dates(start_date)
    for idx, query_date in enumerate(query_dates, 1):
        print(f"  holding [{idx:02d}/{len(query_dates)}] {query_date}", flush=True)
        rows = fetch_finmind_bulk("TaiwanStockHoldingSharesPer", query_date)
        dataset_rows += len(rows)
        for row in rows:
            sid = str(row.get("stock_id") or "").strip()
            if sid not in wanted:
                continue
            key = (str(row.get("date") or ""), str(row.get("HoldingSharesLevel") or ""))
            grouped[sid][key] = {field: row.get(field, "") for field in fields}
    ready = {sid: sorted(items.values(), key=lambda x: (x.get("date", ""), x.get("HoldingSharesLevel", ""))) for sid, items in grouped.items()}
    written = write_grouped_csv(ready, HOLDING_DIR, fields, merge_existing=True)
    return {"query_dates": len(query_dates), "dataset_rows": dataset_rows, "matched_stocks": len(ready), "written_files": written}


def major_accumulation_candidates(universe: dict[str, dict]) -> list[str]:
    out = []
    for sid in universe:
        rows = read_holding_series(sid)
        major_4w = delta(rows, "major", 4)
        major_8w = delta(rows, "major", 8)
        if (major_4w is not None and major_4w >= 0.5) or (major_8w is not None and major_8w >= 1.0):
            out.append(sid)
    return out


def refresh_candidate_prices(stock_ids: list[str], months: int) -> dict:
    ok = 0
    failed = 0
    for idx, sid in enumerate(stock_ids, 1):
        rows = fetch_finmind_prices(sid, months=months)
        if rows:
            write_price_csv(sid, rows)
            ok += 1
            print(f"  price [{idx:03d}/{len(stock_ids)}] {sid} rows={len(rows)}", flush=True)
        else:
            failed += 1
            print(f"  price [{idx:03d}/{len(stock_ids)}] {sid} no data", flush=True)
    return {"candidate_stocks": len(stock_ids), "written_files": ok, "failed": failed, "months": months}


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh full-market price and holder data for the MDA scan.")
    parser.add_argument("--price-start", default=(date.today() - timedelta(days=430)).strftime("%Y-%m-%d"))
    parser.add_argument("--holding-start", default=(date.today() - timedelta(days=140)).strftime("%Y-%m-%d"))
    parser.add_argument("--price-months", type=int, default=15)
    parser.add_argument("--include-etf", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="Debug only: limit stock universe size.")
    parser.add_argument("--skip-price", action="store_true")
    parser.add_argument("--skip-holding", action="store_true")
    parser.add_argument("--one-day-price", action="store_true", help="Debug only: fetch one full-market price snapshot instead of candidate histories.")
    args = parser.parse_args()

    universe = load_stock_universe(include_etf=args.include_etf, limit=args.limit)
    print(f"[mda_full_market_refresh] universe={len(universe)} price_start={args.price_start} holding_start={args.holding_start}")
    summary = {
        "date": date.today().isoformat(),
        "universe_count": len(universe),
        "include_etf": bool(args.include_etf),
        "price_start": args.price_start,
        "price_months": args.price_months,
        "holding_start": args.holding_start,
        "price": None,
        "holding": None,
        "candidate_count": None,
    }
    if not args.skip_holding:
        print("[mda_full_market_refresh] fetching weekly full-market holder distribution...")
        summary["holding"] = refresh_weekly_holdings(universe, args.holding_start)
        print(f"[mda_full_market_refresh] holdings {summary['holding']}")
    if not args.skip_price:
        if args.one_day_price:
            print("[mda_full_market_refresh] fetching one full-market price snapshot...")
            summary["price"] = refresh_one_day_prices(universe, args.price_start)
        else:
            candidates = major_accumulation_candidates(universe)
            summary["candidate_count"] = len(candidates)
            print(f"[mda_full_market_refresh] fetching price histories for major-accumulation candidates={len(candidates)}")
            summary["price"] = refresh_candidate_prices(candidates, args.price_months)
        print(f"[mda_full_market_refresh] prices {summary['price']}")

    SUMMARY_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[mda_full_market_refresh] summary -> {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
