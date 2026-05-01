# -*- coding: utf-8 -*-
"""
Refresh FinMind/v44 price cache for the static site.

This keeps GitHub Pages static and fast:
1. fetch price data once
2. save CSV under data/prices/
3. generate_site.py reads the local cache
"""

import csv
import os
import sys
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
    V44_ROOT,
    find_all_reports,
    load_reports,
    parse_report,
)


def collect_stock_ids() -> list[str]:
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
    for report in reports:
        for s in report.get("stocks", []):
            sid = str(s.get("id", "")).strip()
            if sid:
                ids.add(sid)
    return sorted(ids)


def write_price_csv(stock_id: str, rows: list[dict]) -> None:
    LOCAL_PRICE_DIR.mkdir(parents=True, exist_ok=True)
    out = LOCAL_PRICE_DIR / f"{stock_id}.csv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["date", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)


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
    stock_ids = collect_stock_ids()
    print(f"[refresh_prices] stocks={len(stock_ids)} months={months}")
    ok = 0
    for i, sid in enumerate(stock_ids, 1):
        rows = fetch_finmind_prices(sid, months=months)
        if rows:
            write_price_csv(sid, rows)
            ok += 1
            print(f"  [{i:02d}/{len(stock_ids)}] {sid} rows={len(rows)}")
        else:
            print(f"  [{i:02d}/{len(stock_ids)}] {sid} no data")
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
    print(f"[refresh_prices] done ok={ok}/{len(stock_ids)} -> {LOCAL_PRICE_DIR}")


if __name__ == "__main__":
    main()
