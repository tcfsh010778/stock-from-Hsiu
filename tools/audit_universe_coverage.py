from __future__ import annotations

import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any


MIN_CLOSE = 20.0
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
HOLDING_DIR = DATA_DIR / "holding_shares"
SITE_REPORTS_PATH = DATA_DIR / "site_reports.json"
OUTPUT_PATH = DATA_DIR / "universe_coverage_audit.json"


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _month_samples(start: date, end: date) -> list[date]:
    samples = [start]
    year, month = start.year, start.month + 1
    if month == 13:
        year, month = year + 1, 1

    while date(year, month, 1) <= end:
        samples.append(date(year, month, 1))
        month += 1
        if month == 13:
            year, month = year + 1, 1

    if samples[-1] != end and samples[-1] < end:
        samples.append(end)
    return samples


def _available_price_dates(price_index: dict[str, dict[str, Any]]) -> list[date]:
    dates = {
        parsed
        for info in price_index.values()
        for raw_date in info["dates"]
        if (parsed := _parse_date(raw_date)) is not None
    }
    return sorted(dates)


def _align_to_trading_dates(targets: list[date], available_dates: list[date]) -> list[date]:
    aligned: list[date] = []
    for target in targets:
        candidate = next((day for day in available_dates if day >= target), None)
        if candidate is None:
            candidate = available_dates[-1] if available_dates else target
        if candidate not in aligned:
            aligned.append(candidate)
    return aligned


def _load_price_index() -> dict[str, dict[str, Any]]:
    price_index: dict[str, dict[str, Any]] = {}
    for path in sorted(PRICES_DIR.glob("*.csv")):
        stock_id = path.stem
        dates: set[str] = set()
        closes: dict[str, float] = {}
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                row_date = _parse_date(row.get("date", ""))
                if row_date is None:
                    continue
                date_key = row_date.isoformat()
                dates.add(date_key)
                close = _parse_float(row.get("close"))
                if close is not None:
                    closes[date_key] = close

        if dates:
            price_index[stock_id] = {
                "first": min(dates),
                "last": max(dates),
                "dates": dates,
                "closes": closes,
            }
    return price_index


def _load_holding_dates() -> dict[str, set[str]]:
    holding_dates: dict[str, set[str]] = {}
    for path in sorted(HOLDING_DIR.glob("*.csv")):
        stock_id = path.stem
        dates: set[str] = set()
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                row_date = _parse_date(row.get("date", ""))
                if row_date is not None:
                    dates.add(row_date.isoformat())
        if dates:
            holding_dates[stock_id] = dates
    return holding_dates


def _load_report_pools() -> dict[str, set[str]]:
    if not SITE_REPORTS_PATH.exists():
        return {}

    with SITE_REPORTS_PATH.open("r", encoding="utf-8-sig") as fh:
        reports = json.load(fh)

    pools: dict[str, set[str]] = {}
    if not isinstance(reports, list):
        return pools

    for report in reports:
        if not isinstance(report, dict):
            continue
        report_date = _parse_date(report.get("date", ""))
        stocks = report.get("stocks") or []
        if report_date is None or not isinstance(stocks, list):
            continue
        pool = pools.setdefault(report_date.isoformat(), set())
        for stock in stocks:
            if isinstance(stock, dict) and stock.get("id"):
                pool.add(str(stock["id"]).strip())
    return pools


def build_audit() -> dict[str, Any]:
    price_index = _load_price_index()
    holding_dates = _load_holding_dates()
    report_pools = _load_report_pools()
    available_dates = _available_price_dates(price_index)

    start = date(2024, 7, 15)
    today = min(date.today(), available_dates[-1]) if available_dates else date.today()
    samples = []
    for sample_date in _align_to_trading_dates(_month_samples(start, today), available_dates):
        date_key = sample_date.isoformat()
        stocks_with_price = {
            stock_id
            for stock_id, info in price_index.items()
            if date_key in info["dates"]
        }
        stocks_with_price_above_20 = {
            stock_id
            for stock_id in stocks_with_price
            if price_index[stock_id]["closes"].get(date_key, 0.0) >= MIN_CLOSE
        }
        stocks_with_holding = {
            stock_id
            for stock_id, dates in holding_dates.items()
            if date_key in dates
        }
        report_pool = report_pools.get(date_key, set())
        missing_from_current_price = report_pool - stocks_with_price

        samples.append(
            {
                "date": date_key,
                "stocks_with_price": len(stocks_with_price),
                "stocks_with_price_above_20": len(stocks_with_price_above_20),
                "stocks_with_holding": len(stocks_with_holding),
                "report_pool_size_that_day": len(report_pool),
                "delisted_or_missing_count": len(missing_from_current_price),
            }
        )

    earliest = min((info["first"] for info in price_index.values()), default=None)
    latest = max((info["last"] for info in price_index.values()), default=None)
    full_history = 0
    partial_history = 0
    for info in price_index.values():
        if earliest and latest and info["first"] <= start.isoformat() and info["last"] >= today.isoformat():
            full_history += 1
        else:
            partial_history += 1

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "samples": samples,
        "summary": {
            "earliest_data_date": earliest,
            "latest_data_date": latest,
            "stocks_with_full_history": full_history,
            "stocks_with_partial_history": partial_history,
        },
    }


def print_summary(audit: dict[str, Any]) -> None:
    summary = audit["summary"]
    samples = audit["samples"]
    print("# Universe Coverage Audit")
    print()
    print(f"- Generated at: {audit['generated_at']}")
    print(f"- Earliest data date: {summary['earliest_data_date']}")
    print(f"- Latest data date: {summary['latest_data_date']}")
    print(f"- Stocks with full history: {summary['stocks_with_full_history']}")
    print(f"- Stocks with partial history: {summary['stocks_with_partial_history']}")
    print()
    print("| Date | Price rows | Close >= 20 | Holding data | Report pool | Missing estimate |")
    print("|---|---:|---:|---:|---:|---:|")
    for item in samples:
        print(
            "| {date} | {stocks_with_price} | {stocks_with_price_above_20} | "
            "{stocks_with_holding} | {report_pool_size_that_day} | {delisted_or_missing_count} |".format(
                **item
            )
        )


def main() -> None:
    audit = build_audit()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print_summary(audit)
    print()
    print(f"Wrote {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
