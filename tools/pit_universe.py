from __future__ import annotations

import bisect
import csv
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
HOLDING_DIR = DATA_DIR / "holding_shares"


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


@lru_cache(maxsize=1)
def _price_index() -> dict[str, list[tuple[date, float]]]:
    index: dict[str, list[tuple[date, float]]] = {}
    for path in sorted(PRICES_DIR.glob("*.csv")):
        rows: list[tuple[date, float]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                row_date = _parse_date(row.get("date", ""))
                close = _parse_float(row.get("close"))
                if row_date is not None and close is not None:
                    rows.append((row_date, close))
        if rows:
            rows.sort(key=lambda item: item[0])
            index[path.stem] = rows
    return index


@lru_cache(maxsize=1)
def _holding_index() -> dict[str, list[date]]:
    index: dict[str, set[date]] = {}
    for path in sorted(HOLDING_DIR.glob("*.csv")):
        dates = index.setdefault(path.stem, set())
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                row_date = _parse_date(row.get("date", ""))
                if row_date is not None:
                    dates.add(row_date)
    return {stock_id: sorted(dates) for stock_id, dates in index.items() if dates}


def _effective_price_row(stock_id: str, as_of: date) -> tuple[int, date, float] | None:
    rows = _price_index().get(stock_id)
    if not rows:
        return None
    dates = [row_date for row_date, _close in rows]
    pos = bisect.bisect_right(dates, as_of) - 1
    if pos < 0:
        return None
    row_date, close = rows[pos]
    return pos, row_date, close


def _has_recent_holding(stock_id: str, as_of: date) -> bool:
    dates = _holding_index().get(stock_id)
    if not dates:
        return False
    pos = bisect.bisect_right(dates, as_of) - 1
    if pos < 0:
        return False
    return dates[pos] >= as_of - timedelta(days=7)


def _latest_holding_date(stock_id: str, as_of: date) -> date | None:
    dates = _holding_index().get(stock_id)
    if not dates:
        return None
    pos = bisect.bisect_right(dates, as_of) - 1
    if pos < 0:
        return None
    return dates[pos]


def diagnose_universe_eligibility(stock_id: str, as_of_date: str, min_close: float = 20.0) -> dict:
    """
    回傳該股票在 as_of_date 是否合格，以及每個條件的細節。

    return: {
        "eligible": bool,
        "reasons_failed": ["close_below_min", "insufficient_history", ...],
        "details": {
            "close_on_date": float | None,
            "trading_days_available": int,
            "has_holding_data": bool,
            "latest_holding_date": str | None,
        }
    }
    """
    stock_id = str(stock_id)
    as_of = date.fromisoformat(as_of_date[:10])
    reasons_failed: list[str] = []
    effective_row = _effective_price_row(stock_id, as_of)
    latest_holding = _latest_holding_date(stock_id, as_of)

    price_pos = -1
    effective_date = None
    close = None
    if effective_row is None:
        reasons_failed.append("no_price_data_on_date")
    else:
        price_pos, effective_date, close = effective_row
        if close < min_close:
            reasons_failed.append(f"close_below_min_{min_close:g}")
        if price_pos + 1 < 130:
            reasons_failed.append("insufficient_history_lt_130_days")

    has_recent_holding = bool(
        effective_date is not None and latest_holding is not None and latest_holding >= effective_date - timedelta(days=7)
    )
    if not has_recent_holding:
        reasons_failed.append("no_holding_data")

    return {
        "eligible": not reasons_failed,
        "reasons_failed": reasons_failed,
        "details": {
            "close_on_date": close,
            "effective_price_date": effective_date.isoformat() if effective_date else None,
            "trading_days_available": price_pos + 1 if price_pos >= 0 else 0,
            "has_holding_data": has_recent_holding,
            "latest_holding_date": latest_holding.isoformat() if latest_holding else None,
        },
    }


def is_in_universe(stock_id: str, as_of_date: str, min_close: float = 20.0) -> bool:
    """檢查單一股票在 as_of_date 是否合格。"""
    as_of = date.fromisoformat(as_of_date[:10])
    effective_row = _effective_price_row(str(stock_id), as_of)
    if effective_row is None:
        return False

    price_pos, effective_date, close = effective_row
    if close < min_close:
        return False
    if not _has_recent_holding(str(stock_id), effective_date):
        return False
    return price_pos + 1 >= 130


def get_eligible_universe(as_of_date: str, min_close: float = 20.0) -> set[str]:
    """
    回傳 as_of_date 當天「真實合格」的標的 ID 集合。

    合格條件：
    1. 該日或之前最近交易日有 data/prices/<sid>.csv 的資料且 close >= min_close
    2. 該日（或之前最近一週）有 data/holding_shares/<sid>.csv 資料
    3. 該日往前看至少 130 個交易日的價格資料（MA120 計算所需）

    as_of_date: ISO 格式 'YYYY-MM-DD'
    """
    return {
        stock_id
        for stock_id in _price_index()
        if is_in_universe(stock_id, as_of_date, min_close)
    }


def main() -> None:
    today = datetime.now().date().isoformat()
    for sample_date in ("2024-09-01", "2025-09-01", today):
        universe = get_eligible_universe(sample_date)
        print(f"{sample_date} universe: {len(universe)} stocks")


if __name__ == "__main__":
    main()
