from __future__ import annotations

import csv
import json
import math
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import requests


TWSE_LATEST_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_LATEST_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TWSE_HISTORY_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
TPEX_HISTORY_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
USER_AGENT = "stock-from-Hsiu-official-price-refresh/1.0"
CSV_FIELDS = ["date", "open", "high", "low", "close", "volume"]


def source_date_to_iso(value: Any) -> str:
    """Normalize Gregorian or ROC compact dates to ISO-8601."""
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 8 and digits.startswith("20"):
        year, month, day = int(digits[:4]), int(digits[4:6]), int(digits[6:])
    elif len(digits) == 7:
        year, month, day = int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:])
    else:
        return ""
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return ""


def _number(value: Any) -> float:
    text = re.sub(r"<[^>]+>", "", str(value or "")).replace(",", "").strip()
    if text in {"", "--", "---", "-", "N/A", "nan"}:
        raise ValueError("missing numeric value")
    number = float(text)
    if not math.isfinite(number):
        raise ValueError("non-finite numeric value")
    return number


def _price_row(
    *,
    trading_date: str,
    stock_id: Any,
    open_value: Any,
    high_value: Any,
    low_value: Any,
    close_value: Any,
    volume_value: Any,
) -> dict[str, Any] | None:
    sid = str(stock_id or "").strip()
    if not re.fullmatch(r"\d{4}", sid):
        return None
    try:
        open_price = _number(open_value)
        high_price = _number(high_value)
        low_price = _number(low_value)
        close_price = _number(close_value)
        volume = _number(volume_value)
    except (TypeError, ValueError):
        return None
    if min(open_price, high_price, low_price, close_price) <= 0 or volume < 0:
        return None
    if high_price < low_price or high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
        return None
    return {
        "date": trading_date,
        "stock_id": sid,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
    }


def normalize_twse_latest(rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    dates = {source_date_to_iso(row.get("Date")) for row in rows}
    dates.discard("")
    if len(dates) != 1:
        raise ValueError(f"TWSE latest snapshot has ambiguous dates: {sorted(dates)}")
    trading_date = dates.pop()
    normalized = []
    for row in rows:
        item = _price_row(
            trading_date=trading_date,
            stock_id=row.get("Code"),
            open_value=row.get("OpeningPrice"),
            high_value=row.get("HighestPrice"),
            low_value=row.get("LowestPrice"),
            close_value=row.get("ClosingPrice"),
            volume_value=row.get("TradeVolume"),
        )
        if item:
            normalized.append(item)
    return trading_date, normalized


def normalize_tpex_latest(rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    dates = {source_date_to_iso(row.get("Date")) for row in rows}
    dates.discard("")
    if len(dates) != 1:
        raise ValueError(f"TPEx latest snapshot has ambiguous dates: {sorted(dates)}")
    trading_date = dates.pop()
    normalized = []
    for row in rows:
        item = _price_row(
            trading_date=trading_date,
            stock_id=row.get("SecuritiesCompanyCode"),
            open_value=row.get("Open"),
            high_value=row.get("High"),
            low_value=row.get("Low"),
            close_value=row.get("Close"),
            volume_value=row.get("TradingShares"),
        )
        if item:
            normalized.append(item)
    return trading_date, normalized


def _table_with_fields(payload: dict[str, Any], required: set[str]) -> dict[str, Any] | None:
    for table in payload.get("tables") or []:
        fields = {str(field).strip() for field in (table.get("fields") or [])}
        if required.issubset(fields):
            return table
    return None


def normalize_twse_history(payload: dict[str, Any], expected_date: date) -> list[dict[str, Any]]:
    trading_date = source_date_to_iso(payload.get("date"))
    if trading_date != expected_date.isoformat():
        return []
    required = {"證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"}
    table = _table_with_fields(payload, required)
    if not table:
        return []
    fields = [str(field).strip() for field in table["fields"]]
    index = {field: fields.index(field) for field in required}
    normalized = []
    for row in table.get("data") or []:
        item = _price_row(
            trading_date=trading_date,
            stock_id=row[index["證券代號"]],
            open_value=row[index["開盤價"]],
            high_value=row[index["最高價"]],
            low_value=row[index["最低價"]],
            close_value=row[index["收盤價"]],
            volume_value=row[index["成交股數"]],
        )
        if item:
            normalized.append(item)
    return normalized


def normalize_tpex_history(payload: dict[str, Any], expected_date: date) -> list[dict[str, Any]]:
    trading_date = source_date_to_iso(payload.get("date"))
    if trading_date != expected_date.isoformat():
        return []
    required = {"代號", "收盤", "開盤", "最高", "最低", "成交股數"}
    table = _table_with_fields(payload, required)
    if not table:
        return []
    fields = [str(field).strip() for field in table["fields"]]
    index = {field: fields.index(field) for field in required}
    normalized = []
    for row in table.get("data") or []:
        item = _price_row(
            trading_date=trading_date,
            stock_id=row[index["代號"]],
            open_value=row[index["開盤"]],
            high_value=row[index["最高"]],
            low_value=row[index["最低"]],
            close_value=row[index["收盤"]],
            volume_value=row[index["成交股數"]],
        )
        if item:
            normalized.append(item)
    return normalized


def _get_json(url: str, params: dict[str, str] | None = None, *, timeout: int = 60, attempts: int = 3) -> Any:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # requests exposes several transport and JSON errors
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1 + attempt * 2)
    raise RuntimeError(f"official price request failed: {url}: {last_error}")


def fetch_latest_snapshot(fetch_json: Callable[..., Any] = _get_json) -> tuple[str, list[dict[str, Any]], dict[str, int]]:
    twse_payload = fetch_json(TWSE_LATEST_URL)
    tpex_payload = fetch_json(TPEX_LATEST_URL)
    if not isinstance(twse_payload, list) or not isinstance(tpex_payload, list):
        raise RuntimeError("official latest price endpoints returned an unexpected schema")
    twse_date, twse_rows = normalize_twse_latest(twse_payload)
    tpex_date, tpex_rows = normalize_tpex_latest(tpex_payload)
    if twse_date != tpex_date:
        raise RuntimeError(f"official latest price dates do not align: TWSE={twse_date}, TPEx={tpex_date}")
    if not twse_rows or not tpex_rows:
        raise RuntimeError("official latest price snapshot is missing a market partition")
    return twse_date, twse_rows + tpex_rows, {"twse": len(twse_rows), "tpex": len(tpex_rows)}


def fetch_history_snapshot(query_date: date, fetch_json: Callable[..., Any] = _get_json) -> list[dict[str, Any]]:
    twse_payload = fetch_json(
        TWSE_HISTORY_URL,
        {"date": query_date.strftime("%Y%m%d"), "type": "ALLBUT0999", "response": "json"},
    )
    tpex_payload = fetch_json(
        TPEX_HISTORY_URL,
        {"date": query_date.strftime("%Y/%m/%d"), "id": "", "response": "json"},
    )
    if not isinstance(twse_payload, dict) or not isinstance(tpex_payload, dict):
        raise RuntimeError(f"official historical price schema mismatch for {query_date}")
    twse_rows = normalize_twse_history(twse_payload, query_date)
    tpex_rows = normalize_tpex_history(tpex_payload, query_date)
    if bool(twse_rows) != bool(tpex_rows):
        raise RuntimeError(
            f"official historical price partitions do not align for {query_date}: "
            f"TWSE={len(twse_rows)}, TPEx={len(tpex_rows)}"
        )
    return twse_rows + tpex_rows


def _read_existing(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except (OSError, csv.Error):
        return []


def merge_price_rows(price_dir: Path, stock_ids: set[str], rows: list[dict[str, Any]]) -> int:
    price_dir.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        sid = str(row.get("stock_id") or "").strip()
        if sid in stock_ids:
            grouped[sid].append(row)
    for sid, new_rows in grouped.items():
        path = price_dir / f"{sid}.csv"
        by_date: dict[str, dict[str, Any]] = {}
        for item in _read_existing(path):
            row_date = str(item.get("date") or "")
            if row_date:
                by_date[row_date] = {field: item.get(field, "") for field in CSV_FIELDS}
        for item in new_rows:
            row_date = str(item["date"])
            by_date[row_date] = {field: item.get(field, "") for field in CSV_FIELDS}
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(by_date[key] for key in sorted(by_date))
    return len(grouped)


def _last_csv_date(path: Path) -> str:
    rows = _read_existing(path)
    return max((str(row.get("date") or "") for row in rows), default="")


def load_previous_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def choose_start_date(
    latest_date: date,
    previous_summary: dict[str, Any],
    *,
    initial_days: int = 75,
    overlap_days: int = 7,
) -> date:
    previous_date = source_date_to_iso(previous_summary.get("latest_data_date"))
    if previous_summary.get("source") == "official_twse_tpex" and previous_date:
        parsed = date.fromisoformat(previous_date)
        return min(latest_date, parsed - timedelta(days=max(0, overlap_days)))
    return latest_date - timedelta(days=max(0, initial_days - 1))


def refresh_official_prices(
    *,
    stock_ids: set[str],
    price_dir: Path,
    summary_path: Path,
    initial_days: int = 75,
    overlap_days: int = 7,
    fetch_latest: Callable[[], tuple[str, list[dict[str, Any]], dict[str, int]]] = fetch_latest_snapshot,
    fetch_history: Callable[[date], list[dict[str, Any]]] = fetch_history_snapshot,
) -> dict[str, Any]:
    if not stock_ids:
        raise RuntimeError("official price refresh has an empty stock universe")
    latest_iso, latest_rows, latest_partition_counts = fetch_latest()
    latest_date = date.fromisoformat(latest_iso)
    previous = load_previous_summary(summary_path)
    start_date = choose_start_date(
        latest_date,
        previous,
        initial_days=initial_days,
        overlap_days=overlap_days,
    )

    all_rows: list[dict[str, Any]] = []
    common_dates: list[str] = []
    warnings: list[str] = []
    query_date = start_date
    while query_date < latest_date:
        try:
            rows = fetch_history(query_date)
        except Exception as exc:
            warnings.append(f"{query_date.isoformat()}: {exc}")
            rows = []
        if rows:
            all_rows.extend(rows)
            common_dates.append(query_date.isoformat())
        query_date += timedelta(days=1)
    all_rows.extend(latest_rows)
    common_dates.append(latest_iso)

    if warnings:
        raise RuntimeError(
            f"official price backfill was incomplete for {len(warnings)} date(s); "
            f"first={warnings[0]}"
        )

    latest_matched_ids = {row["stock_id"] for row in latest_rows if row["stock_id"] in stock_ids}
    if not latest_matched_ids:
        raise RuntimeError("official latest price snapshot matched zero configured stocks")
    written_files = merge_price_rows(price_dir, stock_ids, all_rows)
    if written_files == 0:
        raise RuntimeError("official price refresh wrote zero files")

    stale_after_refresh = sorted(
        sid for sid in latest_matched_ids if _last_csv_date(price_dir / f"{sid}.csv") != latest_iso
    )
    if stale_after_refresh:
        raise RuntimeError(
            f"official price refresh left {len(stale_after_refresh)} latest-market files stale; "
            f"sample={stale_after_refresh[:10]}"
        )

    summary = {
        "schema_version": "1.0.0",
        "source": "official_twse_tpex",
        "source_urls": {
            "twse_latest": TWSE_LATEST_URL,
            "tpex_latest": TPEX_LATEST_URL,
            "twse_history": TWSE_HISTORY_URL,
            "tpex_history": TPEX_HISTORY_URL,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "query_start_date": start_date.isoformat(),
        "latest_data_date": latest_iso,
        "common_trading_dates": common_dates,
        "stock_scope_count": len(stock_ids),
        "latest_partition_rows": latest_partition_counts,
        "latest_matched_stocks": len(latest_matched_ids),
        "written_files": written_files,
        "history_warning_count": len(warnings),
        "history_warnings": warnings[:20],
        "status": "fresh",
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary
