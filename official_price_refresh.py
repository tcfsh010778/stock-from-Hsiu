from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from stock_v2_public.analysis.price_basis import PRICE_BASIS_MODE, price_basis_metadata, project_adjusted_rows


TWSE_LATEST_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_LATEST_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TWSE_HISTORY_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
TPEX_HISTORY_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
USER_AGENT = "stock-from-Hsiu-official-price-refresh/1.0"
CSV_FIELDS = [
    "date", "open", "high", "low", "close", "volume",
    "raw_open", "raw_high", "raw_low", "raw_close", "raw_volume", "adjustment_factor",
]
TWSE_ACTIONS_URL = "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
TPEX_ACTIONS_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ"
RECOVERY_MIN_UNIQUE_IDS = {"twse": 800, "tpex": 600}
RECOVERY_MIN_REFERENCE_RATIO = 0.99


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


def fetch_history_partitions(
    query_date: date,
    fetch_json: Callable[..., Any] = _get_json,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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
    if not twse_rows and not tpex_rows:
        twse_closed = "沒有符合條件的資料" in str(twse_payload.get("stat") or "") and not (twse_payload.get("tables") or [])
        tpex_tables = tpex_payload.get("tables") or []
        tpex_closed = (
            str(tpex_payload.get("stat") or "").lower() == "ok"
            and source_date_to_iso(tpex_payload.get("date")) == query_date.isoformat()
            and bool(tpex_tables)
            and all(not (table.get("data") or []) for table in tpex_tables if isinstance(table, dict))
        )
        if not (twse_closed and tpex_closed):
            raise RuntimeError(f"official historical empty response is not a recognized joint market closure for {query_date}")
    if bool(twse_rows) != bool(tpex_rows):
        raise RuntimeError(
            f"official historical price partitions do not align for {query_date}: "
            f"TWSE={len(twse_rows)}, TPEx={len(tpex_rows)}"
        )
    return twse_rows, tpex_rows


def partition_security_ids(rows: list[dict[str, Any]], label: str) -> set[str]:
    security_ids: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        security_id = str(row.get("stock_id") or "").strip()
        if security_id in security_ids:
            duplicates.add(security_id)
        elif security_id:
            security_ids.add(security_id)
    if duplicates:
        raise RuntimeError(
            f"official price {label} partition contains duplicate security IDs: "
            f"count={len(duplicates)}, sample={sorted(duplicates)[:10]}"
        )
    return security_ids


def fetch_latest_snapshot(
    fetch_json: Callable[..., Any] = _get_json,
    *,
    recovery_min_unique_ids: dict[str, int] | None = None,
    recovery_min_reference_ratio: float = RECOVERY_MIN_REFERENCE_RATIO,
) -> tuple[str, list[dict[str, Any]], dict[str, int], dict[str, Any]]:
    twse_payload = fetch_json(TWSE_LATEST_URL)
    tpex_payload = fetch_json(TPEX_LATEST_URL)
    if not isinstance(twse_payload, list) or not isinstance(tpex_payload, list):
        raise RuntimeError("official latest price endpoints returned an unexpected schema")
    twse_date, twse_rows = normalize_twse_latest(twse_payload)
    tpex_date, tpex_rows = normalize_tpex_latest(tpex_payload)
    twse_latest_ids = partition_security_ids(twse_rows, "TWSE latest")
    tpex_latest_ids = partition_security_ids(tpex_rows, "TPEx latest")
    if twse_date != tpex_date:
        reference_ids = {"twse": twse_latest_ids, "tpex": tpex_latest_ids}
        reference_counts = {market: len(ids) for market, ids in reference_ids.items()}
        target_iso = max(twse_date, tpex_date)
        target_date = date.fromisoformat(target_iso)
        try:
            twse_rows, tpex_rows = fetch_history_partitions(target_date, fetch_json)
        except Exception as exc:
            raise RuntimeError(
                "official latest price date-skew recovery failed: "
                f"TWSE latest={twse_date}, TPEx latest={tpex_date}, target={target_iso}: {exc}"
            ) from exc
        if not twse_rows or not tpex_rows:
            raise RuntimeError(
                "official latest price date-skew recovery failed: "
                f"TWSE latest={twse_date}, TPEx latest={tpex_date}, target={target_iso}, "
                f"historical TWSE={len(twse_rows)}, historical TPEx={len(tpex_rows)}"
            )
        recovered_ids = {
            "twse": partition_security_ids(twse_rows, "TWSE recovered"),
            "tpex": partition_security_ids(tpex_rows, "TPEx recovered"),
        }
        recovered_counts = {market: len(ids) for market, ids in recovered_ids.items()}
        minimum_ids = recovery_min_unique_ids or RECOVERY_MIN_UNIQUE_IDS
        minimum_counts = {
            market: int(minimum_ids.get(market, RECOVERY_MIN_UNIQUE_IDS[market]))
            for market in ("twse", "tpex")
        }
        required_reference_counts = {
            market: math.ceil(reference_counts[market] * recovery_min_reference_ratio)
            for market in ("twse", "tpex")
        }
        covered_reference_counts = {
            market: len(reference_ids[market] & recovered_ids[market])
            for market in ("twse", "tpex")
        }
        incomplete = {
            market: {
                "unique_ids": recovered_counts[market],
                "minimum_unique_ids": minimum_counts[market],
                "covered_reference_ids": covered_reference_counts[market],
                "required_reference_ids": required_reference_counts[market],
            }
            for market in ("twse", "tpex")
            if (
                recovered_counts[market] < minimum_counts[market]
                or covered_reference_counts[market] < required_reference_counts[market]
            )
        }
        if incomplete:
            raise RuntimeError(
                "official latest price date-skew recovery coverage is incomplete: "
                f"TWSE latest={twse_date}, TPEx latest={tpex_date}, target={target_iso}, "
                f"partitions={incomplete}"
            )
        metadata = {
            "mode": "historical_exact_date_recovery",
            "date_skew_recovered": True,
            "twse_latest_date": twse_date,
            "tpex_latest_date": tpex_date,
            "target_date": target_iso,
            "recovery_coverage": {
                "count_basis": "unique_security_ids",
                "reference_unique_ids": reference_counts,
                "minimum_unique_ids": minimum_counts,
                "required_reference_ids": required_reference_counts,
                "covered_reference_ids": covered_reference_counts,
                "recovered_unique_ids": recovered_counts,
                "min_reference_ratio": recovery_min_reference_ratio,
            },
        }
        return target_iso, twse_rows + tpex_rows, recovered_counts, metadata
    if not twse_rows or not tpex_rows:
        raise RuntimeError("official latest price snapshot is missing a market partition")
    metadata = {
        "mode": "latest_openapi",
        "date_skew_recovered": False,
        "twse_latest_date": twse_date,
        "tpex_latest_date": tpex_date,
        "target_date": twse_date,
    }
    return twse_date, twse_rows + tpex_rows, {"twse": len(twse_latest_ids), "tpex": len(tpex_latest_ids)}, metadata


def fetch_history_snapshot(query_date: date, fetch_json: Callable[..., Any] = _get_json) -> list[dict[str, Any]]:
    twse_rows, tpex_rows = fetch_history_partitions(query_date, fetch_json)
    return twse_rows + tpex_rows


def _roc_date(value: date) -> str:
    return f"{value.year - 1911:03d}/{value.month:02d}/{value.day:02d}"


def normalize_twse_actions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    fields = [str(field).strip() for field in payload.get("fields") or []]
    required = {"資料日期", "股票代號", "除權息前收盤價", "除權息參考價", "權/息"}
    if not required.issubset(fields):
        raise RuntimeError("TWSE corporate-action schema mismatch")
    ix = {field: fields.index(field) for field in required}
    rows = []
    for raw in payload.get("data") or []:
        sid = str(raw[ix["股票代號"]]).strip() if len(raw) > ix["股票代號"] else ""
        if not re.fullmatch(r"\d{4}", sid):
            continue
        try:
            event_date = source_date_to_iso(raw[ix["資料日期"]])
            previous = _number(raw[ix["除權息前收盤價"]])
            reference = _number(raw[ix["除權息參考價"]])
        except (IndexError, TypeError, ValueError) as exc:
            raise RuntimeError(f"TWSE corporate action has invalid ordinary-security values: {sid}") from exc
        if not event_date or previous <= 0 or reference <= 0:
            raise RuntimeError(f"TWSE corporate action has invalid ordinary-security values: {sid}")
        rows.append({"date": event_date, "stock_id": sid, "previous_close": previous,
                     "reference_price": reference, "kind": str(raw[ix["權/息"]]).strip(), "market": "twse"})
    return rows


def normalize_tpex_actions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    table = (payload.get("tables") or [None])[0]
    if not isinstance(table, dict):
        raise RuntimeError("TPEx corporate-action schema mismatch")
    fields = [str(field).strip() for field in table.get("fields") or []]
    required = {"除權息日期", "代號", "除權息前收盤價", "除權息參考價", "權/息"}
    if not required.issubset(fields):
        raise RuntimeError("TPEx corporate-action schema mismatch")
    ix = {field: fields.index(field) for field in required}
    rows = []
    for raw in table.get("data") or []:
        sid = str(raw[ix["代號"]]).strip() if len(raw) > ix["代號"] else ""
        if not re.fullmatch(r"\d{4}", sid):
            continue
        try:
            event_date = source_date_to_iso(raw[ix["除權息日期"]])
            previous = _number(raw[ix["除權息前收盤價"]])
            reference = _number(raw[ix["除權息參考價"]])
        except (IndexError, TypeError, ValueError) as exc:
            raise RuntimeError(f"TPEx corporate action has invalid ordinary-security values: {sid}") from exc
        if not event_date or previous <= 0 or reference <= 0:
            raise RuntimeError(f"TPEx corporate action has invalid ordinary-security values: {sid}")
        rows.append({"date": event_date, "stock_id": sid, "previous_close": previous,
                     "reference_price": reference, "kind": str(raw[ix["權/息"]]).strip(), "market": "tpex"})
    return rows


def fetch_corporate_actions(start: date, end: date, fetch_json: Callable[..., Any] = _get_json) -> list[dict[str, Any]]:
    twse = fetch_json(TWSE_ACTIONS_URL, {"startDate": start.strftime("%Y%m%d"), "endDate": end.strftime("%Y%m%d"), "response": "json"})
    tpex = fetch_json(TPEX_ACTIONS_URL, {"startDate": _roc_date(start), "endDate": _roc_date(end)})
    if not isinstance(twse, dict) or not isinstance(tpex, dict):
        raise RuntimeError("official corporate-action response is not JSON object data")
    return normalize_twse_actions(twse) + normalize_tpex_actions(tpex)


def default_partition_cache() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA") or tempfile.gettempdir())
    return base / "stock-from-Hsiu" / "official-price-partitions-v1"


def _partition_digest(payload: dict[str, Any]) -> str:
    material = {key: payload[key] for key in ("schema_version", "date", "twse", "tpex")}
    return hashlib.sha256(json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _validate_cached_partition(payload: dict[str, Any], query_date: date, minimum_unique_ids: dict[str, int] | None = None) -> list[dict[str, Any]]:
    if payload.get("date") != query_date.isoformat() or payload.get("schema_version") != "1.0.0":
        raise RuntimeError("cached partition date/schema mismatch")
    twse = payload.get("twse")
    tpex = payload.get("tpex")
    if not isinstance(twse, list) or not isinstance(tpex, list):
        raise RuntimeError("cached partition markets are missing")
    if bool(twse) != bool(tpex):
        raise RuntimeError("cached partition markets do not align")
    partition_security_ids(twse, "cached TWSE")
    partition_security_ids(tpex, "cached TPEx")
    if payload.get("sha256") != _partition_digest(payload):
        raise RuntimeError("cached partition digest mismatch")
    minimum = minimum_unique_ids or RECOVERY_MIN_UNIQUE_IDS
    if twse or tpex:
        counts = {"twse": len(partition_security_ids(twse, "cached TWSE")),
                  "tpex": len(partition_security_ids(tpex, "cached TPEx"))}
        if any(counts[market] < int(minimum[market]) for market in ("twse", "tpex")):
            raise RuntimeError(f"cached partition market coverage is incomplete: counts={counts}, minimum={minimum}")
    for market, rows in (("twse", twse), ("tpex", tpex)):
        if any(row.get("date") != query_date.isoformat() or row.get("market") not in (None, market) for row in rows):
            raise RuntimeError(f"cached {market} partition is not exact-date data")
    return twse + tpex


def cached_history_snapshot(
    query_date: date,
    *,
    cache_dir: Path,
    fetch_partitions: Callable[[date], tuple[list[dict[str, Any]], list[dict[str, Any]]]] = fetch_history_partitions,
    minimum_unique_ids: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    path = cache_dir / f"{query_date.isoformat()}.json"
    cached_error: Exception | None = None
    if path.exists():
        try:
            return _validate_cached_partition(json.loads(path.read_text(encoding="utf-8")), query_date, minimum_unique_ids)
        except Exception as exc:
            cached_error = exc
    try:
        twse, tpex = fetch_partitions(query_date)
    except Exception as exc:
        if cached_error is not None:
            raise cached_error from exc
        raise
    payload = {"schema_version": "1.0.0", "date": query_date.isoformat(),
               "twse": [{**row, "market": "twse"} for row in twse],
               "tpex": [{**row, "market": "tpex"} for row in tpex]}
    payload["sha256"] = _partition_digest(payload)
    _validate_cached_partition(payload, query_date, minimum_unique_ids)
    cache_dir.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(temporary, path)
    return payload["twse"] + payload["tpex"]


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


def write_adjusted_price_rows(
    price_dir: Path,
    basis_dir: Path,
    stock_ids: set[str],
    raw_rows: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    *,
    adjustment_as_of: str,
    rebuild: bool,
) -> tuple[int, dict[str, dict[str, Any]]]:
    """Write projected files only after every input row has been validated."""
    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    if not rebuild:
        for sid in stock_ids:
            existing = _read_existing(price_dir / f"{sid}.csv")
            if existing and not set(CSV_FIELDS).issubset(existing[0]):
                raise RuntimeError(f"legacy or mixed price cache requires --rebuild-history: {sid}")
            for row in existing:
                grouped[sid][row["date"]] = {
                    "date": row["date"], "stock_id": sid,
                    "open": row["raw_open"], "high": row["raw_high"], "low": row["raw_low"],
                    "close": row["raw_close"], "volume": row["raw_volume"],
                }
    for row in raw_rows:
        sid = str(row.get("stock_id") or "")
        if sid in stock_ids:
            grouped[sid][str(row["date"])] = row
    flattened = [row for sid in grouped for row in grouped[sid].values()]
    projected = project_adjusted_rows(flattened, actions, adjustment_as_of=adjustment_as_of)
    projected_by_stock: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in projected:
        projected_by_stock[row["stock_id"]].append(row)
    if not projected_by_stock:
        return 0, {}
    if rebuild:
        missing = sorted(sid for sid in stock_ids if (price_dir / f"{sid}.csv").exists() and sid not in projected_by_stock)
        if missing:
            raise RuntimeError(f"official rebuild would leave legacy price files untouched: count={len(missing)}, sample={missing[:10]}")

    price_dir.parent.mkdir(parents=True, exist_ok=True)
    price_dir.mkdir(parents=True, exist_ok=True)
    basis_dir.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix="price-basis-stage-", dir=str(price_dir.parent)))
    metadata: dict[str, dict[str, Any]] = {}
    try:
        for sid, stock_rows in projected_by_stock.items():
            with (stage / f"{sid}.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                writer.writeheader()
                writer.writerows({field: row[field] for field in CSV_FIELDS} for row in stock_rows)
            metadata[sid] = price_basis_metadata(stock_id=sid, adjustment_as_of=adjustment_as_of, actions=actions)
        for staged in stage.glob("*.csv"):
            os.replace(staged, price_dir / staged.name)
        for sid, item in metadata.items():
            target = basis_dir / f"{sid}.json"
            temporary = target.with_suffix(".tmp")
            temporary.write_text(json.dumps(item, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            os.replace(temporary, target)
    finally:
        try:
            stage.rmdir()
        except OSError:
            pass
    return len(projected_by_stock), metadata


def _last_csv_date(path: Path) -> str:
    rows = _read_existing(path)
    return max((str(row.get("date") or "") for row in rows), default="")


def earliest_existing_raw_date(price_dir: Path, stock_ids: set[str]) -> date | None:
    earliest: str | None = None
    for sid in stock_ids:
        rows = _read_existing(price_dir / f"{sid}.csv")
        if not rows:
            continue
        if not set(CSV_FIELDS).issubset(rows[0]):
            raise RuntimeError(f"legacy or mixed price cache requires --rebuild-history: {sid}")
        candidate = min((str(row.get("date") or "") for row in rows if row.get("date")), default="")
        if candidate and (earliest is None or candidate < earliest):
            earliest = candidate
    return date.fromisoformat(earliest) if earliest else None


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
    fetch_latest: Callable[..., tuple] = fetch_latest_snapshot,
    fetch_history: Callable[[date], list[dict[str, Any]]] = fetch_history_snapshot,
    rebuild_history_start: date | None = None,
    partition_cache_dir: Path | None = None,
    price_basis_dir: Path | None = None,
    fetch_actions: Callable[[date, date], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if not stock_ids:
        raise RuntimeError("official price refresh has an empty stock universe")
    latest_result = fetch_latest()
    if len(latest_result) == 4:
        latest_iso, latest_rows, latest_partition_counts, latest_snapshot = latest_result
    elif len(latest_result) == 3:
        latest_iso, latest_rows, latest_partition_counts = latest_result
        latest_snapshot = {
            "mode": "injected_or_legacy",
            "date_skew_recovered": False,
            "twse_latest_date": latest_iso,
            "tpex_latest_date": latest_iso,
            "target_date": latest_iso,
        }
    else:
        raise RuntimeError(f"official latest price provider returned {len(latest_result)} fields; expected 3 or 4")
    latest_date = date.fromisoformat(latest_iso)
    previous = load_previous_summary(summary_path)
    start_date = rebuild_history_start or choose_start_date(
        latest_date,
        previous,
        initial_days=initial_days,
        overlap_days=overlap_days,
    )

    all_rows: list[dict[str, Any]] = []
    common_dates: list[str] = []
    warnings: list[str] = []
    history_dates: list[date] = []
    query_date = start_date
    while query_date < latest_date:
        history_dates.append(query_date)
        query_date += timedelta(days=1)

    def load_day(day: date) -> tuple[date, list[dict[str, Any]]]:
        if rebuild_history_start is not None and fetch_history is fetch_history_snapshot:
            return day, cached_history_snapshot(day, cache_dir=partition_cache_dir or default_partition_cache())
        return day, fetch_history(day)

    results: dict[date, list[dict[str, Any]]] = {}
    errors: dict[date, Exception] = {}
    if rebuild_history_start is not None and fetch_history is fetch_history_snapshot:
        workers = max(1, min(8, int(os.environ.get("V44_OFFICIAL_HISTORY_WORKERS", "6"))))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(load_day, day): day for day in history_dates}
            for future in as_completed(futures):
                day = futures[future]
                try:
                    _, results[day] = future.result()
                except Exception as exc:
                    errors[day] = exc
    else:
        for day in history_dates:
            try:
                _, results[day] = load_day(day)
            except Exception as exc:
                errors[day] = exc

    for query_date in history_dates:
        try:
            if query_date in errors:
                raise errors[query_date]
            rows = results.get(query_date, [])
        except Exception as exc:
            warnings.append(f"{query_date.isoformat()}: {exc}")
            rows = []
        if rows:
            all_rows.extend(rows)
            common_dates.append(query_date.isoformat())
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
    action_start = rebuild_history_start or earliest_existing_raw_date(price_dir, stock_ids) or start_date
    if fetch_actions is None:
        actions = fetch_corporate_actions(action_start, latest_date) if fetch_latest is fetch_latest_snapshot else []
    else:
        actions = fetch_actions(action_start, latest_date)
    written_files, basis_by_stock = write_adjusted_price_rows(
        price_dir, price_basis_dir or price_dir.parent / "price_basis", stock_ids, all_rows, actions,
        adjustment_as_of=latest_iso,
        rebuild=rebuild_history_start is not None,
    )
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
        "schema_version": "2.0.0",
        "source": "official_twse_tpex",
        "source_urls": {
            "twse_latest": TWSE_LATEST_URL,
            "tpex_latest": TPEX_LATEST_URL,
            "twse_history": TWSE_HISTORY_URL,
            "tpex_history": TPEX_HISTORY_URL,
            "twse_corporate_actions": TWSE_ACTIONS_URL,
            "tpex_corporate_actions": TPEX_ACTIONS_URL,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "query_start_date": start_date.isoformat(),
        "latest_data_date": latest_iso,
        "latest_snapshot": latest_snapshot,
        "common_trading_dates": common_dates,
        "stock_scope_count": len(stock_ids),
        "latest_partition_rows": latest_partition_counts,
        "latest_matched_stocks": len(latest_matched_ids),
        "written_files": written_files,
        "price_basis": {
            "mode": PRICE_BASIS_MODE,
            "adjustment_as_of": latest_iso,
            "volume_basis": "official_raw_shares",
            "raw_columns": ["raw_open", "raw_high", "raw_low", "raw_close", "raw_volume"],
            "factor_column": "adjustment_factor",
            "verified_stock_count": len(basis_by_stock),
        },
        "history_warning_count": len(warnings),
        "history_warnings": warnings[:20],
        "status": "fresh",
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary
