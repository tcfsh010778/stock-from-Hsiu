# -*- coding: utf-8 -*-
"""Archive a compact weekly 400-lot holder snapshot from official TDCC data.

Only the derived 400+ lot aggregate is retained. The 68k-row raw TDCC response
is never written to disk or published.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from market_flow import is_ordinary_equity


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MARKET_CACHE_PATH = DATA_DIR / "stock_markets.json"
OUTPUT_PATH = DATA_DIR / "holder_weekly_snapshots.json"
TDCC_URL = "https://openapi.tdcc.com.tw/v1/opendata/1-5"
TAIPEI_TZ = timezone(timedelta(hours=8))
MAJOR_LEVELS = {"12", "13", "14", "15"}  # 400,001 shares and above


def _field(row: dict[str, Any], name: str) -> Any:
    for key, value in row.items():
        if str(key).lstrip("\ufeff") == name:
            return value
    return None


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return default


def _date(value: Any) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return ""


def load_security_map(path: Path = MARKET_CACHE_PATH) -> dict[str, dict[str, str]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    stocks = payload.get("stocks") if isinstance(payload.get("stocks"), dict) else {}
    markets = payload.get("markets") if isinstance(payload.get("markets"), dict) else {}
    output: dict[str, dict[str, str]] = {}
    for code in set(stocks) | set(markets):
        item = stocks.get(code) if isinstance(stocks.get(code), dict) else {}
        market_text = str(item.get("market") or markets.get(code) or "").strip()
        market = "listed" if market_text in {"上市", "listed"} else "otc" if market_text in {"上櫃", "otc"} else ""
        ref = {"security_id": str(code), "name": str(item.get("name") or "").strip(), "market": market}
        if market and is_ordinary_equity(ref):
            output[str(code)] = {"name": ref["name"], "market": market}
    return output


def fetch_rows(session: requests.Session | None = None, timeout: int = 60) -> list[dict[str, Any]]:
    client = session or requests.Session()
    response = client.get(TDCC_URL, timeout=timeout, headers={"Accept": "application/json"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("TDCC 1-5 returned no rows")
    return [row for row in payload if isinstance(row, dict)]


def aggregate_snapshot(raw_rows: list[dict[str, Any]], security_map: dict[str, dict[str, str]]) -> dict[str, Any]:
    by_code: dict[str, dict[str, Any]] = {}
    observed_dates: set[str] = set()
    for raw in raw_rows:
        code = str(_field(raw, "證券代號") or "").strip()
        if code not in security_map or str(_field(raw, "持股分級") or "").strip() not in MAJOR_LEVELS:
            continue
        data_date = _date(_field(raw, "資料日期"))
        if not data_date:
            continue
        observed_dates.add(data_date)
        ref = security_map[code]
        item = by_code.setdefault(
            code,
            {
                "security_id": code,
                "name": ref.get("name") or "",
                "market": ref.get("market") or "",
                "major_percent": 0.0,
                "major_people": 0,
            },
        )
        item["major_percent"] += _number(_field(raw, "占集保庫存數比例%"))
        item["major_people"] += int(_number(_field(raw, "人數")))
    if len(observed_dates) != 1:
        raise RuntimeError(f"TDCC snapshot dates are not aligned: {sorted(observed_dates)}")
    rows = sorted(by_code.values(), key=lambda row: str(row["security_id"]))
    for row in rows:
        row["major_percent"] = round(float(row["major_percent"]), 2)
    if not rows:
        raise RuntimeError("TDCC snapshot contained no listed/OTC common security rows")
    return {"date": next(iter(observed_dates)), "rows": rows}


def merge_archive(snapshot: dict[str, Any], existing: dict[str, Any] | None = None, keep_weeks: int = 60) -> dict[str, Any]:
    existing = existing if isinstance(existing, dict) else {}
    by_date = {
        str(item.get("date")): item
        for item in existing.get("snapshots") or []
        if isinstance(item, dict) and item.get("date") and isinstance(item.get("rows"), list)
    }
    by_date[str(snapshot["date"])] = snapshot
    dates = sorted(by_date)[-keep_weeks:]
    return {
        "dataset_id": "holder_weekly_snapshots",
        "schema_version": "1.0.0",
        "source_id": "tdcc_shareholder_distribution",
        "source_url": TDCC_URL,
        "updated_at": datetime.now(TAIPEI_TZ).isoformat(),
        "latest_date": dates[-1] if dates else "",
        "snapshot_count": len(dates),
        "snapshots": [by_date[data_date] for data_date in dates],
    }


def write_archive(payload: dict[str, Any], path: Path = OUTPUT_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive the latest official TDCC 400+ lot holder aggregate.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()
    raw_rows = fetch_rows()
    snapshot = aggregate_snapshot(raw_rows, load_security_map())
    try:
        existing = json.loads(args.output.read_text(encoding="utf-8-sig"))
    except Exception:
        existing = {}
    payload = merge_archive(snapshot, existing)
    write_archive(payload, args.output)
    print(f"[tdcc_holder_snapshot] date={snapshot['date']} stocks={len(snapshot['rows'])} snapshots={payload['snapshot_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
