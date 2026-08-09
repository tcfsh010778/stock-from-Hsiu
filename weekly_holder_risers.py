# -*- coding: utf-8 -*-
"""Build the weekly major-holder ownership-risers display artifact."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data_contract import DEFAULT_MANIFEST_PATH, prepare_artifact_manifest, update_manifest_file
from market_flow import is_ordinary_equity
from stock_rules import holding_group

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
HOLDING_DIR = DATA_DIR / "holding_shares"
MARKET_CACHE_PATH = DATA_DIR / "stock_markets.json"
OUTPUT_PATH = DATA_DIR / "weekly_holder_risers.json"
HOLDER_SNAPSHOT_PATH = DATA_DIR / "holder_weekly_snapshots.json"
DERIVED_SOURCE_ID = "weekly_holder_risers_derived"
TAIPEI_TZ = timezone(timedelta(hours=8))
DEFAULT_RANKING_LIMIT = 50


def _number(value: Any, default: float | None = None) -> float | None:
    if value in (None, "", "--", "－", "-"):
        return default
    try:
        parsed = float(str(value).replace(",", "").replace("%", "").strip())
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp950"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                return list(csv.DictReader(handle))
        except UnicodeDecodeError:
            continue
    return []


def _market_map(path: Path = MARKET_CACHE_PATH) -> dict[str, dict[str, str]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    output = {}
    for code, item in (payload.get("stocks") or {}).items():
        if not isinstance(item, dict):
            continue
        market_text = str(item.get("market") or "").strip()
        market = "listed" if market_text in {"上市", "listed"} else "otc" if market_text in {"上櫃", "otc"} else market_text
        output[str(code)] = {**item, "market": market}
    return output


def _snapshot_series(path: Path = HOLDER_SNAPSHOT_PATH) -> dict[str, list[dict[str, Any]]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    by_code: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for snapshot in payload.get("snapshots") or []:
        if not isinstance(snapshot, dict):
            continue
        data_date = str(snapshot.get("date") or "")
        for row in snapshot.get("rows") or []:
            if not isinstance(row, dict):
                continue
            code = str(row.get("security_id") or "")
            if not code or not data_date:
                continue
            by_code[code][data_date] = {
                "date": data_date,
                "major": _number(row.get("major_percent"), 0.0) or 0.0,
                "major_people": int(_number(row.get("major_people"), 0.0) or 0),
                "name": str(row.get("name") or ""),
                "market": str(row.get("market") or ""),
            }
    return {code: [items[data_date] for data_date in sorted(items)] for code, items in by_code.items()}


def _snapshot_ranking_scope(path: Path = HOLDER_SNAPSHOT_PATH) -> set[str]:
    """Return the fail-closed TDCC Top-N selection when a backfill recorded one."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return set()
    backfill = payload.get("history_backfill") if isinstance(payload.get("history_backfill"), dict) else {}
    return {str(code) for code in backfill.get("selected_security_ids") or [] if str(code)}


def _weekly_gap_ok(previous_date: str, data_date: str) -> bool:
    """Allow exchange-holiday shifts, but never label a multi-week gap as one week."""

    try:
        gap = (datetime.fromisoformat(data_date).date() - datetime.fromisoformat(previous_date).date()).days
    except ValueError:
        return False
    return 4 <= gap <= 10


def _latest_complete_window(dates: set[str], size: int = 7) -> list[str]:
    """Return the newest complete weekly run, ignoring a newer partial run."""

    runs: list[list[str]] = []
    for data_date in sorted(dates):
        if not runs or not _weekly_gap_ok(runs[-1][-1], data_date):
            runs.append([data_date])
        else:
            runs[-1].append(data_date)
    complete = [run[-size:] for run in runs if len(run) >= size]
    return complete[-1] if complete else []


def _series(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        if str(row.get("date") or "").strip():
            by_date[str(row["date"]).strip()].append(row)
    output = []
    for data_date in sorted(by_date):
        item: dict[str, Any] = {"date": data_date, "major": 0.0, "middle": 0.0, "retail": 0.0, "major_people": 0, "total_people": None}
        for row in by_date[data_date]:
            group = holding_group(str(row.get("HoldingSharesLevel") or ""))
            percent = _number(row.get("percent"), 0.0) or 0.0
            if group in {"major", "middle", "retail"}:
                item[group] += percent
            if group == "major":
                item["major_people"] += int(_number(row.get("people"), 0.0) or 0)
            if str(row.get("HoldingSharesLevel") or "").strip() == "total":
                item["total_people"] = int(_number(row.get("people"), 0.0) or 0)
        output.append(item)
    return output


def build_rows(
    *,
    holding_dir: Path = HOLDING_DIR,
    market_map: dict[str, dict[str, str]] | None = None,
    snapshot_path: Path = HOLDER_SNAPSHOT_PATH,
    limit: int | None = DEFAULT_RANKING_LIMIT,
) -> list[dict[str, Any]]:
    refs = market_map if market_map is not None else _market_map()
    combined: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for path in sorted(holding_dir.glob("*.csv")) if holding_dir.exists() else []:
        stock_id = path.stem
        for item in _series(_read_csv(path)):
            combined[stock_id][str(item["date"])] = item
    for stock_id, series in _snapshot_series(snapshot_path).items():
        for item in series:
            combined[stock_id][str(item["date"])] = item

    all_dates = _latest_complete_window({data_date for items in combined.values() for data_date in items})
    if len(all_dates) < 7:
        return []
    ranking_scope = _snapshot_ranking_scope(snapshot_path)
    change_dates = all_dates[1:]
    output = []
    for stock_id, items in combined.items():
        if ranking_scope and stock_id not in ranking_scope:
            continue
        previous = items.get(all_dates[-2])
        latest = items.get(all_dates[-1])
        if not previous or not latest:
            continue
        delta = float(latest.get("major") or 0.0) - float(previous.get("major") or 0.0)
        if delta <= 0:
            continue
        ref = refs.get(stock_id) or {}
        identity = {
            "security_id": stock_id,
            "name": str(ref.get("name") or latest.get("name") or "").strip(),
        }
        if not is_ordinary_equity(identity):
            continue
        weekly_changes = []
        for previous_date, data_date in zip(all_dates, change_dates):
            earlier, later = items.get(previous_date), items.get(data_date)
            weekly_delta = None if not earlier or not later or not _weekly_gap_ok(previous_date, data_date) else round(float(later.get("major") or 0.0) - float(earlier.get("major") or 0.0), 2)
            weekly_changes.append({"date": data_date, "delta_pctpt": weekly_delta})
        available_changes = [item["delta_pctpt"] for item in weekly_changes if item["delta_pctpt"] is not None]
        output.append({
            "security_id": stock_id,
            "name": identity["name"],
            "market": str(ref.get("market") or latest.get("market") or "").strip(),
            "data_date": latest.get("date"),
            "previous_date": previous.get("date"),
            "major_percent": round(float(latest.get("major") or 0.0), 2),
            "previous_major_percent": round(float(previous.get("major") or 0.0), 2),
            "major_delta_pctpt": round(delta, 2),
            "major_people": latest.get("major_people"),
            "weekly_changes": weekly_changes,
            "six_week_delta_pctpt": round(sum(available_changes), 2),
            "positive_week_count": sum(1 for value in available_changes if value > 0),
            "six_week_complete": len(available_changes) == 6,
        })
    output.sort(key=lambda row: (-float(row.get("major_delta_pctpt") or 0), str(row.get("security_id") or "")))
    return output[:limit] if limit is not None and limit > 0 else output


def build_payload(
    rows: list[dict[str, Any]],
    *,
    updated_at: datetime | str,
    source_state: str = "tdcc_official",
    ranking_limit: int = DEFAULT_RANKING_LIMIT,
) -> dict[str, Any]:
    dates = sorted({str(row.get("data_date") or "") for row in rows if row.get("data_date")})
    latest_date = dates[-1] if dates else ""
    previous_dates = sorted({str(row.get("previous_date") or "") for row in rows if row.get("previous_date")})
    previous_date = previous_dates[-1] if previous_dates else ""
    weekly_dates = sorted({str(item.get("date") or "") for row in rows for item in row.get("weekly_changes") or [] if item.get("date")})[-6:]
    complete_rows = sum(1 for row in rows if row.get("six_week_complete"))
    return {
        "dataset_id": "weekly_holder_risers",
        "schema_version": "1.3.0",
        "date": latest_date,
        "previous_date": previous_date,
        "weekly_dates": weekly_dates,
        "updated_at": updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at),
        "rows": rows,
        "row_count": len(rows),
        "six_week_complete_count": complete_rows,
        "ranking_limit": ranking_limit,
        "ranking_basis": "latest_week_major_holder_change_pctpt_desc",
        "complete_positive_set": False,
        "source_state": source_state,
        "data_quality": {
            "state": "ok" if rows and len(weekly_dates) == 6 else "partial" if rows else "missing",
            "warnings": [] if rows and len(weekly_dates) == 6 else ["six weekly holder changes are not yet complete"] if rows else ["holder cache has fewer than two aligned weekly snapshots"],
        },
    }


def write_payload(payload: dict[str, Any], output_path: Path | str = OUTPUT_PATH, manifest_path: Path | str = DEFAULT_MANIFEST_PATH) -> Path:
    path = Path(output_path)
    rows = [
        {
            "data_date": row.get("data_date"),
            "security_id": row.get("security_id"),
            "market": row.get("market") or "listed",
            "major_percent": row.get("major_percent"),
            "previous_major_percent": row.get("previous_major_percent"),
            "major_delta_pctpt": row.get("major_delta_pctpt"),
            "six_week_delta_pctpt": row.get("six_week_delta_pctpt"),
            "positive_week_count": row.get("positive_week_count"),
        }
        for row in payload.get("rows") or []
    ]
    data_date = str(payload.get("date") or "")
    expected_date = datetime.now(TAIPEI_TZ).date().isoformat()
    _, manifest, artifact_bytes = prepare_artifact_manifest(
        payload,
        dataset_id="weekly_holder_risers",
        source_id=DERIVED_SOURCE_ID,
        rows=rows,
        data_date=data_date,
        expected_data_date=expected_date,
        fetched_at=str(payload.get("updated_at") or datetime.now(TAIPEI_TZ).isoformat()),
        missing_partitions=["holding_shares"] if not rows else [],
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(artifact_bytes)
    update_manifest_file(manifest, manifest_path)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build weekly major-holder ownership-risers artifact.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--limit", type=int, default=DEFAULT_RANKING_LIMIT, help="Latest-week positive-change ranking cap (default: 50).")
    args = parser.parse_args()
    rows = build_rows(limit=args.limit or None)
    payload = build_payload(rows, updated_at=datetime.now(TAIPEI_TZ), ranking_limit=args.limit)
    if args.output.exists():
        try:
            previous = json.loads(args.output.read_text(encoding="utf-8-sig"))
        except Exception:
            previous = None
        if isinstance(previous, dict) and previous.get("date") == payload.get("date") and previous.get("rows") == rows:
            payload = previous
    write_payload(payload, args.output, args.manifest)
    print(f"[weekly_holder_risers] wrote {args.output} date={payload.get('date')} rows={len(payload.get('rows') or [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
