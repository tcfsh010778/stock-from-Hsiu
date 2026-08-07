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
from stock_rules import holding_group

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
HOLDING_DIR = DATA_DIR / "holding_shares"
MARKET_CACHE_PATH = DATA_DIR / "stock_markets.json"
OUTPUT_PATH = DATA_DIR / "weekly_holder_risers.json"
DERIVED_SOURCE_ID = "weekly_holder_risers_derived"
TAIPEI_TZ = timezone(timedelta(hours=8))


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
    return {str(code): dict(item) for code, item in (payload.get("stocks") or {}).items() if isinstance(item, dict)}


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


def build_rows(*, holding_dir: Path = HOLDING_DIR, market_map: dict[str, dict[str, str]] | None = None, limit: int = 50) -> list[dict[str, Any]]:
    refs = market_map if market_map is not None else _market_map()
    output = []
    for path in sorted(holding_dir.glob("*.csv")) if holding_dir.exists() else []:
        stock_id = path.stem
        series = _series(_read_csv(path))
        if len(series) < 2:
            continue
        previous, latest = series[-2], series[-1]
        delta = (latest.get("major") or 0.0) - (previous.get("major") or 0.0)
        if delta <= 0:
            continue
        ref = refs.get(stock_id) or {}
        output.append({
            "security_id": stock_id,
            "name": str(ref.get("name") or "").strip(),
            "market": str(ref.get("market") or "").strip(),
            "data_date": latest.get("date"),
            "previous_date": previous.get("date"),
            "major_percent": round(float(latest.get("major") or 0.0), 2),
            "previous_major_percent": round(float(previous.get("major") or 0.0), 2),
            "major_delta_pctpt": round(delta, 2),
            "major_people": latest.get("major_people"),
        })
    output.sort(key=lambda row: (-float(row.get("major_delta_pctpt") or 0), str(row.get("security_id") or "")))
    return output[:limit]


def build_payload(rows: list[dict[str, Any]], *, updated_at: datetime | str, source_state: str = "local_cache") -> dict[str, Any]:
    dates = sorted({str(row.get("data_date") or "") for row in rows if row.get("data_date")})
    latest_date = dates[-1] if dates else ""
    previous_dates = sorted({str(row.get("previous_date") or "") for row in rows if row.get("previous_date")})
    previous_date = previous_dates[-1] if previous_dates else ""
    return {
        "dataset_id": "weekly_holder_risers",
        "schema_version": "1.0.0",
        "date": latest_date,
        "previous_date": previous_date,
        "updated_at": updated_at.isoformat() if isinstance(updated_at, datetime) else str(updated_at),
        "rows": rows,
        "source_state": source_state,
        "data_quality": {"state": "ok" if rows else "missing", "warnings": [] if rows else ["holding_shares weekly cache has fewer than two snapshots"]},
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
        }
        for row in payload.get("rows") or []
    ]
    data_date = str(payload.get("date") or "")
    _, manifest, artifact_bytes = prepare_artifact_manifest(
        payload,
        dataset_id="weekly_holder_risers",
        source_id=DERIVED_SOURCE_ID,
        rows=rows,
        data_date=data_date,
        expected_data_date=data_date or datetime.now(TAIPEI_TZ).date().isoformat(),
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
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    rows = build_rows(limit=args.limit)
    payload = build_payload(rows, updated_at=datetime.now(TAIPEI_TZ))
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
