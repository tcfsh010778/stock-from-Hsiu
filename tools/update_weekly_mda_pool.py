"""Update the verified weekly MDA Top50 from one complete TDCC snapshot."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Callable

from tools.recover_weekly_holder_pool import BANDS, MAJOR_LEVELS, strict_saved_row

ARCHIVE_ID = "tdcc_compact_weekly_snapshots"
SOURCE = "https://openapi.tdcc.com.tw/v1/opendata/1-5"


def field(row: dict[str, Any], name: str) -> Any:
    for key, value in row.items():
        if str(key).lstrip("\ufeff") == name:
            return value
    return None


def iso_day(value: Any) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError("TDCC date is invalid")
    return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}").isoformat()


def number(value: Any) -> float:
    result = float(str(value).replace(",", "").replace("%", "").strip())
    if not math.isfinite(result):
        raise ValueError("TDCC numeric value is non-finite")
    return result


def normalize_latest(raw_rows: list[dict[str, Any]], universe: list[dict[str, str]], *, as_of: str) -> dict[str, Any]:
    refs = {row["security_id"]: row for row in universe}
    grouped: dict[str, dict[int, dict[str, Any]]] = {}
    dates: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, dict):
            raise ValueError("TDCC row is not an object")
        day = iso_day(field(raw, "資料日期")); dates.add(day)
        code = str(field(raw, "證券代號") or "").strip()
        if code not in refs:
            continue
        try:
            level = int(field(raw, "持股分級"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"TDCC holding level is invalid for {code}") from exc
        if level not in range(1, 18) or level in grouped.setdefault(code, {}):
            raise ValueError(f"TDCC duplicate/invalid level for {code}")
        grouped[code][level] = {
            "people": number(field(raw, "人數")),
            "shares": number(field(raw, "股數")),
            "percent": number(field(raw, "占集保庫存數比例%")),
        }
    if len(dates) != 1:
        raise ValueError("TDCC latest response does not have one data date")
    data_date = next(iter(dates))
    if data_date > date.fromisoformat(as_of).isoformat():
        raise ValueError("TDCC latest response is newer than as-of")
    missing = sorted(set(refs) - set(grouped))
    if missing:
        raise ValueError(f"TDCC latest response is missing universe securities: {len(missing)}")
    rows = []
    for code in sorted(refs):
        levels = grouped[code]
        if set(levels) != set(range(1, 18)):
            raise ValueError(f"TDCC incomplete levels for {code}")
        item = {
            **refs[code],
            "levels": {str(level): levels[level] for level in BANDS},
            "adjustment": levels[16], "total": levels[17],
        }
        item["major_percent"] = round(sum(float(item["levels"][str(k)]["percent"]) for k in MAJOR_LEVELS), 2)
        item["major_people"] = sum(int(item["levels"][str(k)]["people"]) for k in MAJOR_LEVELS)
        if not strict_saved_row(item):
            raise ValueError(f"TDCC numeric/level validation failed for {code}")
        rows.append({"security_id": code, "name": item.get("name", ""), "market": item["market"],
                     "major_percent": item["major_percent"], "major_people": item["major_people"]})
    counts = {market: sum(row["market"] == market for row in rows) for market in ("listed", "otc")}
    if not all(counts.values()):
        raise ValueError("TDCC snapshot must cover both markets")
    return {"date": data_date, "row_count": len(rows), "market_counts": counts, "rows": rows}


def validate_compact(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get("rows") or []
    ids = [str(row.get("security_id") or "") for row in rows]
    if not rows or len(ids) != len(set(ids)) or snapshot.get("row_count") != len(rows):
        raise ValueError("compact snapshot row coverage is invalid")
    result = {}
    for row in rows:
        sid = str(row.get("security_id") or "")
        if len(sid) != 4 or not sid.isdigit() or sid.startswith("0") or row.get("market") not in {"listed", "otc"}:
            raise ValueError("compact snapshot security identity is invalid")
        pct, people = number(row.get("major_percent")), number(row.get("major_people"))
        if not 0 <= pct <= 100 or people < 0 or people != int(people):
            raise ValueError("compact snapshot values are invalid")
        result[sid] = row
    counts = {market: sum(row["market"] == market for row in rows) for market in ("listed", "otc")}
    if snapshot.get("market_counts") != counts or not all(counts.values()):
        raise ValueError("compact snapshot market coverage is invalid")
    date.fromisoformat(snapshot["date"])
    return result


def compact_core(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Same-date identity comparison, excluding labels and incidental metadata."""
    rows = validate_compact(snapshot)
    return {sid: (row["market"], float(row["major_percent"]), int(row["major_people"]))
            for sid, row in rows.items()}


def seed_compact_snapshot(recovered: dict[str, Any]) -> dict[str, Any]:
    """Convert one complete strict recovery artifact without retaining levels."""
    if (recovered.get("complete") is not True or recovered.get("row_count") != recovered.get("expected_count")
            or recovered.get("market_counts") != recovered.get("expected_market_counts")):
        raise ValueError("recovered TDCC snapshot is not complete")
    rows = recovered.get("rows") or []
    if len(rows) != recovered["row_count"] or any(not strict_saved_row(row) for row in rows):
        raise ValueError("recovered TDCC rows are invalid")
    compact = {"date": date.fromisoformat(recovered["date"]).isoformat(), "row_count": len(rows),
        "market_counts": recovered["market_counts"], "rows": [
            {"security_id": row["security_id"], "name": row.get("name", ""), "market": row["market"],
             "major_percent": float(row["major_percent"]), "major_people": int(row["major_people"])}
            for row in rows]}
    validate_compact(compact)
    return compact


def top50(current: dict[str, Any], prior: dict[str, Any]) -> dict[str, Any]:
    now, old = validate_compact(current), validate_compact(prior)
    gap = (date.fromisoformat(current["date"]) - date.fromisoformat(prior["date"])).days
    if not 4 <= gap <= 10:
        raise ValueError("TDCC snapshots are not consecutive weeks")
    common = sorted(set(now) & set(old)); new_ids = sorted(set(now) - set(old)); departed = sorted(set(old) - set(now))
    candidates = []
    for sid in common:
        delta = round(float(now[sid]["major_percent"]) - float(old[sid]["major_percent"]), 2)
        if delta > 0:
            candidates.append({"security_id": sid, "name": now[sid].get("name", ""), "market": now[sid]["market"],
                "major_400_percent": float(now[sid]["major_percent"]), "prior_major_400_percent": float(old[sid]["major_percent"]),
                "delta_percentage_points": delta})
    candidates.sort(key=lambda row: (-row["delta_percentage_points"], row["security_id"])); selected = candidates[:50]
    if not selected:
        raise ValueError("no positive weekly increases; refusing an invalid empty Top50 artifact")
    for rank, row in enumerate(selected, 1): row["rank"] = rank
    counts = {market: sum(now[sid]["market"] == market for sid in common) for market in ("listed", "otc")}
    if not all(counts.values()):
        raise ValueError("paired weekly universe must cover both markets")
    return {"schema_version": 1, "dataset_id": "mda_weekly_top50", "quality": "complete", "status": "ok",
        "data_date": current["date"], "previous_date": prior["date"], "source": SOURCE,
        "paired_universe_count": len(common), "paired_market_counts": counts,
        "excluded_new_security_ids": new_ids, "excluded_departed_security_ids": departed,
        "selection_status": "top50" if len(selected) == 50 else "fewer_than_50_positive_increases",
        "positive_increase_count": len(candidates), "rows": selected}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False); handle.write("\n")
        os.replace(name, path)
    finally:
        if os.path.exists(name): os.unlink(name)


def load_reliable_universe(path: Path) -> tuple[list[dict[str, str]], str]:
    """Load an explicitly classified ordinary-stock universe and retain its byte provenance."""
    content = path.read_bytes()
    payload = json.loads(content.decode("utf-8-sig"))
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError("universe must be a list or snapshot with rows")
    result: dict[str, dict[str, str]] = {}
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("universe row is not an object")
        code = str(row.get("security_id") or row.get("stock_id") or "").strip()
        market = str(row.get("market") or "").strip().lower()
        if code in seen:
            raise ValueError(f"universe has duplicate security: {code}")
        seen.add(code)
        if len(code) != 4 or not code.isdigit() or code.startswith("0") or market not in {"listed", "otc"}:
            raise ValueError(f"universe identity/market is invalid: {code}")
        result[code] = {"security_id": code, "name": str(row.get("name") or ""), "market": market}
    if not result or {r["market"] for r in result.values()} != {"listed", "otc"}:
        raise ValueError("universe must contain both listed and otc ordinary stocks")
    return [result[k] for k in sorted(result)], hashlib.sha256(content).hexdigest()


def update(universe: list[dict[str, str]], archive: dict[str, Any], *, as_of: str,
           universe_as_of: str | None = None,
           universe_source_sha256: str | None = None,
           fetch_rows: Callable[[], list[dict[str, Any]]]) -> tuple[dict[str, Any], dict[str, Any]]:
    if archive.get("dataset_id") != ARCHIVE_ID or not isinstance(archive.get("snapshots"), list):
        raise ValueError("compact snapshot archive is missing")
    raw_snapshots = [item for item in archive["snapshots"] if isinstance(item, dict) and item.get("date")]
    raw_dates = [item["date"] for item in raw_snapshots]
    if len(raw_dates) != len(set(raw_dates)):
        raise ValueError("compact snapshot archive has duplicate dates")
    snapshots = {item["date"]: item for item in raw_snapshots}
    for item in snapshots.values(): validate_compact(item)
    current = normalize_latest(fetch_rows(), universe, as_of=as_of)
    universe_day = date.fromisoformat(universe_as_of or as_of).isoformat()
    if not current["date"] <= universe_day <= date.fromisoformat(as_of).isoformat():
        raise ValueError("ordinary-stock universe is stale or from the future")
    current["universe_as_of"] = universe_day
    if universe_source_sha256 is not None:
        if len(universe_source_sha256) != 64 or any(c not in "0123456789abcdef" for c in universe_source_sha256):
            raise ValueError("ordinary-stock universe SHA-256 is invalid")
        current["universe_source_sha256"] = universe_source_sha256
    if snapshots and current["date"] < max(snapshots):
        raise ValueError("TDCC latest response predates compact archive")
    if snapshots:
        latest = snapshots[max(snapshots)]
        for market in ("listed", "otc"):
            if current["market_counts"][market] < math.ceil(latest["market_counts"][market] * 0.99):
                raise ValueError(f"TDCC {market} coverage regressed")
    existing = snapshots.get(current["date"])
    if existing and compact_core(existing) != compact_core(current):
        raise ValueError("same-date TDCC compact snapshot changed")
    snapshots[current["date"]] = current
    dates = sorted(snapshots)
    if len(dates) < 2:
        raise ValueError("two verified weekly snapshots are required")
    prior = snapshots[dates[-2]] if dates[-1] == current["date"] else snapshots[dates[-1]]
    pool = top50(current, prior)
    output_archive = {"schema_version": 1, "dataset_id": ARCHIVE_ID, "source": SOURCE,
                      "latest_date": current["date"], "snapshot_count": len(dates),
                      "snapshots": [snapshots[d] for d in dates[-60:]]}
    return output_archive, pool


def load_fetcher(root: Path) -> Callable[[], list[dict[str, Any]]]:
    path = root / "tdcc_holder_snapshot.py"
    spec = importlib.util.spec_from_file_location("tdcc_holder_snapshot_provider", path)
    if not spec or not spec.loader: raise ValueError("TDCC provider module is unavailable")
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module.fetch_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", type=Path, required=True); parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True); parser.add_argument("--as-of", required=True)
    parser.add_argument("--tdcc-module-root", type=Path, required=True)
    parser.add_argument("--universe-as-of", required=True,
                        help="date of the separately verified listed+OTC ordinary-stock universe")
    args = parser.parse_args()
    archive = json.loads(args.archive.read_text(encoding="utf-8-sig"))
    universe, universe_hash = load_reliable_universe(args.universe)
    updated, pool = update(universe, archive, as_of=args.as_of,
                           universe_as_of=args.universe_as_of, universe_source_sha256=universe_hash,
                           fetch_rows=load_fetcher(args.tdcc_module_root))
    atomic_json(args.archive, updated); atomic_json(args.output, pool)
    print(f"[weekly-mda] date={pool['data_date']} rows={len(pool['rows'])} paired={pool['paired_universe_count']}")
    return 0


if __name__ == "__main__": raise SystemExit(main())
