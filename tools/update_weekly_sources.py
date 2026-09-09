"""Atomically prepare all verified weekly holder-source artifacts."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from tools.build_official_stock_universe import SOURCES, atomic_json, build_universe, decode_rows, fetch_bytes
from tools.update_weekly_mda_pool import ARCHIVE_ID, normalize_latest, update

TAIPEI = timezone(timedelta(hours=8))


def load_json(path: Path, *, required: bool) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise ValueError(f"required weekly source artifact is missing: {path.name}")
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"weekly source artifact is invalid: {path.name}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"weekly source artifact is not an object: {path.name}")
    return payload


def load_public_module(root: Path) -> ModuleType:
    path = root / "tdcc_holder_snapshot.py"
    if not path.is_file():
        raise ValueError("official-root does not contain tdcc_holder_snapshot.py")
    spec = importlib.util.spec_from_file_location("weekly_public_tdcc_holder_snapshot", path)
    if not spec or not spec.loader:
        raise ValueError("cannot load public TDCC helper")
    sys.path.insert(0, str(root))
    try:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    for name in ("fetch_rows", "aggregate_snapshot", "merge_archive"):
        if not callable(getattr(module, name, None)):
            raise ValueError(f"public TDCC helper lacks {name}")
    return module


def infer_roster_date(raw_rosters: dict[str, bytes]) -> tuple[str, dict[str, list[dict[str, str]]]]:
    decoded = {market: decode_rows(raw_rosters[market], market) for market in ("listed", "otc")}
    for market, rows in decoded.items():
        raw_rows = json.loads(raw_rosters[market].decode("utf-8-sig"))
        short_names = {str(raw.get("公司代號") or "").strip():
                       str(raw.get("公司簡稱") or raw.get("公司名稱") or "").strip() for raw in raw_rows}
        for row in rows:
            if short_names.get(row["security_id"]):
                row["name"] = short_names[row["security_id"]]
    dates = {row["roster_as_of"] for rows in decoded.values() for row in rows}
    if len(dates) != 1:
        raise ValueError("TWSE and TPEx official roster report dates differ")
    return next(iter(dates)), decoded


def tdcc_raw_date(raw_rows: list[dict[str, Any]], *, as_of: str) -> str:
    # normalize_latest performs the full 1..17 and numeric validation later.
    dates: set[str] = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            raise ValueError("TDCC raw row is not an object")
        value = next((v for k, v in row.items() if str(k).lstrip("\ufeff") == "資料日期"), None)
        text = str(value or "").strip().replace("-", "")
        if len(text) != 8 or not text.isdigit():
            raise ValueError("TDCC raw date is invalid")
        dates.add(date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}").isoformat())
    if len(dates) != 1:
        raise ValueError("TDCC raw response does not have one distinct date")
    result = next(iter(dates))
    if result > as_of:
        raise ValueError("TDCC raw date is newer than as-of")
    return result


def full_market_map(decoded: dict[str, list[dict[str, str]]], *, roster_date: str,
                    retrieved_at: str, universe_sources: dict[str, Any]) -> dict[str, Any]:
    rows = [row for market in ("listed", "otc") for row in decoded[market]]
    ids = [row["security_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("full current roster has cross-market duplicates")
    labels = {"listed": "上市", "otc": "上櫃"}
    stocks = {row["security_id"]: {"name": row["name"], "market": labels[row["market"]],
                                    "listing_date": row["listing_date"]} for row in rows}
    markets = {sid: item["market"] for sid, item in stocks.items()}
    counts = {market: len(decoded[market]) for market in ("listed", "otc")}
    return {"schema_version": 1, "dataset_id": "official_stock_markets", "status": "ok",
            "updated_at": retrieved_at, "universe_as_of": roster_date, "market_counts": counts,
            "sources": universe_sources, "markets": dict(sorted(markets.items())),
            "stocks": dict(sorted(stocks.items()))}


def validate_legacy_matches(legacy_snapshot: dict[str, Any], compact_snapshot: dict[str, Any]) -> None:
    legacy = {str(row.get("security_id")): row for row in legacy_snapshot.get("rows") or []}
    compact = {str(row.get("security_id")): row for row in compact_snapshot.get("rows") or []}
    if legacy_snapshot.get("date") != compact_snapshot.get("date") or set(legacy) != set(compact):
        raise ValueError("legacy holder aggregate coverage differs from validated compact snapshot")
    for sid, expected in compact.items():
        actual = legacy[sid]
        if (actual.get("market") != expected.get("market")
                or abs(float(actual.get("major_percent")) - float(expected.get("major_percent"))) > 0.001
                or int(actual.get("major_people")) != int(expected.get("major_people"))):
            raise ValueError(f"legacy holder aggregate differs for {sid}")


def prepare(*, data_dir: Path, official_root: Path, as_of: str,
            tdcc_fetch: Callable[[], list[dict[str, Any]]] | None = None,
            roster_fetch: Callable[[str], bytes] = fetch_bytes,
            public_module: ModuleType | Any | None = None,
            now: Callable[[], datetime] = lambda: datetime.now(TAIPEI)) -> dict[str, dict[str, Any]]:
    cutoff = date.fromisoformat(as_of).isoformat()
    module = public_module or load_public_module(official_root)
    raw_tdcc = (tdcc_fetch or module.fetch_rows)()
    if not isinstance(raw_tdcc, list) or not raw_tdcc:
        raise ValueError("TDCC provider returned no raw rows")
    holder_date = tdcc_raw_date(raw_tdcc, as_of=cutoff)
    raw_rosters = {market: roster_fetch(url) for market, url in SOURCES.items()}
    roster_date, decoded = infer_roster_date(raw_rosters)
    roster_age = (date.fromisoformat(cutoff) - date.fromisoformat(roster_date)).days
    holder_age = (date.fromisoformat(cutoff) - date.fromisoformat(holder_date)).days
    if not 0 <= roster_age <= 7:
        raise ValueError("official roster report date is not fresh within seven days")
    if not 0 <= holder_age <= 7:
        raise ValueError("TDCC weekly date is not fresh within seven days")
    if roster_date < holder_date:
        raise ValueError("official roster predates TDCC weekly snapshot")
    observed = now().astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    universe = build_universe(raw_rosters, universe_as_of=roster_date, tdcc_date=holder_date,
                              retrieved_at=observed)
    current_names = {row["security_id"]: row["name"] for rows in decoded.values() for row in rows}
    for row in universe["rows"]:
        row["name"] = current_names[row["security_id"]]
    effective_rows = universe["rows"]
    # Validate full TDCC distributions before calling the legacy partial-level aggregator.
    compact_check = normalize_latest(raw_tdcc, effective_rows, as_of=cutoff)
    compact_path = data_dir / "tdcc_compact_weekly_snapshots.json"
    compact_existing = load_json(compact_path, required=True)
    compact_archive, pool = update(effective_rows, compact_existing, as_of=cutoff,
                                   universe_as_of=roster_date,
                                   universe_source_sha256=hashlib.sha256(
                                       (json.dumps(universe, ensure_ascii=False, sort_keys=True,
                                                   separators=(",", ":")) + "\n").encode()).hexdigest(),
                                   fetch_rows=lambda: raw_tdcc)
    latest_compact = compact_archive["snapshots"][-1]
    if latest_compact["date"] != compact_check["date"] or set(r["security_id"] for r in latest_compact["rows"]) != set(r["security_id"] for r in compact_check["rows"]):
        raise ValueError("compact archive latest snapshot identity mismatch")
    security_map = {row["security_id"]: {"name": row["name"], "market": row["market"]}
                    for row in effective_rows}
    legacy_snapshot = module.aggregate_snapshot(raw_tdcc, security_map)
    validate_legacy_matches(legacy_snapshot, compact_check)
    legacy_existing = load_json(data_dir / "holder_weekly_snapshots.json", required=False)
    legacy_archive = module.merge_archive(legacy_snapshot, legacy_existing)
    if legacy_archive.get("latest_date") != holder_date:
        raise ValueError("legacy holder archive latest date mismatch")
    market_map = full_market_map(decoded, roster_date=roster_date, retrieved_at=observed,
                                 universe_sources=universe["sources"])
    return {"official_stock_universe.json": universe,
            "tdcc_compact_weekly_snapshots.json": compact_archive,
            "mda_weekly_top50.json": pool,
            "holder_weekly_snapshots.json": legacy_archive,
            "stock_markets.json": market_map}


def run(**kwargs: Any) -> dict[str, dict[str, Any]]:
    data_dir = Path(kwargs["data_dir"])
    outputs = prepare(**kwargs)
    # No destination is touched until every source and cross-artifact check above passes.
    for name, payload in outputs.items():
        atomic_json(data_dir / name, payload)
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--official-root", type=Path, required=True)
    parser.add_argument("--as-of", default=datetime.now(TAIPEI).date().isoformat())
    args = parser.parse_args()
    outputs = run(data_dir=args.data_dir, official_root=args.official_root, as_of=args.as_of)
    pool = outputs["mda_weekly_top50.json"]
    print(f"[weekly-sources] tdcc={pool['data_date']} top={len(pool['rows'])} "
          f"universe={outputs['official_stock_universe.json']['row_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
