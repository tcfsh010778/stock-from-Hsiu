"""Atomically prepare all verified weekly holder-source artifacts."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any, Callable

from tools.build_official_stock_universe import SOURCES, atomic_json, build_universe, decode_rows, fetch_bytes
from tools.build_official_stock_universe import parse_listing_date
from tools.update_weekly_mda_pool import ARCHIVE_ID, normalize_latest, update

TAIPEI = timezone(timedelta(hours=8))
EVENT_SOURCES = {
    ("listed", "reduction"): "https://www.twse.com.tw/rwd/zh/reducation/TWTAUU",
    ("listed", "par_value"): "https://www.twse.com.tw/rwd/zh/change/TWTB8U",
    ("otc", "reduction"): "https://www.tpex.org.tw/www/zh-tw/bulletin/revivt",
    ("otc", "par_value"): "https://www.tpex.org.tw/www/zh-tw/bulletin/pvChgRslt",
}


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


def infer_roster_dates(raw_rosters: dict[str, bytes]) -> tuple[dict[str, str], dict[str, list[dict[str, str]]]]:
    decoded = {market: decode_rows(raw_rosters[market], market) for market in ("listed", "otc")}
    for market, rows in decoded.items():
        raw_rows = json.loads(raw_rosters[market].decode("utf-8-sig"))
        short_names = {str(raw.get("公司代號") or raw.get("SecuritiesCompanyCode") or "").strip():
                       str(raw.get("公司簡稱") or raw.get("CompanyAbbreviation")
                           or raw.get("公司名稱") or raw.get("CompanyName") or "").strip() for raw in raw_rows}
        for row in rows:
            if short_names.get(row["security_id"]):
                row["name"] = short_names[row["security_id"]]
    source_dates = {market: next(iter({row["roster_as_of"] for row in rows}))
                    for market, rows in decoded.items()}
    return source_dates, decoded


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


def fetch_event_bytes(market: str, kind: str, start: str, end: str) -> bytes:
    import requests
    url = EVENT_SOURCES[(market, kind)]
    if market == "listed":
        params = {"startDate": start.replace("-", ""), "endDate": end.replace("-", ""), "response": "json"}
    else:
        params = {"startDate": start.replace("-", "/"), "endDate": end.replace("-", "/"), "response": "json"}
    response = requests.get(url, params=params, timeout=45, headers={"Accept": "application/json"})
    if response.status_code in {402, 403, 428, 429}:
        raise RuntimeError(f"official suspension evidence access/rate response HTTP {response.status_code}")
    response.raise_for_status()
    return response.content


def event_rows(content: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(content.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("official suspension evidence is not UTF-8 JSON") from exc
    tables = payload.get("tables") if isinstance(payload, dict) else None
    if isinstance(tables, list):
        output = []
        for table in tables:
            fields = table.get("fields") or []
            output.extend(dict(zip(fields, values)) for values in table.get("data") or [] if isinstance(values, list))
        return output
    if isinstance(payload, dict) and isinstance(payload.get("fields"), list):
        return [dict(zip(payload["fields"], values)) for values in payload.get("data") or [] if isinstance(values, list)]
    raise ValueError("official suspension evidence schema is invalid")


def resolve_suspensions(missing_refs: list[dict[str, str]], *, tdcc_date: str, as_of: str,
                        known_at: str, fetcher: Callable[[str, str, str, str], bytes]) -> dict[str, dict[str, Any]]:
    missing = {row["security_id"]: row for row in missing_refs}
    resolved: dict[str, dict[str, Any]] = {}
    markets = sorted({row["market"] for row in missing_refs})
    for market in markets:
        for kind in ("reduction", "par_value"):
            content = fetcher(market, kind, tdcc_date, as_of)
            digest = hashlib.sha256(content).hexdigest()
            for row in event_rows(content):
                sid = str(row.get("股票代號") or row.get("證券代號") or "").strip()
                if sid not in missing or missing[sid]["market"] != market:
                    continue
                resume = parse_listing_date(row.get("恢復買賣日期"))
                detail = str(row.get("詳細資料") or "")
                match = re.search(r"停止買賣日期\s*[:：]\s*</?[^>]*>*\s*(\d{3,4}/\d{2}/\d{2})", detail)
                if not match:
                    # The official cells are HTML; allow tags/entities between label and value.
                    plain = re.sub(r"<[^>]+>", " ", detail).replace("&nbsp", " ").replace(";", " ")
                    match = re.search(r"停止買賣日期\s*[:：]?\s*(\d{3,4}/\d{2}/\d{2})", plain)
                if not match:
                    raise ValueError(f"official suspension evidence lacks stop date for {sid}")
                stop = parse_listing_date(match.group(1))
                if not stop <= tdcc_date < resume:
                    continue
                evidence = {"security_id": sid, "name": missing[sid].get("name", ""), "market": market,
                            "event_type": kind, "stop_date": stop, "resume_date": resume,
                            "reason": str(row.get("減資原因") or ("面額變更" if kind == "par_value" else "")).strip(),
                            "source_url": EVENT_SOURCES[(market, kind)], "raw_sha256": digest,
                            "query_start": tdcc_date, "query_end": as_of, "known_at": known_at}
                if sid in resolved and resolved[sid] != evidence:
                    raise ValueError(f"conflicting official suspension evidence for {sid}")
                resolved[sid] = evidence
    unresolved = sorted(set(missing) - set(resolved))
    if unresolved:
        raise ValueError(f"TDCC missing securities lack official suspension evidence: {unresolved[:20]}")
    return resolved


def full_market_map(decoded: dict[str, list[dict[str, str]]], *, roster_dates: dict[str, str],
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
            "updated_at": retrieved_at, "universe_as_of": min(roster_dates.values()),
            "roster_as_of_by_market": roster_dates, "market_counts": counts,
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
            event_fetch: Callable[[str, str, str, str], bytes] = fetch_event_bytes,
            public_module: ModuleType | Any | None = None,
            now: Callable[[], datetime] = lambda: datetime.now(TAIPEI)) -> dict[str, dict[str, Any]]:
    cutoff = date.fromisoformat(as_of).isoformat()
    module = public_module or load_public_module(official_root)
    raw_tdcc = (tdcc_fetch or module.fetch_rows)()
    if not isinstance(raw_tdcc, list) or not raw_tdcc:
        raise ValueError("TDCC provider returned no raw rows")
    holder_date = tdcc_raw_date(raw_tdcc, as_of=cutoff)
    raw_rosters = {market: roster_fetch(url) for market, url in SOURCES.items()}
    roster_dates, decoded = infer_roster_dates(raw_rosters)
    roster_ages = {market: (date.fromisoformat(cutoff) - date.fromisoformat(source_date)).days
                   for market, source_date in roster_dates.items()}
    holder_age = (date.fromisoformat(cutoff) - date.fromisoformat(holder_date)).days
    if any(not 0 <= age <= 7 for age in roster_ages.values()):
        raise ValueError("an official roster report date is not fresh within seven days")
    if not 0 <= holder_age <= 7:
        raise ValueError("TDCC weekly date is not fresh within seven days")
    if any(source_date < holder_date for source_date in roster_dates.values()):
        raise ValueError("an official roster predates TDCC weekly snapshot")
    observed = now().astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    universe = build_universe(raw_rosters, universe_as_of=roster_dates, tdcc_date=holder_date,
                              retrieved_at=observed)
    current_names = {row["security_id"]: row["name"] for rows in decoded.values() for row in rows}
    for row in universe["rows"]:
        row["name"] = current_names[row["security_id"]]
    effective_rows = universe["rows"]
    raw_ids = {str(next((v for k, v in row.items() if str(k).lstrip("\ufeff") == "證券代號"), "")).strip()
               for row in raw_tdcc}
    missing_refs = [row for row in effective_rows if row["security_id"] not in raw_ids]
    exclusions = resolve_suspensions(missing_refs, tdcc_date=holder_date, as_of=cutoff,
                                     known_at=observed, fetcher=event_fetch) if missing_refs else {}
    # Validate full TDCC distributions before calling the legacy partial-level aggregator.
    compact_check = normalize_latest(raw_tdcc, effective_rows, as_of=cutoff,
                                     documented_exclusions=exclusions)
    universe.update({
        "status": compact_check["status"], "quality": compact_check["quality"],
        "expected_tdcc_count": compact_check["expected_count"],
        "observed_tdcc_count": compact_check["observed_count"],
        "expected_tdcc_market_counts": compact_check["expected_market_counts"],
        "observed_tdcc_market_counts": compact_check["market_counts"],
        "excluded_official_suspensions": compact_check["excluded_official_suspensions"],
    })
    compact_path = data_dir / "tdcc_compact_weekly_snapshots.json"
    compact_existing = load_json(compact_path, required=True)
    compact_archive, pool = update(effective_rows, compact_existing, as_of=cutoff,
                                   universe_as_of=min(roster_dates.values()),
                                   universe_source_sha256=hashlib.sha256(
                                       (json.dumps(universe, ensure_ascii=False, sort_keys=True,
                                                   separators=(",", ":")) + "\n").encode()).hexdigest(),
                                   documented_exclusions=exclusions,
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
    market_map = full_market_map(decoded, roster_dates=roster_dates, retrieved_at=observed,
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
