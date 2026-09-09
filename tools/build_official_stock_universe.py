"""Build a dated TWSE+TPEx ordinary-company universe for a TDCC week.

The company-basic-data datasets contain operating companies and an explicit
listing date.  This avoids treating every four-digit quote symbol as stock and
allows a current roster to be projected back to an earlier TDCC snapshot date.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests


SOURCES = {
    "listed": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "otc": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
}
MINIMUM_COUNTS = {"listed": 800, "otc": 600}


class UniverseSourceError(RuntimeError):
    pass


def _field(row: dict[str, Any], *names: str) -> Any:
    normalized = {str(k).lstrip("\ufeff").strip(): v for k, v in row.items()}
    for name in names:
        if name in normalized:
            return normalized[name]
    return None


def is_known_tdr_id(security_id: str) -> bool:
    """MOPS company rosters include numeric 91-prefix TDR codes."""
    return security_id.isdigit() and len(security_id) in {4, 6} and security_id.startswith("91")


def parse_listing_date(value: Any) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "")
    if len(text) == 7 and text.isdigit():  # ROC calendar, if supplied by an official export.
        text = f"{int(text[:3]) + 1911:04d}{text[3:]}"
    if len(text) != 8 or not text.isdigit():
        raise ValueError("official roster listing date is invalid")
    return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}").isoformat()


def decode_rows(content: bytes, market: str) -> list[dict[str, str]]:
    try:
        payload = json.loads(content.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{market} official roster is not valid UTF-8 JSON") from exc
    if not isinstance(payload, list):
        raise ValueError(f"{market} official roster is not a JSON array")
    output: list[dict[str, str]] = []
    seen: set[str] = set()
    report_dates: set[str] = set()
    date_names = ("上市日期",) if market == "listed" else ("上櫃日期", "上市日期")
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError(f"{market} official roster row is not an object")
        sid = str(_field(raw, "公司代號", "SecuritiesCompanyCode") or "").strip()
        name = str(_field(raw, "公司簡稱", "CompanyAbbreviation", "公司名稱", "CompanyName") or "").strip()
        if is_known_tdr_id(sid):
            continue
        if len(sid) != 4 or not sid.isdigit() or sid.startswith("0") or not name:
            safe_keys = sorted(str(key).lstrip("\ufeff") for key in raw)
            raise ValueError(f"{market} official roster identity is invalid: security_id={sid!r} "
                             f"name={name!r} available_keys={safe_keys}")
        if sid in seen:
            raise ValueError(f"{market} official roster has duplicate security {sid}")
        seen.add(sid)
        report_dates.add(parse_listing_date(_field(raw, "出表日期", "Date")))
        output.append({"security_id": sid, "name": name, "market": market,
                       "listing_date": parse_listing_date(_field(raw, *date_names, "DateOfListing"))})
    if len(report_dates) != 1:
        raise ValueError(f"{market} official roster does not have one report date")
    report_date = next(iter(report_dates))
    for row in output:
        row["roster_as_of"] = report_date
    if len(output) < MINIMUM_COUNTS[market]:
        raise ValueError(f"{market} official roster coverage is too small: {len(output)}")
    return output


def fetch_bytes(url: str, *, attempts: int = 3, timeout: float = 30,
                session: requests.Session | None = None) -> bytes:
    client = session or requests.Session()
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            response = client.get(url, headers={"User-Agent": "stock-source-universe/1.0"}, timeout=timeout)
            if response.status_code in {402, 403, 428, 429}:
                raise UniverseSourceError(f"official roster access/rate response HTTP {response.status_code}")
            response.raise_for_status()
            if not response.content:
                raise UniverseSourceError("official roster response is empty")
            return response.content
        except UniverseSourceError:
            raise
        except requests.RequestException as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(1.0 * (attempt + 1))
    raise UniverseSourceError(f"official roster transport failed after {attempts} attempts: {type(last).__name__}")


def build_universe(raw_by_market: dict[str, bytes], *, universe_as_of: str | dict[str, str],
                   tdcc_date: str, retrieved_at: str) -> dict[str, Any]:
    if isinstance(universe_as_of, dict):
        source_dates = {market: date.fromisoformat(universe_as_of[market]).isoformat()
                        for market in ("listed", "otc")}
    else:
        shared_date = date.fromisoformat(universe_as_of).isoformat()
        source_dates = {market: shared_date for market in ("listed", "otc")}
    as_of = min(source_dates.values())
    target = date.fromisoformat(tdcc_date).isoformat()
    if target > as_of:
        raise ValueError("TDCC target date cannot be newer than universe as-of")
    retrieved = datetime.fromisoformat(retrieved_at.replace("Z", "+00:00"))
    if retrieved.tzinfo is None:
        raise ValueError("retrieved-at must include a timezone")
    all_rows: list[dict[str, str]] = []
    hashes: dict[str, str] = {}
    raw_counts: dict[str, int] = {}
    excluded: dict[str, list[str]] = {}
    excluded_tdr: dict[str, list[str]] = {}
    for market in ("listed", "otc"):
        content = raw_by_market.get(market)
        if not isinstance(content, bytes):
            raise ValueError(f"missing {market} official roster bytes")
        raw_payload = json.loads(content.decode("utf-8-sig"))
        excluded_tdr[market] = sorted(
            str(_field(row, "公司代號", "SecuritiesCompanyCode") or "").strip()
            for row in raw_payload if isinstance(row, dict)
            and is_known_tdr_id(str(_field(row, "公司代號", "SecuritiesCompanyCode") or "").strip()))
        decoded = decode_rows(content, market)
        if {row["roster_as_of"] for row in decoded} != {source_dates[market]}:
            raise ValueError(f"{market} official roster report date differs from universe as-of")
        hashes[market] = hashlib.sha256(content).hexdigest()
        raw_counts[market] = len(decoded)
        excluded[market] = sorted(row["security_id"] for row in decoded if row["listing_date"] > target)
        all_rows.extend({k: v for k, v in row.items() if k != "roster_as_of"}
                        for row in decoded if row["listing_date"] <= target)
    ids = [row["security_id"] for row in all_rows]
    if len(ids) != len(set(ids)):
        raise ValueError("security appears in both TWSE and TPEx official rosters")
    all_rows.sort(key=lambda row: row["security_id"])
    counts = {market: sum(row["market"] == market for row in all_rows) for market in ("listed", "otc")}
    if any(counts[m] < MINIMUM_COUNTS[m] for m in counts):
        raise ValueError(f"effective official universe coverage is too small: {counts}")
    return {
        "schema_version": 1,
        "dataset_id": "official_twse_tpex_ordinary_stock_universe",
        "status": "ok",
        "quality": "complete_for_current_rosters_as_of_target",
        "universe_as_of": as_of,
        "roster_as_of_by_market": source_dates,
        "effective_for_tdcc_date": target,
        "retrieved_at": retrieved.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "sources": {market: {"url": SOURCES[market], "raw_sha256": hashes[market],
                             "raw_row_count": raw_counts[market] + len(excluded_tdr[market]),
                             "roster_as_of": source_dates[market],
                             "excluded_tdr_security_ids": excluded_tdr[market]}
                    for market in ("listed", "otc")},
        "row_count": len(all_rows),
        "market_counts": counts,
        "excluded_not_yet_listed_security_ids": excluded,
        "excluded_tdr_security_ids": excluded_tdr,
        "rows": all_rows,
    }


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run(*, output: Path, universe_as_of: str, tdcc_date: str,
        fetcher: Callable[[str], bytes] = fetch_bytes,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    raw = {market: fetcher(url) for market, url in SOURCES.items()}
    retrieved = now().astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    payload = build_universe(raw, universe_as_of=universe_as_of, tdcc_date=tdcc_date, retrieved_at=retrieved)
    atomic_json(output, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--universe-as-of", required=True, help="official roster observation date (YYYY-MM-DD)")
    parser.add_argument("--tdcc-date", required=True, help="TDCC weekly snapshot date to project the roster to")
    args = parser.parse_args()
    payload = run(output=args.output, universe_as_of=args.universe_as_of, tdcc_date=args.tdcc_date)
    print(f"[official-universe] effective={payload['effective_for_tdcc_date']} rows={payload['row_count']} "
          f"listed={payload['market_counts']['listed']} otc={payload['market_counts']['otc']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
