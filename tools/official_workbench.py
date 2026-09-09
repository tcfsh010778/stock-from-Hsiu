"""Pure bridge from validated official aggregates to public workbench evidence."""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import math
from typing import Any, Mapping

from tools.update_weekly_mda_pool import ARCHIVE_ID, validate_compact


SERIES = ("institutional", "foreign_ownership", "margin", "holdings")


def _day(value: Any) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid source date: {text}") from exc


def _finite(value: Any) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("official workbench value is not finite")
    return number


def _merge(existing: list[dict], additions: list[dict], as_of: str) -> list[dict]:
    by_date: dict[str, dict] = {}
    for row in [*(existing or []), *additions]:
        if not isinstance(row, Mapping):
            continue
        day = _day(row.get("date"))
        if day <= as_of:
            by_date[day] = dict(row, date=day)
    return [by_date[key] for key in sorted(by_date)]


def _compact_rows(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get("rows")
    if isinstance(rows, list):
        parsed = [dict(row) for row in rows if isinstance(row, Mapping)]
    else:
        parsed = []
        for line in str(snapshot.get("rows_csv") or "").splitlines():
            values = line.split(",")
            if len(values) != 4:
                raise ValueError("institutional compact row format is invalid")
            sid, market, foreign, trust = values
            parsed.append({"security_id": sid, "market": "listed" if market == "l" else "otc" if market == "o" else market,
                           "foreign_net": _finite(foreign), "investment_trust_net": _finite(trust)})
    result: dict[str, dict[str, Any]] = {}
    for row in parsed:
        sid = str(row.get("security_id") or "")
        if not sid or sid in result or row.get("market") not in {"listed", "otc"}:
            raise ValueError("institutional compact identities are invalid or duplicated")
        result[sid] = row
    if snapshot.get("row_count") is not None and int(snapshot["row_count"]) != len(result):
        raise ValueError("institutional compact row count is invalid")
    return result


def _details(flow: Mapping[str, Any], key: str) -> dict[str, dict[str, Any]]:
    rows = (flow.get("workbench_details") or {}).get(key)
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"official {key} details are empty")
    result: dict[str, dict[str, Any]] = {}
    markets = set()
    for row in rows:
        sid = str(row.get("security_id") or "") if isinstance(row, Mapping) else ""
        market = row.get("market") if isinstance(row, Mapping) else None
        if not sid or sid in result or market not in {"listed", "otc"}:
            raise ValueError(f"official {key} identities are invalid or duplicated")
        for field, value in row.items():
            if field not in {"security_id", "market"} and value is not None:
                _finite(value)
        result[sid] = dict(row)
        markets.add(market)
    if markets != {"listed", "otc"}:
        raise ValueError(f"official {key} must contain both markets")
    return result


def prepare_official_evidence(as_of: str, flow: dict, weekly_archive: dict) -> dict:
    """Validate shared sources once and index additions by security id."""
    cutoff = _day(as_of)
    prepared = {"as_of": cutoff, "institutional": {}, "margin": {}, "holdings": {},
                "units": {"institutional": "lots", "margin": "official_report_balance", "holdings": "percentage_points"}}

    quality_ok = (flow.get("data_quality") or {}).get("state") == "ok"
    flow_date = _day(flow.get("date")) if flow.get("date") else ""
    details = flow.get("workbench_details") or {}
    details_date = _day(details.get("date")) if details.get("date") else ""
    if quality_ok and flow_date <= cutoff and details_date == flow_date:
        if details.get("institutional_unit") != "shares" or details.get("margin_unit") != "official_report_balance":
            raise ValueError("official workbench units are invalid")
        current_i, current_m = _details(flow, "institutional"), _details(flow, "margin")
        seen_dates = set()
        history = flow.get("institutional_history") or {}
        for snapshot in history.get("snapshots") or []:
            day = _day(snapshot.get("date"))
            if day in seen_dates:
                raise ValueError("institutional history snapshot dates are duplicated")
            seen_dates.add(day)
            rows = _compact_rows(snapshot)
            if {row.get("market") for row in rows.values()} != {"listed", "otc"}:
                raise ValueError("institutional history snapshot must contain both markets")
            if day <= cutoff:
                for sid, row in rows.items():
                    prepared["institutional"].setdefault(sid, []).append(
                        {"date": day, "foreign": _finite(row.get("foreign_net")) / 1000.0,
                         "trust": _finite(row.get("investment_trust_net")) / 1000.0, "dealer": None, "total": None})
        for sid, latest in current_i.items():
            prepared["institutional"].setdefault(sid, []).append(
                {"date": flow_date, "foreign": _finite(latest.get("foreign_net")) / 1000.0,
                              "trust": _finite(latest.get("investment_trust_net")) / 1000.0,
                              "dealer": None if latest.get("dealer_net") is None else _finite(latest["dealer_net"]) / 1000.0,
                              "total": None if latest.get("institutional_total_net") is None else _finite(latest["institutional_total_net"]) / 1000.0})
        for sid, margin in current_m.items():
            prepared["margin"].setdefault(sid, []).append({"date": flow_date,
                "margin_balance": _finite(margin.get("margin_balance")), "short_balance": _finite(margin.get("short_balance")),
                "unit": "official_report_balance"})

    if weekly_archive:
        if weekly_archive.get("dataset_id") != ARCHIVE_ID:
            raise ValueError("weekly archive dataset is invalid")
        seen_dates = set()
        for snapshot in weekly_archive.get("snapshots") or []:
            day = _day(snapshot.get("date"))
            if day in seen_dates:
                raise ValueError("weekly snapshot dates are duplicated")
            seen_dates.add(day)
            rows = validate_compact(snapshot)
            if day <= cutoff:
                for sid, row in rows.items():
                    prepared["holdings"].setdefault(sid, []).append(
                        {"date": day, "major": _finite(row.get("major_percent")), "middle": None,
                         "retail": None, "total_people": None})
    return prepared


def merge_official_evidence(existing: dict, stock_id: str, as_of: str, flow: dict, weekly_archive: dict, *, prepared: dict | None = None) -> dict:
    """Return evidence merged through ``as_of`` without fabricating unavailable fields."""
    cutoff = _day(as_of)
    prepared = prepared if prepared is not None else prepare_official_evidence(cutoff, flow, weekly_archive)
    if prepared.get("as_of") != cutoff:
        raise ValueError("prepared official evidence as_of mismatch")
    output = deepcopy(existing or {})
    for key in SERIES:
        output.setdefault(key, [])
    sid = str(stock_id)
    for key in ("institutional", "margin", "holdings"):
        output[key] = _merge(output[key], list((prepared.get(key) or {}).get(sid) or []), cutoff)

    for key in SERIES:
        output[key] = _merge(output[key], [], cutoff)
    output["source_dates"] = {key: output[key][-1]["date"] for key in SERIES if output[key]}
    output["gaps"] = [key for key in SERIES if not output[key]]
    output["units"] = dict(prepared.get("units") or {})
    return output
