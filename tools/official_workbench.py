"""Pure bridge from validated official aggregates to public workbench evidence."""
from __future__ import annotations

from copy import deepcopy
from datetime import date
import math
from typing import Any, Mapping

from tools.update_weekly_mda_pool import ARCHIVE_ID, validate_compact


SERIES = ("institutional", "foreign_ownership", "margin", "holdings")
HISTORY_LIMIT = 260


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


def _validated_snapshot(details: Mapping[str, Any], expected_date: str) -> dict[str, Any]:
    day = _day(details.get("date"))
    if day != expected_date or details.get("institutional_unit") != "shares" or details.get("margin_unit") != "official_report_balance":
        raise ValueError("official workbench snapshot date or units are invalid")
    institutional, margin = _details({"workbench_details": details}, "institutional"), _details({"workbench_details": details}, "margin")
    return {"date": day, "institutional_unit": "shares", "margin_unit": "official_report_balance",
            "institutional": list(institutional.values()), "margin": list(margin.values())}


def _margin_snapshot(details: Mapping[str, Any], expected_date: str) -> dict[str, Any]:
    validated = _validated_snapshot(details, expected_date)
    rows = sorted(validated["margin"], key=lambda row: str(row["security_id"]))
    text = "\n".join(f"{row['security_id']},{'l' if row['market']=='listed' else 'o'},{_finite(row['margin_balance'])},{_finite(row['short_balance'])}" for row in rows)
    return {"date": expected_date, "margin_unit": "official_report_balance", "row_count": len(rows), "rows_csv": text}


def _margin_snapshot_rows(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if snapshot.get("margin_unit") != "official_report_balance":
        raise ValueError("official margin history unit is invalid")
    result = {}
    for line in str(snapshot.get("rows_csv") or "").splitlines():
        values = line.split(",")
        if len(values) != 4:
            raise ValueError("official margin history row format is invalid")
        sid, market_code, balance, short = values
        market = "listed" if market_code == "l" else "otc" if market_code == "o" else ""
        if not sid or sid in result or not market:
            raise ValueError("official margin history identities are invalid or duplicated")
        result[sid] = {"security_id": sid, "market": market, "margin_balance": _finite(balance), "short_balance": _finite(short)}
    if int(snapshot.get("row_count", -1)) != len(result) or {row["market"] for row in result.values()} != {"listed", "otc"}:
        raise ValueError("official margin history coverage is invalid")
    return result


def accumulate_workbench_history(current_flow: dict, previous_flow: dict) -> dict:
    """Persist validated official detail snapshots without importing legacy CSVs."""
    current = deepcopy(current_flow)
    if current.get("dataset_id") != "daily_market_flow" or (current.get("data_quality") or {}).get("state") != "ok":
        raise ValueError("current daily market flow is not verified")
    if previous_flow and previous_flow.get("dataset_id") != "daily_market_flow":
        raise ValueError("previous daily market flow schema is invalid")
    current_date = _day(current.get("date"))
    snapshots: list[Mapping[str, Any]] = []
    old_history = previous_flow.get("workbench_history") if isinstance(previous_flow, Mapping) else None
    if "workbench_history" in previous_flow and not isinstance(old_history, list):
        raise ValueError("previous official workbench history is not a list")
    if isinstance(old_history, list):
        if any(not isinstance(item, Mapping) for item in old_history):
            raise ValueError("previous official workbench history contains a non-object item")
        snapshots.extend(old_history)
    elif isinstance(previous_flow, Mapping) and previous_flow.get("workbench_details") and (previous_flow.get("data_quality") or {}).get("state") == "ok":
        previous_date = _day(previous_flow.get("date"))
        snapshots.append(_margin_snapshot(previous_flow["workbench_details"], previous_date))
    by_date: dict[str, dict[str, Any]] = {}
    for snapshot in snapshots:
        day = _day(snapshot.get("date"))
        if day > current_date:
            raise ValueError("official workbench history contains a future snapshot")
        if day in by_date:
            raise ValueError("official workbench history contains duplicate dates")
        _margin_snapshot_rows(snapshot)
        by_date[day] = dict(snapshot)
    by_date[current_date] = _margin_snapshot(current.get("workbench_details") or {}, current_date)
    current["workbench_history"] = [by_date[key] for key in sorted(by_date)[-HISTORY_LIMIT:]]
    return current


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
        detail_history = flow.get("workbench_history")
        snapshots = detail_history if isinstance(detail_history, list) else [_margin_snapshot(details, flow_date)]
        seen_detail_dates = set()
        for snapshot in snapshots:
            snapshot_date = _day(snapshot.get("date"))
            if snapshot_date > flow_date:
                raise ValueError("official workbench history contains a date later than flow date")
            if snapshot_date in seen_detail_dates:
                raise ValueError("official workbench detail dates are duplicated")
            seen_detail_dates.add(snapshot_date)
            margin_rows = _margin_snapshot_rows(snapshot)
            if snapshot_date <= cutoff:
                for sid, margin in margin_rows.items():
                    prepared["margin"].setdefault(sid, []).append({"date": snapshot_date,
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
        additions = list((prepared.get(key) or {}).get(sid) or [])
        old_rows = output[key]
        if key == "margin" and additions:
            old_rows = [row for row in old_rows if row.get("unit") == "official_report_balance"]
        output[key] = _merge(old_rows, additions, cutoff)

    for key in SERIES:
        output[key] = _merge(output[key], [], cutoff)
    output["source_dates"] = {key: output[key][-1]["date"] for key in SERIES if output[key]}
    output["gaps"] = [key for key in SERIES if not output[key]]
    output["units"] = dict(prepared.get("units") or {})
    if output["margin"] and any(row.get("unit") != "official_report_balance" for row in output["margin"]):
        output["units"]["margin"] = "unknown_legacy"
    output["unit_notes"] = {
        "institutional": "淨股數除以 1,000，顯示單位為張",
        "margin": "官方公告餘額（原單位）；未證明同口徑的舊列不串接",
        "holdings": "TDCC 400 張以上持股比例，單位為百分點",
    }
    status = {}
    for key in SERIES:
        source_day = output["source_dates"].get(key)
        if not source_day:
            status[key] = {"state": "missing", "date": None}
            continue
        age = (date.fromisoformat(cutoff) - date.fromisoformat(source_day)).days
        fresh = 0 <= age <= (7 if key == "holdings" else 0)
        status[key] = {"state": "current" if fresh else "stale", "date": source_day, "age_days": age,
                       "freshness_window_days": 7 if key == "holdings" else 0}
    output["source_status"] = status
    return output
