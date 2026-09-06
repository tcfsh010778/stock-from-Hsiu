"""Build a score-free, two-route stock review queue.

This module compares durable SFZ and MDA route state.  It does not change
strategy decisions and deliberately ignores noisy daily price/score changes.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "1.0.0"
NOISY_METRIC_TOKENS = ("price", "close", "open", "high", "low", "score", "rank", "return", "pct")
CHIP_TOKENS = ("chip", "holder", "foreign", "trust", "dealer", "retail", "籌碼", "大戶", "散戶", "外資", "投信")
EVENT_PRIORITY = {
    "stage_invalidated": 1,
    "sfz_breakout": 2,
    "sfz_retest": 3,
    "first_qualified": 4,
    "mda_chip_changed": 5,
}


def _data_date(payload: Mapping[str, Any]) -> str:
    return str(payload.get("data_date") or payload.get("date") or "")


def _rule_version(payload: Mapping[str, Any]) -> str:
    return str(payload.get("rule_version") or payload.get("version") or payload.get("schema_version") or "")


def _source_quality(payload: Mapping[str, Any]) -> dict[str, Any]:
    quality = payload.get("quality") or payload.get("data_quality") or {}
    state = str(
        (quality.get("state") if isinstance(quality, Mapping) else quality)
        or payload.get("status") or "unknown"
    ).lower()
    freshness = payload.get("freshness") or {}
    freshness_state = str(freshness.get("status") if isinstance(freshness, Mapping) else "").lower()
    price_verified = payload.get("price_verified")
    if price_verified is None and isinstance(payload.get("price_basis"), Mapping):
        price_verified = payload["price_basis"].get("verified")
    blocked = state in {"missing", "blocked", "error", "failed", "stale", "warning"}
    blocked = blocked or freshness_state in {"stale", "fallback_stale", "missing", "schema_error"}
    blocked = blocked or price_verified is False
    return {
        "state": state,
        "freshness": freshness_state or None,
        "price_verified": price_verified,
        "alert_eligible": not blocked,
    }


def _rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("stocks", "rows", "signals", "candidates", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
    return []


def _clean_metrics(metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value for key, value in sorted(metrics.items())
        if not any(token in str(key).lower() for token in NOISY_METRIC_TOKENS)
        and isinstance(value, (str, int, float, bool, type(None)))
    }


def _evidence(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = row.get("evidence") or []
    output: list[dict[str, Any]] = []
    if isinstance(raw, list):
        for index, item in enumerate(raw):
            if not isinstance(item, Mapping):
                continue
            output.append({
                "id": str(item.get("id") or item.get("indicator_id") or f"evidence_{index + 1}"),
                "summary": str(item.get("summary") or item.get("reason") or ""),
                "metrics": _clean_metrics(item.get("metrics") or {}),
                "data_date": str(item.get("data_date") or item.get("date") or ""),
            })
    if not output:
        metrics = _clean_metrics(row.get("metrics") or {})
        output.append({
            "id": "route_state",
            "summary": str(row.get("reason") or row.get("summary") or ""),
            "metrics": metrics,
            "data_date": str(row.get("data_date") or row.get("date") or ""),
        })
    return output


def _candidate(row: Mapping[str, Any]) -> bool:
    for key in ("candidate", "qualified", "is_candidate", "signal"):
        if key in row:
            return bool(row.get(key))
    status = str(row.get("status") or row.get("stage") or "").lower()
    return status not in {"", "invalidated", "rejected", "blocked", "missing"}


def _normalize_route(route_id: str, payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    quality = _source_quality(payload)
    data_date = _data_date(payload)
    output: dict[str, dict[str, Any]] = {}
    for row in _rows(payload):
        stock_id = str(row.get("stock_id") or row.get("id") or row.get("code") or "").strip()
        if not stock_id:
            continue
        stage = str(row.get("stage") or row.get("basket") or row.get("basket_type") or row.get("status") or "")
        reasons = row.get("reasons")
        if not isinstance(reasons, list):
            reasons = [row.get("reason") or row.get("summary")] if row.get("reason") or row.get("summary") else []
        output[stock_id] = {
            "route_id": route_id,
            "candidate": _candidate(row),
            "stage": stage,
            "reasons": [str(reason) for reason in reasons if reason],
            "evidence": _evidence(row),
            "missing": list(row.get("missing") or []),
            "next_observation": row.get("next_observation"),
            "data_date": str(row.get("data_date") or data_date),
            "quality": quality,
        }
    return output


def _chip_signature(route: Mapping[str, Any]) -> tuple[Any, ...]:
    values = []
    for evidence in route.get("evidence") or []:
        if not str(evidence.get("data_date") or ""):
            continue
        evidence_id = str(evidence.get("id") or "").lower()
        summary = str(evidence.get("summary") or "").lower()
        for key, value in sorted((evidence.get("metrics") or {}).items()):
            token = f"{evidence_id} {summary} {key}".lower()
            if any(chip in token for chip in CHIP_TOKENS) and not any(noisy in str(key).lower() for noisy in NOISY_METRIC_TOKENS):
                values.append((evidence_id, str(key), value))
    return tuple(values)


def _event(route_id: str, current: Mapping[str, Any], previous: Mapping[str, Any]) -> str | None:
    if not previous.get("candidate") and current.get("candidate"):
        return "first_qualified"
    if route_id == "sfz":
        stage = str(current.get("stage") or "").lower()
        old_stage = str(previous.get("stage") or "").lower()
        if previous.get("candidate") and (not current.get("candidate") or stage == "invalidated"):
            return "stage_invalidated"
        if stage != old_stage and stage in {"breakout", "retest"}:
            return f"sfz_{stage}"
    if route_id == "mda" and _chip_signature(current) != _chip_signature(previous):
        if _chip_signature(current) or _chip_signature(previous):
            return "mda_chip_changed"
    return None


def build_review_queue(
    sfz_payload: Mapping[str, Any],
    mda_payload: Mapping[str, Any],
    previous: Mapping[str, Any] | None = None,
    *,
    as_of: str,
) -> dict[str, Any]:
    """Return a union queue and meaningful route events without total scores."""
    if previous and str(previous.get("as_of") or "") > as_of:
        raise ValueError("previous review queue date is later than as_of")
    current_routes = {"sfz": _normalize_route("sfz", sfz_payload), "mda": _normalize_route("mda", mda_payload)}
    previous_state = deepcopy((previous or {}).get("route_state") or {})
    previous_versions = (previous or {}).get("rule_versions") or {}
    versions = {"sfz": _rule_version(sfz_payload), "mda": _rule_version(mda_payload)}
    baseline_only = not previous or str(previous.get("as_of") or "") == as_of
    alerts: list[dict[str, Any]] = []
    route_state: dict[str, dict[str, Any]] = {}
    stock_ids = set(previous_state)
    stock_ids.update(current_routes["sfz"])
    stock_ids.update(current_routes["mda"])

    for stock_id in sorted(stock_ids):
        route_state[stock_id] = {}
        for route_id in ("sfz", "mda"):
            current = current_routes[route_id].get(stock_id)
            old = (previous_state.get(stock_id) or {}).get(route_id)
            source_quality = _source_quality(sfz_payload if route_id == "sfz" else mda_payload)
            if not source_quality["alert_eligible"]:
                if old:
                    current = deepcopy(old)
                    current["quality"] = source_quality
                    current["missing"] = sorted(set(current.get("missing") or []) | {"current_source_unavailable"})
                elif current:
                    current["quality"] = source_quality
            if not current:
                continue
            route_state[stock_id][route_id] = current
            rule_changed = bool(previous and previous_versions.get(route_id) != versions[route_id])
            old_was_eligible = bool((old or {}).get("quality", {}).get("alert_eligible"))
            if baseline_only or rule_changed or not source_quality["alert_eligible"] or not old_was_eligible:
                continue
            event_type = _event(route_id, current, old or {"candidate": False, "stage": "", "evidence": []})
            if event_type:
                alerts.append({
                    "stock_id": stock_id,
                    "route_id": route_id,
                    "event_type": event_type,
                    "priority": EVENT_PRIORITY[event_type],
                })

    cards = []
    for stock_id in sorted(route_state):
        routes = route_state[stock_id]
        route_ids = sorted(routes)
        if not route_ids:
            continue
        candidates = {route_id: bool(routes[route_id].get("candidate")) for route_id in route_ids}
        conflicts = []
        if len(set(candidates.values())) > 1:
            conflicts.append("route_candidate_disagreement")
        cards.append({
            "stock_id": stock_id,
            "candidate": any(candidates.values()),
            "status": "review" if any(candidates.values()) else (
                "blocked" if not any(route.get("quality", {}).get("alert_eligible") for route in routes.values()) else "observe"
            ),
            "stage": {route_id: routes[route_id].get("stage") for route_id in route_ids},
            "route_ids": route_ids,
            "routes": [routes[route_id] for route_id in route_ids],
            "evidence": [dict(item, route_id=route_id) for route_id in route_ids for item in routes[route_id].get("evidence") or []],
            "conflicts": conflicts,
            "missing": sorted({str(item) for route in routes.values() for item in route.get("missing") or []}),
            "next_observation": {route_id: routes[route_id].get("next_observation") for route_id in route_ids},
            "data_date": {route_id: routes[route_id].get("data_date") for route_id in route_ids},
            "reasons": {route_id: list(routes[route_id].get("reasons") or []) for route_id in route_ids},
        })
    return {
        "dataset_id": "review_queue",
        "schema_version": SCHEMA_VERSION,
        "as_of": as_of,
        "data_date": {"sfz": _data_date(sfz_payload), "mda": _data_date(mda_payload)},
        "quality": {"sfz": _source_quality(sfz_payload), "mda": _source_quality(mda_payload)},
        "status": "ready" if any(_source_quality(item)["alert_eligible"] for item in (sfz_payload, mda_payload)) else "blocked",
        "rule_versions": versions,
        "stocks": cards,
        "alerts": sorted(alerts, key=lambda item: (item["priority"], item["stock_id"], item["route_id"])),
        "route_state": route_state,
    }
