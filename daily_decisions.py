# -*- coding: utf-8 -*-
"""Build the daily operation-decision contract.

This module intentionally structures existing evidence instead of changing
strategy thresholds.  It combines the MDA candidate pool, CaryBot B1/B2 timing
signals, shared traffic-light rules, and visible freshness metadata into a
small JSON artifact for the next website/notification layer.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from data_contract import DEFAULT_MANIFEST_PATH, prepare_artifact_manifest, update_manifest_file
from stock_rules import evaluate_traffic_light, to_number


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
MDA_CANDIDATES_PATH = DATA_DIR / "mda_candidates.json"
CARYBOT_SIGNALS_PATH = DATA_DIR / "carybot_signals.json"
ATTENTION_DISPOSITION_PATH = DATA_DIR / "attention_disposition.json"
DAILY_DECISIONS_PATH = DATA_DIR / "daily_decisions.json"
TAIPEI_TZ = timezone(timedelta(hours=8))

SCHEMA_VERSION = "1.1.0"
RULE_VERSION = "daily_decisions_v1_1_market_risk"
ACTION_STATES = {
    "WATCH",
    "SETUP",
    "ENTRY_CANDIDATE",
    "HOLD",
    "RISK_REDUCE",
    "EXIT_CANDIDATE",
    "NO-GO",
}
BAD_FRESHNESS_STATUSES = {"missing", "stale", "fallback_stale", "schema_error"}


def load_json_payload(path: Path | str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    path_obj = Path(path)
    if not path_obj.exists():
        return dict(default or {})
    try:
        payload = json.loads(path_obj.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default or {})
    return payload if isinstance(payload, dict) else dict(default or {})


def _iso_now(now: datetime | None = None) -> datetime:
    value = now or datetime.now(TAIPEI_TZ)
    if value.tzinfo is None:
        value = value.replace(tzinfo=TAIPEI_TZ)
    return value.astimezone(TAIPEI_TZ)


def _fmt_rr(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "1:─"
    return f"1:{value:.1f}"


def _rr_from_stock(stock: Mapping[str, Any]) -> tuple[float | None, str]:
    entry = to_number(stock.get("entry"))
    target = to_number(stock.get("target"))
    stop = to_number(stock.get("stop"))
    if entry is None or target is None or stop is None:
        return None, "1:─"
    risk = entry - stop
    reward = target - entry
    if risk <= 0 or reward <= 0:
        return None, "1:─"
    rr = reward / risk
    return rr, _fmt_rr(rr)


def _carybot_signal_rank(signal: Mapping[str, Any]) -> tuple[Any, ...]:
    signal_type_rank = {"B1": 2, "B2": 1}.get(str(signal.get("signal_type") or ""), 0)
    score = to_number(signal.get("score"), 0) or 0
    rank = to_number(signal.get("rank"), 999999) or 999999
    return (str(signal.get("date") or ""), signal_type_rank, score, -rank)


def latest_carybot_by_stock(payload: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if not isinstance(payload, Mapping):
        return latest
    for row in payload.get("signals") or []:
        if not isinstance(row, Mapping):
            continue
        stock_id = str(row.get("stock_id") or row.get("stock") or "").strip()
        if not stock_id:
            continue
        current = latest.get(stock_id)
        if current is None or _carybot_signal_rank(row) >= _carybot_signal_rank(current):
            latest[stock_id] = dict(row)
    return latest


def _manifest_artifact(manifest_payload: Mapping[str, Any] | None, dataset_id: str) -> dict[str, Any] | None:
    if not isinstance(manifest_payload, Mapping):
        return None
    artifacts = manifest_payload.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return None
    candidates = [
        dict(item)
        for item in artifacts.values()
        if isinstance(item, Mapping) and item.get("dataset_id") == dataset_id
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: str(item.get("fetched_at") or ""), reverse=True)
    return candidates[0]


def _freshness_from_payload(
    payload: Mapping[str, Any] | None,
    manifest_payload: Mapping[str, Any] | None,
    dataset_id: str,
) -> dict[str, Any]:
    if isinstance(payload, Mapping) and isinstance(payload.get("freshness"), Mapping):
        return dict(payload["freshness"])
    artifact = _manifest_artifact(manifest_payload, dataset_id)
    if artifact:
        freshness = dict(artifact.get("freshness") or {})
        freshness.setdefault("dataset_id", dataset_id)
        freshness.setdefault("source_id", artifact.get("source_id"))
        freshness.setdefault("source_tier", artifact.get("source_tier"))
        freshness.setdefault("data_date", artifact.get("data_date"))
        freshness.setdefault("expected_data_date", artifact.get("expected_data_date"))
        freshness.setdefault("row_count", artifact.get("row_count"))
        freshness.setdefault("fallback", artifact.get("fallback"))
        return freshness
    return {
        "dataset_id": dataset_id,
        "status": "missing",
        "data_date": None,
        "expected_data_date": None,
        "row_count": 0,
    }


def _source_artifact(
    dataset_id: str,
    path: Path,
    payload: Mapping[str, Any] | None,
    manifest_payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    freshness = _freshness_from_payload(payload, manifest_payload, dataset_id)
    return {
        "dataset_id": dataset_id,
        "path": str(path.as_posix()),
        "freshness": {
            "status": freshness.get("status") or "missing",
            "data_date": freshness.get("data_date"),
            "expected_data_date": freshness.get("expected_data_date"),
            "source_id": freshness.get("source_id"),
            "source_tier": freshness.get("source_tier"),
            "row_count": freshness.get("row_count"),
            "fallback": freshness.get("fallback"),
            "missing": freshness.get("missing"),
        },
    }


def _traffic_inputs_from_stock(stock: Mapping[str, Any]) -> dict[str, Any]:
    rr, rr_text = _rr_from_stock(stock)
    basket = str(stock.get("basket") or stock.get("mda_basket") or "")
    status = str(stock.get("status") or "")
    is_risk = "風險" in basket or "過熱" in basket or "風險" in status or "過熱" in status
    trend = ""
    if is_risk:
        trend = "轉弱/風險"
    elif "已發動" in basket or "強勢" in status:
        trend = "多方"
    return {
        "tech": {
            "trend": trend,
            "volume_price": "",
            "close": stock.get("close"),
            "ma20": stock.get("ma20"),
            "ma60": stock.get("ma60"),
        },
        "decision": {"rr": rr, "rr_text": rr_text},
        "indicator": {},
        "chip_total_5d": None,
    }


def _bad_source_warnings(source_artifacts: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for artifact in source_artifacts:
        status = str((artifact.get("freshness") or {}).get("status") or "missing")
        if status in BAD_FRESHNESS_STATUSES:
            warnings.append(f"{artifact.get('dataset_id')} freshness is {status}")
        missing_status = str(((artifact.get("freshness") or {}).get("missing") or {}).get("status") or "complete")
        if missing_status in {"partial", "missing"}:
            warnings.append(f"{artifact.get('dataset_id')} completeness is {missing_status}")
    return warnings


def _action_state(traffic: Mapping[str, Any], carybot_signal: Mapping[str, Any] | None) -> str:
    if traffic.get("exit") or traffic.get("state") == "NO-GO":
        return "NO-GO"
    signal_type = str((carybot_signal or {}).get("signal_type") or "")
    if traffic.get("entry") and signal_type == "B1":
        return "ENTRY_CANDIDATE"
    if traffic.get("entry") or traffic.get("armed") or signal_type in {"B1", "B2"}:
        return "SETUP"
    return "WATCH"


def _normalized_market(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"上市", "twse", "listed"}:
        return "listed"
    if text in {"上櫃", "tpex", "otc"}:
        return "otc"
    return ""


def market_risk_by_stock(payload: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not isinstance(payload, Mapping):
        return result
    priority = {"attention": 1, "near_disposition": 2, "disposition": 3}
    for row in payload.get("risk_summary") or []:
        if not isinstance(row, Mapping):
            continue
        stock_id = str(row.get("security_id") or row.get("stock_id") or "").strip()
        if not stock_id:
            continue
        current = result.get(stock_id)
        if current is None or priority.get(str(row.get("risk_level") or ""), 0) >= priority.get(str(current.get("risk_level") or ""), 0):
            result[stock_id] = dict(row)
    return result


def _market_risk_source_state(payload: Mapping[str, Any] | None, market: str) -> tuple[str, list[str]]:
    if not isinstance(payload, Mapping):
        return "unknown", ["attention_disposition_risk artifact is missing"]
    artifacts = [item for item in payload.get("source_artifacts") or [] if isinstance(item, Mapping)]
    relevant = [item for item in artifacts if not market or _normalized_market(item.get("market")) == market]
    required_kinds = {"attention", "disposition", "near_disposition"}
    present_fresh = {str(item.get("kind") or "") for item in relevant if item.get("status") == "fresh"}
    bad = [str(item.get("source_id") or "unknown") for item in relevant if item.get("status") != "fresh"]
    missing_kinds = sorted(required_kinds - present_fresh)
    if bad or missing_kinds:
        warnings = [f"{source_id} is not fresh" for source_id in bad]
        warnings.extend(f"market-risk partition missing: {kind}" for kind in missing_kinds)
        return "unknown", warnings
    return "complete", []


def market_risk_for_stock(
    stock: Mapping[str, Any],
    payload: Mapping[str, Any] | None,
    risk_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    stock_id = str(stock.get("stock_id") or stock.get("security_id") or stock.get("id") or "").strip()
    market = _normalized_market(stock.get("market") or stock.get("exchange"))
    source_state, warnings = _market_risk_source_state(payload, market)
    row = dict((risk_map or market_risk_by_stock(payload)).get(stock_id) or {})
    if row:
        return {
            "risk_level": str(row.get("risk_level") or "attention"),
            "source_state": source_state,
            "market": row.get("market") or market or None,
            "rule_version": row.get("rule_version") or (payload or {}).get("rule_version"),
            "effective_end_date": row.get("effective_end_date"),
            "matching_interval_minutes": row.get("matching_interval_minutes"),
            "transition_revised": row.get("transition_revised"),
            "reasons": list(row.get("reasons") or []),
            "warnings": warnings,
        }
    return {
        "risk_level": "none" if source_state == "complete" else "unknown",
        "source_state": source_state,
        "market": market or None,
        "rule_version": (payload or {}).get("rule_version") if isinstance(payload, Mapping) else None,
        "effective_end_date": None,
        "matching_interval_minutes": None,
        "transition_revised": False,
        "reasons": [],
        "warnings": warnings,
    }


def build_decision_for_stock(
    stock: Mapping[str, Any],
    *,
    carybot_signal: Mapping[str, Any] | None = None,
    source_warnings: list[str] | None = None,
    traffic_inputs: Mapping[str, Any] | None = None,
    market_risk: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    stock_id = str(stock.get("stock_id") or stock.get("security_id") or stock.get("id") or "").strip()
    inputs = dict(traffic_inputs or _traffic_inputs_from_stock(stock))
    traffic = evaluate_traffic_light(
        stock,
        inputs.get("tech") or {},
        inputs.get("decision") or {},
        inputs.get("indicator") or {},
        inputs.get("chip_total_5d"),
    )
    carybot_signal = dict(carybot_signal or {})
    action_state = _action_state(traffic, carybot_signal)
    warnings = list(source_warnings or [])
    conflicts: list[str] = []
    market_risk = dict(market_risk or {"risk_level": "unknown", "source_state": "unknown"})
    risk_level = str(market_risk.get("risk_level") or "unknown")
    warnings.extend(str(item) for item in market_risk.get("warnings") or [] if str(item))
    signal_type = str(carybot_signal.get("signal_type") or "")
    if signal_type == "B1" and traffic.get("state") == "NO-GO":
        conflicts.append("carybot_b1_but_traffic_no_go")
    if signal_type and not traffic.get("armed") and traffic.get("state") == "WATCH":
        conflicts.append("carybot_signal_waiting_for_traffic_confirmation")

    entry_allowed = bool(traffic.get("entry"))
    if risk_level == "disposition":
        if action_state != "NO-GO":
            conflicts.append("market_disposition_overrides_strategy_entry")
        action_state = "NO-GO"
        entry_allowed = False
        warnings.append("official disposition is active; new entry is blocked")
    elif risk_level == "near_disposition":
        if action_state != "NO-GO":
            conflicts.append("official_near_disposition_overrides_strategy_entry")
        action_state = "NO-GO"
        entry_allowed = False
        warnings.append("official exchange warning shows this security is nearing disposition")
    elif risk_level == "attention":
        if action_state == "ENTRY_CANDIDATE":
            action_state = "SETUP"
            entry_allowed = False
            conflicts.append("official_attention_downgrades_entry_to_setup")
        warnings.append("security is on the official attention list")
    elif risk_level == "unknown":
        if action_state == "ENTRY_CANDIDATE":
            action_state = "SETUP"
            entry_allowed = False
            conflicts.append("market_risk_unknown_downgrades_entry_to_setup")
        warnings.append("official attention/disposition coverage is incomplete; no-risk status cannot be asserted")

    reason_parts = [str(traffic.get("reason") or "")]
    if signal_type:
        reason_parts.append(f"CaryBot {signal_type} on {carybot_signal.get('date') or '-'}")
    else:
        reason_parts.append("No current CaryBot B1/B2 confirmation")
    if risk_level != "none":
        reason_parts.append(f"Official market risk: {risk_level}")

    return {
        "data_date": str(stock.get("data_date") or stock.get("date") or ""),
        "security_id": stock_id,
        "stock_id": stock_id,
        "name": str(stock.get("name") or stock_id),
        "rank": int(to_number(stock.get("rank"), 0) or 0),
        "action_state": action_state,
        "rule_version": RULE_VERSION,
        "candidate": bool(traffic.get("candidate")),
        "armed": bool(traffic.get("armed")),
        "entry": entry_allowed,
        "exit": bool(traffic.get("exit")),
        "traffic_light": {
            "state": traffic.get("state"),
            "candidate": bool(traffic.get("candidate")),
            "armed": bool(traffic.get("armed")),
            "entry": bool(traffic.get("entry")),
            "exit": bool(traffic.get("exit")),
            "reason": traffic.get("reason"),
            "basket": traffic.get("basket"),
            "checks": traffic.get("checks") or {},
            "blockers": traffic.get("blockers") or [],
        },
        "evidence": {
            "sfz": {
                "semantic_role": "legacy_site_sfz_listing_from_mda_candidate_pool",
                "rank": int(to_number(stock.get("rank"), 0) or 0),
                "score": to_number(stock.get("score")),
                "basket": stock.get("basket"),
            },
            "mda": {
                "basket": stock.get("mda_basket") or stock.get("basket"),
                "rank_basket": stock.get("rank_basket"),
                "score": to_number(stock.get("mda_score") or stock.get("score")),
                "status": stock.get("status"),
                "reason": stock.get("reason") or "",
                "pit_eligible": stock.get("pit_eligible"),
            },
            "carybot": {
                "signal_type": signal_type or None,
                "score": to_number(carybot_signal.get("score")),
                "thermometer_score": to_number(carybot_signal.get("thermometer_score")),
                "date": carybot_signal.get("date"),
                "phase": carybot_signal.get("phase"),
                "is_current": carybot_signal.get("is_current"),
            },
            "market_risk": market_risk,
        },
        "conflicts": conflicts,
        "warnings": warnings,
        "reasons": [part for part in reason_parts if part],
    }


def _action_counts(decisions: list[dict[str, Any]]) -> dict[str, int]:
    counts = {state: 0 for state in sorted(ACTION_STATES)}
    for row in decisions:
        state = str(row.get("action_state") or "")
        if state in counts:
            counts[state] += 1
    return {key: value for key, value in counts.items() if value}


def build_payload(
    *,
    mda_payload: Mapping[str, Any] | None = None,
    carybot_payload: Mapping[str, Any] | None = None,
    market_risk_payload: Mapping[str, Any] | None = None,
    freshness_manifest: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    limit: int | None = None,
    traffic_inputs_by_stock: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    updated = _iso_now(now)
    mda_payload = mda_payload if isinstance(mda_payload, Mapping) else {}
    carybot_payload = carybot_payload if isinstance(carybot_payload, Mapping) else {}
    market_risk_payload = market_risk_payload if isinstance(market_risk_payload, Mapping) else {}
    stocks = [dict(row) for row in (mda_payload.get("stocks") or []) if isinstance(row, Mapping)]
    if limit is not None:
        stocks = stocks[: max(0, int(limit))]
    source_artifacts = [
        _source_artifact("mda_candidate_pool", Path("data/mda_candidates.json"), mda_payload, freshness_manifest),
        _source_artifact("carybot_signals", Path("data/carybot_signals.json"), carybot_payload, freshness_manifest),
        _source_artifact("attention_disposition_risk", Path("data/attention_disposition.json"), market_risk_payload, freshness_manifest),
    ]
    source_warnings = _bad_source_warnings(source_artifacts)
    carybot_map = latest_carybot_by_stock(carybot_payload)
    market_risk_map = market_risk_by_stock(market_risk_payload)
    traffic_inputs_by_stock = traffic_inputs_by_stock or {}
    decisions = [
        build_decision_for_stock(
            stock,
            carybot_signal=carybot_map.get(str(stock.get("stock_id") or stock.get("security_id") or "")),
            source_warnings=source_warnings,
            traffic_inputs=traffic_inputs_by_stock.get(str(stock.get("stock_id") or stock.get("security_id") or "")),
            market_risk=market_risk_for_stock(stock, market_risk_payload, market_risk_map),
        )
        for stock in stocks
    ]
    data_date = str(mda_payload.get("date") or max((row.get("data_date") or row.get("date") or "" for row in stocks), default=""))
    return {
        "dataset_id": "daily_decisions",
        "schema_version": SCHEMA_VERSION,
        "rule_version": RULE_VERSION,
        "date": data_date,
        "updated_at": updated.isoformat(timespec="seconds"),
        "source_artifacts": source_artifacts,
        "data_quality": {
            "state": "warning" if source_warnings else "ok",
            "warnings": source_warnings,
        },
        "action_counts": _action_counts(decisions),
        "count": len(decisions),
        "decisions": decisions,
        "notes": [
            "This artifact structures existing evidence; it does not change strategy thresholds or place orders.",
            "HOLD/RISK_REDUCE/EXIT_CANDIDATE are reserved for future holdings integration.",
            "Active disposition and official near-disposition warnings block new entries; attention or unknown coverage downgrades an entry candidate to setup.",
        ],
    }


def contract_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in payload.get("decisions") or []:
        if not isinstance(item, Mapping):
            continue
        carybot = ((item.get("evidence") or {}).get("carybot") or {}) if isinstance(item.get("evidence"), Mapping) else {}
        market_risk = ((item.get("evidence") or {}).get("market_risk") or {}) if isinstance(item.get("evidence"), Mapping) else {}
        rows.append(
            {
                "data_date": str(item.get("data_date") or payload.get("date") or ""),
                "security_id": str(item.get("security_id") or ""),
                "action_state": str(item.get("action_state") or ""),
                "traffic_state": str((item.get("traffic_light") or {}).get("state") or ""),
                "carybot_signal_type": str(carybot.get("signal_type") or ""),
                "data_quality_state": str((payload.get("data_quality") or {}).get("state") or "unknown"),
                "market_risk_level": str(market_risk.get("risk_level") or "unknown"),
                "rule_version": str(item.get("rule_version") or payload.get("rule_version") or ""),
            }
        )
    return rows


def write_payload(
    *,
    output_path: Path | str = DAILY_DECISIONS_PATH,
    mda_path: Path | str = MDA_CANDIDATES_PATH,
    carybot_path: Path | str = CARYBOT_SIGNALS_PATH,
    market_risk_path: Path | str = ATTENTION_DISPOSITION_PATH,
    freshness_manifest_path: Path | str = DEFAULT_MANIFEST_PATH,
    manifest_path: Path | str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    updated = _iso_now(now)
    output_path = Path(output_path)
    payload = build_payload(
        mda_payload=load_json_payload(mda_path, {"stocks": [], "date": ""}),
        carybot_payload=load_json_payload(carybot_path, {"signals": [], "history": [], "date": ""}),
        market_risk_payload=load_json_payload(market_risk_path, {"risk_summary": [], "source_artifacts": [], "date": ""}),
        freshness_manifest=load_json_payload(freshness_manifest_path, {"artifacts": {}}),
        now=updated,
    )
    payload, manifest, artifact_bytes = prepare_artifact_manifest(
        payload,
        dataset_id="daily_decisions",
        source_id="daily_decisions_derived",
        rows=contract_rows(payload),
        data_date=str(payload.get("date") or ""),
        expected_data_date=updated.date().isoformat(),
        fetched_at=updated,
        evaluated_at=updated,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(artifact_bytes)
    resolved_manifest_path = Path(manifest_path) if manifest_path else (
        DEFAULT_MANIFEST_PATH if output_path.resolve() == DAILY_DECISIONS_PATH.resolve() else output_path.parent / "freshness_manifest.json"
    )
    update_manifest_file(manifest, resolved_manifest_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build data/daily_decisions.json")
    parser.add_argument("--output", type=Path, default=DAILY_DECISIONS_PATH)
    parser.add_argument("--mda", type=Path, default=MDA_CANDIDATES_PATH)
    parser.add_argument("--carybot", type=Path, default=CARYBOT_SIGNALS_PATH)
    parser.add_argument("--market-risk", type=Path, default=ATTENTION_DISPOSITION_PATH)
    parser.add_argument("--freshness-manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    args = parser.parse_args()
    payload = write_payload(
        output_path=args.output,
        mda_path=args.mda,
        carybot_path=args.carybot,
        market_risk_path=args.market_risk,
        freshness_manifest_path=args.freshness_manifest,
    )
    print(f"[daily_decisions] wrote {args.output} decisions={payload.get('count', 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
