# -*- coding: utf-8 -*-
"""
Build the static CaryBot signal bridge consumed by GitHub Pages.

The website reads data/carybot_signals.json. This module can refresh that file
from the local CaryBot v50/v51 CSV exports when they are available, while
preserving an existing JSON file on GitHub Actions where the sibling research
workspace is usually absent.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data_contract import DEFAULT_MANIFEST_PATH, prepare_artifact_manifest, update_manifest_file

PROJECT_ROOT = Path(__file__).resolve().parent
LOCAL_DATA_DIR = PROJECT_ROOT / "data"
CARYBOT_SIGNALS_PATH = LOCAL_DATA_DIR / "carybot_signals.json"
TAIPEI_TZ = timezone(timedelta(hours=8))

BACKTEST_DIR_NAME = "\u56de\u6e2c"
TRADING_PROJECT_DIR_NAME = "\u81ea\u52d5\u4ea4\u6613\u7a0b\u5f0f"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT.parent / TRADING_PROJECT_DIR_NAME / BACKTEST_DIR_NAME / "v6_outputs"

CURRENT_SOURCE_FILE = "carybot_daily_ai_buy_v51.csv"
HISTORY_SOURCE_FILE = "carybot_daily_ai_buy_v51_history.csv"
V50_SOURCE_FILE = "carybot_signal_master_v50.csv"

SIGNAL_TYPE_MAP = {
    "AI_Buy": "B1",
    "AI_Buy_like_v51": "B1",
    "PreBuy": "B2",
}
DEFAULT_SCORES = {"B1": 85, "B2": 75}
NUMERIC_FIELDS = [
    "Close",
    "Price",
    "entry_watch_price",
    "support_price",
    "stop_price",
    "target_price",
    "risk_pct",
    "QZ",
    "QTYR",
    "VAM",
    "VAM5",
    "VAM20",
    "VAM60",
    "ATRB20",
    "ATRB60",
    "ATRB120",
    "ATRB240",
    "ATRB480",
    "VPA120",
    "VPA240",
    "VPA480",
    "MA20",
    "MA60",
    "MA120",
    "MA240",
    "MA480",
]


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(str(value).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def discover_source_dir(explicit: Path | str | None = None) -> Path | None:
    if explicit:
        candidate = Path(explicit)
        if candidate.exists() and any((candidate / name).exists() for name in (CURRENT_SOURCE_FILE, V50_SOURCE_FILE)):
            return candidate
        return None

    candidates: list[Path] = []
    for env_name in ("CARYBOT_OUTPUT_DIR", "V44_BACKTEST_OUTPUT_DIR"):
        env_value = os.environ.get(env_name)
        if env_value:
            candidates.append(Path(env_value))
    candidates.append(DEFAULT_OUTPUT_DIR)

    parent = PROJECT_ROOT.parent
    if parent.exists():
        for child in parent.iterdir():
            if child.is_dir():
                candidates.append(child / BACKTEST_DIR_NAME / "v6_outputs")

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists() and any((candidate / name).exists() for name in (CURRENT_SOURCE_FILE, V50_SOURCE_FILE)):
            return candidate
    return None


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def normalize_signal_row(row: dict[str, Any], source: str, is_current: bool = False) -> dict[str, Any] | None:
    raw_type = str(row.get("signal_type") or row.get("marker_type") or "").strip()
    signal_type = SIGNAL_TYPE_MAP.get(raw_type)
    if not signal_type:
        return None
    if str(row.get("marker_side") or "buy").strip().lower() not in {"", "buy"}:
        return None

    stock_id = str(row.get("stock_id") or row.get("stock") or row.get("id") or "").strip()
    if not stock_id:
        return None

    signal_date = str(row.get("data_date") or row.get("date") or row.get("run_date") or "").strip()
    score = to_float(row.get("quality_score"))
    if score is None:
        score = DEFAULT_SCORES[signal_type]
    score = int(round(score))

    metrics = {name: to_float(row.get(name)) for name in NUMERIC_FIELDS if to_float(row.get(name)) is not None}
    thermometer_score = to_float(row.get("thermometer_score"))
    if thermometer_score is None:
        thermometer_score = score

    normalized: dict[str, Any] = {
        "stock_id": stock_id,
        "name": str(row.get("stock_name") or row.get("name") or "").strip(),
        "date": signal_date,
        "signal_type": signal_type,
        "raw_signal_type": raw_type,
        "score": score,
        "thermometer_score": int(round(float(thermometer_score))),
        "source": source,
        "is_current": bool(is_current),
        "rank": int(to_float(row.get("recommendation_rank")) or 0) or None,
        "quality_grade": str(row.get("quality_grade") or "").strip(),
        "phase": str(row.get("carybot_phase") or "").strip(),
        "transition_5d": str(row.get("transition_5d") or "").strip(),
        "reason": str(row.get("reason") or row.get("note") or "").strip(),
        "formula_status": str(row.get("signal_formula_status") or "").strip(),
        "metrics": metrics,
    }
    return {key: value for key, value in normalized.items() if value not in ("", None, {})}


def normalize_existing_payload(payload: dict[str, Any]) -> dict[str, Any]:
    signals = []
    for row in payload.get("signals") or []:
        if isinstance(row, dict) and row.get("stock_id") and row.get("signal_type"):
            signal = dict(row)
            signal.setdefault("date", payload.get("date") or "")
            signal.setdefault("score", DEFAULT_SCORES.get(str(signal.get("signal_type")), 75))
            signal.setdefault("is_current", True)
            signals.append(signal)
    history = [dict(row) for row in (payload.get("history") or []) if isinstance(row, dict) and row.get("stock_id")]
    return {
        "date": str(payload.get("date") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
        "signals": signals,
        "history": history,
        "sources": {"mode": "preserved_existing_json"},
    }


def load_existing_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    return normalize_existing_payload(payload)


def dedupe_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for signal in signals:
        key = (
            str(signal.get("stock_id") or ""),
            str(signal.get("date") or ""),
            str(signal.get("signal_type") or ""),
            str(signal.get("source") or ""),
        )
        old = best.get(key)
        if old is None or float(signal.get("score") or 0) >= float(old.get("score") or 0):
            best[key] = signal
    return sorted(
        best.values(),
        key=lambda item: (
            str(item.get("date") or ""),
            int(item.get("rank") or 999999) * -1,
            float(item.get("score") or 0),
            str(item.get("stock_id") or ""),
        ),
        reverse=True,
    )


def latest_v50_buy_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    buy_rows = [row for row in rows if SIGNAL_TYPE_MAP.get(str(row.get("signal_type") or "").strip())]
    latest_date = max((str(row.get("date") or "") for row in buy_rows), default="")
    return [row for row in buy_rows if str(row.get("date") or "") == latest_date] if latest_date else []


def build_payload(
    source_dir: Path | str | None = None,
    existing_path: Path | str = CARYBOT_SIGNALS_PATH,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    updated = now or datetime.now(TAIPEI_TZ)
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=TAIPEI_TZ)
    resolved_source = discover_source_dir(source_dir)
    existing_payload = load_existing_payload(Path(existing_path))
    if not resolved_source:
        if existing_payload:
            return existing_payload
        return {
            "date": "",
            "updated_at": updated.astimezone(TAIPEI_TZ).isoformat(),
            "signals": [],
            "history": [],
            "sources": {"mode": "empty_no_source"},
        }

    current_rows = read_csv_rows(resolved_source / CURRENT_SOURCE_FILE)
    history_rows = read_csv_rows(resolved_source / HISTORY_SOURCE_FILE)
    v50_rows = read_csv_rows(resolved_source / V50_SOURCE_FILE)

    signals: list[dict[str, Any]] = []
    if current_rows:
        signals = [
            signal
            for row in current_rows
            if truthy(row.get("candidate_pass", "True"))
            for signal in [normalize_signal_row(row, CURRENT_SOURCE_FILE, is_current=True)]
            if signal
        ]
    elif v50_rows:
        signals = [
            signal
            for row in latest_v50_buy_rows(v50_rows)
            for signal in [normalize_signal_row(row, V50_SOURCE_FILE, is_current=True)]
            if signal
        ]

    history = [
        signal
        for row in history_rows
        for signal in [normalize_signal_row(row, HISTORY_SOURCE_FILE)]
        if signal
    ]
    history.extend(
        signal
        for row in v50_rows
        for signal in [normalize_signal_row(row, V50_SOURCE_FILE)]
        if signal
    )

    signals = dedupe_signals(signals)
    history = dedupe_signals(history)
    latest_date = max((str(signal.get("date") or "") for signal in signals), default="")
    if not latest_date:
        latest_date = max((str(signal.get("date") or "") for signal in history), default="")

    return {
        "date": latest_date,
        "updated_at": updated.astimezone(TAIPEI_TZ).isoformat(),
        "signals": signals,
        "history": history,
        "sources": {
            "mode": "local_csv",
            "source_dir": str(resolved_source),
            "current": CURRENT_SOURCE_FILE if current_rows else (V50_SOURCE_FILE if signals else ""),
            "history": [name for name, rows in ((HISTORY_SOURCE_FILE, history_rows), (V50_SOURCE_FILE, v50_rows)) if rows],
            "current_count": len(signals),
            "history_count": len(history),
        },
    }


def contract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for is_current_default, items in ((True, payload.get("signals") or []), (False, payload.get("history") or [])):
        for item in items:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "data_date": str(item.get("date") or payload.get("date") or ""),
                    "security_id": str(item.get("stock_id") or ""),
                    "signal_type": str(item.get("signal_type") or ""),
                    "score": int(round(to_float(item.get("score")) or 0)),
                    "is_current": bool(item.get("is_current", is_current_default)),
                }
            )
    return rows


def write_payload(
    output_path: Path | str = CARYBOT_SIGNALS_PATH,
    source_dir: Path | str | None = None,
    *,
    now: datetime | None = None,
    manifest_path: Path | str | None = None,
) -> dict[str, Any]:
    output_path = Path(output_path)
    updated = now or datetime.now(TAIPEI_TZ)
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=TAIPEI_TZ)
    payload = build_payload(source_dir=source_dir, existing_path=output_path, now=updated)
    source_mode = str((payload.get("sources") or {}).get("mode") or "")
    is_fallback = source_mode == "preserved_existing_json"
    source_id = "carybot_preserved_json" if is_fallback else "carybot_local_csv_derived"
    fallback_args = {
        "fallback_from_source_id": "carybot_local_csv_derived" if is_fallback else None,
        "fallback_reason": "Local CaryBot CSV exports were unavailable; preserved existing JSON." if is_fallback else None,
    }
    payload, manifest, artifact_bytes = prepare_artifact_manifest(
        payload,
        dataset_id="carybot_signals",
        source_id=source_id,
        rows=contract_rows(payload),
        data_date=str(payload.get("date") or ""),
        expected_data_date=updated.astimezone(TAIPEI_TZ).date().isoformat(),
        fetched_at=updated.astimezone(TAIPEI_TZ),
        evaluated_at=updated.astimezone(TAIPEI_TZ),
        **fallback_args,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(artifact_bytes)
    resolved_manifest_path = Path(manifest_path) if manifest_path else (
        DEFAULT_MANIFEST_PATH if output_path.resolve() == CARYBOT_SIGNALS_PATH.resolve() else output_path.parent / "freshness_manifest.json"
    )
    update_manifest_file(manifest, resolved_manifest_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build data/carybot_signals.json")
    parser.add_argument("--source-dir", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=CARYBOT_SIGNALS_PATH)
    args = parser.parse_args()
    payload = write_payload(output_path=args.output, source_dir=args.source_dir)
    print(
        f"[OK] CaryBot signals: {len(payload.get('signals') or [])} current, "
        f"{len(payload.get('history') or [])} history -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
