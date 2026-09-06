"""Publish sparse official per-security chip and margin sidecars.

Values remain net shares or reported lots.  They are never expanded into
fabricated buy/sell transactions.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parent
FLOW_PATH = ROOT / "data" / "daily_market_flow.json"
CHIP_PATH = ROOT / "data" / "official_chip_history.json"
MARGIN_PATH = ROOT / "data" / "official_margin_snapshot.json"
DEFAULT_MIN_PARTITION_COUNTS = {"listed": 800, "otc": 600}


def _snapshot_rows(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("rows")
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, Mapping)]
    output = []
    for line in str(snapshot.get("rows_csv") or "").splitlines():
        values = line.split(",")
        if len(values) not in {4, 5}:
            continue
        output.append({
            "security_id": values[0],
            "market": "listed" if values[1] == "l" else "otc",
            "foreign_net_shares": int(values[2]),
            "investment_trust_net_shares": int(values[3]),
            "dealer_net_shares": int(values[4]) if len(values) == 5 else None,
        })
    return output


def _validate_partition_rows(
    rows: list[dict[str, Any]],
    *,
    min_partition_counts: Mapping[str, int],
    require_dealer: bool = False,
) -> None:
    ids = [str(row.get("security_id") or "") for row in rows]
    if not rows or any(not security_id for security_id in ids) or len(ids) != len(set(ids)):
        raise ValueError("official sidecar partition contains missing or duplicate security ids")
    if require_dealer and any(row.get("dealer_net_shares") is None for row in rows):
        raise ValueError("official chip history contains legacy rows without dealer net values")
    counts = {market: sum(1 for row in rows if row.get("market") == market) for market in ("listed", "otc")}
    if any(counts[market] < int(min_partition_counts[market]) for market in counts):
        raise ValueError(f"official sidecar partition coverage is incomplete: {counts}")


def build_sidecars(
    flow: Mapping[str, Any],
    *,
    min_partition_counts: Mapping[str, int] = DEFAULT_MIN_PARTITION_COUNTS,
) -> tuple[dict[str, Any], dict[str, Any]]:
    data_date = str(flow.get("date") or "")
    quality = flow.get("data_quality") or {}
    if quality.get("state") != "ok" or not data_date:
        raise ValueError("daily market flow is not a verified complete snapshot")
    snapshots = list((flow.get("institutional_history") or {}).get("snapshots") or [])
    dates = [str(snapshot.get("date") or "") for snapshot in snapshots]
    if len(snapshots) != 20 or len(set(dates)) != 20 or sorted(dates)[-1:] != [data_date]:
        raise ValueError("official chip history must contain 20 distinct sessions ending on the flow date")
    history_rows = []
    for snapshot in snapshots:
        snapshot_date = str(snapshot.get("date") or "")
        snapshot_rows = _snapshot_rows(snapshot)
        _validate_partition_rows(snapshot_rows, min_partition_counts=min_partition_counts, require_dealer=True)
        for row in snapshot_rows:
            history_rows.append({"date": snapshot_date, **row})
    current = flow.get("official_security_data") or {}
    margin_rows = [{"date": data_date, **dict(row)} for row in current.get("margin_balance") or []]
    if not history_rows or not margin_rows:
        raise ValueError("verified flow lacks official per-security chip or margin rows")
    if str(current.get("date") or "") != data_date or any(row.get("date") != data_date for row in margin_rows):
        raise ValueError("official margin snapshot date does not match the flow date")
    _validate_partition_rows(margin_rows, min_partition_counts=min_partition_counts)
    chip = {
        "dataset_id": "official_chip_history",
        "schema_version": "1.0.0",
        "date": data_date,
        "unit": "shares",
        "source": "TWSE T86 + TPEx institutional daily detail",
        "semantics": "reported net values; buy and sell legs are not inferred",
        "rows": sorted(history_rows, key=lambda row: (str(row.get("date")), str(row.get("security_id")))),
    }
    margin = {
        "dataset_id": "official_margin_snapshot",
        "schema_version": "1.0.0",
        "date": data_date,
        "unit": "lots",
        "source": "TWSE MI_MARGN + TPEx dated margin balance",
        "semantics": "reported previous and current balances; transaction legs are not inferred",
        "rows": sorted(margin_rows, key=lambda row: str(row.get("security_id"))),
    }
    return chip, margin


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow", type=Path, default=FLOW_PATH)
    parser.add_argument("--chip-output", type=Path, default=CHIP_PATH)
    parser.add_argument("--margin-output", type=Path, default=MARGIN_PATH)
    args = parser.parse_args()
    flow = json.loads(args.flow.read_text(encoding="utf-8-sig"))
    chip, margin = build_sidecars(flow)
    _write_json(args.chip_output, chip)
    _write_json(args.margin_output, margin)
    print(f"[official_chip_history] date={chip['date']} chip_rows={len(chip['rows'])} margin_rows={len(margin['rows'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
