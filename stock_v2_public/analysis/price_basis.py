from __future__ import annotations

from datetime import date
import math
from typing import Any, Iterable


PRICE_BASIS_MODE = "official_reference_ratio_back_adjusted_v1"


def _iso(value: Any) -> str:
    text = str(value or "")[:10]
    date.fromisoformat(text)
    return text


def validate_actions(actions: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in actions:
        event_date = _iso(item.get("date"))
        stock_id = str(item.get("stock_id") or "").strip()
        previous_close = float(item["previous_close"])
        reference_price = float(item["reference_price"])
        if not stock_id or not math.isfinite(previous_close) or not math.isfinite(reference_price) or previous_close <= 0 or reference_price <= 0:
            raise ValueError("corporate action has invalid stock, previous close, or reference price")
        ratio = reference_price / previous_close
        if not 0 < ratio <= 2:
            raise ValueError(f"corporate action ratio is implausible: {stock_id} {event_date} {ratio}")
        key = (stock_id, event_date)
        if key in seen:
            raise ValueError(f"duplicate corporate action: {stock_id} {event_date}")
        seen.add(key)
        normalized.append(
            {
                **item,
                "date": event_date,
                "stock_id": stock_id,
                "previous_close": previous_close,
                "reference_price": reference_price,
                "ratio": ratio,
            }
        )
    return sorted(normalized, key=lambda row: (row["stock_id"], row["date"]))


def project_adjusted_rows(
    raw_rows: Iterable[dict[str, Any]],
    actions: Iterable[dict[str, Any]],
    *,
    adjustment_as_of: str,
) -> list[dict[str, Any]]:
    """Back-adjust official raw OHLC using only actions known by adjustment_as_of.

    An event ratio applies strictly before its ex-right/ex-dividend date.  The
    latest row therefore remains anchored at factor 1.  Volume is retained as
    official raw shares; it is deliberately not split-adjusted by this module.
    """
    as_of = _iso(adjustment_as_of)
    valid_actions = [row for row in validate_actions(actions) if row["date"] <= as_of]
    by_stock: dict[str, list[dict[str, Any]]] = {}
    for action in valid_actions:
        by_stock.setdefault(action["stock_id"], []).append(action)

    output: list[dict[str, Any]] = []
    for item in raw_rows:
        row_date = _iso(item.get("date"))
        stock_id = str(item.get("stock_id") or "").strip()
        if not stock_id:
            raise ValueError("raw price row is missing stock_id")
        if row_date > as_of:
            raise ValueError(f"raw price row is later than adjustment_as_of: {stock_id} {row_date}")
        factor = 1.0
        for action in by_stock.get(stock_id, []):
            if row_date < action["date"]:
                factor *= float(action["ratio"])
        raw = {field: float(item[field]) for field in ("open", "high", "low", "close")}
        volume = float(item["volume"])
        if any(not math.isfinite(value) or value <= 0 for value in raw.values()):
            raise ValueError(f"raw OHLC contains non-finite or non-positive value: {stock_id} {row_date}")
        if not math.isfinite(volume) or volume < 0:
            raise ValueError(f"raw volume is invalid: {stock_id} {row_date}")
        if raw["high"] < max(raw["open"], raw["close"], raw["low"]) or raw["low"] > min(raw["open"], raw["close"], raw["high"]):
            raise ValueError(f"raw OHLC geometry is invalid: {stock_id} {row_date}")
        output.append(
            {
                "date": row_date,
                "stock_id": stock_id,
                "open": raw["open"] * factor,
                "high": raw["high"] * factor,
                "low": raw["low"] * factor,
                "close": raw["close"] * factor,
                "volume": volume,
                "raw_open": raw["open"],
                "raw_high": raw["high"],
                "raw_low": raw["low"],
                "raw_close": raw["close"],
                "raw_volume": volume,
                "adjustment_factor": factor,
            }
        )
    return sorted(output, key=lambda row: (row["stock_id"], row["date"]))


def price_basis_metadata(*, stock_id: str, adjustment_as_of: str, actions: Iterable[dict[str, Any]]) -> dict[str, Any]:
    applicable = [
        row for row in validate_actions(actions)
        if row["stock_id"] == str(stock_id) and row["date"] <= _iso(adjustment_as_of)
    ]
    return {
        "stock_id": str(stock_id),
        "mode": PRICE_BASIS_MODE,
        "source": "TWSE TWT49U / TPEx exDailyQ official reference prices",
        "verified": True,
        "adjustment_as_of": _iso(adjustment_as_of),
        "event_count": len(applicable),
        "volume_basis": "official_raw_shares",
    }
