from __future__ import annotations

import csv
import json
import sys
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data"
PRICE_DIR = DATA_DIR / "prices"
FILTERED_SIGNALS_PATH = DATA_DIR / "pit_filtered_signals.json"
OUTPUT_PATH = DATA_DIR / "legacy_data_hole_audit.json"
MIN_HISTORY_DAYS = 130


def _parse_date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _pct(count: int, total: int) -> float:
    return round(count / total * 100, 1) if total else 0.0


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_price_rows(stock_id: str) -> list[dict[str, Any]]:
    path = PRICE_DIR / f"{stock_id}.csv"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            item = dict(row)
            for key in ("open", "high", "low", "close", "volume"):
                item[key] = _parse_float(item.get(key))
            rows.append(item)
    return sorted(rows, key=lambda row: str(row.get("date", "")))


def _ma(rows: list[dict[str, Any]], window: int) -> float | None:
    if len(rows) < window:
        return None
    closes = [row.get("close") for row in rows[-window:]]
    if any(value is None for value in closes):
        return None
    return sum(closes) / window


def _try_finmind_listed_dates(stock_ids: set[str]) -> tuple[dict[str, str], str | None]:
    if not stock_ids:
        return {}, None
    url = "https://api.finmindtrade.com/api/v4/data?" + urllib.parse.urlencode(
        {"dataset": "TaiwanStockInfo"}
    )
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            payload = json.load(response)
    except Exception as exc:
        return {}, f"FinMind TaiwanStockInfo lookup failed: {type(exc).__name__}: {exc}"

    candidate_dates: dict[str, str] = {}
    for row in payload.get("data") or []:
        stock_id = str(row.get("stock_id", ""))
        raw_date = str(row.get("date", ""))
        if stock_id in stock_ids and _parse_date(raw_date):
            candidate_dates[stock_id] = raw_date[:10]

    if candidate_dates:
        return {}, (
            "FinMind TaiwanStockInfo was reachable, but its `date` field is not a reliable listed_date "
            f"for these samples (example values: {dict(list(candidate_dates.items())[:3])})."
        )
    return {}, "FinMind TaiwanStockInfo returned no usable listed_date values."


def _load_insufficient_history_signals() -> list[dict[str, str]]:
    from tools.pit_universe import diagnose_universe_eligibility

    data = _read_json(FILTERED_SIGNALS_PATH)
    signals = data.get("filtered_out", []) if isinstance(data, dict) else []
    out: list[dict[str, str]] = []
    for item in signals:
        stock_id = str(item.get("stock_id", ""))
        signal_date = str(item.get("signal_date", ""))
        if not stock_id or not signal_date:
            continue
        diagnosis = diagnose_universe_eligibility(stock_id, signal_date)
        reasons = set(diagnosis.get("reasons_failed") or [])
        # Match the prior audit's exclusive bucket so the count matches 106.
        if reasons == {"insufficient_history_lt_130_days"}:
            out.append(
                {
                    "stock_id": stock_id,
                    "signal_date": signal_date,
                    "basket": str(item.get("basket", "")),
                }
            )
    return out


def _classify_sample(sample: dict[str, Any]) -> str:
    if not sample["same_data_source"]:
        return "function_inconsistency"

    listed_date = _parse_date(sample.get("actual_listed_date"))
    signal_date = _parse_date(sample.get("signal_date"))
    if listed_date and signal_date:
        days_since_listed = (signal_date - listed_date).days
        if days_since_listed < MIN_HISTORY_DAYS:
            return "stock_too_new"
        return "data_coverage_shallow"

    if sample["legacy_days_used_for_ma120"] == sample["pit_days_available"]:
        return "function_inconsistency"
    return "other"


def _build_sample(signal: dict[str, str], listed_dates: dict[str, str]) -> dict[str, Any]:
    stock_id = signal["stock_id"]
    signal_date = signal["signal_date"]
    price_path = PRICE_DIR / f"{stock_id}.csv"
    rows = _read_price_rows(stock_id)
    target = _parse_date(signal_date)
    past_rows = [
        row for row in rows
        if target is not None and (row_date := _parse_date(str(row.get("date", "")))) is not None and row_date <= target
    ]
    earliest = str(rows[0].get("date", ""))[:10] if rows else None
    pit_days_available = len(past_rows)
    actual_listed_date = listed_dates.get(stock_id)
    listed = _parse_date(actual_listed_date)
    parsed_signal_date = _parse_date(signal_date)
    return {
        "stock_id": stock_id,
        "signal_date": signal_date,
        "basket": signal["basket"],
        "legacy_data_source_path": str(price_path.relative_to(PROJECT_ROOT)),
        "legacy_earliest_date_seen": earliest,
        "legacy_days_used_for_ma120": len(past_rows),
        "legacy_ma120_value": _ma(past_rows, 120),
        "legacy_ma240_value": _ma(past_rows, 240),
        "pit_data_source_path": str(price_path.relative_to(PROJECT_ROOT)),
        "pit_earliest_date_seen": earliest,
        "pit_days_available": pit_days_available,
        "same_data_source": True,
        "actual_listed_date": actual_listed_date,
        "days_between_listed_and_signal": (parsed_signal_date - listed).days if parsed_signal_date and listed else None,
        "legacy_signal_note": (
            "Legacy scan reads the same data/prices CSV. MA120/MA240 are None when there are not enough rows; "
            "the historical scan can still emit this method because its entry rule does not require MA120/MA240."
        ),
    }


def _verdict(by_root_cause: dict[str, dict[str, Any]], total: int) -> str:
    stock_too_new = by_root_cause["stock_too_new"]["count"]
    data_coverage = by_root_cause["data_coverage_shallow"]["count"]
    inconsistency = by_root_cause["function_inconsistency"]["count"]
    if total and stock_too_new / total >= 0.70:
        return "Legacy bug 確認，PIT 為正確版本，可直接推進 A4"
    if total and data_coverage / total >= 0.50:
        return "需要補抓 2023 年起的歷史資料"
    if total and inconsistency / total >= 0.30:
        return "PIT 與 Legacy 程式邏輯需要對齊"
    return (
        f"混合情況：stock_too_new={_pct(stock_too_new, total)}%, "
        f"data_coverage_shallow={_pct(data_coverage, total)}%, "
        f"function_inconsistency={_pct(inconsistency, total)}%"
    )


def build_audit() -> dict[str, Any]:
    signals = _load_insufficient_history_signals()
    sample_signals = signals[:20]
    listed_dates, listed_date_warning = _try_finmind_listed_dates({item["stock_id"] for item in sample_signals})
    samples = [_build_sample(signal, listed_dates) for signal in sample_signals]
    for sample in samples:
        sample["root_cause"] = _classify_sample(sample)

    counts = Counter(sample["root_cause"] for sample in samples)
    descriptions = {
        "stock_too_new": "股票上市未滿 130 天，Legacy 用不足資料硬算 MA → bug，PIT 修正正確",
        "data_coverage_shallow": "股票上市夠久但價格檔抓得不夠早，需要補抓歷史資料",
        "function_inconsistency": "Legacy 與 PIT 讀同一份資料但 gating 條件不同；Legacy 訊號規則未要求 MA120/MA240，PIT universe 要求 130 天歷史",
        "other": "缺少可靠上市日 metadata 或其他無法歸類情況",
    }
    by_root_cause = {}
    for key in ("stock_too_new", "data_coverage_shallow", "function_inconsistency", "other"):
        by_root_cause[key] = {
            "count": counts.get(key, 0),
            "percentage": _pct(counts.get(key, 0), len(samples)),
            "description": descriptions[key],
            "samples": [sample for sample in samples if sample["root_cause"] == key][:10],
        }

    missing_listed = sorted({sample["stock_id"] for sample in samples if not sample.get("actual_listed_date")})
    warning = None
    if missing_listed:
        warning = (
            "WARNING: 無法判斷實際上市日，root cause 分類不完整。"
            " FinMind TaiwanStockInfo 的 date 欄位不一定是可靠上市日；請手動查詢 listed_date。"
        )
    if listed_date_warning:
        warning = f"{warning or ''} {listed_date_warning}".strip()

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_filtered_signals": str(FILTERED_SIGNALS_PATH.relative_to(PROJECT_ROOT)),
        "insufficient_history_signals": len(signals),
        "total_examined": len(samples),
        "by_root_cause": by_root_cause,
        "verdict": _verdict(by_root_cause, len(samples)),
        "listed_date_warning": warning,
        "manual_listed_date_lookup_stock_ids": missing_listed[:5],
    }


def print_summary(audit: dict[str, Any]) -> None:
    print("=== Legacy Data Hole Investigation ===")
    print()
    print(
        f"Examined: {audit['total_examined']} samples "
        f"(out of {audit['insufficient_history_signals']} insufficient_history filtered signals)"
    )
    print()
    print("Root cause distribution:")
    for key in ("stock_too_new", "data_coverage_shallow", "function_inconsistency", "other"):
        row = audit["by_root_cause"][key]
        print(f"  {key}: {row['count']} 筆 ({row['percentage']:.1f}%)")
    print()
    print(f"Verdict: {audit['verdict']}")
    if audit.get("listed_date_warning"):
        print()
        print(audit["listed_date_warning"])
        print("建議用戶在 FinMind 上手動查詢以下 5 筆 stock_id 的 listed_date:")
        for stock_id in audit.get("manual_listed_date_lookup_stock_ids", []):
            print(f"  {stock_id}")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    audit = build_audit()
    OUTPUT_PATH.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print_summary(audit)
    print()
    print(f"Wrote {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
