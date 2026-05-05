from __future__ import annotations

import contextlib
import csv
import io
import json
import random
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data"
FILTERED_SIGNALS_PATH = DATA_DIR / "pit_filtered_signals.json"
SAMPLES_OUTPUT_PATH = DATA_DIR / "pit_filter_samples.json"
REASONS_OUTPUT_PATH = DATA_DIR / "pit_filter_reasons_audit.json"
SITE_REPORTS_PATH = DATA_DIR / "site_reports.json"
PRICE_DIR = DATA_DIR / "prices"
HOLDING_DIR = DATA_DIR / "holding_shares"
MIN_CLOSE = 20.0
RANDOM_SEED = 20260505

REASON_KEYS = [
    "close_below_min_20",
    "insufficient_history_lt_130_days",
    "no_holding_data",
    "no_price_data_on_date",
    "multiple_reasons",
    "eligible_unexpected",
]

METHODS = [
    ("sfz_ta3", "SFZ_TA3"),
    ("wr_after_attack", "Williams"),
]


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _normalize_reason(reason: str) -> str:
    return "close_below_min_20" if reason.startswith("close_below_min_") else reason


def _exclusive_reason(reasons: list[str]) -> str:
    normalized = [_normalize_reason(reason) for reason in reasons]
    unique = sorted(set(normalized))
    if len(unique) > 1:
        return "multiple_reasons"
    return unique[0] if unique else "eligible_unexpected"


def _load_filtered_signals() -> list[dict[str, str]]:
    data = _read_json(FILTERED_SIGNALS_PATH)
    signals = data.get("filtered_out", []) if isinstance(data, dict) else []
    return [
        {
            "stock_id": str(item.get("stock_id", "")),
            "signal_date": str(item.get("signal_date", "")),
            "basket": str(item.get("basket", "")),
        }
        for item in signals
        if item.get("stock_id") and item.get("signal_date")
    ]


def _read_price_rows(stock_id: str) -> list[dict[str, Any]]:
    path = PRICE_DIR / f"{stock_id}.csv"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            parsed = dict(row)
            for key in ("open", "high", "low", "close", "volume"):
                parsed[key] = _parse_float(parsed.get(key))
            rows.append(parsed)
    return rows


def _read_holding_rows(stock_id: str) -> list[dict[str, Any]]:
    path = HOLDING_DIR / f"{stock_id}.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _latest_holding_snapshot(stock_id: str, signal_date: str) -> tuple[str | None, list[dict[str, Any]]]:
    target = _parse_date(signal_date)
    if target is None:
        return None, []
    rows = _read_holding_rows(stock_id)
    eligible = [row for row in rows if (parsed := _parse_date(str(row.get("date", "")))) is not None and parsed <= target]
    if not eligible:
        return None, []
    latest_date = max(str(row.get("date", ""))[:10] for row in eligible)
    latest_rows = [row for row in eligible if str(row.get("date", ""))[:10] == latest_date]
    return latest_date, latest_rows[:8]


def _ma(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def _price_context(stock_id: str, signal_date: str) -> dict[str, Any]:
    target = _parse_date(signal_date)
    rows = _read_price_rows(stock_id)
    if target is None:
        return {
            "actual_price_row": None,
            "actual_close": None,
            "actual_volume": None,
            "trading_days_history": 0,
            "ma20_value": None,
            "ma60_value": None,
            "ma120_value": None,
            "ma240_value": None,
            "ma120_above_close": None,
        }

    exact = next((row for row in rows if str(row.get("date", ""))[:10] == signal_date), None)
    past_rows = [row for row in rows if (parsed := _parse_date(str(row.get("date", "")))) is not None and parsed <= target]
    closes = [row["close"] for row in past_rows if row.get("close") is not None]
    actual_close = exact.get("close") if exact else None
    ma120 = _ma(closes, 120)
    return {
        "actual_price_row": exact,
        "actual_close": actual_close,
        "actual_volume": exact.get("volume") if exact else None,
        "trading_days_history": len(past_rows),
        "ma20_value": _ma(closes, 20),
        "ma60_value": _ma(closes, 60),
        "ma120_value": ma120,
        "ma240_value": _ma(closes, 240),
        "ma120_above_close": (ma120 is not None and actual_close is not None and ma120 > actual_close) if exact else None,
    }


def _human_judgment(signal: dict[str, str], diagnosis: dict[str, Any], context: dict[str, Any]) -> tuple[str, str]:
    reasons = {_normalize_reason(reason) for reason in diagnosis.get("reasons_failed", [])}
    close = context.get("actual_close")
    history = context.get("trading_days_history")
    latest_holding = context.get("latest_holding_date")

    if diagnosis.get("eligible"):
        return "suspicious_filter", "diagnose 顯示 eligible=True，但此訊號在 filtered_out，需檢查 filtered list 生成方式。"
    if "no_price_data_on_date" in reasons:
        return "suspicious_filter", "診斷找不到 signal_date 前的價格資料；Legacy 卻曾產生訊號，這需要追查日期對齊或資料讀取。"
    if "close_below_min_20" in reasons and close is not None and close < MIN_CLOSE:
        return "confirmed_correct_filter", f"close {close:.2f} < {MIN_CLOSE:.0f}，低於策略最低股價門檻，過濾合理。"
    if "close_below_min_20" in reasons:
        return "need_human_review", "診斷指出低於最低股價門檻，但抽樣 row 無法確認 close，需人工核對原始價格資料。"
    if "insufficient_history_lt_130_days" in reasons:
        return "need_human_review", f"signal_date 前只有 {history} 筆價格資料，未達 MA120 所需 130 筆；可能是歷史資料起點不足。"
    if "no_holding_data" in reasons:
        return "need_human_review", f"最近股權分散資料為 {latest_holding or '無'}，不符合最近一週條件；需確認 holding_shares 歷史覆蓋。"
    return "need_human_review", f"失敗原因為 {', '.join(sorted(reasons)) or 'unknown'}，需人工核對。"


def _inspect_signal(signal: dict[str, str], diagnosis: dict[str, Any], filter_reason: str) -> dict[str, Any]:
    stock_id = signal["stock_id"]
    signal_date = signal["signal_date"]
    context = _price_context(stock_id, signal_date)
    latest_holding_date, latest_holding_rows = _latest_holding_snapshot(stock_id, signal_date)
    context["latest_holding_date"] = latest_holding_date
    status, text = _human_judgment(signal, diagnosis, context)
    return {
        "stock_id": stock_id,
        "signal_date": signal_date,
        "basket": signal["basket"],
        "filter_reason": filter_reason,
        "reasons_failed": [_normalize_reason(reason) for reason in diagnosis.get("reasons_failed", [])],
        "actual_price_row": context["actual_price_row"],
        "actual_close": context["actual_close"],
        "actual_volume": context["actual_volume"],
        "trading_days_history": context["trading_days_history"],
        "latest_holding_date": latest_holding_date,
        "latest_holding_rows": latest_holding_rows,
        "ma20_value": context["ma20_value"],
        "ma60_value": context["ma60_value"],
        "ma120_value": context["ma120_value"],
        "ma240_value": context["ma240_value"],
        "ma120_above_close": context["ma120_above_close"],
        "human_judgment_status": status,
        "human_judgment": text,
    }


def _pct(count: int, total: int) -> float:
    return round(count / total * 100, 1) if total else 0.0


def _load_reports_for_backtest() -> list[dict[str, Any]]:
    import generate_site as gs

    reports = _read_json(SITE_REPORTS_PATH)
    with contextlib.redirect_stdout(io.StringIO()):
        return gs.filter_listed_otc_reports(reports)


def _retained_pit_sanity_check() -> dict[str, Any]:
    import generate_site as gs
    from tools.pit_universe import diagnose_universe_eligibility

    reports = _load_reports_for_backtest()
    retained: list[dict[str, str]] = []
    for method, basket in METHODS:
        for row in gs._run_backtest_pit(reports, "2024-01-01", method):
            if row.get("sid") and row.get("signal_date"):
                retained.append(
                    {
                        "stock_id": str(row["sid"]),
                        "signal_date": str(row["signal_date"]),
                        "basket": basket,
                    }
                )

    rng = random.Random(RANDOM_SEED + 1)
    samples = rng.sample(retained, min(10, len(retained)))
    checked = []
    failures = []
    for sample in samples:
        diagnosis = diagnose_universe_eligibility(sample["stock_id"], sample["signal_date"], MIN_CLOSE)
        item = {**sample, "eligible": diagnosis["eligible"], "reasons_failed": diagnosis["reasons_failed"]}
        checked.append(item)
        if not diagnosis["eligible"]:
            failures.append(item)
    return {
        "total_retained": len(retained),
        "sample_size": len(samples),
        "checked": checked,
        "failures": failures,
    }


def build_audit() -> dict[str, Any]:
    from tools.pit_universe import diagnose_universe_eligibility

    signals = _load_filtered_signals()
    diagnosed = []
    reason_counts = Counter()
    reason_occurrences = Counter()
    by_basket_and_reason: dict[str, Counter] = defaultdict(Counter)
    groups: dict[str, list[tuple[dict[str, str], dict[str, Any]]]] = defaultdict(list)

    for signal in signals:
        diagnosis = diagnose_universe_eligibility(signal["stock_id"], signal["signal_date"], MIN_CLOSE)
        normalized = [_normalize_reason(reason) for reason in diagnosis.get("reasons_failed", [])]
        exclusive = _exclusive_reason(normalized)
        reason_counts[exclusive] += 1
        by_basket_and_reason[signal["basket"]][exclusive] += 1
        if len(set(normalized)) > 1:
            groups["multiple_reasons"].append((signal, diagnosis))
        for reason in sorted(set(normalized)):
            reason_occurrences[reason] += 1
            groups[reason].append((signal, diagnosis))
        diagnosed.append({**signal, "filter_reason": exclusive, "diagnosis": diagnosis})

    total = len(signals)
    rng = random.Random(RANDOM_SEED)
    sampled_ids: set[tuple[str, str, str, str]] = set()
    samples: list[dict[str, Any]] = []
    for reason in REASON_KEYS:
        candidates = groups.get(reason, [])
        if not candidates:
            continue
        picked = rng.sample(candidates, min(5, len(candidates)))
        for signal, diagnosis in picked:
            identity = (signal["stock_id"], signal["signal_date"], signal["basket"], reason)
            if identity in sampled_ids:
                continue
            sampled_ids.add(identity)
            samples.append(_inspect_signal(signal, diagnosis, reason))

    sample_status_counts = Counter(sample["human_judgment_status"] for sample in samples)
    retained_sanity_check = _retained_pit_sanity_check()

    audit = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total": total,
        "by_reason": {
            reason: {
                "count": reason_counts.get(reason, 0),
                "percentage": _pct(reason_counts.get(reason, 0), total),
            }
            for reason in REASON_KEYS
        },
        "reason_occurrences": {
            reason: {
                "count": reason_occurrences.get(reason, 0),
                "percentage_of_total": _pct(reason_occurrences.get(reason, 0), total),
            }
            for reason in REASON_KEYS
            if reason != "multiple_reasons"
        },
        "by_basket_and_reason": {
            basket: {reason: by_basket_and_reason[basket].get(reason, 0) for reason in REASON_KEYS}
            for _method, basket in METHODS
        },
        "sample_verdicts": {
            "confirmed_correct_filter": sample_status_counts.get("confirmed_correct_filter", 0),
            "suspicious_filter": sample_status_counts.get("suspicious_filter", 0),
            "need_human_review": sample_status_counts.get("need_human_review", 0),
            "sample_size": len(samples),
        },
        "samples": samples,
        "retained_pit_sanity_check": retained_sanity_check,
    }
    return audit


def _recommendation(sample_verdicts: dict[str, int]) -> str:
    size = sample_verdicts.get("sample_size", 0)
    confirmed = sample_verdicts.get("confirmed_correct_filter", 0)
    suspicious = sample_verdicts.get("suspicious_filter", 0)
    if size and suspicious / size >= 0.30:
        return "PIT 條件可能過嚴，建議審視"
    if size and confirmed / size >= 0.80:
        return "PIT 過濾邏輯正確"
    return "抽樣多屬資料覆蓋問題，建議補齊歷史價格或股權分散資料後再驗證"


def print_summary(audit: dict[str, Any]) -> None:
    print("=== PIT Filter Reasons Audit ===")
    print()
    print(f"Total filtered: {audit['total']}")
    print()
    print("By reason:")
    for reason in REASON_KEYS:
        item = audit["by_reason"][reason]
        print(f"  {reason}: {item['count']} 筆 ({item['percentage']:.1f}%)")
    print()
    verdicts = audit["sample_verdicts"]
    size = verdicts["sample_size"]
    print("Sample verdicts (manual judgment):")
    print(f"  Confirmed correct filter:   {verdicts['confirmed_correct_filter']} / {size} 樣本")
    print(f"  Suspicious filter:          {verdicts['suspicious_filter']} / {size} 樣本")
    print(f"  Need human review:          {verdicts['need_human_review']} / {size} 樣本")
    print()
    sanity = audit["retained_pit_sanity_check"]
    print("Retained PIT sanity check:")
    print(f"  Checked {sanity['sample_size']} / {sanity['total_retained']} retained PIT signals")
    print(f"  Ineligible retained samples: {len(sanity['failures'])}")
    print()
    print("Recommendation:")
    print(f"  {_recommendation(verdicts)}")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    audit = build_audit()
    SAMPLES_OUTPUT_PATH.write_text(
        json.dumps({"generated_at": audit["generated_at"], "samples": audit["samples"]}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    REASONS_OUTPUT_PATH.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print_summary(audit)
    print()
    print(f"Wrote {SAMPLES_OUTPUT_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Wrote {REASONS_OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
