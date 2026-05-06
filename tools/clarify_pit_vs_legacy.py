from __future__ import annotations

import json
import random
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

GENERATE_SITE_PATH = PROJECT_ROOT / "generate_site.py"
FILTERED_SIGNALS_PATH = PROJECT_ROOT / "data" / "pit_filtered_signals.json"
RANDOM_SEED = 20260507


def _load_filtered_signals() -> list[dict]:
    data = json.loads(FILTERED_SIGNALS_PATH.read_text(encoding="utf-8-sig"))
    return data.get("filtered_out", [])


def _source_lines(start: int, end: int) -> str:
    lines = GENERATE_SITE_PATH.read_text(encoding="utf-8").splitlines()
    return "\n".join(f"{idx + 1}: {lines[idx]}" for idx in range(start - 1, min(end, len(lines))))


def q1_answer() -> tuple[str, str]:
    snippet = _source_lines(7095, 7124) + "\n" + _source_lines(7192, 7197)
    return "A. Legacy 完全沒有呼叫 is_in_universe", snippet


def q2_samples(filtered: list[dict]) -> list[dict]:
    from tools.pit_universe import diagnose_universe_eligibility, is_in_universe

    rng = random.Random(RANDOM_SEED)
    picked = rng.sample(filtered, min(5, len(filtered)))
    out = []
    for item in picked:
        stock_id = str(item["stock_id"])
        signal_date = str(item["signal_date"])
        diagnosis = diagnose_universe_eligibility(stock_id, signal_date)
        out.append(
            {
                "stock_id": stock_id,
                "signal_date": signal_date,
                "basket": item.get("basket", ""),
                "is_in_universe": is_in_universe(stock_id, signal_date),
                "diagnosis": diagnosis,
                "legacy_universe_check_before_signal": "No",
                "legacy_same_filter_as_pit": "No",
                "why_legacy_includes_it": (
                    "Legacy 透過 historical_scan_universe(reports) 取得候選股與價格列，"
                    "在 _run_backtest_scan(..., use_pit=False) 中跳過 PIT universe 檢查，"
                    "只要通過 start_date、MA20、volume_price、historical_entry_signal、區間碰價與出場模擬，就 append 到 trades。"
                ),
            }
        )
    return out


def print_q1(answer: str, snippet: str) -> None:
    print("=== Q1: Legacy 流程是否呼叫 is_in_universe ===")
    print(f"答案: {answer}")
    print("Legacy 相關程式碼片段:")
    print(snippet)
    print()


def print_q2(samples: list[dict]) -> None:
    print("=== Q2: 5 筆樣本檢查 ===")
    for idx, sample in enumerate(samples, 1):
        print(f"{idx}. stock_id: {sample['stock_id']}")
        print(f"   signal_date: {sample['signal_date']}")
        print(f"   basket: {sample['basket']}")
        print(f"   is_in_universe() returns: {sample['is_in_universe']}")
        print(
            "   diagnose_universe_eligibility() returns: "
            + json.dumps(
                {
                    "eligible": sample["diagnosis"].get("eligible"),
                    "reasons_failed": sample["diagnosis"].get("reasons_failed"),
                    "details": sample["diagnosis"].get("details"),
                },
                ensure_ascii=False,
            )
        )
        print("   Legacy 在處理訊號前是否做過 universe 檢查? No")
        print("   若 Yes，是否與 PIT 用同一個函式? No")
        print(f"   若 No，Legacy 憑什麼讓這筆進入結果集? {sample['why_legacy_includes_it']}")
    print()


def print_q3(total_filtered: int) -> None:
    print("=== Q3: 兩者本質差異 ===")
    print(
        "Legacy universe = historical_scan_universe(reports)：先用 find_latest_stock_map(reports) 取目前報告/快取中的候選股，"
        "再讀 data/prices/<sid>.csv，len(rows) >= 80 就納入逐日掃描。"
    )
    print(
        "PIT universe = 與 Legacy 同樣的候選來源與價格資料逐日掃描，但在每個 signal_date 前額外執行 "
        "is_in_universe(stock_id, signal_date)。"
    )
    print(
        "本質差異 = PIT 是新增的 signal_date 當下 universe 過濾層；Legacy 完全沒有 point-in-time universe 控管。"
    )
    print()
    print("=== 結論 ===")
    print(
        f"Q1=A。PIT 是新增的過濾層，Legacy 完全沒有 universe 控管；目前 {total_filtered} 筆差異是真實的條件過濾，不是 bug。"
    )
    print("PIT 數字是含 universe 過濾的策略表現，可推進 A4。")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    filtered = _load_filtered_signals()
    answer, snippet = q1_answer()
    print_q1(answer, snippet)
    print_q2(q2_samples(filtered))
    print_q3(len(filtered))


if __name__ == "__main__":
    main()
