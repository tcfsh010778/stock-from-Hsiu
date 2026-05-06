from __future__ import annotations

import contextlib
import io
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DATA_DIR = PROJECT_ROOT / "data"
SITE_REPORTS_PATH = DATA_DIR / "site_reports.json"
DAILY_REPORTS_DIR = DATA_DIR / "daily_reports"
DOCS_REPORTS_DIR = PROJECT_ROOT / "docs" / "reports"
FILTERED_OUTPUT_PATH = DATA_DIR / "pit_filtered_signals.json"
AUDIT_OUTPUT_PATH = DATA_DIR / "pit_filter_audit.json"

METHODS = [
    ("sfz_ta3", "SFZ_TA3"),
    ("wr_after_attack", "Williams"),
]


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _stock_ids_from_report(report: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for key in ("stocks", "abc", "sfz", "sfz_pool", "williams", "williams_pool", "candidates"):
        value = report.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    stock_id = item.get("id") or item.get("stock_id") or item.get("sid")
                    if stock_id:
                        ids.add(str(stock_id).strip())
                elif isinstance(item, (str, int)):
                    match = re.search(r"\b\d{4}\b", str(item))
                    if match:
                        ids.add(match.group(0))
        elif isinstance(value, dict):
            ids.update(_stock_ids_from_report(value))
    return ids


def _validate_site_reports_shape(data: Any) -> None:
    if isinstance(data, list) and (not data or isinstance(data[0], dict)):
        return

    print("[ERROR] site_reports.json structure is not the expected list[dict].")
    if isinstance(data, dict):
        print(f"Top-level keys: {sorted(data.keys())}")
        first_value = next(iter(data.values()), None)
        print("First value shape:")
        print(json.dumps(first_value, ensure_ascii=False, indent=2)[:1200])
    else:
        print(f"Top-level type: {type(data).__name__}")
        print("First item:")
        try:
            print(json.dumps(data[0], ensure_ascii=False, indent=2)[:1200])
        except Exception:
            print(repr(data)[:1200])
    raise SystemExit(1)


def _add_report(report_index: dict[str, dict[str, Any]], report_date: str, stock_ids: set[str], source: str) -> None:
    if not report_date or not stock_ids:
        return
    item = report_index.setdefault(report_date, {"stock_ids": set(), "files": []})
    item["stock_ids"].update(stock_ids)
    item["files"].append(source)


def _load_site_reports(report_index: dict[str, dict[str, Any]], attempted_sources: list[str]) -> None:
    attempted_sources.append(str(SITE_REPORTS_PATH.relative_to(PROJECT_ROOT)))
    if not SITE_REPORTS_PATH.exists():
        return

    data = _read_json(SITE_REPORTS_PATH)
    _validate_site_reports_shape(data)
    for report in data:
        if not isinstance(report, dict):
            continue
        report_date = str(report.get("date") or "")
        _add_report(
            report_index,
            report_date,
            _stock_ids_from_report(report),
            str(SITE_REPORTS_PATH.relative_to(PROJECT_ROOT)),
        )


def _load_daily_reports(report_index: dict[str, dict[str, Any]], attempted_sources: list[str]) -> None:
    attempted_sources.append(str(DAILY_REPORTS_DIR.relative_to(PROJECT_ROOT)))
    if not DAILY_REPORTS_DIR.exists():
        return

    for path in sorted(DAILY_REPORTS_DIR.glob("*.json")):
        data = _read_json(path)
        reports = data if isinstance(data, list) else [data]
        for report in reports:
            if not isinstance(report, dict):
                continue
            report_date = str(report.get("date") or path.stem)
            _add_report(
                report_index,
                report_date,
                _stock_ids_from_report(report),
                str(path.relative_to(PROJECT_ROOT)),
            )


def _load_docs_reports(report_index: dict[str, dict[str, Any]], attempted_sources: list[str]) -> None:
    attempted_sources.append(str(DOCS_REPORTS_DIR.relative_to(PROJECT_ROOT)))
    if not DOCS_REPORTS_DIR.exists():
        return

    for path in sorted(DOCS_REPORTS_DIR.glob("*")):
        if path.suffix.lower() not in {".html", ".json", ".md"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        date_match = re.search(r"20\d{2}-\d{2}-\d{2}", path.name) or re.search(r"20\d{2}-\d{2}-\d{2}", text)
        if not date_match:
            continue
        stock_ids = set(re.findall(r"\b\d{4}\b", text))
        _add_report(
            report_index,
            date_match.group(0),
            stock_ids,
            str(path.relative_to(PROJECT_ROOT)),
        )


def _git(args: list[str]) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=PROJECT_ROOT,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )


def _load_git_history_reports(report_index: dict[str, dict[str, Any]], attempted_sources: list[str]) -> None:
    attempted_sources.append("git history:data/site_reports.json")
    try:
        commits = [
            line.strip()
            for line in _git(["log", "--format=%H", "--", "data/site_reports.json"]).splitlines()
            if line.strip()
        ]
    except Exception:
        return

    for commit in commits:
        try:
            raw = _git(["show", f"{commit}:data/site_reports.json"])
            data = json.loads(raw)
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for report in data:
            if not isinstance(report, dict):
                continue
            report_date = str(report.get("date") or "")
            _add_report(
                report_index,
                report_date,
                _stock_ids_from_report(report),
                f"git:{commit[:12]}:data/site_reports.json",
            )


def load_report_index() -> tuple[dict[str, dict[str, Any]], list[str]]:
    report_index: dict[str, dict[str, Any]] = {}
    attempted_sources: list[str] = []
    _load_site_reports(report_index, attempted_sources)
    _load_daily_reports(report_index, attempted_sources)
    _load_docs_reports(report_index, attempted_sources)
    _load_git_history_reports(report_index, attempted_sources)
    return report_index, attempted_sources


def load_reports_for_backtest() -> list[dict[str, Any]]:
    import generate_site as gs

    if not SITE_REPORTS_PATH.exists():
        return []
    reports = _read_json(SITE_REPORTS_PATH)
    _validate_site_reports_shape(reports)
    # filter_listed_otc_reports prints excluded names; suppress that noise for this audit script.
    with contextlib.redirect_stdout(io.StringIO()):
        return gs.filter_listed_otc_reports(reports)


def build_filtered_signals(reports: list[dict[str, Any]]) -> tuple[list[dict[str, str]], dict[str, dict[str, int]]]:
    import generate_site as gs
    from tools.pit_universe import is_in_universe

    filtered: list[dict[str, str]] = []
    method_counts: dict[str, dict[str, int]] = {}
    for method, basket in METHODS:
        legacy = gs._run_backtest_legacy(reports, "2024-01-01", method)
        pit = gs._run_backtest_pit(reports, "2024-01-01", method)
        missing = sorted(
            {
                (str(row.get("sid", "")), str(row.get("signal_date", "")), basket)
                for row in legacy
                if row.get("sid")
                and row.get("signal_date")
                and not is_in_universe(str(row.get("sid", "")), str(row.get("signal_date", "")))
            },
            key=lambda item: (item[1], item[0], item[2]),
        )
        filtered.extend(
            {"stock_id": stock_id, "signal_date": signal_date, "basket": basket}
            for stock_id, signal_date, basket in missing
        )
        method_counts[basket] = {
            "legacy": len(legacy),
            "pit": len(pit),
            "direct_pit_rejections": len(missing),
        }
    return filtered, method_counts


def find_matching_report(signal_date: str, stock_id: str, report_index: dict[str, dict[str, Any]]) -> tuple[str, str | None]:
    parsed_signal_date = _parse_date(signal_date)
    if parsed_signal_date is None or not report_index:
        return "no_report_for_date", None

    candidate_dates: list[tuple[int, str]] = []
    for report_date in report_index:
        parsed_report_date = _parse_date(report_date)
        if parsed_report_date is None:
            continue
        delta = abs((parsed_report_date - parsed_signal_date).days)
        if delta <= 3:
            candidate_dates.append((delta, report_date))

    if not candidate_dates:
        return "no_report_for_date", None

    for _delta, report_date in sorted(candidate_dates):
        report = report_index[report_date]
        if stock_id in report["stock_ids"]:
            return "in_report", report["files"][0]
    return "not_in_report", report_index[sorted(candidate_dates)[0][1]]["files"][0]


def build_audit(filtered: list[dict[str, str]], method_counts: dict[str, dict[str, int]], report_index: dict[str, dict[str, Any]], attempted_sources: list[str]) -> dict[str, Any]:
    enriched: list[dict[str, str]] = []
    for signal in filtered:
        status, report_file = find_matching_report(signal["signal_date"], signal["stock_id"], report_index)
        item = dict(signal)
        item["status"] = status
        if report_file:
            item["report_file"] = report_file
        enriched.append(item)

    total = len(enriched)
    status_counts = Counter(item["status"] for item in enriched)
    by_status = {
        status: {
            "count": status_counts.get(status, 0),
            "percentage": round(status_counts.get(status, 0) / total * 100, 1) if total else 0.0,
        }
        for status in ("in_report", "not_in_report", "no_report_for_date")
    }

    basket_counts: dict[str, Counter] = defaultdict(Counter)
    for item in enriched:
        basket_counts[item["basket"]][item["status"]] += 1
        basket_counts[item["basket"]]["total"] += 1

    by_basket = {}
    for _method, basket in METHODS:
        counts = basket_counts[basket]
        by_basket[basket] = {
            "total": counts.get("total", 0),
            "in_report": counts.get("in_report", 0),
            "not_in_report": counts.get("not_in_report", 0),
            "no_report_for_date": counts.get("no_report_for_date", 0),
            "legacy": method_counts.get(basket, {}).get("legacy", 0),
            "pit": method_counts.get(basket, {}).get("pit", 0),
            "direct_pit_rejections": method_counts.get(basket, {}).get("direct_pit_rejections", 0),
        }

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_filtered_signals": total,
        "by_status": by_status,
        "by_basket": by_basket,
        "samples_in_report": [
            item for item in enriched if item["status"] == "in_report"
        ][:10],
        "samples_not_in_report": [
            item for item in enriched if item["status"] == "not_in_report"
        ][:10],
        "samples_no_report_for_date": [
            item for item in enriched if item["status"] == "no_report_for_date"
        ][:10],
        "report_sources_attempted": attempted_sources,
        "report_dates_available": sorted(report_index),
    }


def print_summary(audit: dict[str, Any]) -> None:
    total = audit["total_filtered_signals"]
    by_status = audit["by_status"]
    in_pct = by_status["in_report"]["percentage"]
    no_report_pct = by_status["no_report_for_date"]["percentage"]
    if no_report_pct > 50:
        verdict = "無法判斷：歷史報告資料不足"
    elif in_pct < 10:
        verdict = "PIT 過濾正確"
    elif in_pct <= 30:
        verdict = "PIT 略微過頭，建議審視條件"
    else:
        verdict = "PIT 過頭，需要放寬條件"

    print("=== PIT Filter Audit Result ===")
    print(f"Total filtered signals: {total}")
    print()
    print(f"In Report (PIT 過頭，誤刪真實訊號):  {by_status['in_report']['count']} 筆 ({by_status['in_report']['percentage']:.1f}%)")
    print(f"Not In Report (PIT 正確過濾):       {by_status['not_in_report']['count']} 筆 ({by_status['not_in_report']['percentage']:.1f}%)")
    print(f"No Report (無法判斷):                {by_status['no_report_for_date']['count']} 筆 ({by_status['no_report_for_date']['percentage']:.1f}%)")
    print()
    print("By basket:")
    for basket, row in audit["by_basket"].items():
        print(f"  {basket}: in={row['in_report']}, not_in={row['not_in_report']}, no_report={row['no_report_for_date']}")
    print()
    print("Verdict:")
    print(f"  {verdict}")
    if no_report_pct > 50:
        print("  注意：no_report_for_date > 50%，歷史報告資料不足，這次驗證仍有資料覆蓋限制。")
        print("  嘗試過的資料源：")
        for source in audit.get("report_sources_attempted", []):
            print(f"    - {source}")
        earliest = min(audit.get("report_dates_available", []) or ["無"])
        print(f"  當前能找到的最早報告日期：{earliest}")


def warn_if_report_sources_missing(report_index: dict[str, dict[str, Any]], attempted_sources: list[str]) -> None:
    if report_index:
        return
    print("[WARN] 找不到歷史報告資料源，無法完成驗證")
    print("嘗試過的路徑：")
    for source in attempted_sources:
        print(f"  - {source}")
    print("當前能找到的最早報告日期：無")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    reports = load_reports_for_backtest()
    if not reports:
        print("[WARN] 找不到歷史報告資料源，無法完成驗證")
        print(f"嘗試過的路徑：\n  - {SITE_REPORTS_PATH.relative_to(PROJECT_ROOT)}")
        return

    report_index, attempted_sources = load_report_index()
    warn_if_report_sources_missing(report_index, attempted_sources)
    earliest_report = min(report_index) if report_index else "無"
    if report_index:
        print(f"[Info] Earliest report date found: {earliest_report}")

    filtered, method_counts = build_filtered_signals(reports)
    FILTERED_OUTPUT_PATH.write_text(
        json.dumps({"filtered_out": filtered, "total": len(filtered)}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    audit = build_audit(filtered, method_counts, report_index, attempted_sources)
    AUDIT_OUTPUT_PATH.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print_summary(audit)
    print()
    print(f"Wrote {FILTERED_OUTPUT_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Wrote {AUDIT_OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
