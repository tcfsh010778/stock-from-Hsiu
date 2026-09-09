from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class VerificationResult:
    latest_report_date: str
    report_count: int


def report_dates(reports_dir: Path) -> list[str]:
    dates: list[str] = []
    for path in reports_dir.glob("*.md"):
        match = DATE_PATTERN.search(path.name)
        if match:
            dates.append(match.group(1))
    return sorted(set(dates))


def collect_json_dates(value: Any) -> set[str]:
    dates: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"date", "report_date"} and isinstance(item, str) and DATE_PATTERN.fullmatch(item):
                dates.add(item)
            else:
                dates.update(collect_json_dates(item))
    elif isinstance(value, list):
        for item in value:
            dates.update(collect_json_dates(item))
    return dates


def html_pages(docs_dir: Path) -> list[Path]:
    if not docs_dir.exists():
        fail(f"missing {docs_dir}")
    return sorted(
        path
        for path in docs_dir.rglob("*.html")
        if path.is_file() and "v2" not in path.relative_to(docs_dir).parts
    )


def contains_text(path: Path, needle: str) -> bool:
    """Stop at the freshness header instead of loading megabytes of chart JSON."""
    carry = ""
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        while chunk := handle.read(65536):
            text = carry + chunk
            if needle in text:
                return True
            carry = text[-max(len(needle) - 1, 0):]
    return False


def fail(message: str) -> None:
    raise SystemExit(f"daily update artifact verification failed: {message}")


def verify_artifacts(root: Path, expected_session: str | None = None) -> VerificationResult:
    reports = root / "reports"
    index = root / "docs" / "index.html"
    site_reports = root / "data" / "site_reports.json"
    market_flow = root / "data" / "daily_market_flow.json"

    dates = report_dates(reports)
    if not dates:
        fail(f"no report dates found under {reports}")
    latest = dates[-1]

    if not index.exists():
        fail(f"missing {index}")
    index_text = index.read_text(encoding="utf-8", errors="replace")
    if latest not in index_text:
        fail(f"latest report date {latest} is missing from {index}")

    stale_pages: list[Path] = []
    for page in html_pages(root / "docs"):
        if not contains_text(page, latest):
            stale_pages.append(page.relative_to(root))
    if stale_pages:
        sample = ", ".join(str(path) for path in stale_pages[:10])
        more = "" if len(stale_pages) <= 10 else f", ... (+{len(stale_pages) - 10} more)"
        fail(f"latest report date {latest} is missing from generated HTML pages: {sample}{more}")

    if not site_reports.exists():
        fail(f"missing {site_reports}")
    try:
        payload = json.loads(site_reports.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        fail(f"{site_reports} is not valid JSON: {exc}")
    json_dates = collect_json_dates(payload)
    if latest not in json_dates:
        fail(f"latest report date {latest} is missing from {site_reports}")

    if not market_flow.exists():
        fail(f"missing {market_flow}")
    try:
        flow_payload = json.loads(market_flow.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        fail(f"{market_flow} is not valid JSON: {exc}")
    flow_date = str(flow_payload.get("date") or "")
    if not DATE_PATTERN.fullmatch(flow_date) or flow_date < latest:
        fail(f"market-flow date {flow_date or 'missing'} is older than latest report date {latest}")
    if (flow_payload.get("data_quality") or {}).get("state") != "ok":
        fail("market-flow official partitions are incomplete")
    markets = flow_payload.get("markets") or {}
    if any(int((markets.get(market) or {}).get("stock_count") or 0) <= 0 for market in ("listed", "otc")):
        fail("market-flow listed or OTC detail rows are empty")

    if expected_session is not None:
        if not DATE_PATTERN.fullmatch(expected_session) or latest != expected_session or flow_date != expected_session:
            fail(f"common session mismatch: expected={expected_session}, report={latest}, flow={flow_date}")
        manifest_path = root / "data" / "official_adjusted_update_manifest.json"
        if not manifest_path.exists():
            fail("verified adjusted update manifest is missing")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest.get("dataset_id") != "official_adjusted_daily_update"
                or manifest.get("data_as_of") != expected_session
                or manifest.get("status") not in {"complete", "partial", "current"}):
            fail("verified adjusted update manifest does not match the common session")
        queue = json.loads((root / "data" / "review_queue.json").read_text(encoding="utf-8"))
        if queue.get("as_of") != expected_session:
            fail(f"review queue session mismatch: {queue.get('as_of')}")
        for name in (".v2-staging", ".v2-previous"):
            if (root / "docs" / name).exists():
                fail(f"unfinished V2 recovery directory exists: {name}")

    print(f"verified latest report date: {latest}")
    print(f"report date count: {len(dates)}")
    return VerificationResult(latest_report_date=latest, report_count=len(dates))


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify daily update outputs are internally current.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--expected-session")
    args = parser.parse_args()
    verify_artifacts(args.root.resolve(), args.expected_session)


if __name__ == "__main__":
    main()
