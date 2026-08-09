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


def fail(message: str) -> None:
    raise SystemExit(f"daily update artifact verification failed: {message}")


def verify_artifacts(root: Path) -> VerificationResult:
    reports = root / "reports"
    index = root / "docs" / "index.html"
    site_reports = root / "data" / "site_reports.json"

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
        page_text = page.read_text(encoding="utf-8", errors="replace")
        if latest not in page_text:
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

    print(f"verified latest report date: {latest}")
    print(f"report date count: {len(dates)}")
    return VerificationResult(latest_report_date=latest, report_count=len(dates))


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify daily update outputs are internally current.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    verify_artifacts(args.root.resolve())


if __name__ == "__main__":
    main()
