# -*- coding: utf-8 -*-
"""Record TDCC holder refresh checks and expose a GitHub Actions update gate."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from tdcc_holder_snapshot import _date, _field, fetch_rows


ROOT = Path(__file__).resolve().parent
STATUS_PATH = ROOT / "data" / "holder_update_status.json"
PUBLISHED_PATH = ROOT / "data" / "weekly_holder_risers.json"
TAIPEI_TZ = timezone(timedelta(hours=8))
MAX_ATTEMPTS = 20
SCHEDULE_TEXT = "週五 21:30；週六、週日、週一 09:30（Asia/Taipei）"
PUBLISH_CONDITION = (
    "TDCC 官方資料日期晚於目前頁面日期，且六週日期連續、Top 50 每列皆有完整六週資料"
)


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def official_source_date(rows: list[dict[str, Any]]) -> str:
    dates = {_date(_field(row, "資料日期")) for row in rows}
    dates.discard("")
    if len(dates) != 1:
        raise RuntimeError(f"TDCC snapshot dates are not aligned: {sorted(dates)}")
    return next(iter(dates))


def run_url() -> str:
    repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
    run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    return f"https://github.com/{repository}/actions/runs/{run_id}" if repository and run_id else ""


def checked_at() -> str:
    return datetime.now(TAIPEI_TZ).isoformat(timespec="seconds")


def write_github_output(path: str, **values: str) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def append_check(
    status: dict[str, Any],
    *,
    source_date: str,
    published_date: str,
    timestamp: str,
    run_id: str,
    trigger: str,
    schedule: str,
    url: str,
) -> tuple[dict[str, Any], bool]:
    update_required = bool(source_date and source_date > published_date)
    result = "update_available" if update_required else "waiting_for_tdcc"
    attempt = {
        "checked_at": timestamp,
        "trigger": trigger or "manual",
        "schedule": schedule,
        "official_date": source_date,
        "published_date_before": published_date,
        "published_date_after": published_date,
        "result": result,
        "run_id": run_id,
        "run_url": url,
    }
    attempts = [item for item in status.get("attempts") or [] if isinstance(item, dict)]
    attempts.append(attempt)
    status.update(
        {
            "dataset_id": "holder_update_status",
            "schema_version": "1.0.0",
            "schedule_timezone": "Asia/Taipei",
            "check_schedule": SCHEDULE_TEXT,
            "publish_condition": PUBLISH_CONDITION,
            "last_checked_at": timestamp,
            "official_latest_date": source_date,
            "published_data_date": published_date,
            "state": result,
            "attempts": attempts[-MAX_ATTEMPTS:],
        }
    )
    return status, update_required


def finalize_run(status: dict[str, Any], published_date: str, run_id: str) -> dict[str, Any]:
    attempts = [item for item in status.get("attempts") or [] if isinstance(item, dict)]
    target = next((item for item in reversed(attempts) if str(item.get("run_id") or "") == run_id), None)
    if target is None and attempts:
        target = attempts[-1]
    if target is not None:
        target["published_date_after"] = published_date
        source_date = str(target.get("official_date") or "")
        before = str(target.get("published_date_before") or "")
        target["result"] = "published" if published_date == source_date and published_date > before else "verification_failed"
        status["state"] = target["result"]
    status["published_data_date"] = published_date
    status["attempts"] = attempts[-MAX_ATTEMPTS:]
    return status


def write_status(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check TDCC holder date and record refresh status.")
    parser.add_argument("--status", type=Path, default=STATUS_PATH)
    parser.add_argument("--published", type=Path, default=PUBLISHED_PATH)
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    parser.add_argument("--finalize", action="store_true")
    args = parser.parse_args()

    status = read_json(args.status)
    published_date = str(read_json(args.published).get("date") or "")
    current_run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    if args.finalize:
        status = finalize_run(status, published_date, current_run_id)
        write_status(status, args.status)
        if status.get("state") != "published":
            raise RuntimeError("holder refresh did not publish the official latest date")
        print(f"[holder_update_status] published={published_date}")
        return 0

    source_date = official_source_date(fetch_rows())
    status, update_required = append_check(
        status,
        source_date=source_date,
        published_date=published_date,
        timestamp=checked_at(),
        run_id=current_run_id,
        trigger=os.environ.get("GITHUB_EVENT_NAME", "manual"),
        schedule=os.environ.get("HOLDER_EVENT_SCHEDULE", ""),
        url=run_url(),
    )
    write_status(status, args.status)
    write_github_output(
        args.github_output,
        update_required="true" if update_required else "false",
        official_date=source_date,
        published_date=published_date,
    )
    print(
        f"[holder_update_status] official={source_date} published={published_date or '-'} "
        f"update_required={str(update_required).lower()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
