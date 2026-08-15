# -*- coding: utf-8 -*-
"""Backfill the latest six-week major-holder ranking from official TDCC pages.

The current full-market snapshot comes from TDCC OpenAPI 1-5. TDCC exposes
historical shareholder-distribution data through its public per-security query
page, so this module uses that page conservatively: query the immediately prior
week for the current ordinary-equity universe, rank the latest positive changes,
then fetch the remaining five weeks only for enough leading candidates to
produce a complete Top 50. Only compact 400+ lot and 200-lot-or-less
aggregates are retained.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Iterable

import requests

from tdcc_holder_snapshot import (
    OUTPUT_PATH,
    TAIPEI_TZ,
    aggregate_snapshot,
    fetch_rows,
    load_security_map,
    merge_archive,
    write_archive,
)


TDCC_HISTORY_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
USER_AGENT = "stock-from-Hsiu/1.0 (+https://github.com/tcfsh010778/stock-from-Hsiu)"
MAJOR_SEQUENCES = {"12", "13", "14", "15"}
RETAIL_200_SEQUENCES = {str(level) for level in range(1, 11)}


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return default


def _iso_date(value: str) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return ""


class TdccPageParser(HTMLParser):
    """Extract query tokens, available dates, and result-table cells."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.inputs: dict[str, str] = {}
        self.available_dates: list[str] = []
        self.tables: list[list[list[str]]] = []
        self._sca_select_depth = 0
        self._table_depth = 0
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    @staticmethod
    def _attrs(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {str(key): str(value or "") for key, value in attrs}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = self._attrs(attrs)
        if tag == "input" and values.get("name"):
            self.inputs[values["name"]] = values.get("value", "")
        if tag == "select" and values.get("name") == "scaDate":
            self._sca_select_depth = 1
        elif self._sca_select_depth:
            self._sca_select_depth += 1
        if tag == "option" and self._sca_select_depth and values.get("value"):
            data_date = _iso_date(values["value"])
            if data_date and data_date not in self.available_dates:
                self.available_dates.append(data_date)

        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._table = []
        elif tag == "tr" and self._table_depth == 1:
            self._row = []
        elif tag in {"td", "th"} and self._table_depth == 1 and self._row is not None:
            self._cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._sca_select_depth:
            self._sca_select_depth -= 1
        if tag in {"td", "th"} and self._cell_parts is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell_parts).split()))
            self._cell_parts = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if self._row:
                self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1 and self._table is not None:
                self.tables.append(self._table)
                self._table = None
            self._table_depth -= 1


def parse_query_page(html: str) -> TdccPageParser:
    parser = TdccPageParser()
    parser.feed(html)
    parser.close()
    return parser


def extract_holder_aggregates(parser: TdccPageParser) -> dict[str, Any] | None:
    for table in parser.tables:
        if not table:
            continue
        header = " ".join(table[0])
        if "持股/單位數分級" not in header or "占集保庫存數比例" not in header:
            continue
        major_percent = 0.0
        major_people = 0
        retail_200_percent = 0.0
        found_major: set[str] = set()
        found_retail: set[str] = set()
        for cells in table[1:]:
            if len(cells) < 5:
                continue
            if cells[0] in MAJOR_SEQUENCES:
                found_major.add(cells[0])
                major_people += int(_number(cells[2]))
                major_percent += _number(cells[4])
            elif cells[0] in RETAIL_200_SEQUENCES:
                found_retail.add(cells[0])
                retail_200_percent += _number(cells[4])
        if found_major == MAJOR_SEQUENCES and found_retail == RETAIL_200_SEQUENCES:
            return {
                "major_percent": round(major_percent, 2),
                "major_people": major_people,
                "retail_200_percent": round(retail_200_percent, 2),
            }
    return None


def extract_major_aggregate(parser: TdccPageParser) -> dict[str, Any] | None:
    """Backward-compatible name for callers of the former major-only parser."""

    return extract_holder_aggregates(parser)


class RequestRateLimiter:
    def __init__(self, minimum_interval: float) -> None:
        self.minimum_interval = max(0.0, float(minimum_interval))
        self._lock = threading.Lock()
        self._next_start = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_start - now)
            self._next_start = max(now, self._next_start) + self.minimum_interval
        if delay:
            time.sleep(delay)


class TdccHistoryClient:
    def __init__(self, *, limiter: RequestRateLimiter, timeout: int = 45) -> None:
        self.session = requests.Session()
        self.limiter = limiter
        self.timeout = timeout
        self.token = ""
        self.first_date = ""
        self.available_dates: list[str] = []

    def _request(self, method: str, **kwargs: Any) -> requests.Response:
        self.limiter.wait()
        response = self.session.request(
            method,
            TDCC_HISTORY_URL,
            timeout=self.timeout,
            headers={"User-Agent": USER_AGENT},
            **kwargs,
        )
        response.raise_for_status()
        return response

    def bootstrap(self) -> TdccPageParser:
        parser = parse_query_page(self._request("GET").text)
        self.token = parser.inputs.get("SYNCHRONIZER_TOKEN", "")
        self.first_date = parser.inputs.get("firDate", "")
        self.available_dates = parser.available_dates
        if not self.token or len(self.available_dates) < 7:
            raise RuntimeError("TDCC historical query form is missing its token or weekly dates")
        return parser

    def query(self, security_id: str, data_date: str, attempts: int = 3) -> dict[str, Any] | None:
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                if not self.token:
                    self.bootstrap()
                compact_date = data_date.replace("-", "")
                response = self._request(
                    "POST",
                    data={
                        "SYNCHRONIZER_TOKEN": self.token,
                        "SYNCHRONIZER_URI": "/portal/zh/smWeb/qryStock",
                        "method": "submit",
                        "firDate": self.first_date,
                        "scaDate": compact_date,
                        "sqlMethod": "StockNo",
                        "stockNo": security_id,
                        "stockName": "",
                    },
                )
                parser = parse_query_page(response.text)
                self.token = parser.inputs.get("SYNCHRONIZER_TOKEN", "")
                if not self.token:
                    raise RuntimeError("TDCC historical response omitted the synchronizer token")
                return extract_holder_aggregates(parser)
            except (requests.RequestException, RuntimeError) as exc:
                last_error = exc
                self.session.close()
                self.session = requests.Session()
                self.token = ""
                if attempt + 1 < attempts:
                    time.sleep(1.0 + attempt * 2.0)
        raise RuntimeError(f"TDCC historical query failed for {security_id} {data_date}: {last_error}")


class TdccHistoryFetcher:
    def __init__(self, *, workers: int = 4, request_interval: float = 0.25, timeout: int = 45) -> None:
        self.workers = max(1, min(int(workers), 6))
        self.limiter = RequestRateLimiter(request_interval)
        self.timeout = timeout
        self._local = threading.local()

    def client(self) -> TdccHistoryClient:
        client = getattr(self._local, "client", None)
        if client is None:
            client = TdccHistoryClient(limiter=self.limiter, timeout=self.timeout)
            self._local.client = client
        return client

    def available_dates(self) -> list[str]:
        return TdccHistoryClient(limiter=self.limiter, timeout=self.timeout).bootstrap().available_dates

    def fetch_many(
        self,
        security_ids: Iterable[str],
        data_date: str,
        security_map: dict[str, dict[str, str]],
        *,
        progress: Callable[[int, int, str], None] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        codes = list(dict.fromkeys(str(code) for code in security_ids if str(code)))

        def task(code: str) -> tuple[str, dict[str, Any] | None]:
            return code, self.client().query(code, data_date)

        rows: list[dict[str, Any]] = []
        no_data = 0
        errors: list[str] = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(task, code): code for code in codes}
            for completed, future in enumerate(as_completed(futures), 1):
                code = futures[future]
                try:
                    _, aggregate = future.result()
                except Exception as exc:
                    errors.append(f"{code}: {exc}")
                else:
                    if aggregate is None:
                        no_data += 1
                    else:
                        ref = security_map.get(code) or {}
                        rows.append({
                            "security_id": code,
                            "name": str(ref.get("name") or ""),
                            "market": str(ref.get("market") or ""),
                            **aggregate,
                        })
                if progress and (completed == 1 or completed % 100 == 0 or completed == len(codes)):
                    progress(completed, len(codes), data_date)
        if errors:
            sample = "; ".join(errors[:5])
            raise RuntimeError(f"TDCC historical backfill had {len(errors)} failed queries: {sample}")
        rows.sort(key=lambda row: str(row["security_id"]))
        return rows, no_data


def _snapshot_by_date(archive: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(snapshot.get("date")): snapshot
        for snapshot in archive.get("snapshots") or []
        if isinstance(snapshot, dict) and snapshot.get("date") and isinstance(snapshot.get("rows"), list)
    }


def _rows_by_code(snapshot: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("security_id")): row
        for row in (snapshot or {}).get("rows") or []
        if isinstance(row, dict) and row.get("security_id")
    }


def _progress(done: int, total: int, data_date: str) -> None:
    print(f"[tdcc_holder_history] {data_date} {done}/{total}", flush=True)


def build_latest_history(
    archive: dict[str, Any],
    *,
    security_map: dict[str, dict[str, str]],
    fetcher: TdccHistoryFetcher,
    ranking_limit: int = 50,
) -> dict[str, Any]:
    by_date = _snapshot_by_date(archive)
    latest_date = str(archive.get("latest_date") or "")
    latest = by_date.get(latest_date)
    if not latest or not latest.get("rows"):
        latest = aggregate_snapshot(fetch_rows(), security_map)
        latest_date = str(latest["date"])
        archive = merge_archive(latest, archive)
        by_date = _snapshot_by_date(archive)

    available = fetcher.available_dates()
    if latest_date not in available:
        raise RuntimeError(f"TDCC historical form does not contain current archive date {latest_date}")
    latest_index = available.index(latest_date)
    window_dates = available[latest_index:latest_index + 7]
    if len(window_dates) < 7:
        raise RuntimeError("TDCC historical form exposes fewer than seven dates from the current snapshot")
    previous_date = window_dates[1]

    latest_rows = _rows_by_code(latest)
    universe = [code for code in latest_rows if code in security_map]
    previous = by_date.get(previous_date)
    previous_rows = _rows_by_code(previous)
    previous_has_retail = previous_rows and all("retail_200_percent" in row for row in previous_rows.values())
    if len(previous_rows) < max(1, int(len(universe) * 0.9)) or not previous_has_retail:
        rows, no_data = fetcher.fetch_many(universe, previous_date, security_map, progress=_progress)
        previous = {"date": previous_date, "rows": rows}
    else:
        no_data = max(0, len(universe) - len(previous_rows))
    previous_rows = _rows_by_code(previous)

    ranked = []
    for code, current in latest_rows.items():
        earlier = previous_rows.get(code)
        if earlier is None:
            continue
        delta = round(_number(current.get("major_percent")) - _number(earlier.get("major_percent")), 2)
        if delta > 0:
            ranked.append((delta, code))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    if len(ranked) < ranking_limit:
        raise RuntimeError(f"TDCC latest comparison produced only {len(ranked)} positive stocks")

    older_dates = window_dates[2:]
    historical_rows = {data_date: _rows_by_code(by_date.get(data_date)) for data_date in older_dates}
    complete_codes: list[str] = []
    queried_codes: set[str] = set()
    batch_start = 0
    first_batch = min(len(ranked), ranking_limit + 10)
    while len(complete_codes) < ranking_limit and batch_start < len(ranked):
        batch_end = first_batch if batch_start == 0 else min(len(ranked), batch_start + 20)
        batch_codes = [code for _, code in ranked[batch_start:batch_end]]
        for data_date in older_dates:
            missing = [code for code in batch_codes if code not in historical_rows[data_date]]
            if missing:
                rows, _ = fetcher.fetch_many(missing, data_date, security_map, progress=_progress)
                historical_rows[data_date].update({str(row["security_id"]): row for row in rows})
        queried_codes.update(batch_codes)
        complete_codes = [
            code
            for _, code in ranked
            if code in queried_codes and all(code in historical_rows[data_date] for data_date in older_dates)
        ]
        batch_start = batch_end
    if len(complete_codes) < ranking_limit:
        raise RuntimeError(f"TDCC history produced only {len(complete_codes)} complete leading candidates")
    selected = complete_codes[:ranking_limit]

    output = archive
    output = merge_archive({"date": previous_date, "rows": list(previous_rows.values())}, output)
    for data_date in older_dates:
        rows = [historical_rows[data_date][code] for code in queried_codes if code in historical_rows[data_date]]
        rows.sort(key=lambda row: str(row["security_id"]))
        output = merge_archive({"date": data_date, "rows": rows}, output)
    output["schema_version"] = "1.1.0"
    output["source_urls"] = [
        "https://openapi.tdcc.com.tw/v1/opendata/1-5",
        TDCC_HISTORY_URL,
    ]
    output["history_backfill"] = {
        "method": "official_tdcc_current_bulk_then_rate_limited_per_security_history",
        "window_dates": window_dates,
        "ranking_limit": ranking_limit,
        "ranking_basis": "latest_week_major_holder_change_pctpt_desc",
        "universe_count": len(universe),
        "previous_week_row_count": len(previous_rows),
        "previous_week_no_data_count": no_data,
        "candidate_history_query_count": len(queried_codes),
        "complete_candidate_count": len(complete_codes),
        "selected_security_ids": selected,
        "completed_at": datetime.now(TAIPEI_TZ).isoformat(),
    }
    output["updated_at"] = datetime.now(TAIPEI_TZ).isoformat()
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill the latest official TDCC six-week Top 50 holder history.")
    parser.add_argument("--archive", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--request-interval", type=float, default=0.25)
    args = parser.parse_args()

    try:
        archive = json.loads(args.archive.read_text(encoding="utf-8-sig"))
    except Exception:
        archive = {}
    security_map = load_security_map()
    if not security_map:
        raise RuntimeError("stock market cache has no ordinary listed/OTC equities")
    payload = build_latest_history(
        archive,
        security_map=security_map,
        fetcher=TdccHistoryFetcher(workers=args.workers, request_interval=args.request_interval),
        ranking_limit=max(1, args.limit),
    )
    write_archive(payload, args.archive)
    meta = payload["history_backfill"]
    print(
        f"[tdcc_holder_history] wrote {args.archive} dates={','.join(meta['window_dates'])} "
        f"selected={len(meta['selected_security_ids'])}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
