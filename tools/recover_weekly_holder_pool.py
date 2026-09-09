"""Recover one complete historical TDCC weekly holder snapshot.

This uses TDCC's public qryStock form conservatively.  A row is accepted only
when all distribution levels 1..17 occur exactly once and their numeric fields
are finite.  The checkpoint is local market data and must not be committed.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
import time
from datetime import date as calendar_date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

import requests

URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryStock"
USER_AGENT = "stock-from-Hsiu/1.0 (+https://github.com/tcfsh010778/stock-from-Hsiu)"
BANDS = set(range(1, 16))
MAJOR_LEVELS = {12, 13, 14, 15}


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.inputs: dict[str, str] = {}
        self.dates: list[str] = []
        self.selected_date = ""
        self.tables: list[list[list[str]]] = []
        self._select = 0
        self._depth = 0
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k: str(v or "") for k, v in attrs}
        if tag == "input" and a.get("name"):
            self.inputs[a["name"]] = a.get("value", "")
        if tag == "select" and a.get("name") == "scaDate": self._select = 1
        elif self._select: self._select += 1
        if tag == "option" and self._select and a.get("value"):
            v = a["value"].replace("-", "")
            if len(v) == 8 and v.isdigit():
                d = f"{v[:4]}-{v[4:6]}-{v[6:]}"
                if d not in self.dates: self.dates.append(d)
                if "selected" in a: self.selected_date = d
        if tag == "table":
            self._depth += 1
            if self._depth == 1: self._table = []
        elif tag == "tr" and self._depth == 1: self._row = []
        elif tag in {"td", "th"} and self._depth == 1 and self._row is not None: self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None: self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._select: self._select -= 1
        if tag in {"td", "th"} and self._cell is not None and self._row is not None:
            self._row.append(" ".join("".join(self._cell).split())); self._cell = None
        elif tag == "tr" and self._row is not None and self._table is not None:
            if self._row: self._table.append(self._row)
            self._row = None
        elif tag == "table" and self._depth:
            if self._depth == 1 and self._table is not None: self.tables.append(self._table); self._table = None
            self._depth -= 1


def parse_page(text: str) -> PageParser:
    p = PageParser(); p.feed(text); p.close(); return p


def strict_distribution(parser: PageParser) -> dict[str, Any] | None:
    for table in parser.tables:
        if not table or "持股/單位數分級" not in " ".join(table[0]): continue
        rows: dict[int, dict[str, Any]] = {}
        for cells in table[1:]:
            if len(cells) < 5 or not cells[0].strip().isdigit(): continue
            level = int(cells[0].strip())
            if level not in set(range(1, 18)) or level in rows: return None
            try:
                shares = float(cells[3].replace(",", ""))
                percent = float(cells[4].replace(",", "").replace("%", ""))
            except ValueError: return None
            is_adjustment = "差異數調整" in cells[1]
            people_text = cells[2].replace(",", "").strip()
            if is_adjustment and not people_text: people = 0.0
            else:
                try: people = float(people_text)
                except ValueError: return None
            if not all(math.isfinite(x) for x in (people, shares, percent)): return None
            if not is_adjustment and any(x < 0 for x in (people, shares, percent)): return None
            if people != int(people) or shares != int(shares) or (not is_adjustment and percent > 100): return None
            rows[level] = {"people": int(people), "shares": int(shares), "percent": percent}
        if not BANDS.issubset(rows): return None
        total_level = 17 if 17 in rows else 16
        expected = BANDS | ({16, 17} if total_level == 17 else {16})
        if set(rows) != expected or abs(rows[total_level]["percent"] - 100.0) > 0.01: return None
        return {
            "levels": {str(k): rows[k] for k in sorted(BANDS)},
            "adjustment": rows.get(16) if total_level == 17 else None,
            "total": rows[total_level],
            "major_percent": round(sum(rows[k]["percent"] for k in MAJOR_LEVELS), 2),
            "major_people": sum(rows[k]["people"] for k in MAJOR_LEVELS),
        }
    return None


class Client:
    def __init__(self, interval: float = 0.5, timeout: int = 45) -> None:
        self.interval, self.timeout = interval, timeout
        self.session = requests.Session(); self.token = ""; self.first_date = ""; self.dates: list[str] = []
        self._last = 0.0

    def _request(self, method: str, **kwargs: Any) -> requests.Response:
        delay = self.interval - (time.monotonic() - self._last)
        if delay > 0: time.sleep(delay)
        response = self.session.request(method, URL, headers={"User-Agent": USER_AGENT}, timeout=self.timeout, **kwargs)
        self._last = time.monotonic()
        if response.status_code in {403, 428, 429}: raise RuntimeError(f"TDCC challenge/rate response HTTP {response.status_code}; stopped")
        response.raise_for_status(); return response

    def bootstrap(self) -> None:
        p = parse_page(self._request("GET").text)
        self.token, self.first_date, self.dates = p.inputs.get("SYNCHRONIZER_TOKEN", ""), p.inputs.get("firDate", ""), p.dates
        if not self.token or not self.dates: raise RuntimeError("TDCC query form token/dates missing")

    def query(self, code: str, date: str) -> dict[str, Any] | None:
        last: Exception | None = None
        for attempt in range(3):
            try:
                if not self.token: self.bootstrap()
                response = self._request("POST", data={"SYNCHRONIZER_TOKEN": self.token, "SYNCHRONIZER_URI": "/portal/zh/smWeb/qryStock", "method": "submit", "firDate": self.first_date, "scaDate": date.replace("-", ""), "sqlMethod": "StockNo", "stockNo": code, "stockName": ""})
                p = parse_page(response.text); self.token = p.inputs.get("SYNCHRONIZER_TOKEN", "")
                if not self.token: raise RuntimeError("TDCC response token missing")
                if p.inputs.get("stockNo") != code or p.selected_date != date:
                    raise RuntimeError(f"TDCC response identity/date mismatch for {code} {date}")
                return strict_distribution(p)
            except requests.RequestException as exc:
                last = exc; self.session.close(); self.session = requests.Session(); self.token = ""
                if attempt < 2: time.sleep(1 + attempt * 2)
        raise RuntimeError(f"TDCC transport failed for {code} {date}: {last}")


def load_universe(path: Path) -> list[dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(rows, list): raise ValueError("universe must be a list or snapshot with rows")
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict): continue
        code = str(row.get("security_id") or row.get("stock_id") or "").strip()
        market = str(row.get("market") or "").strip().lower()
        if code.isdigit() and len(code) == 4 and market in {"listed", "otc"}:
            result[code] = {"security_id": code, "name": str(row.get("name") or ""), "market": market}
    if not result or {r["market"] for r in result.values()} != {"listed", "otc"}: raise ValueError("universe must contain both listed and otc securities")
    return [result[k] for k in sorted(result)]


def atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=path.name, suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f: json.dump(payload, f, ensure_ascii=False, indent=2); f.write("\n")
        os.replace(temp, path)
    finally:
        if os.path.exists(temp): os.unlink(temp)


def recover(universe: Iterable[dict[str, str]], date: str, output: Path, client: Client, *, limit: int | None = None) -> dict[str, Any]:
    items = list(universe)
    targets = items[:limit] if limit else items
    saved: dict[str, Any] = {}
    if output.exists():
        old = json.loads(output.read_text(encoding="utf-8"))
        if old.get("date") != date: raise ValueError("checkpoint date differs from requested date")
        saved = {str(r["security_id"]): r for r in old.get("rows", []) if strict_saved_row(r)}
    if not client.token: client.bootstrap()
    if date not in client.dates: raise ValueError(f"requested date {date} is absent from TDCC form")
    for index, ref in enumerate(targets, 1):
        code = ref["security_id"]
        if code in saved: continue
        data = client.query(code, date)
        if data is None: raise RuntimeError(f"incomplete TDCC levels for {code} {date}")
        saved[code] = {**ref, **data}
        payload = build_payload(date, items, saved)
        atomic_write(output, payload)
        if index == 1 or index % 100 == 0: print(f"[holder-recovery] {index}/{len(targets)}", flush=True)
    payload = build_payload(date, items, saved); atomic_write(output, payload); return payload


def strict_saved_row(row: Any) -> bool:
    if not isinstance(row, dict) or set((row.get("levels") or {}).keys()) != {str(i) for i in BANDS}: return False
    try:
        levels = row["levels"]
        for item in list(levels.values()) + [row["total"]]:
            people, shares, percent = float(item["people"]), float(item["shares"]), float(item["percent"])
            if not all(math.isfinite(x) and x >= 0 for x in (people, shares, percent)): return False
            if people != int(people) or shares != int(shares) or percent > 100: return False
        adjustment = row.get("adjustment")
        if adjustment is not None and not all(math.isfinite(float(adjustment[k])) for k in ("people", "shares", "percent")): return False
        expected_percent = round(sum(float(levels[str(k)]["percent"]) for k in MAJOR_LEVELS), 2)
        expected_people = sum(int(levels[str(k)]["people"]) for k in MAJOR_LEVELS)
        return abs(float(row["total"]["percent"]) - 100.0) <= 0.01 and float(row["major_percent"]) == expected_percent and int(row["major_people"]) == expected_people
    except (KeyError, TypeError, ValueError): return False


def normalize_openapi_rows(rows: Iterable[dict[str, Any]], refs: dict[str, dict[str, str]], date: str) -> dict[str, Any]:
    """Convert TDCC OpenAPI sequences 1..17 into the historical strict shape."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    dates: set[str] = set()
    for raw in rows:
        code = str(raw.get("證券代號") or raw.get("security_id") or "").strip()
        dates.add(str(raw.get("資料日期") or raw.get("\ufeff資料日期") or raw.get("date") or "").replace("-", ""))
        if code in refs: grouped.setdefault(code, []).append(raw)
    if dates != {date.replace("-", "")}: raise ValueError("OpenAPI rows do not have one requested date")
    normalized = []
    for code, parts in grouped.items():
        converted = {}
        for raw in parts:
            seq = int(raw.get("持股分級") or raw.get("level") or 0)
            converted[seq] = {"people": raw.get("人數") if "人數" in raw else raw.get("people"), "shares": raw.get("股數") if "股數" in raw else raw.get("shares"), "percent": raw.get("占集保庫存數比例%") if "占集保庫存數比例%" in raw else raw.get("percent")}
        if set(converted) != set(range(1, 18)): raise ValueError(f"OpenAPI incomplete levels for {code}")
        item = {**refs[code], "levels": {str(k): converted[k] for k in BANDS}, "adjustment": converted[16], "total": converted[17]}
        item["major_percent"] = round(sum(float(item["levels"][str(k)]["percent"]) for k in MAJOR_LEVELS), 2)
        item["major_people"] = sum(int(item["levels"][str(k)]["people"]) for k in MAJOR_LEVELS)
        if not strict_saved_row(item): raise ValueError(f"OpenAPI invalid numeric levels for {code}")
        normalized.append(item)
    saved = {r["security_id"]: r for r in normalized}
    return build_payload(date, list(refs.values()), saved)


def build_payload(date: str, universe: list[dict[str, str]], saved: dict[str, Any]) -> dict[str, Any]:
    counts = {m: sum(1 for r in saved.values() if r.get("market") == m) for m in ("listed", "otc")}
    expected = {m: sum(1 for r in universe if r.get("market") == m) for m in ("listed", "otc")}
    return {"schema_version": 1, "source": URL, "date": date, "complete": len(saved) == len(universe), "row_count": len(saved), "expected_count": len(universe), "market_counts": counts, "expected_market_counts": expected, "missing_security_ids": sorted({r["security_id"] for r in universe} - set(saved)), "rows": [saved[k] for k in sorted(saved)]}


def build_top50(current: dict[str, Any], prior: dict[str, Any], *, as_of: str) -> dict[str, Any]:
    """Build the publishable contract only from two strictly complete pools."""
    if not current.get("complete") or not prior.get("complete"):
        raise ValueError("both weekly pools must be complete")
    if current.get("date") != as_of or not prior.get("date") or prior["date"] >= as_of:
        raise ValueError("weekly dates are inconsistent")
    gap = (calendar_date.fromisoformat(as_of) - calendar_date.fromisoformat(prior["date"])).days
    if not 4 <= gap <= 10: raise ValueError("weekly dates must be 4 to 10 days apart")
    def validate_pool(pool: dict[str, Any]) -> dict[str, dict[str, Any]]:
        raw = pool.get("rows") or []
        ids = [str(r.get("security_id") or "") for r in raw if isinstance(r, dict)]
        if len(ids) != len(set(ids)): raise ValueError("weekly pool has duplicate security IDs")
        valid = {str(r["security_id"]): r for r in raw if strict_saved_row(r)}
        expected = int(pool.get("expected_count", -1))
        market_counts = {m: sum(1 for r in valid.values() if r.get("market") == m) for m in ("listed", "otc")}
        if len(valid) != len(raw) or len(valid) != expected or pool.get("row_count") != expected:
            raise ValueError("strict row coverage differs from expected count")
        if pool.get("market_counts") != market_counts or pool.get("expected_market_counts") != market_counts:
            raise ValueError("market coverage metadata mismatch")
        if set(market_counts) != {"listed", "otc"} or not all(market_counts.values()):
            raise ValueError("both markets must be present")
        return valid
    now, old = validate_pool(current), validate_pool(prior)
    paired = sorted(set(now) & set(old))
    excluded_new = sorted(set(now) - set(old))
    excluded_departed = sorted(set(old) - set(now))
    rows = []
    for code in paired:
        a, b = now[code], old[code]
        if a.get("market") not in {"listed", "otc"} or b.get("market") not in {"listed", "otc"}: continue
        latest, previous = float(a["major_percent"]), float(b["major_percent"])
        delta = round(latest - previous, 2)
        if delta > 0:
            rows.append({"security_id": code, "name": a.get("name", ""), "market": a["market"], "major_400_percent": latest, "prior_major_400_percent": previous, "delta_percentage_points": delta})
    rows.sort(key=lambda r: (-r["delta_percentage_points"], r["security_id"]))
    selected = rows[:50]
    for rank, row in enumerate(selected, 1): row["rank"] = rank
    counts = {m: sum(1 for code in paired if now[code].get("market") == m) for m in ("listed", "otc")}
    return {"schema_version": 1, "dataset_id": "mda_weekly_top50", "quality": "complete", "status": "ok", "data_date": as_of, "previous_date": prior["date"], "source": URL, "paired_universe_count": len(paired), "paired_market_counts": counts, "excluded_new_security_ids": excluded_new, "excluded_departed_security_ids": excluded_departed, "rows": selected}


def main() -> None:
    ap = argparse.ArgumentParser(); ap.add_argument("--universe", type=Path, required=True); ap.add_argument("--date", required=True); ap.add_argument("--output", type=Path, required=True); ap.add_argument("--interval", type=float, default=0.5); ap.add_argument("--limit", type=int)
    args = ap.parse_args(); payload = recover(load_universe(args.universe), args.date, args.output, Client(args.interval), limit=args.limit)
    print(json.dumps({k: payload[k] for k in ("date", "complete", "row_count", "expected_count", "market_counts")}, ensure_ascii=False))


if __name__ == "__main__": main()
