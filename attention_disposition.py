# -*- coding: utf-8 -*-
"""Collect official TWSE/TPEx attention and disposition risk snapshots.

Only official, unauthenticated exchange endpoints are used.  Raw exchange
responses are not persisted; the published artifact contains normalized rows,
source metadata, response hashes, and explicit missing/schema states.
"""

from __future__ import annotations

import argparse
import html
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from data_contract import (
    DEFAULT_MANIFEST_PATH,
    build_manifest,
    canonical_json_bytes,
    prepare_artifact_manifest,
    sha256_bytes,
    update_manifest_file,
)


ROOT = Path(__file__).resolve().parent
OUTPUT_PATH = ROOT / "data" / "attention_disposition.json"
TAIPEI_TZ = timezone(timedelta(hours=8))

SCHEMA_VERSION = "1.0.0"
DATASET_ID = "attention_disposition_risk"
SOURCE_ID = "attention_disposition_derived"
RULE_CHANGE_DATE = date(2026, 8, 10)
PRE_RULE_VERSION = "tw_attention_disposition_pre_2026_08_10"
CURRENT_RULE_VERSION = "tw_attention_disposition_2026_08_10"

TWSE_NOTICE_URL = "https://www.twse.com.tw/rwd/zh/announcement/notice"
TWSE_DISPOSITION_URL = "https://www.twse.com.tw/rwd/zh/announcement/punish"
TWSE_NEAR_URL = "https://www.twse.com.tw/rwd/zh/announcement/notetrans"
TPEX_NOTICE_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"
TPEX_DISPOSITION_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"
TPEX_NEAR_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/warning"
TWSE_CALENDAR_URL = "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule"
TPEX_CALENDAR_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/tradingDate"
CALENDAR_SOURCE_IDS = ["twse_trading_calendar", "tpex_trading_calendar"]

SOURCE_DEFINITIONS = {
    "twse_attention": {"market": "listed", "kind": "attention", "url": TWSE_NOTICE_URL},
    "twse_disposition": {"market": "listed", "kind": "disposition", "url": TWSE_DISPOSITION_URL},
    "twse_near_disposition": {"market": "listed", "kind": "near_disposition", "url": TWSE_NEAR_URL},
    "tpex_attention": {"market": "otc", "kind": "attention", "url": TPEX_NOTICE_URL},
    "tpex_disposition": {"market": "otc", "kind": "disposition", "url": TPEX_DISPOSITION_URL},
    "tpex_near_disposition": {"market": "otc", "kind": "near_disposition", "url": TPEX_NEAR_URL},
}

DATASET_BY_KIND = {
    "attention": "attention_securities",
    "disposition": "disposition_securities",
    "near_disposition": "near_disposition_risk",
}

RULE_METADATA = {
    "effective_from": RULE_CHANGE_DATE.isoformat(),
    "announcement_date": "2026-08-03",
    "twse_announcement_no": "臺證監字第1150402582號",
    "tpex_announcement_no": "證櫃視字第11500051351號",
    "twse_announcement_url": "https://www.twse.com.tw/zh/announcement/announcement_detail.html?id=13F5B5AA8F1911F19A80005056BE3760",
    "tpex_announcement_url": "https://www.tpex.org.tw/zh-tw/announce/market/announce/detail.html?content_file=MTE1MDAwNTEzNTEuaHRtbA%3D%3D&docId=MTE1MDAwNTEzNTE%3D",
    "versions": [
        {
            "rule_version": PRE_RULE_VERSION,
            "effective_until": "2026-08-09",
            "general_disposition_business_days": 10,
            "day_trade_trigger_business_days": 12,
            "general_match_interval_minutes_first": 5,
            "general_match_interval_minutes_repeat": 20,
        },
        {
            "rule_version": CURRENT_RULE_VERSION,
            "effective_from": "2026-08-10",
            "general_disposition_business_days": 5,
            "day_trade_trigger_business_days": 7,
            "general_match_interval_minutes_first": 2,
            "general_match_interval_minutes_repeat": 2,
            "high_price_attention": {
                "minimum_close_exclusive": 1000,
                "six_day_price_difference_at_or_below_2000": 300,
                "additional_price_band": 1000,
                "additional_difference_per_band": 150,
            },
            "transition": "Dispositions spanning 2026-08-10 use the new 5/7-business-day duration; eligible records are released or shortened and general matching changes to about two minutes from that date.",
            "exceptions": "Altered-trading-method, periodic-auction, and TPEx managed-stock special intervals remain governed by their applicable special provisions.",
        },
    ],
}


def _iso_now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(TAIPEI_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=TAIPEI_TZ)
    return current.astimezone(TAIPEI_TZ)


def _active_rule_version(target: date) -> str:
    return CURRENT_RULE_VERSION if target >= RULE_CHANGE_DATE else PRE_RULE_VERSION


def _roc_date(value: Any) -> str | None:
    text = str(value or "").strip()
    match = re.search(r"(?<!\d)(\d{3})[./-]?(\d{2})[./-]?(\d{2})(?!\d)", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)) + 1911, int(match.group(2)), int(match.group(3))).isoformat()
    except ValueError:
        return None


def _snapshot_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if re.fullmatch(r"\d{8}", text):
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8])).isoformat()
        except ValueError:
            return None
    return _roc_date(text)


_ZH_DIGITS = {"零": 0, "〇": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


def _zh_number(text: str) -> int:
    if text.isdigit():
        return int(text)
    total = 0
    current = 0
    for char in text:
        if char in _ZH_DIGITS:
            current = _ZH_DIGITS[char]
        elif char == "十":
            total += (current or 1) * 10
            current = 0
        elif char == "百":
            total += (current or 1) * 100
            current = 0
    return total + current


def _chinese_roc_date(value: Any) -> str | None:
    text = str(value or "")
    match = re.search(
        r"(?:民國)?([零〇一二三四五六七八九十百]+)年([零〇一二三四五六七八九十]+)月([零〇一二三四五六七八九十]+)日",
        text,
    )
    if not match:
        return None
    try:
        return date(_zh_number(match.group(1)) + 1911, _zh_number(match.group(2)), _zh_number(match.group(3))).isoformat()
    except ValueError:
        return None


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\((?:\.\./|\./)[^)]+\)", "", text)
    return re.sub(r"[ \t\r\f\v]+", " ", text).strip()


def _int_or_none(value: Any) -> int | None:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group()) if match else None


def _table(payload: Mapping[str, Any]) -> tuple[list[str], list[list[Any]]]:
    fields = payload.get("fields")
    rows = payload.get("data")
    if isinstance(fields, list) and isinstance(rows, list):
        return [str(item) for item in fields], rows
    tables = payload.get("tables")
    if isinstance(tables, list) and tables and isinstance(tables[0], Mapping):
        fields = tables[0].get("fields")
        rows = tables[0].get("data")
        if isinstance(fields, list) and isinstance(rows, list):
            return [str(item) for item in fields], rows
    raise ValueError("official response does not contain a fields/data table")


def _mapped_rows(payload: Mapping[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    fields, rows = _table(payload)
    return fields, [dict(zip(fields, row)) for row in rows if isinstance(row, list)]


def _field(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return ""


def _period(value: Any) -> tuple[str | None, str | None]:
    dates = re.findall(r"\d{3}[./-]\d{2}[./-]\d{2}", str(value or ""))
    normalized = [_roc_date(item) for item in dates]
    return (normalized[0] if normalized else None, normalized[1] if len(normalized) > 1 else None)


def _transition_end(content: str) -> str | None:
    numeric = re.search(r"修正其處置至(\d{3}年\d{1,2}月\d{1,2}日)", content)
    if numeric:
        token = re.sub(r"年|月", "/", numeric.group(1)).replace("日", "")
        parts = token.split("/")
        return date(int(parts[0]) + 1911, int(parts[1]), int(parts[2])).isoformat()
    chinese = re.search(
        r"修正其處置至((?:民國)?[零〇一二三四五六七八九十百]+年[零〇一二三四五六七八九十]+月[零〇一二三四五六七八九十]+日)",
        content,
    )
    return _chinese_roc_date(chinese.group(1)) if chinese else None


def _interval_minutes(content: str, target: date) -> int | None:
    if target >= RULE_CHANGE_DATE and ("每二分鐘" in content or re.search(r"每\s*2\s*分鐘", content)):
        return 2
    match = re.search(r"每\s*(\d+)\s*分鐘", content)
    if match:
        return int(match.group(1))
    match = re.search(r"每([一二三四五六七八九十]+)分鐘", content)
    return _zh_number(match.group(1)) if match else None


def normalize_attention(payload: Mapping[str, Any], *, market: str, target: date) -> list[dict[str, Any]]:
    _, rows = _mapped_rows(payload)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        security_id = _clean_text(_field(row, "證券代號"))
        if not security_id:
            continue
        announcement_date = _roc_date(_field(row, "日期", "公告日期")) or target.isoformat()
        normalized.append(
            {
                "data_date": announcement_date,
                "market": market,
                "security_id": security_id,
                "security_name": _clean_text(_field(row, "證券名稱")),
                "attention_count": _int_or_none(_field(row, "累計次數", "累計")),
                "reason": _clean_text(_field(row, "注意交易資訊")),
                "rule_version": _active_rule_version(target),
            }
        )
    return normalized


def normalize_near_disposition(payload: Mapping[str, Any], *, market: str, target: date) -> list[dict[str, Any]]:
    _, rows = _mapped_rows(payload)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        security_id = _clean_text(_field(row, "證券代號"))
        if not security_id:
            continue
        reason = _clean_text(_field(row, "近期達本公司「公布注意交易資訊」標準之情形"))
        normalized.append(
            {
                "data_date": target.isoformat(),
                "market": market,
                "security_id": security_id,
                "security_name": _clean_text(_field(row, "證券名稱")),
                "reason": reason,
                "official_near_disposition": True,
                "rule_version": _active_rule_version(target),
            }
        )
    return normalized


def normalize_disposition(payload: Mapping[str, Any], *, market: str, target: date) -> list[dict[str, Any]]:
    _, rows = _mapped_rows(payload)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        security_id = _clean_text(_field(row, "證券代號"))
        content = _clean_text(_field(row, "處置內容"))
        if not security_id or "本日無處置資料" in content:
            continue
        original_start, original_end = _period(_field(row, "處置起迄時間", "處置起訖時間"))
        revised_end = _transition_end(content)
        effective_end = revised_end or original_end
        active = bool(
            original_start
            and effective_end
            and date.fromisoformat(original_start) <= target <= date.fromisoformat(effective_end)
        )
        if not active:
            continue
        normalized.append(
            {
                "data_date": target.isoformat(),
                "publication_date": _roc_date(_field(row, "公布日期")),
                "market": market,
                "security_id": security_id,
                "security_name": _clean_text(_field(row, "證券名稱")),
                "occurrence_count": _int_or_none(_field(row, "累計")),
                "reason": _clean_text(_field(row, "處置條件", "處置原因")),
                "original_start_date": original_start,
                "original_end_date": original_end,
                "effective_end_date": effective_end,
                "transition_revised": bool(revised_end),
                "active_on_data_date": active,
                "day_trade_trigger": bool("第十三款" in content or "當沖" in content or "沖銷" in content),
                "matching_interval_minutes": _interval_minutes(content, target),
                "rule_version": _active_rule_version(target),
            }
        )
    return normalized


NORMALIZERS = {
    "attention": normalize_attention,
    "disposition": normalize_disposition,
    "near_disposition": normalize_near_disposition,
}

REQUIRED_FIELD_GROUPS = {
    "attention": [("證券代號",), ("注意交易資訊",)],
    "disposition": [("證券代號",), ("處置起迄時間", "處置起訖時間"), ("處置內容",)],
    "near_disposition": [("證券代號",), ("近期達本公司「公布注意交易資訊」標準之情形",)],
}


def _source_artifact(
    source_id: str,
    payload: Mapping[str, Any] | None,
    *,
    target: date,
    fetched_at: datetime,
    error: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    definition = SOURCE_DEFINITIONS[source_id]
    if error or not isinstance(payload, Mapping):
        return (
            {
                "dataset_id": DATASET_BY_KIND[definition["kind"]],
                "dataset_schema_version": SCHEMA_VERSION,
                "source_id": source_id,
                "source_tier": "primary",
                "market": definition["market"],
                "market_coverage": [definition["market"]],
                "kind": definition["kind"],
                "url": definition["url"],
                "status": "missing",
                "data_date": None,
                "trading_date": None,
                "expected_data_date": target.isoformat(),
                "fetched_at": fetched_at.isoformat(timespec="seconds"),
                "row_count": 0,
                "response_row_count": 0,
                "sha256": None,
                "fallback": {"used": False, "reason": None},
                "missing": {"status": "missing", "missing_fields": [], "missing_partitions": [source_id]},
                "schema_validation": {"status": "not_evaluated"},
                "error": error or "missing official response",
            },
            [],
        )
    response_date: str | None = None
    response_rows: list[list[Any]] = []
    try:
        fields, response_rows = _table(payload)
        missing_groups = [
            "/".join(group)
            for group in REQUIRED_FIELD_GROUPS[definition["kind"]]
            if not any(name in fields for name in group)
        ]
        if missing_groups:
            raise ValueError(f"required official fields missing: {', '.join(missing_groups)}")
        response_date = _snapshot_date(payload.get("date"))
        if definition["kind"] == "near_disposition" and response_date and response_date != target.isoformat():
            rows = []
            status = "stale"
            problem = f"official snapshot date {response_date} does not match requested {target.isoformat()}"
        else:
            rows = NORMALIZERS[definition["kind"]](payload, market=definition["market"], target=target)
            status = "fresh"
            problem = None
        for row in rows:
            row["source_id"] = source_id
    except (KeyError, TypeError, ValueError) as exc:
        rows = []
        status = "schema_error"
        problem = str(exc)
    return (
        {
            "dataset_id": DATASET_BY_KIND[definition["kind"]],
            "dataset_schema_version": SCHEMA_VERSION,
            "source_id": source_id,
            "source_tier": "primary",
            "market": definition["market"],
            "market_coverage": [definition["market"]],
            "kind": definition["kind"],
            "url": definition["url"],
            "status": status,
            "data_date": response_date if status == "stale" else target.isoformat(),
            "trading_date": response_date if status == "stale" else target.isoformat(),
            "expected_data_date": target.isoformat(),
            "fetched_at": fetched_at.isoformat(timespec="seconds"),
            "row_count": len(rows),
            "response_row_count": len(response_rows),
            "sha256": sha256_bytes(canonical_json_bytes(payload)),
            "fallback": {"used": False, "reason": None},
            "missing": {
                "status": "complete" if status == "fresh" else "partial",
                "missing_fields": ["official_response_schema"] if status == "schema_error" else [],
                "missing_partitions": [source_id] if status != "fresh" else [],
            },
            "schema_validation": {"status": "ok" if status in {"fresh", "stale"} else "error"},
            "error": problem,
        },
        rows,
    )


def _risk_summary(
    attention: Sequence[Mapping[str, Any]],
    disposition: Sequence[Mapping[str, Any]],
    near: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    def apply(row: Mapping[str, Any], level: str, priority: int) -> None:
        key = (str(row.get("market") or ""), str(row.get("security_id") or ""))
        current = merged.setdefault(
            key,
            {
                "data_date": row.get("data_date"),
                "market": key[0],
                "security_id": key[1],
                "security_name": row.get("security_name") or key[1],
                "risk_level": "none",
                "risk_priority": 0,
                "attention": False,
                "near_disposition": False,
                "disposition": False,
                "rule_version": row.get("rule_version"),
                "reasons": [],
            },
        )
        current[level] = True
        if priority >= current["risk_priority"]:
            current["risk_level"] = level
            current["risk_priority"] = priority
        reason = str(row.get("reason") or "").strip()
        if reason and reason not in current["reasons"]:
            current["reasons"].append(reason)
        if level == "disposition":
            current["effective_end_date"] = row.get("effective_end_date")
            current["matching_interval_minutes"] = row.get("matching_interval_minutes")
            current["transition_revised"] = row.get("transition_revised")

    for row in attention:
        apply(row, "attention", 1)
    for row in near:
        apply(row, "near_disposition", 2)
    for row in disposition:
        if row.get("active_on_data_date"):
            apply(row, "disposition", 3)
    for item in merged.values():
        item.pop("risk_priority", None)
    return sorted(merged.values(), key=lambda row: (row["market"], row["security_id"]))


def collect_snapshot(
    source_payloads: Mapping[str, Mapping[str, Any] | None],
    *,
    target_date: date,
    fetched_at: datetime | None = None,
    source_errors: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    fetched = _iso_now(fetched_at)
    errors = source_errors or {}
    source_artifacts: list[dict[str, Any]] = []
    attention: list[dict[str, Any]] = []
    disposition: list[dict[str, Any]] = []
    near: list[dict[str, Any]] = []
    buckets = {"attention": attention, "disposition": disposition, "near_disposition": near}
    for source_id, definition in SOURCE_DEFINITIONS.items():
        artifact, rows = _source_artifact(
            source_id,
            source_payloads.get(source_id),
            target=target_date,
            fetched_at=fetched,
            error=errors.get(source_id),
        )
        source_artifacts.append(artifact)
        buckets[definition["kind"]].extend(rows)

    missing = [item["source_id"] for item in source_artifacts if item["status"] != "fresh"]
    return {
        "dataset_id": DATASET_ID,
        "schema_version": SCHEMA_VERSION,
        "rule_version": _active_rule_version(target_date),
        "date": target_date.isoformat(),
        "updated_at": fetched.isoformat(timespec="seconds"),
        "rule_metadata": RULE_METADATA,
        "source_artifacts": source_artifacts,
        "data_quality": {
            "state": "partial" if missing else "complete",
            "missing_partitions": missing,
            "fallback_used": False,
            "warning": "Absence is not treated as no risk when any required official partition is missing." if missing else None,
        },
        "attention": attention,
        "disposition": disposition,
        "near_disposition": near,
        "risk_summary": _risk_summary(attention, disposition, near),
    }


def contract_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "data_date": str(row.get("data_date") or payload.get("date") or ""),
            "market": str(row.get("market") or ""),
            "security_id": str(row.get("security_id") or ""),
            "risk_level": str(row.get("risk_level") or ""),
            "rule_version": str(row.get("rule_version") or payload.get("rule_version") or ""),
        }
        for row in payload.get("risk_summary") or []
        if isinstance(row, Mapping)
    ]


def fetch_official_payloads(
    target_date: date,
    *,
    timeout: int = 30,
) -> tuple[dict[str, Mapping[str, Any] | None], dict[str, str]]:
    target_compact = target_date.strftime("%Y%m%d")
    target_slash = target_date.strftime("%Y/%m/%d")
    start = (target_date - timedelta(days=60))
    calls = {
        "twse_attention": ("GET", TWSE_NOTICE_URL, {"startDate": target_compact, "endDate": target_compact, "response": "json"}),
        "twse_disposition": ("GET", TWSE_DISPOSITION_URL, {"startDate": start.strftime("%Y%m%d"), "endDate": target_compact, "response": "json"}),
        "twse_near_disposition": ("GET", TWSE_NEAR_URL, {"date": target_compact, "response": "json"}),
        "tpex_attention": ("POST", TPEX_NOTICE_URL, {"startDate": target_slash, "endDate": target_slash, "type": "all", "order": "date", "response": "json"}),
        "tpex_disposition": ("POST", TPEX_DISPOSITION_URL, {"startDate": start.strftime("%Y/%m/%d"), "endDate": target_slash, "type": "all", "order": "date", "response": "json"}),
        "tpex_near_disposition": ("POST", TPEX_NEAR_URL, {"response": "json"}),
    }
    payloads: dict[str, Mapping[str, Any] | None] = {}
    errors: dict[str, str] = {}
    for source_id, (method, url, values) in calls.items():
        try:
            encoded = urlencode(values)
            request = Request(
                f"{url}?{encoded}" if method == "GET" else url,
                data=None if method == "GET" else encoded.encode("utf-8"),
                headers={"User-Agent": "stock-from-Hsiu/attention-disposition-contract"},
                method=method,
            )
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8-sig"))
            if not isinstance(payload, Mapping):
                raise ValueError("official endpoint returned a non-object JSON value")
            payloads[source_id] = payload
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            payloads[source_id] = None
            errors[source_id] = f"{type(exc).__name__}: {exc}"
    return payloads, errors


def fetch_trading_sessions(
    target_date: date,
    *,
    timeout: int = 30,
) -> list[str]:
    headers = {"User-Agent": "stock-from-Hsiu/attention-disposition-contract"}
    request = Request(TWSE_CALENDAR_URL, headers=headers)
    with urlopen(request, timeout=timeout) as response:
        rows = json.loads(response.read().decode("utf-8-sig"))
    if not isinstance(rows, list):
        raise ValueError("TWSE calendar endpoint returned a non-list JSON value")
    non_trading: set[date] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        calendar_date = _roc_date(row.get("Date"))
        if not calendar_date:
            continue
        label = f"{row.get('Name') or ''} {row.get('Description') or ''}"
        if "開始交易" not in label and "最後交易" not in label:
            non_trading.add(date.fromisoformat(calendar_date))
    encoded = urlencode({"date": str(target_date.year), "response": "json"}).encode("utf-8")
    tpex_request = Request(TPEX_CALENDAR_URL, data=encoded, headers=headers, method="POST")
    with urlopen(tpex_request, timeout=timeout) as response:
        tpex_payload = json.loads(response.read().decode("utf-8-sig"))
    tpex_html = ((tpex_payload.get("data") or {}).get("html") or "") if isinstance(tpex_payload, Mapping) else ""
    if not tpex_html:
        raise ValueError("TPEx calendar endpoint did not return the official schedule table")
    for table_row in re.findall(r"<tr[^>]*>(.*?)</tr>", tpex_html, flags=re.IGNORECASE | re.DOTALL):
        cleaned = _clean_text(table_row)
        if "開始交易" in cleaned or "最後交易" in cleaned:
            continue
        for month, day_value in re.findall(r"(\d{1,2})月(\d{1,2})日", cleaned):
            try:
                non_trading.add(date(target_date.year, int(month), int(day_value)))
            except ValueError:
                continue
    first = date(target_date.year, 1, 1)
    current = first
    sessions: list[str] = []
    while current <= target_date:
        if current.weekday() < 5 and current not in non_trading:
            sessions.append(current.isoformat())
        current += timedelta(days=1)
    return sessions


def write_snapshot(
    payload: dict[str, Any],
    *,
    output_path: Path | str = OUTPUT_PATH,
    manifest_path: Path | str = DEFAULT_MANIFEST_PATH,
    trading_sessions: Sequence[str],
) -> dict[str, Any]:
    output = Path(output_path)
    fetched_at = str(payload.get("updated_at") or "")
    data_date = str(payload.get("date") or "")
    missing_partitions = list((payload.get("data_quality") or {}).get("missing_partitions") or [])
    prepared, manifest, artifact_bytes = prepare_artifact_manifest(
        payload,
        dataset_id=DATASET_ID,
        source_id=SOURCE_ID,
        rows=contract_rows(payload),
        data_date=data_date,
        trading_date=data_date,
        expected_data_date=data_date,
        fetched_at=fetched_at,
        evaluated_at=fetched_at,
        missing_partitions=missing_partitions,
        trading_sessions=trading_sessions,
        calendar_source_ids=CALENDAR_SOURCE_IDS,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(artifact_bytes)
    resolved_manifest_path = Path(manifest_path)
    normalized_rows = list(payload.get("attention") or []) + list(payload.get("disposition") or []) + list(payload.get("near_disposition") or [])
    for source in payload.get("source_artifacts") or []:
        if not isinstance(source, Mapping):
            continue
        source_id = str(source.get("source_id") or "")
        source_rows = [dict(row) for row in normalized_rows if isinstance(row, Mapping) and row.get("source_id") == source_id]
        source_status = str(source.get("status") or "missing")
        source_manifest = build_manifest(
            str(source.get("dataset_id") or ""),
            source_id,
            source_rows,
            data_date=str(source.get("data_date") or ""),
            trading_date=str(source.get("trading_date") or "") or None,
            expected_data_date=data_date,
            fetched_at=fetched_at,
            missing_fields=["official_response_schema"] if source_status == "schema_error" else [],
            missing_partitions=[source_id] if source_status in {"missing", "schema_error"} else [],
            trading_sessions=trading_sessions,
            calendar_source_ids=CALENDAR_SOURCE_IDS,
        )
        if source_status == "schema_error":
            source_manifest["freshness"]["status"] = "schema_error"
            source_manifest["schema_validation"]["status"] = "error"
        update_manifest_file(source_manifest, resolved_manifest_path)
    update_manifest_file(manifest, resolved_manifest_path)
    return prepared


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect official TWSE/TPEx attention and disposition risk")
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="snapshot date (YYYY-MM-DD)")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    args = parser.parse_args()
    fetched = _iso_now()
    target = args.date or fetched.date()
    payloads, errors = fetch_official_payloads(target)
    payload = collect_snapshot(payloads, target_date=target, fetched_at=fetched, source_errors=errors)
    sessions = fetch_trading_sessions(target)
    written = write_snapshot(payload, output_path=args.output, manifest_path=args.manifest, trading_sessions=sessions)
    print(
        f"[attention_disposition] wrote {args.output} date={target} "
        f"risk={len(written.get('risk_summary') or [])} quality={written['data_quality']['state']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
