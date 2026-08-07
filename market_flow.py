# -*- coding: utf-8 -*-
"""Collect daily listed/OTC institutional-flow aggregates for the site.

The artifact is an explainable display layer.  It does not alter any stock
selection, timing, or exit rule.  TWSE and TPEx rows are normalized first,
then aggregated by market so the homepage can show foreign and investment
trust net buying without publishing the full official response.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from data_contract import DEFAULT_MANIFEST_PATH, prepare_artifact_manifest, update_manifest_file

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "daily_market_flow.json"
TWSE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
TPEX_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
DERIVED_SOURCE_ID = "daily_market_flow_derived"
TAIPEI_TZ = timezone(timedelta(hours=8))


def _number(value: Any, default: int = 0) -> int:
    if value in (None, "", "--", "－", "-", "N/A"):
        return default
    try:
        parsed = float(str(value).replace(",", "").replace("%", "").strip())
        return int(parsed) if math.isfinite(parsed) else default
    except (TypeError, ValueError):
        return default


def _date_text(value: Any) -> str:
    text = str(value or "").strip()
    if re.fullmatch(r"\d{7,8}", text):
        if len(text) == 8 and int(text[:4]) >= 1900:
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) == 7:
            text = "0" + text
        try:
            return f"{int(text[:4]) + 1911:04d}-{text[4:6]}-{text[6:8]}"
        except ValueError:
            return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return ""


def _iso_now(now: datetime | None = None) -> datetime:
    value = now or datetime.now(TAIPEI_TZ)
    if value.tzinfo is None:
        value = value.replace(tzinfo=TAIPEI_TZ)
    return value.astimezone(TAIPEI_TZ)


def _fetch_json(url: str, params: Mapping[str, str] | None = None, timeout: int = 45) -> Any:
    query = f"?{urlencode(params)}" if params else ""
    request = Request(f"{url}{query}", headers={"User-Agent": "stock-from-Hsiu/market-flow"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8-sig"))


def _twse_value(row: Mapping[str, Any], names: Sequence[str]) -> int:
    for name in names:
        if name in row:
            return _number(row.get(name))
    return 0


def normalize_twse_payload(payload: Mapping[str, Any], data_date: str | None = None) -> list[dict[str, Any]]:
    fields = [str(field) for field in payload.get("fields") or []]
    rows: list[dict[str, Any]] = []
    resolved_date = _date_text(data_date or payload.get("date"))
    for values in payload.get("data") or []:
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            continue
        source = dict(zip(fields, values))
        security_id = str(source.get("證券代號") or "").strip()
        if not security_id:
            continue
        foreign_buy = _twse_value(source, ["外陸資買進股數(不含外資自營商)"])
        foreign_sell = _twse_value(source, ["外陸資賣出股數(不含外資自營商)"])
        trust_buy = _twse_value(source, ["投信買進股數"])
        trust_sell = _twse_value(source, ["投信賣出股數"])
        dealer_net = _twse_value(source, ["自營商買賣超股數"])
        rows.append({
            "trading_date": resolved_date,
            "security_id": security_id,
            "name": str(source.get("證券名稱") or "").strip(),
            "market": "listed",
            "foreign_buy": foreign_buy,
            "foreign_sell": foreign_sell,
            "foreign_net": _twse_value(source, ["外陸資買賣超股數(不含外資自營商)"]) or foreign_buy - foreign_sell,
            "investment_trust_buy": trust_buy,
            "investment_trust_sell": trust_sell,
            "investment_trust_net": _twse_value(source, ["投信買賣超股數"]) or trust_buy - trust_sell,
            "dealer_net": dealer_net,
            "institutional_total_net": _twse_value(source, ["三大法人買賣超股數"]),
        })
    return rows


def _key_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("－", "-")


def _tpex_value(row: Mapping[str, Any], *contains: str) -> int:
    wanted = [_key_text(item) for item in contains]
    for key, value in row.items():
        normalized = _key_text(key)
        if any(token in normalized for token in wanted):
            return _number(value)
    return 0


def normalize_tpex_payload(payload: Any, data_date: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in payload if isinstance(payload, list) else []:
        if not isinstance(source, Mapping):
            continue
        security_id = str(source.get("SecuritiesCompanyCode") or "").strip()
        if not security_id:
            continue
        foreign_buy = _tpex_value(source, "ForeignInvestorsIncludeMainlandAreaInvestors-TotalBuy")
        foreign_sell = _tpex_value(source, "ForeignInvestorsIncludeMainlandAreaInvestors-TotalSell")
        trust_buy = _tpex_value(source, "SecuritiesInvestmentTrustCompanies-TotalBuy")
        trust_sell = _tpex_value(source, "SecuritiesInvestmentTrustCompanies-TotalSell")
        rows.append({
            "trading_date": _date_text(data_date or source.get("Date")),
            "security_id": security_id,
            "name": str(source.get("CompanyName") or "").strip(),
            "market": "otc",
            "foreign_buy": foreign_buy,
            "foreign_sell": foreign_sell,
            "foreign_net": _tpex_value(source, "ForeignInvestorsIncludeMainlandAreaInvestors-Difference") or foreign_buy - foreign_sell,
            "investment_trust_buy": trust_buy,
            "investment_trust_sell": trust_sell,
            "investment_trust_net": _tpex_value(source, "SecuritiesInvestmentTrustCompanies-Difference") or trust_buy - trust_sell,
            "dealer_net": _tpex_value(source, "Dealers-Difference"),
            "institutional_total_net": _tpex_value(source, "TotalDifference"),
        })
    return rows


def aggregate_market_rows(rows: Sequence[Mapping[str, Any]], limit: int = 5) -> dict[str, Any]:
    clean = [dict(row) for row in rows if isinstance(row, Mapping)]

    def top(key: str, reverse: bool) -> list[dict[str, Any]]:
        ranked = sorted(clean, key=lambda row: _number(row.get(key)), reverse=reverse)[:limit]
        return [
            {"security_id": row.get("security_id"), "name": row.get("name"), "net": _number(row.get(key))}
            for row in ranked
            if row.get("security_id")
        ]

    return {
        "stock_count": len(clean),
        "foreign_buy": sum(_number(row.get("foreign_buy")) for row in clean),
        "foreign_sell": sum(_number(row.get("foreign_sell")) for row in clean),
        "foreign_net": sum(_number(row.get("foreign_net")) for row in clean),
        "investment_trust_buy": sum(_number(row.get("investment_trust_buy")) for row in clean),
        "investment_trust_sell": sum(_number(row.get("investment_trust_sell")) for row in clean),
        "investment_trust_net": sum(_number(row.get("investment_trust_net")) for row in clean),
        "institutional_total_net": sum(_number(row.get("institutional_total_net")) for row in clean),
        "foreign_top_buy": top("foreign_net", True),
        "foreign_top_sell": top("foreign_net", False),
        "trust_top_buy": top("investment_trust_net", True),
        "trust_top_sell": top("investment_trust_net", False),
    }


def build_payload(
    listed_rows: Sequence[Mapping[str, Any]],
    otc_rows: Sequence[Mapping[str, Any]],
    *,
    data_date: str,
    fetched_at: datetime | str,
    source_errors: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    errors = dict(source_errors or {})
    source_artifacts = []
    for market, rows, source_id in (("listed", listed_rows, "twse_institutional_trading"), ("otc", otc_rows, "tpex_institutional_trading")):
        source_artifacts.append({
            "source_id": source_id,
            "market": market,
            "status": "missing" if market in errors else "fresh",
            "data_date": data_date if market not in errors else None,
            "row_count": len(rows),
            "error": errors.get(market),
        })
    return {
        "dataset_id": "daily_market_flow",
        "schema_version": "1.0.0",
        "date": data_date,
        "updated_at": _iso_now(fetched_at if isinstance(fetched_at, datetime) else None).isoformat(),
        "markets": {"listed": aggregate_market_rows(listed_rows), "otc": aggregate_market_rows(otc_rows)},
        "source_artifacts": source_artifacts,
        "data_quality": {
            "state": "ok" if not errors else ("warning" if listed_rows or otc_rows else "missing"),
            "warnings": [f"{market} official institutional feed unavailable: {message}" for market, message in errors.items()],
        },
    }


def _manifest_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for market, summary in (payload.get("markets") or {}).items():
        if not isinstance(summary, Mapping):
            continue
        rows.append({
            "trading_date": payload.get("date"),
            "market": market,
            "foreign_net": _number(summary.get("foreign_net")),
            "investment_trust_net": _number(summary.get("investment_trust_net")),
            "institutional_total_net": _number(summary.get("institutional_total_net")),
        })
    return rows


def write_payload(payload: dict[str, Any], output_path: Path | str = OUTPUT_PATH, manifest_path: Path | str = DEFAULT_MANIFEST_PATH) -> Path:
    path = Path(output_path)
    errors = {str(item.get("market")): str(item.get("error") or "missing") for item in payload.get("source_artifacts") or [] if item.get("status") != "fresh"}
    data_date = str(payload.get("date") or "")
    _, manifest, artifact_bytes = prepare_artifact_manifest(
        payload,
        dataset_id="daily_market_flow",
        source_id=DERIVED_SOURCE_ID,
        rows=_manifest_rows(payload),
        data_date=data_date,
        trading_date=data_date,
        expected_data_date=data_date,
        fetched_at=str(payload.get("updated_at") or _iso_now().isoformat()),
        missing_partitions=sorted(errors),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(artifact_bytes)
    update_manifest_file(manifest, manifest_path)
    return path


def collect(target_date: date | None = None, *, now: datetime | None = None) -> dict[str, Any]:
    fetched_at = _iso_now(now)
    target = target_date or fetched_at.date()
    date_param = target.strftime("%Y%m%d")
    listed_rows: list[dict[str, Any]] = []
    otc_rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    try:
        listed_rows = normalize_twse_payload(_fetch_json(TWSE_URL, {"response": "json", "selectType": "ALLBUT0999", "date": date_param}), date_param)
    except Exception as exc:
        errors["listed"] = str(exc)[:200]
    try:
        otc_rows = normalize_tpex_payload(_fetch_json(TPEX_URL), date_param)
    except Exception as exc:
        errors["otc"] = str(exc)[:200]
    available_dates = [row.get("trading_date") for row in (*listed_rows, *otc_rows) if row.get("trading_date")]
    resolved_date = max(available_dates) if available_dates else target.isoformat()
    return build_payload(listed_rows, otc_rows, data_date=resolved_date, fetched_at=fetched_at, source_errors=errors)


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect daily TWSE/TPEx institutional-flow aggregates.")
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    args = parser.parse_args()
    payload = collect(args.date)
    write_payload(payload, args.output, args.manifest)
    print(f"[market_flow] wrote {args.output} date={payload.get('date')} state={(payload.get('data_quality') or {}).get('state')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
