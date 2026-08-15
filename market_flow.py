# -*- coding: utf-8 -*-
"""Collect daily listed/OTC institutional-flow aggregates for the site.

The artifact is an explainable display layer.  It does not alter any stock
selection, timing, or exit rule.  TWSE and TPEx rows are normalized first,
then aggregated by market so the homepage can show exact official monetary
totals and a dedicated page can publish ordinary-equity rankings without
publishing either official response verbatim.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from data_contract import DEFAULT_MANIFEST_PATH, prepare_artifact_manifest, update_manifest_file

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "daily_market_flow.json"
HOLDER_ARCHIVE_PATH = DATA_DIR / "holder_weekly_snapshots.json"
TWSE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"
TPEX_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
TWSE_AMOUNT_URL = "https://www.twse.com.tw/rwd/zh/fund/BFI82U"
TPEX_AMOUNT_URL = "https://www.tpex.org.tw/www/zh-tw/insti/summary"
TWSE_MARGIN_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TPEX_MARGIN_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"
DERIVED_SOURCE_ID = "daily_market_flow_derived"
TAIPEI_TZ = timezone(timedelta(hours=8))
RANKING_POLICY = "ordinary_equity_v1"
RANKING_SUPPLEMENTAL_LIMIT = 50
NON_ORDINARY_NAME_TOKENS = ("ETF", "ETN", "TDR", "-DR", "權證", "特別股", "受益證券")
MAX_AUTO_LOOKBACK_DAYS = 10
HTTP_ATTEMPTS = 3


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


def _read_json(request: Request, *, timeout: int, attempts: int = HTTP_ATTEMPTS) -> Any:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8-sig"))
        except Exception as exc:
            last_error = exc
            if attempt + 1 < attempts:
                sleep(2**attempt)
    assert last_error is not None
    raise last_error


def _fetch_json(url: str, params: Mapping[str, str] | None = None, timeout: int = 45) -> Any:
    query = f"?{urlencode(params)}" if params else ""
    request = Request(f"{url}{query}", headers={"User-Agent": "stock-from-Hsiu/market-flow"})
    return _read_json(request, timeout=timeout)


def _post_json(url: str, params: Mapping[str, str], timeout: int = 45) -> Any:
    request = Request(
        url,
        data=urlencode(params).encode("utf-8"),
        headers={
            "User-Agent": "stock-from-Hsiu/market-flow",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/summary/day.html",
        },
    )
    return _read_json(request, timeout=timeout)


def _twse_value(row: Mapping[str, Any], names: Sequence[str]) -> int:
    for name in names:
        if name in row:
            return _number(row.get(name))
    return 0


def normalize_twse_payload(payload: Mapping[str, Any], data_date: str | None = None) -> list[dict[str, Any]]:
    fields = [str(field) for field in payload.get("fields") or []]
    rows: list[dict[str, Any]] = []
    resolved_date = _date_text(payload.get("date") or data_date)
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
            "trading_date": _date_text(source.get("Date") or data_date),
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


def is_ordinary_equity(row: Mapping[str, Any]) -> bool:
    """Return whether a normalized row is eligible for stock rankings.

    Listed/OTC common stocks use a four-digit numeric code.  The 91xx range is
    reserved for TDRs; ETF, ETN, warrant, bond and preferred-share codes are
    longer or suffixed.  Name checks provide a second guard for source changes.
    """

    security_id = str(row.get("security_id") or "").strip()
    name = _key_text(row.get("name")).upper()
    if not re.fullmatch(r"\d{4}", security_id) or security_id.startswith(("0", "91")):
        return False
    return not any(token.upper() in name for token in NON_ORDINARY_NAME_TOKENS)


def _amount_rows(payload: Any, market: str) -> tuple[str, list[str], list[Any]]:
    if market == "listed" and isinstance(payload, Mapping):
        return _date_text(payload.get("date")), [str(field) for field in payload.get("fields") or []], list(payload.get("data") or [])
    if market == "otc" and isinstance(payload, Mapping):
        tables = payload.get("tables") or []
        table = tables[0] if tables and isinstance(tables[0], Mapping) else {}
        return _date_text(payload.get("date") or table.get("date")), [str(field) for field in table.get("fields") or []], list(table.get("data") or [])
    return "", [], []


def normalize_amount_payload(payload: Any, market: str) -> dict[str, Any]:
    """Normalize official TWSE/TPEx market-wide institutional amounts."""

    data_date, fields, values = _amount_rows(payload, market)
    parsed: dict[str, dict[str, int]] = {}
    dealer_parts: list[dict[str, int]] = []
    for item in values:
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes)):
            continue
        source = dict(zip(fields, item))
        label = _key_text(source.get("單位名稱")).replace("*", "")
        amount = {
            "buy": _twse_value(source, ["買進金額", "買進金額(元)"]),
            "sell": _twse_value(source, ["賣出金額", "賣出金額(元)"]),
            "net": _twse_value(source, ["買賣差額", "買賣超(元)"]),
        }
        if "外資及陸資" in label and ("不含外資自營商" in label or "不含自營商" in label):
            parsed["foreign"] = amount
        elif label == "投信":
            parsed["investment_trust"] = amount
        elif label == "自營商合計":
            parsed["dealer"] = amount
        elif label.startswith("自營商("):
            dealer_parts.append(amount)
        elif label in {"合計", "三大法人合計"}:
            parsed["institutional_total"] = amount

    if "dealer" not in parsed and dealer_parts:
        parsed["dealer"] = {key: sum(part[key] for part in dealer_parts) for key in ("buy", "sell", "net")}
    for key in ("foreign", "investment_trust", "dealer"):
        parsed.setdefault(key, {"buy": 0, "sell": 0, "net": 0})
    if "institutional_total" not in parsed:
        parsed["institutional_total"] = {
            key: sum(parsed[item][key] for item in ("foreign", "investment_trust", "dealer"))
            for key in ("buy", "sell", "net")
        }
    return {
        "trading_date": data_date,
        "unit": "TWD",
        "scope": "official_market_total_all_instruments",
        "foreign_buy_amount": parsed["foreign"]["buy"],
        "foreign_sell_amount": parsed["foreign"]["sell"],
        "foreign_net_amount": parsed["foreign"]["net"],
        "investment_trust_buy_amount": parsed["investment_trust"]["buy"],
        "investment_trust_sell_amount": parsed["investment_trust"]["sell"],
        "investment_trust_net_amount": parsed["investment_trust"]["net"],
        "dealer_buy_amount": parsed["dealer"]["buy"],
        "dealer_sell_amount": parsed["dealer"]["sell"],
        "dealer_net_amount": parsed["dealer"]["net"],
        "institutional_total_buy_amount": parsed["institutional_total"]["buy"],
        "institutional_total_sell_amount": parsed["institutional_total"]["sell"],
        "institutional_total_net_amount": parsed["institutional_total"]["net"],
    }


def normalize_twse_margin_payload(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize the per-security table in the official TWSE margin report."""

    data_date = _date_text(payload.get("date"))
    rows: list[dict[str, Any]] = []
    for table in payload.get("tables") or []:
        if not isinstance(table, Mapping):
            continue
        fields = [str(field) for field in table.get("fields") or []]
        if len(fields) < 13 or _key_text(fields[0]) != "代號" or _key_text(fields[1]) != "名稱":
            continue
        for values in table.get("data") or []:
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or len(values) < 13:
                continue
            security_id = str(values[0] or "").strip()
            if not security_id:
                continue
            rows.append({
                "trading_date": data_date,
                "security_id": security_id,
                "name": str(values[1] or "").strip(),
                "market": "listed",
                "margin_balance_previous": _number(values[5]),
                "margin_balance": _number(values[6]),
                "short_balance_previous": _number(values[11]),
                "short_balance": _number(values[12]),
            })
    return rows


def normalize_tpex_margin_payload(payload: Any) -> list[dict[str, Any]]:
    """Normalize the official TPEx margin-balance OpenAPI rows."""

    rows: list[dict[str, Any]] = []
    for source in payload if isinstance(payload, list) else []:
        if not isinstance(source, Mapping):
            continue
        security_id = str(source.get("SecuritiesCompanyCode") or "").strip()
        if not security_id:
            continue
        rows.append({
            "trading_date": _date_text(source.get("Date")),
            "security_id": security_id,
            "name": str(source.get("CompanyName") or "").strip(),
            "market": "otc",
            "margin_balance_previous": _number(source.get("MarginPurchaseBalancePreviousDay")),
            "margin_balance": _number(source.get("MarginPurchaseBalance")),
            "short_balance_previous": _number(source.get("ShortSaleBalancePreviousDay")),
            "short_balance": _number(source.get("ShortSaleBalance")),
        })
    return rows


def margin_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    for row in rows:
        security_id = str(row.get("security_id") or "")
        if not security_id:
            continue
        margin_balance = _number(row.get("margin_balance"))
        margin_previous = _number(row.get("margin_balance_previous"))
        short_balance = _number(row.get("short_balance"))
        metrics[security_id] = {
            "margin_balance_delta": margin_balance - margin_previous,
            "short_margin_ratio_pct": round(short_balance / margin_balance * 100, 2) if margin_balance > 0 else None,
        }
    return metrics


def load_retail_weekly_metrics(path: Path | str = HOLDER_ARCHIVE_PATH) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Return weekly reduction in TDCC 200-lot-or-less ownership by security."""

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception:
        return {}, {}
    snapshots = [
        snapshot
        for snapshot in payload.get("snapshots") or []
        if isinstance(snapshot, Mapping) and snapshot.get("date") and isinstance(snapshot.get("rows"), list)
    ]
    snapshots.sort(key=lambda snapshot: str(snapshot.get("date")))
    usable = [
        snapshot
        for snapshot in snapshots
        if any(isinstance(row, Mapping) and "retail_200_percent" in row for row in snapshot.get("rows") or [])
    ]
    if len(usable) < 2:
        return {}, {}
    previous, current = usable[-2:]
    previous_rows = {
        str(row.get("security_id")): row
        for row in previous.get("rows") or []
        if isinstance(row, Mapping) and row.get("security_id") and "retail_200_percent" in row
    }
    current_rows = {
        str(row.get("security_id")): row
        for row in current.get("rows") or []
        if isinstance(row, Mapping) and row.get("security_id") and "retail_200_percent" in row
    }
    metrics = {
        security_id: {
            "retail_sell_pctpt": round(
                float(previous_rows[security_id].get("retail_200_percent") or 0)
                - float(row.get("retail_200_percent") or 0),
                2,
            )
        }
        for security_id, row in current_rows.items()
        if security_id in previous_rows
    }
    reference = {
        "date": str(current.get("date") or ""),
        "previous_date": str(previous.get("date") or ""),
        "definition": "previous_week_200_lots_or_less_percent_minus_current_week_percent",
        "coverage_count": len(metrics),
    }
    return metrics, reference


def build_rankings(
    rows: Sequence[Mapping[str, Any]],
    supplemental_by_security: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    clean = [dict(row) for row in rows if isinstance(row, Mapping)]
    eligible = [row for row in clean if is_ordinary_equity(row)]
    supplemental = supplemental_by_security or {}

    def ranked(key: str, positive: bool) -> list[dict[str, Any]]:
        candidates = [row for row in eligible if (_number(row.get(key)) > 0) == positive and _number(row.get(key)) != 0]
        candidates.sort(key=lambda row: (_number(row.get(key)), str(row.get("security_id") or "")), reverse=positive)
        output: list[dict[str, Any]] = []
        for index, row in enumerate(candidates):
            item = {
                "security_id": str(row.get("security_id") or ""),
                "name": str(row.get("name") or ""),
                "market": str(row.get("market") or ""),
                "net_shares": _number(row.get(key)),
            }
            if index < RANKING_SUPPLEMENTAL_LIMIT:
                values = supplemental.get(item["security_id"], {})
                item.update({
                    "retail_sell_pctpt": values.get("retail_sell_pctpt"),
                    "margin_balance_delta": values.get("margin_balance_delta"),
                    "short_margin_ratio_pct": values.get("short_margin_ratio_pct"),
                })
            output.append(item)
        return output

    return {
        "eligibility_policy": RANKING_POLICY,
        "eligible_count": len(eligible),
        "excluded_count": len(clean) - len(eligible),
        "foreign_buy": ranked("foreign_net", True),
        "foreign_sell": ranked("foreign_net", False),
        "investment_trust_buy": ranked("investment_trust_net", True),
        "investment_trust_sell": ranked("investment_trust_net", False),
    }


def aggregate_market_rows(rows: Sequence[Mapping[str, Any]], limit: int = 5) -> dict[str, Any]:
    clean = [dict(row) for row in rows if isinstance(row, Mapping)]
    eligible = [row for row in clean if is_ordinary_equity(row)]

    def top(key: str, reverse: bool) -> list[dict[str, Any]]:
        ranked = sorted(eligible, key=lambda row: _number(row.get(key)), reverse=reverse)[:limit]
        return [
            {"security_id": row.get("security_id"), "name": row.get("name"), "net": _number(row.get(key))}
            for row in ranked
            if row.get("security_id")
        ]

    return {
        "stock_count": len(clean),
        "ranking_eligible_count": len(eligible),
        "ranking_excluded_count": len(clean) - len(eligible),
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
    listed_amounts: Mapping[str, Any] | None = None,
    otc_amounts: Mapping[str, Any] | None = None,
    amount_source_errors: Mapping[str, str] | None = None,
    listed_margin_rows: Sequence[Mapping[str, Any]] | None = None,
    otc_margin_rows: Sequence[Mapping[str, Any]] | None = None,
    margin_source_errors: Mapping[str, str] | None = None,
    retail_metrics: Mapping[str, Mapping[str, Any]] | None = None,
    retail_reference: Mapping[str, Any] | None = None,
    retail_source_error: str | None = None,
) -> dict[str, Any]:
    errors = dict(source_errors or {})
    amount_errors = dict(amount_source_errors or {})
    margin_errors = dict(margin_source_errors or {})
    amount_map = {"listed": dict(listed_amounts or {}), "otc": dict(otc_amounts or {})}
    margin_map = {"listed": list(listed_margin_rows or []), "otc": list(otc_margin_rows or [])}
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
    for market, source_id in (("listed", "twse_institutional_amount_summary"), ("otc", "tpex_institutional_amount_summary")):
        amounts = amount_map[market]
        missing = market in amount_errors or not amounts
        source_artifacts.append({
            "source_id": source_id,
            "market": market,
            "metric": "amount",
            "status": "missing" if missing else "fresh",
            "data_date": amounts.get("trading_date") if not missing else None,
            "row_count": 1 if not missing else 0,
            "error": amount_errors.get(market) or ("amount summary unavailable" if missing else None),
        })
    for market, source_id in (("listed", "twse_margin_short"), ("otc", "tpex_margin_short")):
        rows = margin_map[market]
        missing = market in margin_errors or not rows
        source_artifacts.append({
            "source_id": source_id,
            "market": market,
            "metric": "margin_short",
            "status": "missing" if missing else "fresh",
            "data_date": str(rows[0].get("trading_date") or "") if rows and not missing else None,
            "row_count": len(rows),
            "error": margin_errors.get(market) or ("margin balance unavailable" if missing else None),
        })
    retail_reference = dict(retail_reference or {})
    retail_metrics = dict(retail_metrics or {})
    retail_missing = bool(retail_source_error or not retail_metrics or not retail_reference)
    source_artifacts.append({
        "source_id": "tdcc_shareholder_distribution",
        "market": "listed_otc",
        "metric": "weekly_retail_200",
        "status": "missing" if retail_missing else "fresh",
        "data_date": retail_reference.get("date") if not retail_missing else None,
        "row_count": len(retail_metrics),
        "error": retail_source_error or ("weekly retail holder comparison unavailable" if retail_missing else None),
    })
    markets = {"listed": aggregate_market_rows(listed_rows), "otc": aggregate_market_rows(otc_rows)}
    for market in ("listed", "otc"):
        markets[market]["amounts"] = amount_map[market]
    all_errors = {
        **{f"{market}_shares": message for market, message in errors.items()},
        **{f"{market}_amount": message for market, message in amount_errors.items()},
        **{f"{market}_margin": message for market, message in margin_errors.items()},
    }
    for market in ("listed", "otc"):
        if not amount_map[market] and market not in amount_errors:
            all_errors[f"{market}_amount"] = "amount summary unavailable"
        if not margin_map[market] and market not in margin_errors:
            all_errors[f"{market}_margin"] = "margin balance unavailable"
    if retail_missing:
        all_errors["weekly_retail_200"] = retail_source_error or "weekly retail holder comparison unavailable"
    supplemental: dict[str, dict[str, Any]] = {}
    for security_id, values in margin_metrics([*margin_map["listed"], *margin_map["otc"]]).items():
        supplemental.setdefault(security_id, {}).update(values)
    for security_id, values in retail_metrics.items():
        supplemental.setdefault(str(security_id), {}).update(dict(values))
    return {
        "dataset_id": "daily_market_flow",
        "schema_version": "1.2.0",
        "date": data_date,
        "updated_at": _iso_now(fetched_at if isinstance(fetched_at, datetime) else None).isoformat(),
        "markets": markets,
        "rankings": build_rankings([*listed_rows, *otc_rows], supplemental),
        "supplemental_data": {
            "margin_date": data_date,
            "retail_200": retail_reference,
        },
        "source_artifacts": source_artifacts,
        "data_quality": {
            "state": "ok" if not all_errors else ("warning" if listed_rows or otc_rows else "missing"),
            "warnings": [f"{partition} source unavailable: {message}" for partition, message in all_errors.items()],
        },
    }


def _manifest_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for market, summary in (payload.get("markets") or {}).items():
        if not isinstance(summary, Mapping):
            continue
        amounts = summary.get("amounts") if isinstance(summary.get("amounts"), Mapping) else {}
        rows.append({
            "trading_date": payload.get("date"),
            "market": market,
            "foreign_net": _number(summary.get("foreign_net")),
            "investment_trust_net": _number(summary.get("investment_trust_net")),
            "institutional_total_net": _number(summary.get("institutional_total_net")),
            "foreign_net_amount": _number(amounts.get("foreign_net_amount")),
            "investment_trust_net_amount": _number(amounts.get("investment_trust_net_amount")),
            "institutional_total_net_amount": _number(amounts.get("institutional_total_net_amount")),
            "ranking_eligible_count": _number(summary.get("ranking_eligible_count")),
            "ranking_excluded_count": _number(summary.get("ranking_excluded_count")),
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


def _collect_for_target(target: date, fetched_at: datetime) -> dict[str, Any]:
    date_param = target.strftime("%Y%m%d")
    expected_date = target.isoformat()
    listed_rows: list[dict[str, Any]] = []
    otc_rows: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    amounts: dict[str, dict[str, Any]] = {}
    amount_errors: dict[str, str] = {}
    margin_rows: dict[str, list[dict[str, Any]]] = {"listed": [], "otc": []}
    margin_errors: dict[str, str] = {}
    try:
        listed_rows = normalize_twse_payload(_fetch_json(TWSE_URL, {"response": "json", "selectType": "ALLBUT0999", "date": date_param}), date_param)
    except Exception as exc:
        errors["listed"] = str(exc)[:200]
    try:
        otc_rows = normalize_tpex_payload(_fetch_json(TPEX_URL), date_param)
    except Exception as exc:
        errors["otc"] = str(exc)[:200]
    try:
        amounts["listed"] = normalize_amount_payload(
            _fetch_json(TWSE_AMOUNT_URL, {"response": "json", "dayDate": date_param, "type": "day"}),
            "listed",
        )
        if not amounts["listed"].get("trading_date"):
            raise ValueError("TWSE amount summary returned no trading date")
        if not amounts["listed"].get("institutional_total_buy_amount") and not amounts["listed"].get("institutional_total_sell_amount"):
            raise ValueError("TWSE amount summary returned no monetary totals")
    except Exception as exc:
        amount_errors["listed"] = str(exc)[:200]
    try:
        amounts["otc"] = normalize_amount_payload(
            _post_json(TPEX_AMOUNT_URL, {"type": "Daily", "prod": "1", "date": date_param, "response": "json"}),
            "otc",
        )
        if not amounts["otc"].get("trading_date"):
            raise ValueError("TPEx amount summary returned no trading date")
        if not amounts["otc"].get("institutional_total_buy_amount") and not amounts["otc"].get("institutional_total_sell_amount"):
            raise ValueError("TPEx amount summary returned no monetary totals")
    except Exception as exc:
        amount_errors["otc"] = str(exc)[:200]
    try:
        margin_rows["listed"] = normalize_twse_margin_payload(
            _fetch_json(TWSE_MARGIN_URL, {"response": "json", "selectType": "ALL", "date": date_param})
        )
    except Exception as exc:
        margin_errors["listed"] = str(exc)[:200]
    try:
        margin_rows["otc"] = normalize_tpex_margin_payload(_fetch_json(TPEX_MARGIN_URL))
    except Exception as exc:
        margin_errors["otc"] = str(exc)[:200]

    detail_rows = {"listed": listed_rows, "otc": otc_rows}
    for market in ("listed", "otc"):
        if market in errors:
            continue
        rows = detail_rows[market]
        if not rows:
            errors[market] = f"no rows returned for {expected_date}"
            continue
        row_dates = {str(row.get("trading_date") or "") for row in rows}
        if row_dates != {expected_date}:
            actual = ", ".join(sorted(item or "missing" for item in row_dates))
            errors[market] = f"detail date {actual} does not align with requested date {expected_date}"
            detail_rows[market] = []

    for market in ("listed", "otc"):
        amount_date = str((amounts.get(market) or {}).get("trading_date") or "")
        if amount_date and amount_date != expected_date:
            amount_errors[market] = f"amount date {amount_date} does not align with requested date {expected_date}"
            amounts.pop(market, None)
    for market in ("listed", "otc"):
        if market in margin_errors:
            continue
        rows = margin_rows[market]
        if not rows:
            margin_errors[market] = f"no margin rows returned for {expected_date}"
            continue
        row_dates = {str(row.get("trading_date") or "") for row in rows}
        if row_dates != {expected_date}:
            actual = ", ".join(sorted(item or "missing" for item in row_dates))
            margin_errors[market] = f"margin date {actual} does not align with requested date {expected_date}"
            margin_rows[market] = []
    retail_metrics, retail_reference = load_retail_weekly_metrics()
    retail_error = None if retail_metrics and retail_reference else "TDCC archive lacks two complete 200-lot-or-less snapshots"
    return build_payload(
        detail_rows["listed"],
        detail_rows["otc"],
        data_date=expected_date,
        fetched_at=fetched_at,
        source_errors=errors,
        listed_amounts=amounts.get("listed"),
        otc_amounts=amounts.get("otc"),
        amount_source_errors=amount_errors,
        listed_margin_rows=margin_rows["listed"],
        otc_margin_rows=margin_rows["otc"],
        margin_source_errors=margin_errors,
        retail_metrics=retail_metrics,
        retail_reference=retail_reference,
        retail_source_error=retail_error,
    )


def _is_complete_snapshot(payload: Mapping[str, Any]) -> bool:
    artifacts = payload.get("source_artifacts") or []
    required = {
        "twse_institutional_trading",
        "tpex_institutional_trading",
        "twse_institutional_amount_summary",
        "tpex_institutional_amount_summary",
        "twse_margin_short",
        "tpex_margin_short",
        "tdcc_shareholder_distribution",
    }
    fresh = {
        str(item.get("source_id"))
        for item in artifacts
        if item.get("status") == "fresh" and _number(item.get("row_count")) > 0
    }
    return required.issubset(fresh) and (payload.get("data_quality") or {}).get("state") == "ok"


def collect(target_date: date | None = None, *, now: datetime | None = None) -> dict[str, Any]:
    fetched_at = _iso_now(now)
    if target_date is not None:
        return _collect_for_target(target_date, fetched_at)

    latest_failed: dict[str, Any] | None = None
    for offset in range(MAX_AUTO_LOOKBACK_DAYS):
        candidate = fetched_at.date() - timedelta(days=offset)
        payload = _collect_for_target(candidate, fetched_at)
        if latest_failed is None:
            latest_failed = payload
        if _is_complete_snapshot(payload):
            return payload
    return latest_failed or _collect_for_target(fetched_at.date(), fetched_at)


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect daily TWSE/TPEx institutional-flow aggregates.")
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    args = parser.parse_args()
    payload = collect(args.date)
    if not _is_complete_snapshot(payload):
        warnings = "; ".join((payload.get("data_quality") or {}).get("warnings") or ["official partitions incomplete"])
        print(f"[market_flow][WARN] keeping the existing artifact; {warnings}")
        return 0
    write_payload(payload, args.output, args.manifest)
    print(f"[market_flow] wrote {args.output} date={payload.get('date')} state={(payload.get('data_quality') or {}).get('state')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
