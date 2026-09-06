from __future__ import annotations

import argparse
import json
import math
import re
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
EXPECTED_TECHNICAL_INDICATORS = {
    "rsi_14", "macd_12_26_9", "bollinger_20_2", "volume_vs_avg_3", "volume_vs_avg_5", "volume_vs_avg_10",
}
MA_WINDOWS = (5, 20, 60, 120, 240)
DAILY_RETURNED_BARS = 240
PRICE_BASIS_MODE = "official_reference_ratio_back_adjusted_v1"
PRICE_BASIS_SOURCE = "TWSE TWT49U / TPEx exDailyQ official reference prices"


def verify_daily_packet_contract(daily: dict, expected_price_date: str, *, stock_id: str) -> None:
    if daily.get("timeframe") != "daily":
        raise AssertionError(f"{stock_id} packet is not daily")
    if daily.get("stock_id") != stock_id:
        raise AssertionError(f"{stock_id} daily packet stock id mismatch: {daily.get('stock_id')}")
    if daily.get("data_date") != expected_price_date:
        raise AssertionError(f"{stock_id} daily packet date mismatch: {daily.get('data_date')} != {expected_price_date}")

    coverage = daily.get("series_coverage")
    if not isinstance(coverage, dict):
        raise AssertionError(f"{stock_id} daily packet has no series coverage")
    try:
        requested = int(coverage.get("requested_bars"))
        available = int(coverage.get("available_bars"))
        returned = int(coverage.get("returned_bars"))
    except (TypeError, ValueError) as exc:
        raise AssertionError(f"{stock_id} daily series coverage is not numeric") from exc
    expected_returned = min(available, DAILY_RETURNED_BARS)
    if requested != DAILY_RETURNED_BARS or returned != expected_returned:
        raise AssertionError(
            f"{stock_id} daily series coverage mismatch: requested={requested}, "
            f"available={available}, returned={returned}, expected={expected_returned}"
        )
    expected_status = "available" if available >= DAILY_RETURNED_BARS else "insufficient_history"
    if coverage.get("status") != expected_status:
        raise AssertionError(f"{stock_id} daily series coverage status mismatch: {coverage.get('status')} != {expected_status}")
    series = daily.get("series")
    if not isinstance(series, list) or len(series) != returned or returned <= 0:
        raise AssertionError(f"{stock_id} daily series length differs from returned coverage")

    dates: list[str] = []
    offset = available - returned
    for index, row in enumerate(series):
        if not isinstance(row, dict):
            raise AssertionError(f"{stock_id} daily series row is not an object")
        row_date = str(row.get("date") or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", row_date):
            raise AssertionError(f"{stock_id} daily series contains invalid date: {row_date}")
        try:
            date.fromisoformat(row_date)
        except ValueError as exc:
            raise AssertionError(f"{stock_id} daily series contains invalid date: {row_date}") from exc
        dates.append(row_date)
        for window in MA_WINDOWS:
            value = row.get(f"sma{window}")
            should_exist = offset + index + 1 >= window
            if should_exist:
                try:
                    numeric = float(value)
                except (TypeError, ValueError) as exc:
                    raise AssertionError(f"{stock_id} sma{window} missing after warmup at {row_date}") from exc
                if not math.isfinite(numeric):
                    raise AssertionError(f"{stock_id} sma{window} is non-finite at {row_date}")
            elif value is not None:
                raise AssertionError(f"{stock_id} sma{window} exists before warmup at {row_date}")
        try:
            factor = float(row.get("adjustment_factor"))
            volume = float(row.get("volume"))
        except (TypeError, ValueError) as exc:
            raise AssertionError(f"{stock_id} daily factor or share volume is not numeric at {row_date}") from exc
        if not math.isfinite(factor) or factor <= 0 or not math.isfinite(volume) or volume < 0:
            raise AssertionError(f"{stock_id} daily factor or share volume is invalid at {row_date}")
    if dates != sorted(set(dates)):
        raise AssertionError(f"{stock_id} daily dates are duplicate or not strictly increasing")
    if dates[-1] != expected_price_date:
        raise AssertionError(f"{stock_id} latest daily row differs from official expected date")
    if not math.isclose(float(series[-1]["adjustment_factor"]), 1.0, rel_tol=0.0, abs_tol=1e-12):
        raise AssertionError(f"{stock_id} latest adjustment factor is not anchored at one")

    basis = daily.get("price_adjustment")
    if not isinstance(basis, dict):
        raise AssertionError(f"{stock_id} daily packet has no price basis metadata")
    required_basis = {
        "mode": PRICE_BASIS_MODE,
        "source": PRICE_BASIS_SOURCE,
        "verified": True,
        "adjustment_as_of": expected_price_date,
        "volume_basis": "official_raw_shares",
    }
    mismatched = {key: (basis.get(key), expected) for key, expected in required_basis.items() if basis.get(key) != expected}
    if mismatched:
        raise AssertionError(f"{stock_id} daily price basis metadata mismatch: {mismatched}")


def verify_price_freshness(manifest: dict, price_summary: dict) -> str:
    if price_summary.get("status") != "fresh":
        raise AssertionError(f"official price refresh is not fresh: {price_summary.get('status')}")
    expected_price_date = price_summary.get("latest_data_date")
    if manifest.get("price_refresh_status") != "fresh":
        raise AssertionError(f"V2 manifest price refresh is not fresh: {manifest.get('price_refresh_status')}")
    if not expected_price_date or manifest.get("price_data_date") != expected_price_date:
        raise AssertionError(
            f"V2 price date mismatch: manifest={manifest.get('price_data_date')}, "
            f"official={expected_price_date}"
        )
    return str(expected_price_date)


def verify_fixed_stop(risk: dict, daily: dict, expected_price_date: str) -> float:
    if daily.get("data_date") != expected_price_date:
        raise AssertionError(
            f"2353 daily packet date mismatch: "
            f"packet={daily.get('data_date')}, expected={expected_price_date}"
        )
    series = daily.get("series") or []
    if not series or not isinstance(series[-1], dict):
        raise AssertionError("2353 daily packet has no latest price row")
    latest = series[-1]
    if latest.get("date") != expected_price_date:
        raise AssertionError(
            f"2353 latest price row date mismatch: "
            f"row={latest.get('date')}, expected={expected_price_date}"
        )
    if risk.get("method") != "fixed_percent_from_latest_close":
        raise AssertionError(f"2353 fixed-stop method is invalid: {risk}")
    if risk.get("reference_date") != expected_price_date:
        raise AssertionError(
            f"2353 fixed-stop reference date mismatch: "
            f"risk={risk.get('reference_date')}, expected={expected_price_date}"
        )
    try:
        latest_close = float(latest.get("close"))
        reference_price = float(risk.get("reference_price"))
        stop_loss_pct = float(risk.get("stop_loss_pct"))
        stop_price = float(risk.get("stop_price"))
    except (TypeError, ValueError) as exc:
        raise AssertionError(f"2353 fixed-stop values are not numeric: {risk}") from exc
    if not all(math.isfinite(value) for value in (latest_close, reference_price, stop_loss_pct, stop_price)):
        raise AssertionError(f"2353 fixed-stop values are not finite: {risk}")
    if latest_close <= 0 or reference_price <= 0 or stop_price <= 0 or stop_loss_pct != 15.0:
        raise AssertionError(f"2353 fixed 15% stop was not generated: {risk}")
    expected_reference = round(latest_close, 4)
    if not math.isclose(reference_price, expected_reference, rel_tol=0.0, abs_tol=1e-9):
        raise AssertionError(
            f"2353 fixed-stop reference price mismatch: "
            f"risk={reference_price}, latest_close={expected_reference}"
        )
    expected_stop = round(latest_close * (1.0 - stop_loss_pct / 100.0), 4)
    if not math.isclose(stop_price, expected_stop, rel_tol=0.0, abs_tol=1e-9):
        raise AssertionError(
            f"2353 fixed 15% stop mismatch: actual={stop_price}, expected={expected_stop}, risk={risk}"
        )
    return stop_price


def verify_technical_evidence(daily: dict) -> set[str]:
    items = daily.get("technical_evidence")
    if not isinstance(items, list):
        raise AssertionError("2353 daily packet has no technical evidence cards")
    indicator_ids = {str(item.get("indicator_id")) for item in items if isinstance(item, dict)}
    if indicator_ids != EXPECTED_TECHNICAL_INDICATORS or len(items) != len(EXPECTED_TECHNICAL_INDICATORS):
        raise AssertionError(f"2353 technical evidence ids mismatch: {sorted(indicator_ids)}")
    for item in items:
        if item.get("calculation_basis") != "closed_bar_only":
            raise AssertionError(f"technical evidence is not closed-bar-only: {item.get('indicator_id')}")
        if item.get("evidence_role") != "auxiliary_evidence_only":
            raise AssertionError(f"technical evidence role is not auxiliary-only: {item.get('indicator_id')}")
        if item.get("value_status") not in {"available", "insufficient_history", "missing", "non_finite"}:
            raise AssertionError(f"technical evidence value status is not explicit: {item.get('indicator_id')}")
    return indicator_ids


def verify(navigation: str) -> dict:
    manifest_path = DOCS / "v2" / "data" / "index.json"
    if not manifest_path.exists():
        raise AssertionError("docs/v2/data/index.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    price_summary_path = DOCS / "data" / "price_refresh_summary.json"
    if not price_summary_path.exists():
        raise AssertionError("docs/data/price_refresh_summary.json is missing")
    price_summary = json.loads(price_summary_path.read_text(encoding="utf-8"))
    expected_price_date = verify_price_freshness(manifest, price_summary)
    if manifest.get("failure_count") != 0:
        raise AssertionError(f"V2 manifest contains failures: {manifest.get('failures', [])[:3]}")
    if manifest.get("stock_count", 0) < 400:
        raise AssertionError(f"V2 stock coverage too small: {manifest.get('stock_count')}")
    for stock_id in manifest.get("stocks") or []:
        packet_path = DOCS / "v2" / "data" / f"{stock_id}.json"
        if not packet_path.exists():
            raise AssertionError(f"daily packet artifact is missing: {stock_id}")
        stock_packets = json.loads(packet_path.read_text(encoding="utf-8"))
        daily_packets = [packet for packet in stock_packets if isinstance(packet, dict) and packet.get("timeframe") == "daily"]
        if len(daily_packets) != 1:
            raise AssertionError(f"{stock_id} must have exactly one daily packet")
        verify_daily_packet_contract(daily_packets[0], expected_price_date, stock_id=str(stock_id))
    for relative in ("v2/stock.html", "v2/stocks/2353.html", "v2/data/2353.json", "stocks/2353.html"):
        if not (DOCS / relative).exists():
            raise AssertionError(f"required public artifact missing: docs/{relative}")
    packets = json.loads((DOCS / "v2" / "data" / "2353.json").read_text(encoding="utf-8"))
    daily = next(packet for packet in packets if packet["timeframe"] == "daily")
    if "decision" in daily:
        raise AssertionError("public V2 packet still contains semantic decision output")
    risk = daily.get("risk_control") or {}
    fixed_stop = verify_fixed_stop(risk, daily, expected_price_date)
    technical_indicators = verify_technical_evidence(daily)
    if not daily.get("trendlines"):
        raise AssertionError("2353 has no generated trendline evidence")
    combined = "\n".join((DOCS / rel).read_text(encoding="utf-8", errors="replace") for rel in ("v2/stock.html", "v2/assets/v2.js", "v2/data/2353.json"))
    forbidden = ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "sk-ant-", "sk-proj-", "C:\\\\", "OneDrive")
    found = [token for token in forbidden if token in combined]
    if found:
        raise AssertionError(f"private material found in public V2: {found}")
    for page in (DOCS / "index.html", DOCS / "stocks.html"):
        text = page.read_text(encoding="utf-8")
        v2_links = len(re.findall(r'href="v2/stocks/[0-9A-Za-z]+\.html"', text))
        legacy_links = len(re.findall(r'href="stocks/[0-9A-Za-z]+\.html"', text))
        if navigation == "switched" and not v2_links:
            raise AssertionError(f"{page.name} has no V2 stock navigation links")
        if navigation == "legacy" and v2_links:
            raise AssertionError(f"{page.name} switched before V2 release validation")
    search_text = (DOCS / "stocks.html").read_text(encoding="utf-8")
    if navigation == "switched":
        missing = [sid for sid in manifest["stocks"] if f'href="v2/stocks/{sid}.html"' not in search_text]
        if missing:
            raise AssertionError(f"search page did not switch available V2 ids: {missing[:5]}")
    ui = "\n".join((DOCS / rel).read_text(encoding="utf-8") for rel in ("v2/stock.html", "v2/assets/v2.js"))
    semantic_tokens = ("action-state", "SETUP", "WATCH", "NO-GO", "R:R", "目標價")
    found_semantics = [token for token in semantic_tokens if token in ui]
    if found_semantics:
        raise AssertionError(f"semantic decision labels remain in V2 UI: {found_semantics}")
    if "LightweightCharts" not in ui or "15% 停損" not in ui:
        raise AssertionError("TradingView-style workbench or fixed stop rendering is missing")
    if "技術分析證據卡" not in ui or "technical-evidence" not in ui or not {"RSI", "MACD", "布林"}.issubset(set(re.findall(r"RSI|MACD|布林", ui))):
        raise AssertionError("technical evidence cards are missing from the public V2 UI")
    return {"stocks": manifest["stock_count"], "excluded": manifest.get("excluded_count", 0), "coverage": manifest.get("coverage"), "fixed_stop_2353": fixed_stop, "technical_indicators": sorted(technical_indicators), "navigation": navigation, "price_data_date": expected_price_date}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--navigation", choices=("legacy", "switched"), required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.navigation), ensure_ascii=False))


if __name__ == "__main__":
    main()
