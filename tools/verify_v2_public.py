from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


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
    for relative in ("v2/stock.html", "v2/stocks/2353.html", "v2/data/2353.json", "stocks/2353.html"):
        if not (DOCS / relative).exists():
            raise AssertionError(f"required public artifact missing: docs/{relative}")
    packets = json.loads((DOCS / "v2" / "data" / "2353.json").read_text(encoding="utf-8"))
    daily = next(packet for packet in packets if packet["timeframe"] == "daily")
    if "decision" in daily:
        raise AssertionError("public V2 packet still contains semantic decision output")
    risk = daily.get("risk_control") or {}
    fixed_stop = verify_fixed_stop(risk, daily, expected_price_date)
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
    return {"stocks": manifest["stock_count"], "excluded": manifest.get("excluded_count", 0), "coverage": manifest.get("coverage"), "fixed_stop_2353": fixed_stop, "navigation": navigation, "price_data_date": expected_price_date}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--navigation", choices=("legacy", "switched"), required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.navigation), ensure_ascii=False))


if __name__ == "__main__":
    main()
