from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def verify(navigation: str) -> dict:
    manifest_path = DOCS / "v2" / "data" / "index.json"
    if not manifest_path.exists():
        raise AssertionError("docs/v2/data/index.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
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
    if risk.get("stop_loss_pct") != 15.0 or risk.get("stop_price") != 25.7125:
        raise AssertionError(f"2353 fixed 15% stop was not generated: {risk}")
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
    return {"stocks": manifest["stock_count"], "excluded": manifest.get("excluded_count", 0), "coverage": manifest.get("coverage"), "fixed_stop_2353": risk["stop_price"], "navigation": navigation}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--navigation", choices=("legacy", "switched"), required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.navigation), ensure_ascii=False))


if __name__ == "__main__":
    main()
