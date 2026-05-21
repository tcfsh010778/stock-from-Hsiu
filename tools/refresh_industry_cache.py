from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "data" / "stock_industries.json"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"


def normalize_stock_info(rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        sid = str(row.get("stock_id") or "").strip()
        if not sid:
            continue
        out[sid] = {
            "stock_name": str(row.get("stock_name") or "").strip(),
            "industry_category": str(row.get("industry_category") or "").strip(),
            "type": str(row.get("type") or "").strip(),
            "date": str(row.get("date") or "").strip(),
        }
    return out


def fetch_stock_info(timeout: int = 60) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode({"dataset": "TaiwanStockInfo"})
    req = urllib.request.Request(f"{FINMIND_URL}?{query}", headers={"User-Agent": "stock-from-Hsiu-site"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("status") != 200:
        raise RuntimeError(f"FinMind TaiwanStockInfo status={payload.get('status')} msg={payload.get('msg')}")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise RuntimeError("FinMind TaiwanStockInfo returned no rows")
    return data


def write_cache(stocks: dict[str, dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": FINMIND_URL,
        "dataset": "TaiwanStockInfo",
        "stocks": stocks,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh stock industry category cache from FinMind TaiwanStockInfo.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    try:
        stocks = normalize_stock_info(fetch_stock_info())
        write_cache(stocks, args.out)
        print(f"[refresh_industry_cache] wrote {args.out} stocks={len(stocks)}")
    except Exception as exc:
        if args.out.exists():
            print(f"::warning::industry cache refresh failed; keeping existing cache: {exc}")
            return
        print(f"industry cache refresh failed and no cache exists: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
