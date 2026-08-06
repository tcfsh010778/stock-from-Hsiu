from __future__ import annotations

import csv
import json
import math
import os
from collections import defaultdict
from datetime import date
from pathlib import Path

from stock_rules import is_overheated as _shared_is_overheated
from stock_rules import mda_stock_status
from stock_rules import overheat_reason_text as _shared_overheat_reason_text
from stock_rules import overheat_reasons as _shared_overheat_reasons

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
REPORTS_DIR = ROOT / "reports"
PRICE_DIR = DATA_DIR / "prices"
SCAN_PATH = DATA_DIR / "mda_universe_scan.json"
MDA_CANDIDATES_PATH = DATA_DIR / "mda_candidates.json"
SFZ_ALL_PATH = DATA_DIR / "sfz_all.json"
MARKETS_PATH = DATA_DIR / "stock_markets.json"
INDUSTRY_PATH = DATA_DIR / "stock_industries.json"
MIN_CLOSE = float(os.environ.get("DAILY_TOP20_MIN_CLOSE", "20"))
TOP_N = int(os.environ.get("DAILY_TOP20_N", "20"))
SECTOR_TOP_N = int(os.environ.get("DAILY_TOP20_SECTOR_TOP_N", "8"))
SECTOR_MAX_PER_GROUP = int(os.environ.get("DAILY_TOP20_SECTOR_MAX_PER_GROUP", "4"))


def to_float(value, default=None):
    try:
        if value in (None, ""):
            return default
        value = str(value).replace(",", "").replace("%", "").replace("+", "").strip()
        return float(value)
    except Exception:
        return default


def fmt_num(value, digits=2, unit=""):
    value = to_float(value, None)
    if value is None or not math.isfinite(value):
        return "-"
    if digits <= 0:
        text = f"{value:.0f}"
    else:
        text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return f"{text}{unit}"


def _fmt_signed_pct(value, digits: int = 2) -> str:
    value = to_float(value, None)
    if value is None or not math.isfinite(value):
        return "-"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{digits}f}%"


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp950"):
        try:
            with path.open("r", encoding=encoding, newline="") as fh:
                return list(csv.DictReader(fh))
        except UnicodeDecodeError:
            continue
    return []


def price_rows(stock_id: str) -> list[dict]:
    rows = []
    for row in read_csv_rows(PRICE_DIR / f"{stock_id}.csv"):
        try:
            rows.append({
                "date": row.get("date", ""),
                "open": float(row.get("open") or row.get("Open") or 0),
                "high": float(row.get("high") or row.get("max") or row.get("High") or 0),
                "low": float(row.get("low") or row.get("min") or row.get("Low") or 0),
                "close": float(row.get("close") or row.get("Close") or 0),
                "volume": float(row.get("volume") or row.get("Trading_Volume") or row.get("Volume") or 0),
            })
        except Exception:
            continue
    return sorted([r for r in rows if r["date"] and r["close"] > 0], key=lambda r: r["date"])


def sma(values: list[float], window: int):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def rsi(values: list[float], window: int = 14):
    if len(values) <= window:
        return None
    gains = []
    losses = []
    for i in range(len(values) - window, len(values)):
        diff = values[i] - values[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / window
    avg_loss = sum(losses) / window
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def bb_pct(values: list[float], window: int = 20):
    if len(values) < window:
        return None
    recent = values[-window:]
    mid = sum(recent) / window
    var = sum((x - mid) ** 2 for x in recent) / window
    std = math.sqrt(var)
    upper = mid + 2 * std
    lower = mid - 2 * std
    if upper <= lower:
        return None
    return (values[-1] - lower) / (upper - lower) * 100


def pct_change(now, before):
    if now is None or before in (None, 0):
        return None
    return (now / before - 1) * 100


def is_overheated(stock: dict) -> bool:
    """PR3 guard: any condition forces the stock into 過熱/風險."""
    return _shared_is_overheated(stock)


def overheat_reasons(stock: dict) -> list[str]:
    return _shared_overheat_reasons(stock)


def overheat_reason_text(stock: dict) -> str:
    return _shared_overheat_reason_text(stock)


def load_scan_rows() -> list[dict]:
    if not SCAN_PATH.exists():
        return []
    rows = json.loads(SCAN_PATH.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        return []
    markets = {}
    if MARKETS_PATH.exists():
        try:
            markets = (json.loads(MARKETS_PATH.read_text(encoding="utf-8")).get("stocks") or {})
        except Exception:
            markets = {}
    for row in rows:
        sid = str(row.get("stock_id") or "")
        info = markets.get(sid) or {}
        if info.get("name") and not row.get("name"):
            row["name"] = info["name"]
    return [r for r in rows if to_float(r.get("close"), 0) >= MIN_CLOSE]


def load_industry_map(path: Path = INDUSTRY_PATH) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    stocks = payload.get("stocks") if isinstance(payload, dict) else payload
    if not isinstance(stocks, dict):
        return {}
    out: dict[str, dict] = {}
    for sid, item in stocks.items():
        if isinstance(item, str):
            out[str(sid)] = {"industry_category": item}
        elif isinstance(item, dict):
            out[str(sid)] = item
    return out


def sector_for_stock(stock_id: str, industry_map: dict[str, dict], fallback: str = "") -> str:
    info = industry_map.get(str(stock_id)) or {}
    sector = info.get("industry_category") or info.get("sector") or fallback
    sector = str(sector or "").strip()
    if not sector or sector.upper() == "ETF":
        return "未分類"
    return sector


def price_money_metric(stock_id: str) -> dict | None:
    rows = price_rows(stock_id)
    if len(rows) < 6:
        return None
    latest = rows[-1]
    close = to_float(latest.get("close"))
    volume = to_float(latest.get("volume"), 0)
    if close is None or close < MIN_CLOSE or not volume:
        return None
    ret5 = pct_change(close, rows[-6].get("close"))
    ret20 = pct_change(close, rows[-21].get("close")) if len(rows) >= 21 else None
    vol5 = sum(to_float(r.get("volume"), 0) for r in rows[-5:]) / 5
    vol20 = sum(to_float(r.get("volume"), 0) for r in rows[-20:]) / min(20, len(rows))
    vol_ratio = vol5 / vol20 if vol20 else None
    return {
        "date": latest.get("date") or "",
        "close": close,
        "volume": volume,
        "turnover_billion": close * volume / 100_000_000,
        "ret5": ret5,
        "ret20": ret20,
        "vol_ratio": vol_ratio,
    }


def market_sector_flow(industry_map: dict[str, dict]) -> dict[str, dict]:
    if not industry_map:
        return {}
    stock_metrics = []
    for path in PRICE_DIR.glob("*.csv"):
        sid = path.stem
        sector = sector_for_stock(sid, industry_map)
        if sector == "未分類":
            continue
        metric = price_money_metric(sid)
        if not metric:
            continue
        stock_metrics.append({"stock_id": sid, "sector": sector, **metric})
    latest_date = max((m.get("date", "") for m in stock_metrics), default="")
    if latest_date:
        stock_metrics = [m for m in stock_metrics if m.get("date") == latest_date]

    groups: dict[str, dict] = defaultdict(lambda: {
        "stock_count": 0,
        "turnover_billion": 0.0,
        "weighted_ret5": 0.0,
        "weighted_ret20": 0.0,
        "weighted_vol_ratio": 0.0,
        "advance_count": 0,
    })
    for metric in stock_metrics:
        turnover = to_float(metric.get("turnover_billion"), 0) or 0
        weight = max(turnover, 0.01)
        g = groups[str(metric["sector"])]
        g["stock_count"] += 1
        g["turnover_billion"] += turnover
        g["weighted_ret5"] += (to_float(metric.get("ret5"), 0) or 0) * weight
        g["weighted_ret20"] += (to_float(metric.get("ret20"), 0) or 0) * weight
        g["weighted_vol_ratio"] += (to_float(metric.get("vol_ratio"), 1) or 1) * weight
        if (to_float(metric.get("ret5"), 0) or 0) > 0:
            g["advance_count"] += 1

    summaries = []
    for sector, g in groups.items():
        turnover = g["turnover_billion"]
        if turnover <= 0:
            continue
        avg_ret5 = g["weighted_ret5"] / turnover
        avg_ret20 = g["weighted_ret20"] / turnover
        avg_vol_ratio = g["weighted_vol_ratio"] / turnover
        breadth = g["advance_count"] / max(g["stock_count"], 1)
        score = math.log1p(turnover) * 18 + avg_ret5 * 2.5 + avg_ret20 * 0.8 + min(avg_vol_ratio, 3.0) * 8 + breadth * 12
        summaries.append({
            "sector": sector,
            "rank": 0,
            "score": score,
            "stock_count": g["stock_count"],
            "turnover_billion": turnover,
            "avg_ret5": avg_ret5,
            "avg_ret20": avg_ret20,
            "avg_vol_ratio": avg_vol_ratio,
            "breadth": breadth,
            "date": latest_date,
        })
    summaries.sort(key=lambda r: (-to_float(r.get("score"), 0), str(r.get("sector") or "")))
    for idx, row in enumerate(summaries, 1):
        row["rank"] = idx
    return {str(row["sector"]): row for row in summaries}


def stock_status(row: dict) -> tuple[str, str]:
    return mda_stock_status(row)


def enrich(row: dict, industry_map: dict[str, dict] | None = None) -> dict:
    sid = str(row.get("stock_id") or "")
    rows = price_rows(sid)
    closes = [r["close"] for r in rows]
    latest = rows[-1] if rows else {}
    close = latest.get("close") or to_float(row.get("close"), None)
    latest_volume = to_float(latest.get("volume"), None)
    turnover_value = close * latest_volume if close is not None and latest_volume is not None else None
    high21 = max((r["high"] for r in rows[-21:]), default=None)
    low21 = min((r["low"] for r in rows[-21:]), default=None)
    ma5 = sma(closes, 5)
    ma34 = sma(closes, 34)
    gain_6w = pct_change(close, closes[-31]) if close and len(closes) >= 31 else to_float(row.get("gain_6w"), None)
    gain_3d = pct_change(close, closes[-4]) if close and len(closes) >= 4 else to_float(row.get("gain_3d"), None)
    vol5 = (sum(r["volume"] for r in rows[-5:]) / 5 / 1000) if len(rows) >= 5 else None
    entry = ma5 * 0.985 if ma5 else close
    target = high21 * 1.02 if high21 else (close * 1.12 if close else None)
    stop = low21 * 0.995 if low21 else (close * 0.9 if close else None)
    rsi_value = rsi(closes)
    bb_value = bb_pct(closes)
    enriched = {
        **row,
        "rank_basket": row.get("rank_basket") or row.get("original_basket") or row.get("basket"),
        "stock_id": sid,
        "name": row.get("name") or sid,
        "sector": row.get("sector") or row.get("industry_category") or sector_for_stock(sid, industry_map or {}),
        "date": latest.get("date") or row.get("date") or "",
        "close": close,
        "gain_6w": gain_6w,
        "gain_3d": gain_3d,
        "rsi": rsi_value if rsi_value is not None else to_float(row.get("rsi") or row.get("rsi_14"), None),
        "bb_pct": bb_value if bb_value is not None else to_float(row.get("bb_pct") or row.get("percent_b"), None),
        "ma5": ma5,
        "ma34": ma34,
        "vol5_lot": vol5,
        "latest_volume": latest_volume,
        "turnover_value": turnover_value,
        "high21": high21,
        "low21": low21,
        "entry": entry,
        "target": target,
        "stop": stop,
    }
    icon, status = stock_status(enriched)
    enriched["icon"] = icon
    enriched["status"] = status
    if is_overheated(enriched):
        original_basket = enriched.get("basket")
        if original_basket and original_basket != "過熱/風險":
            enriched["original_basket"] = original_basket
        enriched["basket"] = "過熱/風險"
        hot_reason = overheat_reason_text(enriched)
        original_reason = str(row.get("reason") or "").strip()
        if hot_reason:
            if original_reason.startswith("強制過熱排除"):
                enriched["reason"] = original_reason
            else:
                enriched["reason"] = hot_reason + (f"；原判讀：{original_reason}" if original_reason else "")
    return enriched


def _basket_rank(row: dict) -> int:
    basket = str(row.get("rank_basket") or row.get("basket") or "")
    if basket == "已發動籃" or "已發動" in basket:
        return 0
    if "空轉多" in basket:
        return 1
    if "未發動" in basket:
        return 2
    if "不納入" in basket:
        return 3
    return 9


def rank_candidates_with_sector_flow(
    rows: list[dict],
    sector_scores: dict[str, dict],
    top_n: int = TOP_N,
    sector_top_n: int = SECTOR_TOP_N,
    max_per_sector: int = SECTOR_MAX_PER_GROUP,
) -> list[dict]:
    def base_key(row: dict):
        return (
            _basket_rank(row),
            -to_float(row.get("score"), 0),
            str(row.get("stock_id") or ""),
        )

    if not sector_scores:
        return sorted(rows, key=base_key)[:top_n]

    ranked_rows = []
    for row in rows:
        item = dict(row)
        sector = str(item.get("sector") or "未分類")
        summary = sector_scores.get(sector) or {}
        item["sector_rank"] = summary.get("rank")
        item["sector_flow_score"] = summary.get("score")
        item["sector_turnover_billion"] = summary.get("turnover_billion")
        ranked_rows.append(item)

    def sector_key(row: dict):
        rank = to_float(row.get("sector_rank"), 999) or 999
        hot_rank = rank if rank <= sector_top_n else 999
        return (
            hot_rank,
            -to_float(row.get("sector_flow_score"), 0),
            *base_key(row),
        )

    sorted_rows = sorted(ranked_rows, key=sector_key)
    selected = []
    selected_ids = set()
    sector_counts: dict[str, int] = defaultdict(int)
    for row in sorted_rows:
        sector = str(row.get("sector") or "未分類")
        if sector_counts[sector] >= max_per_sector:
            continue
        selected.append(row)
        selected_ids.add(row.get("stock_id"))
        sector_counts[sector] += 1
        if len(selected) >= top_n:
            return selected
    for row in sorted_rows:
        if row.get("stock_id") in selected_ids:
            continue
        selected.append(row)
        if len(selected) >= top_n:
            break
    return selected


def rank_all_candidates_with_sector_flow(rows: list[dict], sector_scores: dict[str, dict]) -> list[dict]:
    def base_key(row: dict):
        return (
            _basket_rank(row),
            -to_float(row.get("score"), 0),
            str(row.get("stock_id") or ""),
        )

    if not sector_scores:
        ranked = sorted(rows, key=base_key)
    else:
        def sector_key(row: dict):
            rank = to_float(row.get("sector_rank"), 999) or 999
            return (
                rank,
                -to_float(row.get("sector_flow_score"), 0),
                *base_key(row),
            )

        ranked = sorted(rows, key=sector_key)
    for idx, row in enumerate(ranked, 1):
        row["rank"] = idx
    return ranked


def latest_enriched_candidates(
    rows: list[dict],
    industry_map: dict[str, dict] | None = None,
    sector_scores: dict[str, dict] | None = None,
) -> list[dict]:
    industry_map = industry_map if industry_map is not None else load_industry_map()
    sector_scores = sector_scores if sector_scores is not None else market_sector_flow(industry_map)
    enriched = [enrich(r, industry_map) for r in rows]
    enriched = [r for r in enriched if r.get("close") and r.get("date")]
    latest_date = max((r.get("date", "") for r in enriched), default="")
    if latest_date:
        enriched = [r for r in enriched if r.get("date") == latest_date]
    for item in enriched:
        sector = str(item.get("sector") or "")
        summary = sector_scores.get(sector) or {}
        item["sector_rank"] = summary.get("rank")
        item["sector_flow_score"] = summary.get("score")
        item["sector_turnover_billion"] = summary.get("turnover_billion")
    return rank_all_candidates_with_sector_flow(enriched, sector_scores)


def market_cap_bucket(row: dict) -> str:
    for key in ("market_cap", "market_value", "market_capitalization"):
        value = to_float(row.get(key), None)
        if value is not None:
            value_billion = value / 1_000_000_000
            break
    else:
        for key in ("market_cap_billion", "market_value_billion"):
            value = to_float(row.get(key), None)
            if value is not None:
                value_billion = value
                break
        else:
            return "unknown"
    if value_billion >= 100:
        return "large"
    if value_billion >= 20:
        return "mid"
    return "small"


def mda_candidate_json_stock(row: dict, pit_eligible_ids: set[str] | None = None) -> dict:
    turnover_value = to_float(row.get("turnover_value"), None)
    stock_id = str(row.get("stock_id") or "")
    return {
        "rank": int(to_float(row.get("rank"), 0) or 0),
        "stock_id": stock_id,
        "security_id": stock_id,
        "name": row.get("name") or stock_id,
        "date": row.get("date") or "",
        "data_date": row.get("date") or "",
        "basket": row.get("basket") or "",
        "mda_basket": row.get("basket") or "",
        "rank_basket": row.get("rank_basket") or row.get("basket") or "",
        "status": row.get("status") or "",
        "icon": row.get("icon") or "",
        "score": to_float(row.get("score"), None),
        "mda_score": to_float(row.get("score"), None),
        "close": to_float(row.get("close"), None),
        "gain_6w": to_float(row.get("gain_6w"), None),
        "gain_3d": to_float(row.get("gain_3d"), None),
        "rsi": to_float(row.get("rsi"), None),
        "bb_pct": to_float(row.get("bb_pct"), None),
        "vol5_lot": to_float(row.get("vol5_lot"), None),
        "latest_volume": to_float(row.get("latest_volume"), None),
        "turnover_value": turnover_value,
        "turnover_million": turnover_value / 1_000_000 if turnover_value is not None else None,
        "market_cap_bucket": market_cap_bucket(row),
        "sector": row.get("sector") or "",
        "sector_rank": to_float(row.get("sector_rank"), None),
        "sector_flow_score": to_float(row.get("sector_flow_score"), None),
        "sector_turnover_billion": to_float(row.get("sector_turnover_billion"), None),
        "entry": to_float(row.get("entry"), None),
        "target": to_float(row.get("target"), None),
        "stop": to_float(row.get("stop"), None),
        "reason": row.get("reason") or "",
        "pit_eligible": stock_id in pit_eligible_ids if pit_eligible_ids is not None else None,
    }


def build_pit_universe_audit(
    rows: list[dict],
    eligible_ids: set[str] | None = None,
) -> tuple[dict, set[str]]:
    latest_date = max((str(row.get("date") or "") for row in rows), default="")
    latest_ids = {
        str(row.get("stock_id") or "")
        for row in rows
        if str(row.get("date") or "") == latest_date and row.get("stock_id")
    }
    error = ""
    supplied_eligible_ids = eligible_ids is not None
    if eligible_ids is None:
        try:
            from tools.pit_universe import get_eligible_universe

            eligible_ids = get_eligible_universe(latest_date, MIN_CLOSE) if latest_date else set()
        except Exception as exc:
            eligible_ids = set()
            error = f"{type(exc).__name__}: {exc}"
    eligible_ids = {str(stock_id) for stock_id in eligible_ids}
    if not supplied_eligible_ids and latest_ids and not eligible_ids and not error:
        error = "PIT universe returned zero eligible stocks; upstream point-in-time inputs may be unavailable"
    rejected = sorted(latest_ids - eligible_ids)
    audit = {
        "check": "tools.pit_universe.get_eligible_universe",
        "mode": "audit_only_no_strategy_filter",
        "policy_decision": "audit_only",
        "filter_applied": False,
        "decision_reason": (
            "PIT eligibility depends on complete local price and holding caches; missing inputs can "
            "produce an empty universe. Keep candidate selection unchanged until cache completeness "
            "and historical coverage are enforced and monitored."
        ),
        "activation_requirements": [
            "price_and_holding_cache_completeness_gate",
            "historical_survivorship_bias_regression_test",
            "zero_eligible_universe_fail_closed_handling",
        ],
        "as_of_date": latest_date or None,
        "candidate_count": len(latest_ids),
        "eligible_count": None if error else len(latest_ids & eligible_ids),
        "rejected_count": None if error else len(rejected),
        "rejected_ids_sample": [] if error else rejected[:20],
        "status": "unavailable" if error else ("pass" if not rejected else "warn"),
        "error": error or None,
    }
    return audit, eligible_ids


def build_mda_candidates_payload(
    rows: list[dict],
    industry_map: dict[str, dict] | None = None,
    sector_scores: dict[str, dict] | None = None,
    pit_eligible_ids: set[str] | None = None,
) -> dict:
    stocks = latest_enriched_candidates(rows, industry_map, sector_scores)
    pit_audit, checked_eligible_ids = build_pit_universe_audit(stocks, pit_eligible_ids)
    eligible_for_rows = checked_eligible_ids if pit_audit["status"] != "unavailable" else None
    json_stocks = [mda_candidate_json_stock(row, eligible_for_rows) for row in stocks]
    latest_date = json_stocks[0]["date"] if json_stocks else ""
    return {
        "dataset_id": "mda_candidate_pool",
        "schema_version": "1.0.0",
        "semantic_role": "derived_mda_candidate_pool_not_sfz_signal",
        "date": latest_date,
        "source": "data/mda_universe_scan.json",
        "canonical_path": "data/mda_candidates.json",
        "legacy_aliases": ["data/sfz_all.json"],
        "pit_universe": pit_audit,
        "count": len(json_stocks),
        "default_limit": TOP_N,
        "filters": {
            "page_sizes": [20, 50, "all"],
            "turnover_thresholds": [50_000_000, 100_000_000],
            "market_cap_buckets": ["large", "mid", "small", "unknown"],
        },
        "stocks": json_stocks,
    }


def build_sfz_all_payload(
    rows: list[dict],
    industry_map: dict[str, dict] | None = None,
    sector_scores: dict[str, dict] | None = None,
    pit_eligible_ids: set[str] | None = None,
) -> dict:
    """Compatibility alias. The payload is an MDA candidate pool, not an SFZ signal."""

    return build_mda_candidates_payload(rows, industry_map, sector_scores, pit_eligible_ids)


def write_mda_candidates_json(
    rows: list[dict],
    industry_map: dict[str, dict] | None = None,
    sector_scores: dict[str, dict] | None = None,
) -> dict:
    payload = build_mda_candidates_payload(rows, industry_map, sector_scores)
    MDA_CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    SFZ_ALL_PATH.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    MDA_CANDIDATES_PATH.write_text(serialized, encoding="utf-8")
    # Keep the legacy filename until generate_site.py and downstream consumers migrate.
    SFZ_ALL_PATH.write_text(serialized, encoding="utf-8")
    return payload


def write_sfz_all_json(
    rows: list[dict],
    industry_map: dict[str, dict] | None = None,
    sector_scores: dict[str, dict] | None = None,
) -> dict:
    """Compatibility writer for callers that still use the legacy filename."""

    return write_mda_candidates_json(rows, industry_map, sector_scores)


def select_top20(
    rows: list[dict],
    industry_map: dict[str, dict] | None = None,
    sector_scores: dict[str, dict] | None = None,
) -> list[dict]:
    industry_map = industry_map if industry_map is not None else load_industry_map()
    sector_scores = sector_scores if sector_scores is not None else market_sector_flow(industry_map)
    enriched = [enrich(r, industry_map) for r in rows]
    enriched = [r for r in enriched if r.get("close") and r.get("date")]
    latest_date = max((r.get("date", "") for r in enriched), default="")
    if latest_date:
        enriched = [r for r in enriched if r.get("date") == latest_date]
    return rank_candidates_with_sector_flow(enriched, sector_scores, TOP_N, SECTOR_TOP_N, SECTOR_MAX_PER_GROUP)


def sector_flow_markdown(sector_scores: dict[str, dict], top_n: int = SECTOR_TOP_N) -> list[str]:
    if not sector_scores:
        return []
    top = sorted(sector_scores.values(), key=lambda r: to_float(r.get("rank"), 999))[:top_n]
    if not top:
        return []
    lines = [
        "## 市場資金追捧族群",
        "",
        "| 排名 | 族群 | 資金分數 | 成交金額(億) | 5日動能 | 20日動能 | 量能比 | 上漲家數 |",
        "|------|------|----------|--------------|---------|----------|--------|----------|",
    ]
    for row in top:
        lines.append(
            "| "
            f"{int(to_float(row.get('rank'), 0) or 0)} | "
            f"{row.get('sector')} | "
            f"{fmt_num(row.get('score'), 1)} | "
            f"{fmt_num(row.get('turnover_billion'), 1)} | "
            f"{fmt_num(row.get('avg_ret5'), 2, '%')} | "
            f"{fmt_num(row.get('avg_ret20'), 2, '%')} | "
            f"{fmt_num(row.get('avg_vol_ratio'), 2)}x | "
            f"{fmt_num((to_float(row.get('breadth'), 0) or 0) * 100, 0, '%')} |"
        )
    lines.extend([
        "",
        f"> Top20 已改為先看前 {top_n} 名資金熱族群，再回到 M大分數與籃子排序；單一族群最多 {SECTOR_MAX_PER_GROUP} 檔，避免同族群塞滿整張名單。",
        "",
        "---",
        "",
    ])
    return lines


def report_markdown(rows: list[dict], report_date: str, sector_scores: dict[str, dict] | None = None) -> str:
    latest_price_date = max((r.get("date", "") for r in rows), default=report_date)
    lines = [
        f"# 每日選股報告｜{report_date}",
        "",
        "> 自動更新版：先由 M大全市場候選池挑出每日 Top20，再交給網站做 SFZ雙籃、買點雷達、入選追蹤與個股頁更新。",
        "",
        "---",
        "",
        f"## 大盤概況｜{latest_price_date}",
        "本頁由 GitHub Actions 自動產生，重點是讓網站每天有新的母名單可追蹤。實際買賣仍回到個股頁與買點雷達判讀。",
        "",
        "---",
        "",
        "## 篩選條件",
        "",
        "| 條件 | 說明 |",
        "|------|------|",
        f"| 價格 | 收盤價 >= {fmt_num(MIN_CLOSE, 0)} 元 |",
        "| 母名單 | M大全市場掃描候選 |",
        "| 排序 | 已發動優先，其次空轉多與未發動；同籃依 M大分數排序 |",
        "",
        f"**Top {len(rows)}**",
        "",
        "---",
        "",
        f"## Top {len(rows)} 精選個股｜{latest_price_date}",
        "",
    ]
    lines.extend(sector_flow_markdown(sector_scores or {}))
    for idx, r in enumerate(rows, 1):
        sid = r["stock_id"]
        lines.extend([
            f"### {idx}. {r['icon']} {sid} {r.get('name','')} ｜{r['status']}｜ Score: {fmt_num(r.get('score'), 2)}",
            "",
            "| 指標 | 數值 |",
            "|------|------|",
            f"| 收盤價 | **{fmt_num(r.get('close'), 2)} 元** |",
            f"| 近6週漲幅 | **{fmt_num(r.get('gain_6w'), 2, '%')}** |",
            f"| 近3日漲幅 | **{fmt_num(r.get('gain_3d'), 2, '%')}** |",
            f"| 產業族群 | {r.get('sector') or '未分類'} |",
            f"| 族群資金排名 | #{fmt_num(r.get('sector_rank'), 0)}（分數 {fmt_num(r.get('sector_flow_score'), 1)}） |",
            f"| RSI(14) | {fmt_num(r.get('rsi'), 1)} |",
            f"| 布林 %B | {fmt_num(r.get('bb_pct'), 1, '%')} |",
            f"| 近5日量 | {fmt_num(r.get('vol5_lot'), 0)} 張 |",
            f"| 近21日壓力 | {fmt_num(r.get('high21'), 2)} |",
            f"| 近21日支撐 | {fmt_num(r.get('low21'), 2)} |",
            f"| 外資近5日 | ─ |",
            f"| 外資連買天數 | ─ |",
            f"| 📌 進場參考 | {fmt_num(r.get('entry'), 2)} |",
            f"| 🎯 目標價 | {fmt_num(r.get('target'), 2)} |",
            f"| 🛑 停損價 | {fmt_num(r.get('stop'), 2)} |",
            "",
            f"> 籃子：{r.get('basket','')}；判讀：{r.get('reason','')}",
            "",
            "---",
            "",
        ])
    return "\n".join(lines)


def main() -> None:
    industry_map = load_industry_map()
    sector_scores = market_sector_flow(industry_map)
    scan_rows = load_scan_rows()
    mda_candidates = write_mda_candidates_json(scan_rows, industry_map, sector_scores)
    rows = select_top20(scan_rows, industry_map, sector_scores)
    if not rows:
        raise SystemExit("No rows available. Run mda_full_market_refresh.py and mda_universe_scan.py first.")
    report_date = max((r.get("date", "") for r in rows), default=date.today().isoformat())
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORTS_DIR / f"每日選股報告_{report_date}.md"
    out.write_text(report_markdown(rows, report_date, sector_scores), encoding="utf-8")
    print(f"[run_screener] wrote {out} rows={len(rows)}")
    pit = mda_candidates.get("pit_universe") or {}
    print(
        f"[run_screener] wrote {MDA_CANDIDATES_PATH} rows={mda_candidates.get('count', 0)} "
        f"pit_status={pit.get('status')} pit_rejected={pit.get('rejected_count')}"
    )
    print(f"[run_screener] compatibility alias -> {SFZ_ALL_PATH}")


if __name__ == "__main__":
    main()
