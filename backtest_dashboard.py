from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "backtest_results.json"
TAIPEI_TZ = timezone(timedelta(hours=8))

DEFAULT_COST_MODEL = {
    "buy_fee_rate": 0.0006,
    "sell_fee_rate": 0.0006,
    "sell_tax_rate": 0.003,
    "slippage_rate": 0.0002,
}

WINDOWS_V6_OUTPUTS = Path(r"C:\Users\USER\OneDrive\桌面\股票\自動交易程式\回測\v6_outputs")
MAX_STRATEGIES = 80
MIN_TRADES = 2


def round_trip_cost_rate(cost_model: dict[str, float] | None = None) -> float:
    cost = cost_model or DEFAULT_COST_MODEL
    return round(
        float(cost.get("buy_fee_rate", 0.0))
        + float(cost.get("sell_fee_rate", 0.0))
        + float(cost.get("sell_tax_rate", 0.0))
        + float(cost.get("slippage_rate", 0.0)),
        10,
    )


def normalized_cost_model(cost_model: dict[str, float] | None = None) -> dict[str, float]:
    cost = dict(DEFAULT_COST_MODEL)
    if cost_model:
        cost.update({k: float(v) for k, v in cost_model.items() if v is not None})
    cost["round_trip_rate"] = round_trip_cost_rate(cost)
    return cost


def apply_trade_cost(gross_return: float, cost_model: dict[str, float] | None = None) -> float:
    return float(gross_return) - round_trip_cost_rate(cost_model)


def candidate_source_dirs() -> list[Path]:
    paths: list[Path] = []
    for key in ("BACKTEST_SOURCE_DIR", "V44_BACKTEST_OUTPUT_DIR"):
        raw = os.environ.get(key)
        if raw:
            paths.append(Path(raw))
    v44_root = os.environ.get("V44_ROOT")
    if v44_root:
        root = Path(v44_root)
        paths.extend([root / "回測" / "v6_outputs", root / "?葫" / "v6_outputs"])
    paths.extend([WINDOWS_V6_OUTPUTS, ROOT / "backtest_outputs", ROOT / "data" / "backtest_outputs"])
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path)
        if key not in seen:
            out.append(path)
            seen.add(key)
    return out


def resolve_source_dir() -> Path | None:
    for path in candidate_source_dirs():
        if path.exists():
            return path
    return None


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    for encoding in ("utf-8-sig", "utf-8", "cp950"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                return list(csv.DictReader(handle))
        except UnicodeDecodeError:
            continue
    return []


def parse_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    if not math.isfinite(number):
        return None
    return number


def parse_return(value: Any, unit: str = "auto") -> float | None:
    number = parse_float(value)
    if number is None:
        return None
    if unit == "percent":
        return number / 100.0
    if unit == "decimal":
        return number
    return number / 100.0 if abs(number) > 1.0 else number


def clean_date(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 10:
        text = text[:10]
    try:
        datetime.fromisoformat(text)
        return text
    except ValueError:
        return ""


def _safe_factor(ret: float) -> float:
    return max(0.000001, 1.0 + ret)


def _compound_returns(returns: list[float]) -> float:
    value = 1.0
    for ret in returns:
        value *= _safe_factor(ret)
    return value - 1.0


def _max_drawdown(equity_values: list[float]) -> float | None:
    if not equity_values:
        return None
    peak = equity_values[0]
    worst = 0.0
    for value in equity_values:
        peak = max(peak, value)
        if peak:
            worst = min(worst, value / peak - 1.0)
    return worst


def _profit_factor(returns: list[float]) -> float | None:
    wins = sum(ret for ret in returns if ret > 0)
    losses = abs(sum(ret for ret in returns if ret < 0))
    if losses == 0:
        return 999.0 if wins > 0 else None
    return wins / losses


def _round_optional(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def build_strategy_from_trades(
    strategy_name: str,
    trades: list[dict[str, Any]],
    *,
    cost_model: dict[str, float] | None = None,
    parameters: dict[str, Any] | None = None,
    source: dict[str, Any] | None = None,
    category: str = "trade_simulation",
) -> dict[str, Any] | None:
    cost = normalized_cost_model(cost_model)
    normalized: list[dict[str, Any]] = []
    for trade in trades:
        date = clean_date(trade.get("date"))
        gross_return = trade.get("return")
        if date and gross_return is not None:
            net_return = apply_trade_cost(float(gross_return), cost)
            normalized.append({**trade, "date": date, "gross_return": float(gross_return), "net_return": net_return})
    normalized.sort(key=lambda item: (item["date"], str(item.get("stock_id", ""))))
    if len(normalized) < MIN_TRADES:
        return None

    returns = [float(item["net_return"]) for item in normalized]
    dates = [item["date"] for item in normalized]
    start = dates[0]
    end = dates[-1]
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    days = max(1, (end_dt - start_dt).days)
    years = max(days / 365.25, 1.0 / 365.25)

    monthly_groups: dict[str, list[float]] = defaultdict(list)
    for item in normalized:
        monthly_groups[item["date"][:7]].append(float(item["net_return"]))

    monthly_returns = {
        month: round(mean(month_returns), 6)
        for month, month_returns in sorted(monthly_groups.items())
    }
    sequence_returns = list(monthly_returns.values())
    equity = 1.0
    equity_curve: list[list[Any]] = [[start, 1.0]]
    for month, monthly_return in monthly_returns.items():
        equity *= _safe_factor(monthly_return)
        equity_curve.append([f"{month}-01", round(equity, 6)])

    total_return = equity - 1.0
    annual_return = None
    if equity > 0:
        annual_return = equity ** (1.0 / years) - 1.0

    std = pstdev(sequence_returns) if len(sequence_returns) > 1 else 0.0
    observations_per_year = 12.0 if category == "event_study" else (len(sequence_returns) / years if years > 0 else 0.0)
    sharpe = (mean(sequence_returns) / std * math.sqrt(observations_per_year)) if std > 0 and observations_per_year > 0 else None
    wins = [ret for ret in returns if ret > 0]

    params = dict(parameters or {})
    params.setdefault("slippage_rate", cost["slippage_rate"])
    params.setdefault("round_trip_cost_rate", cost["round_trip_rate"])
    params.setdefault("cost_application", "gross_return_minus_round_trip_cost")

    return {
        "strategy_name": strategy_name,
        "category": category,
        "period": {"start": start, "end": end},
        "metrics": {
            "annual_return": _round_optional(annual_return),
            "sharpe_ratio": _round_optional(sharpe),
            "max_drawdown": _round_optional(_max_drawdown([point[1] for point in equity_curve])),
            "win_rate": _round_optional(len(wins) / len(returns), 6),
            "total_trades": len(returns),
            "profit_factor": _round_optional(_profit_factor(returns), 6),
        },
        "monthly_returns": monthly_returns,
        "equity_curve": equity_curve,
        "parameters": params,
        "source": dict(source or {}),
        "notes": [
            "Returns are standardized with Taiwan stock costs: buy fee 0.6 per mille, sell fee 0.6 per mille, sell tax 3 per mille, slippage 0.2 per mille.",
            "Signal rows are aggregated to monthly average net returns before drawing equity curves, so overlapping signals are not treated as executable sequential trades.",
            f"Gross total return before sequence compounding is not used directly; compounded net sequence return is {total_return:.4f}.",
        ],
    }


def _append_strategy(strategies: list[dict[str, Any]], strategy: dict[str, Any] | None) -> None:
    if strategy is None:
        return
    if strategy["metrics"]["total_trades"] < MIN_TRADES:
        return
    strategies.append(strategy)


def _strategy_name(prefix: str, parts: list[Any]) -> str:
    clean_parts = [str(part).strip() for part in parts if str(part or "").strip()]
    return "_".join([prefix, *clean_parts]).replace(" ", "_")


def build_ma_trailing_strategies(source_dir: Path, cost_model: dict[str, float]) -> list[dict[str, Any]]:
    path = source_dir / "sfz_ma_trailing_after_activation_detail.csv"
    rows = read_csv_rows(path)
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        ret = parse_return(row.get("ret"), "percent")
        date = clean_date(row.get("exit_date") or row.get("signal_date"))
        if ret is None or not date:
            continue
        key = (row.get("basket_type", "ALL") or "ALL", row.get("ma_line", "MA20") or "MA20", row.get("activation_pct", "10") or "10")
        groups[key].append({"date": date, "return": ret, "stock_id": row.get("stock")})

    out: list[dict[str, Any]] = []
    for (basket, ma_line, activation), trades in groups.items():
        name = _strategy_name("SFZ_MA_Trailing", [basket, ma_line, f"active{activation}"])
        _append_strategy(
            out,
            build_strategy_from_trades(
                name,
                trades,
                cost_model=cost_model,
                parameters={"entry_rule": "SFZ signal", "exit_rule": f"{ma_line} trailing after +{activation}% activation", "basket_type": basket},
                source={"file": path.name, "return_column": "ret", "return_unit": "percent"},
            ),
        )
    return out


def build_ta3_wait_entry_strategies(source_dir: Path, cost_model: dict[str, float]) -> list[dict[str, Any]]:
    path = source_dir / "sfz_signal_wait_ta3_entry_detail.csv"
    rows = read_csv_rows(path)
    variants = [
        ("base_exit", "exit_date", "exit_ret", "TA3 entry base exit"),
        ("active10_ma20", "ma20_active10_exit_date", "ma20_active10_ret", "+10% activation then MA20"),
        ("active20_ma20", "ma20_active20_exit_date", "ma20_active20_ret", "+20% activation then MA20"),
    ]
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("wait_status", "")).lower() != "filled":
            continue
        tier = row.get("entry_tier") or "TA3"
        for variant, date_col, return_col, _label in variants:
            ret = parse_return(row.get(return_col), "percent")
            date = clean_date(row.get(date_col) or row.get("exit_date") or row.get("entry_date"))
            if ret is None or not date:
                continue
            groups[(tier, variant)].append({"date": date, "return": ret, "stock_id": row.get("stock")})

    out: list[dict[str, Any]] = []
    label_by_variant = {variant: label for variant, _date, _ret, label in variants}
    for (tier, variant), trades in groups.items():
        name = _strategy_name("SFZ_TA3_WaitEntry", [tier, variant])
        _append_strategy(
            out,
            build_strategy_from_trades(
                name,
                trades,
                cost_model=cost_model,
                parameters={"entry_rule": f"TA3 wait-entry {tier}", "exit_rule": label_by_variant[variant]},
                source={"file": path.name, "variant": variant, "return_unit": "percent"},
            ),
        )
    return out


def build_sfz_carybot_path_strategies(source_dir: Path, cost_model: dict[str, float]) -> list[dict[str, Any]]:
    path = source_dir / "sfz_carybot_two_paths_detail.csv"
    rows = read_csv_rows(path)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        strategy = row.get("strategy") or "SFZ_CaryBot"
        ret = parse_return(row.get("trade_ret"), "percent")
        date = clean_date(row.get("exit_date") or row.get("entry_date") or row.get("signal_date"))
        if ret is None or not date:
            continue
        groups[strategy].append({"date": date, "return": ret, "stock_id": row.get("stock")})

    out: list[dict[str, Any]] = []
    for strategy, trades in groups.items():
        name = _strategy_name("SFZ_CaryBot", [strategy])
        _append_strategy(
            out,
            build_strategy_from_trades(
                name,
                trades,
                cost_model=cost_model,
                parameters={"entry_rule": "SFZ plus CaryBot confirmation", "exit_rule": "MA20 / study exit"},
                source={"file": path.name, "return_column": "trade_ret", "return_unit": "percent"},
            ),
        )
    return out


def build_carybot_v51_event_strategies(source_dir: Path, cost_model: dict[str, float]) -> list[dict[str, Any]]:
    path = source_dir / "carybot_vam_v51_market_2024_2026_events.csv"
    rows = read_csv_rows(path)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        side = str(row.get("signal_side", "")).lower()
        if side not in {"buy", "benchmark"}:
            continue
        ret = parse_return(row.get("future_return_20d"), "decimal")
        date = clean_date(row.get("event_date"))
        if ret is None or not date:
            continue
        groups[row.get("signal_group") or "CaryBot_v51"].append({"date": date, "return": ret, "stock_id": row.get("stock")})

    out: list[dict[str, Any]] = []
    for signal_group, trades in groups.items():
        name = _strategy_name("CaryBot_v51_EventStudy", [signal_group])
        _append_strategy(
            out,
            build_strategy_from_trades(
                name,
                trades,
                cost_model=cost_model,
                parameters={"entry_rule": signal_group, "exit_rule": "20 trading-day forward event return", "research_status": "sidecar event study"},
                source={"file": path.name, "return_column": "future_return_20d", "return_unit": "decimal"},
                category="event_study",
            ),
        )
    return out


def build_payload(
    source_dir: str | Path | None = None,
    *,
    now: datetime | None = None,
    cost_model: dict[str, float] | None = None,
) -> dict[str, Any]:
    cost = normalized_cost_model(cost_model)
    resolved = Path(source_dir) if source_dir is not None else resolve_source_dir()
    updated = now or datetime.now(TAIPEI_TZ)
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=TAIPEI_TZ)

    strategies: list[dict[str, Any]] = []
    source_exists = bool(resolved and resolved.exists())
    if source_exists and resolved:
        for builder in (
            build_ma_trailing_strategies,
            build_ta3_wait_entry_strategies,
            build_sfz_carybot_path_strategies,
            build_carybot_v51_event_strategies,
        ):
            strategies.extend(builder(resolved, cost))

    strategies.sort(
        key=lambda item: (
            -int(item["metrics"].get("total_trades") or 0),
            str(item.get("category", "")),
            item.get("strategy_name", ""),
        )
    )
    strategies = strategies[:MAX_STRATEGIES]
    periods = [strategy["period"] for strategy in strategies if strategy.get("period")]
    period_start = min((p["start"] for p in periods), default=None)
    period_end = max((p["end"] for p in periods), default=None)

    notes = [
        "This JSON is the standard static contract for backtest_dashboard.html.",
        "Taiwan cost model is forced to buy fee 0.6 per mille, sell fee 0.6 per mille, sell tax 3 per mille, and default slippage 0.2 per mille.",
    ]
    if not source_exists:
        notes.append("Backtest source directory was not available; no strategies were rebuilt.")

    return {
        "schema_version": 1,
        "date": updated.astimezone(TAIPEI_TZ).date().isoformat(),
        "updated_at": updated.astimezone(TAIPEI_TZ).isoformat(),
        "source_dir": str(resolved) if resolved else "",
        "source_available": source_exists,
        "period": {"start": period_start, "end": period_end},
        "cost_model": cost,
        "strategies": strategies,
        "notes": notes,
        "future_extensions": {
            "markets": ["US", "Japan", "Korea"],
            "sector_rotation": "reserved for daily cross-market rotation inputs",
        },
    }


def write_payload(payload: dict[str, Any], output_path: str | Path = OUTPUT_PATH) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_existing_payload(path: str | Path = OUTPUT_PATH) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build standardized backtest dashboard JSON.")
    parser.add_argument("--source-dir", type=Path, default=None, help="Directory containing v6 backtest CSV outputs.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH, help="Output JSON path.")
    parser.add_argument("--slippage-rate", type=float, default=DEFAULT_COST_MODEL["slippage_rate"], help="Per trade slippage rate, default 0.0002.")
    args = parser.parse_args(argv)

    cost = normalized_cost_model({**DEFAULT_COST_MODEL, "slippage_rate": args.slippage_rate})
    source_dir = args.source_dir or resolve_source_dir()
    if not source_dir or not source_dir.exists():
        if load_existing_payload(args.output):
            print(f"[backtest_dashboard] Source directory unavailable; preserved existing {args.output}")
            return 0
        payload = build_payload(source_dir=source_dir or Path("__missing__"), cost_model=cost)
        write_payload(payload, args.output)
        print(f"[backtest_dashboard] Wrote empty dashboard payload to {args.output}")
        return 0

    payload = build_payload(source_dir=source_dir, cost_model=cost)
    output = write_payload(payload, args.output)
    print(
        f"[backtest_dashboard] Wrote {output} with {len(payload.get('strategies', []))} strategies "
        f"from {source_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
