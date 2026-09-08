"""Synthetic tests for full-history SMA warm-up and adjusted CSV integrity."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys

from jsonschema import Draft202012Validator
import pandas as pd
import pytest


PUBLIC = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PUBLIC))

from generate_v2 import load_price_basis, trim_packet  # noqa: E402
from stock_v2_public.analysis.engine import analyze_multi_timeframe  # noqa: E402
from stock_v2_public.site import V2_JS  # noqa: E402


def fixture(root: Path, count: int, sid: str = "9001") -> pd.DataFrame:
    dates = pd.bdate_range(end="2026-09-04", periods=count)
    rows = []
    for index, day in enumerate(dates):
        raw_close = 100.0 + index
        factor = 0.5 if index < max(0, count - 20) else 1.0
        rows.append({
            "date": day.date().isoformat(),
            "open": raw_close * factor, "high": (raw_close + 1) * factor,
            "low": (raw_close - 1) * factor, "close": raw_close * factor,
            "volume": 1000 + index, "raw_open": raw_close,
            "raw_high": raw_close + 1, "raw_low": raw_close - 1,
            "raw_close": raw_close, "raw_volume": 1000 + index,
            "adjustment_factor": factor,
        })
    frame = pd.DataFrame(rows)
    (root / "prices").mkdir(parents=True, exist_ok=True)
    (root / "price_basis").mkdir(parents=True, exist_ok=True)
    csv_path = root / "prices" / f"{sid}.csv"
    frame.to_csv(csv_path, index=False)
    metadata = {
        "stock_id": sid,
        "mode": "finmind_raw_reconciled_reference_ratio_back_adjusted_v1",
        "verified": True,
        "volume_basis": "finmind_raw_shares",
        "price_source": "FinMind TaiwanStockPrice",
        "action_sources": ["official_twse_reference", "finmind_TaiwanStockDividendResult"],
        "adjustment_as_of": "2026-09-04", "data_start": rows[0]["date"],
        "data_end": "2026-09-04", "row_count": count, "event_count": 1, "available_bars": count,
        "history_status": "complete" if count>=245 else "insufficient_history",
        "ma240_required_bars":240, "direction_required_bars":241, "full_study_recommended_bars":245,
        "csv_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
    }
    (root / "price_basis" / f"{sid}.json").write_text(json.dumps(metadata), encoding="utf-8")
    return frame


@pytest.mark.parametrize("count", [1, 29, 239, 240, 520])
def test_daily_history_warmup_and_schema(tmp_path, count):
    frame = fixture(tmp_path, count)
    basis = load_price_basis(tmp_path, "9001", frame, "2026-09-04")
    packets = analyze_multi_timeframe(frame, stock_id="9001", price_adjustment=basis)
    daily = trim_packet(next(packet for packet in packets if packet["timeframe"] == "daily"))
    assert len(daily["series"]) == min(count, 240)
    assert daily["series_coverage"] == {
        "requested_bars": 240, "available_bars": count,
        "returned_bars": min(count, 240),
        "status": "available" if count >= 240 else "insufficient_history",
        "message": daily["series_coverage"]["message"],
    }
    if count < 240:
        assert all(row["sma240"] is None for row in daily["series"])
    else:
        assert daily["series"][-1]["sma240"] == pytest.approx(frame["close"].tail(240).mean())
    if count == 520:
        assert daily["series"][0]["sma240"] == pytest.approx(frame["close"].iloc[41:281].mean())
    schema = json.loads((PUBLIC / "schemas" / "technical_pattern_packet.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(daily)


def test_new_mode_hash_and_metadata_bounds_fail_closed(tmp_path):
    frame = fixture(tmp_path, 240)
    assert load_price_basis(tmp_path, "9001", frame, "2026-09-04")["volume_basis"] == "finmind_raw_shares"
    csv_path = tmp_path / "prices" / "9001.csv"
    csv_path.write_bytes(csv_path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="驗證紀錄"):
        load_price_basis(tmp_path, "9001", pd.read_csv(csv_path), "2026-09-04")
    for key, value in (("row_count", 239), ("data_start", "2024-01-01"), ("data_end", "2026-09-03")):
        fixture(tmp_path, 240)
        meta_path = tmp_path / "price_basis" / "9001.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")); meta[key] = value
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        with pytest.raises(ValueError, match="驗證紀錄"):
            load_price_basis(tmp_path, "9001", pd.read_csv(tmp_path / "prices" / "9001.csv"), "2026-09-04")


def test_site_javascript_parses(tmp_path):
    script = tmp_path / "v2.js"
    script.write_text(V2_JS, encoding="utf-8")
    result = subprocess.run(["node", "--check", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_verified_stale_history_remains_available_with_warning(tmp_path):
    from generate_v2 import analyze_stock_task
    fixture(tmp_path, 520)
    _, _, packets, error = analyze_stock_task(("9001", "Synthetic", tmp_path / "prices/9001.csv", tmp_path, {}, "fresh", "2026-09-08", [], True))
    assert error is None
    daily = next(p for p in packets if p["timeframe"] == "daily")
    assert daily["freshness"]["status"] == "stale"
    assert daily["freshness"]["warnings"]
    assert daily["data_date"] == "2026-09-04" and len(daily["series"]) == 240
    _, _, packets, error = analyze_stock_task(("9001", "Synthetic", tmp_path / "prices/9001.csv", tmp_path, {}, "fresh", "2026-09-03", [], True))
    assert packets is None and "future OHLCV" in error
