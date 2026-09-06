"""Optional recent-minute feed -> compact daily histograms, without credentials.

Only derived price-bin summaries are written for the public website. Yahoo's
five-minute bars are a limited secondary feed, not official tick history.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, time as wall_time, timedelta, timezone
from pathlib import Path

import requests

TAIPEI = timezone(timedelta(hours=8))
ROOT = Path(__file__).resolve().parents[1]
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_SOURCE_BARS = 1000


def bounded_json(response: requests.Response, *, max_bytes: int = MAX_RESPONSE_BYTES) -> dict:
    length = response.headers.get("Content-Length")
    if length and int(length) > max_bytes:
        raise ValueError("minute source response exceeds byte limit")
    chunks, total = [], 0
    for chunk in response.iter_content(chunk_size=65536):
        total += len(chunk)
        if total > max_bytes:
            raise ValueError("minute source response exceeds byte limit")
        chunks.append(chunk)
    payload = json.loads(b"".join(chunks))
    if not isinstance(payload, dict):
        raise ValueError("minute source response must be a JSON object")
    return payload


def summarize(payload: dict, stock_id: str, *, symbol: str | None = None, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    result = payload.get("chart", {}).get("result") or []
    if not result:
        raise ValueError("minute source returned no result")
    data = result[0]
    meta = data.get("meta", {})
    if meta.get("exchangeTimezoneName") != "Asia/Taipei":
        raise ValueError("unexpected exchange timezone")
    expected_symbol = symbol or str(meta.get("symbol") or "")
    if expected_symbol not in (f"{stock_id}.TW", f"{stock_id}.TWO") or meta.get("symbol") != expected_symbol:
        raise ValueError("minute source symbol mismatch")
    quote = data["indicators"]["quote"][0]
    timestamps = data.get("timestamp") or []
    if not timestamps or len(timestamps) > MAX_SOURCE_BARS:
        raise ValueError("minute source bar count is outside the bounded contract")
    for key in ("open", "high", "low", "close", "volume"):
        if not isinstance(quote.get(key), list) or len(quote[key]) != len(timestamps):
            raise ValueError("minute source quote arrays do not align with timestamps")
    grouped = defaultdict(list)
    seen = set()
    skipped = {"null": 0, "off_session": 0, "unfinished": 0}
    observed_slots: dict[str, set[str]] = defaultdict(set)
    for i, stamp in enumerate(timestamps):
        if not isinstance(stamp, (int, float)) or not math.isfinite(stamp):
            raise ValueError("invalid minute timestamp")
        if stamp in seen:
            raise ValueError("duplicate minute timestamp")
        seen.add(stamp)
        if stamp + 300 > now.timestamp():
            skipped["unfinished"] += 1
            continue  # unfinished source bar is never treated as closed
        local = datetime.fromtimestamp(stamp, TAIPEI)
        local_clock = local.time().replace(tzinfo=None)
        if not (wall_time(9, 0) <= local_clock <= wall_time(13, 30)) or local.second != 0 or local.minute % 5:
            skipped["off_session"] += 1
            continue
        values = [quote[key][i] for key in ("open", "high", "low", "close", "volume")]
        if any(value is None for value in values):
            skipped["null"] += 1
            continue
        o, h, l, c, v = map(float, values)
        if not all(math.isfinite(x) for x in (o, h, l, c, v)) or min(o,h,l,c)<=0 or v<0 or h<max(o,c,l) or l>min(o,c):
            raise ValueError("invalid minute OHLCV")
        day = local.date().isoformat()
        grouped[day].append((stamp,o,l,h,c,v))
        observed_slots[day].add(local.strftime("%H:%M"))
    rows = []
    for day, bars in sorted(grouped.items()):
        bars.sort(key=lambda bar: bar[0])
        low, high = min(b[2] for b in bars), max(b[3] for b in bars)
        step = (high-low)/64 if high>low else max(low*.0001,.000001)
        volumes = [0.0]*64
        for _stamp,o,l,h,c,v in bars:
            index=lambda price:max(0,min(63,int((price-low)/step)))
            if h==l:
                volumes[index(c)]+=v
                continue
            assigned=0.0
            for n in range(index(l),index(h)+1):
                amount=(v-assigned) if n==index(h) else v*max(0,min(h,low+(n+1)*step)-max(l,low+n*step))/(h-l)
                volumes[n]+=amount
                assigned+=amount
        for n, volume in enumerate(volumes):
            if volume>0:
                lo,hi=low+n*step,low+(n+1)*step
                rows.append({"date":day,"open":lo,"high":hi,"low":lo,"close":hi,"volume":volume})
    if not rows:
        raise ValueError("no closed minute observations")
    expected_slots = []
    cursor = datetime(2000, 1, 1, 9, 0)
    while cursor.time() <= wall_time(13, 30):
        expected_slots.append(cursor.strftime("%H:%M"))
        cursor += timedelta(minutes=5)
    missing = {day: [slot for slot in expected_slots if slot not in slots] for day, slots in sorted(observed_slots.items())}
    observed_daily_ohlc = {
        day: {"open": bars[0][1], "high": max(bar[3] for bar in bars), "low": min(bar[2] for bar in bars),
              "close": bars[-1][4], "volume": sum(bar[5] for bar in bars), "basis": "provider_available_bars_only"}
        for day, bars in sorted(grouped.items())
    }
    return {"schema_version":"1.1.0","stock_id":stock_id,"kind":"bins","source":"Yahoo Finance（次要來源）","interval":"5 分鐘 → 每日 64 價格分箱","method":"uniform_minute_range_estimate_then_daily_bins","price_basis":"raw","volume_unit":"shares","data_date":max(grouped),"retrieved_at":now.isoformat(),"observations_by_date":{day:len(bars) for day,bars in sorted(grouped.items())},"observed_volume_by_date":{day:sum(bar[5] for bar in bars) for day,bars in sorted(grouped.items())},"observed_daily_ohlc":observed_daily_ohlc,"first_timestamp_by_date":{day:min(slots) for day,slots in sorted(observed_slots.items())},"last_timestamp_by_date":{day:max(slots) for day,slots in sorted(observed_slots.items())},"expected_slots_per_day":len(expected_slots),"missing_slots_by_date":missing,"skipped_bars":skipped,"partial":True,"coverage":"provider_available_bars_only; not an exchange completeness guarantee; no scaling or imputation","rows":rows}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol",action="append",required=True,help="e.g. 2330.TW or 6488.TWO")
    parser.add_argument("--output",type=Path,default=ROOT/"docs/v2/profiles")
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    for symbol in args.symbol:
        sid=symbol.split('.')[0]
        if not sid.isdigit() or len(sid)!=4 or symbol not in (f"{sid}.TW",f"{sid}.TWO"):
            raise ValueError("invalid Taiwan-equity symbol")
        response=requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",params={"range":"5d","interval":"5m"},headers={"User-Agent":"Mozilla/5.0"},timeout=30,stream=True)
        response.raise_for_status()
        out=summarize(bounded_json(response),sid,symbol=symbol)
        path=args.output/f"{sid}.json"
        temporary=path.with_suffix('.tmp')
        temporary.write_text(json.dumps(out,ensure_ascii=False,separators=(',',':'))+'\n',encoding='utf-8')
        temporary.replace(path)
        print(f"{sid}: {out['data_date']} source bars={sum(out['observations_by_date'].values())}, derived bins={len(out['rows'])}")


if __name__=="__main__":
    main()
