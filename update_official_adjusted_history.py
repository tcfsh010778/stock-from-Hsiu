"""Extend verified FinMind histories with complete official daily partitions."""

from __future__ import annotations
import argparse, csv, hashlib, importlib, json, math, sys, tempfile
from collections import defaultdict
from datetime import date, timedelta, datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Callable
import requests
import re

MODE = "reference_ratio_back_adjusted_mixed_sources_v1"
MIN_TWSE_ROWS = 800
MIN_TPEX_ROWS = 600
INPUT_MODES = {"finmind_raw_reconciled_reference_ratio_back_adjusted_v1", MODE}
FIELDS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_volume",
    "adjustment_factor",
]
EVENT_URLS = {
    "twse_dividend": (
        "https://www.twse.com.tw/rwd/zh/exRight/TWT49U",
        False,
        {
            "date": "資料日期",
            "stock": "股票代號",
            "before": "除權息前收盤價",
            "ref": "除權息參考價",
        },
    ),
    "tpex_dividend": (
        "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ",
        True,
        {
            "date": "除權息日期",
            "stock": "代號",
            "before": "除權息前收盤價",
            "ref": "除權息參考價",
        },
    ),
    "twse_reduction": (
        "https://www.twse.com.tw/rwd/zh/reducation/TWTAUU",
        False,
        {
            "date": "恢復買賣日期",
            "stock": "股票代號",
            "before": "停止買賣前收盤價格",
            "ref": "恢復買賣參考價",
        },
    ),
    "tpex_reduction": (
        "https://www.tpex.org.tw/www/zh-tw/bulletin/revivt",
        True,
        {
            "date": "恢復買賣日期",
            "stock": "股票代號",
            "before": "最後交易日之收盤價格",
            "ref": "減資恢復買賣開始日參考價格",
        },
    ),
    "twse_par_value": (
        "https://www.twse.com.tw/rwd/zh/change/TWTB8U",
        False,
        {
            "date": "恢復買賣日期",
            "stock": "股票代號",
            "before": "停止買賣前收盤價格",
            "ref": "恢復買賣參考價",
        },
    ),
    "tpex_par_value": (
        "https://www.tpex.org.tw/www/zh-tw/bulletin/pvChgRslt",
        True,
        {
            "date": "恢復買賣日期",
            "stock": "證券代號",
            "before": "最後交易日之收盤價格",
            "ref": "恢復買賣開始參考價",
        },
    ),
}


class UpdateError(RuntimeError):
    pass


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def payload_sha(value: Any) -> str:
    return sha(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    )


def atomic_json(path: Path, value: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as h:
        json.dump(
            value,
            h,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        h.write("\n")
        p = Path(h.name)
    p.replace(path)


def load_module(root: Path, name: str):
    sys.path.insert(0, str(root))
    return importlib.import_module(name)


def iso(value: Any) -> str:
    text = str(value or "").strip()
    if re.fullmatch(r"\d{7,8}", text):
        text = text[:-4] + "-" + text[-4:-2] + "-" + text[-2:]
    match = re.fullmatch(r"(\d{3,4})[-/年](\d{1,2})[-/月](\d{1,2})日?", text)
    if not match:
        raise UpdateError(f"invalid official date: {text!r}")
    year, month, day = map(int, match.groups())
    if len(match.group(1)) == 3:
        year += 1911
    return date(year, month, day).isoformat()


def number(v: Any) -> float:
    x = float(str(v).replace(",", "").strip())
    if not math.isfinite(x) or x <= 0:
        raise UpdateError("event price is nonpositive/nonfinite")
    return x


def parse_event(
    payload: dict, source: str, table: bool, labels: dict[str, str]
) -> list[dict]:
    if (not table and source.startswith('twse_')
            and payload.get('stat') == '很抱歉，沒有符合條件的資料!'
            and not payload.get('data') and not payload.get('tables')):
        return []
    body = (payload.get("tables") or [{}])[0] if table else payload
    fields = [str(x).strip() for x in body.get("fields") or []]
    rows = body.get("data")
    if not set(labels.values()) <= set(fields) or not isinstance(rows, list):
        raise UpdateError(f"{source} event schema mismatch")
    ix = {k: fields.index(v) for k, v in labels.items()}
    out = []
    for row in rows:
        sid = str(row[ix["stock"]]).strip()
        if len(sid) == 4 and sid.isdigit():
            out.append(
                {
                    "date": iso(row[ix["date"]]),
                    "stock_id": sid,
                    "previous_close": number(row[ix["before"]]),
                    "reference_price": number(row[ix["ref"]]),
                    "source": source,
                }
            )
    return out


def fetch_events(
    start: str,
    end: str,
    get: Callable = requests.get,
    *,
    cache_dir: Path | None = None,
    retries: int = 3,
) -> tuple[list[dict], dict]:
    events = []
    meta = {}
    for source, (url, table, labels) in EVENT_URLS.items():
        params = {
            "startDate": (
                start.replace("-", "") if not table else start.replace("-", "/")
            ),
            "endDate": end.replace("-", "") if not table else end.replace("-", "/"),
            "response": "json",
        }
        cache = (
            cache_dir / f"actions-{source}-{start}-{end}.json" if cache_dir else None
        )
        payload = None
        raw = b""
        retrieved_at = None
        if cache and cache.exists():
            saved = json.loads(cache.read_text(encoding="utf-8"))
            body = {
                "source": source,
                "start": start,
                "end": end,
                "payload": saved.get("payload"),
            }
            if saved.get("sha256") != payload_sha(body):
                raise UpdateError(f"{source}: invalid action cache hash/range")
            payload = body["payload"]
            retrieved_at = saved.get("retrieved_at")
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
        else:
            last = None
            for attempt in range(1, retries + 1):
                r = None
                raw = b""
                try:
                    r = get(
                        url,
                        params=params,
                        headers={"User-Agent": "stock-from-Hsiu-official-adjusted/1"},
                        timeout=60,
                    )
                    raw = r.content
                    r.raise_for_status()
                    payload = r.json()
                    retrieved_at = datetime.now(timezone.utc).isoformat()
                    break
                except (
                    requests.RequestException,
                    ValueError,
                    json.JSONDecodeError,
                ) as exc:
                    last = {
                        "attempt": attempt,
                        "error": f"{type(exc).__name__}: {exc}",
                        "status": (
                            getattr(r, "status_code", None) if "r" in locals() else None
                        ),
                        "content_type": (
                            getattr(r, "headers", {}).get("content-type")
                            if "r" in locals()
                            else None
                        ),
                        "url": getattr(r, "url", url) if "r" in locals() else url,
                        "body_sha256": sha(raw),
                        "preview": raw[:160].decode("utf-8", "replace"),
                    }
            if payload is None:
                raise UpdateError(
                    f"{source}: action response failed after {retries} attempts: {json.dumps(last, ensure_ascii=False)}"
                )
            if cache:
                body = {
                    "source": source,
                    "start": start,
                    "end": end,
                    "payload": payload,
                }
                atomic_json(
                    cache,
                    {**body, "sha256": payload_sha(body), "retrieved_at": retrieved_at},
                )
        parsed = parse_event(payload, source, table, labels)
        events += parsed
        meta[source] = {
            "url": url,
            "sha256": sha(raw),
            "row_count": len(parsed),
            "retrieved_at": retrieved_at,
        }
    return events, meta


def read_pair(root: Path, sid: str) -> tuple[list[dict], dict]:
    p = root / "prices" / f"{sid}.csv"
    m = root / "price_basis" / f"{sid}.json"
    raw = p.read_bytes()
    meta = json.loads(m.read_text(encoding="utf-8"))
    if (
        meta.get("stock_id") != sid
        or meta.get("mode") not in INPUT_MODES
        or meta.get("verified") is not True
        or meta.get("csv_sha256") != sha(raw)
    ):
        raise UpdateError(f"{sid}: cross-basis/hash validation failed")
    if b"\r\n" in raw:
        raise UpdateError(f"{sid}: serialization is not LF CSV")
    with p.open(encoding="utf-8-sig", newline="") as h:
        r = csv.DictReader(h)
        names = r.fieldnames
        rows = list(r)
    expected_volume = "finmind_raw_shares" if meta["mode"] != MODE else "raw_shares"
    if names != FIELDS or not rows or meta.get("volume_basis") != expected_volume:
        raise UpdateError(f"{sid}: history schema/basis invalid")
    dates = [r["date"] for r in rows]
    try:
        canonical = [iso(d) for d in dates]
    except Exception as exc:
        raise UpdateError(f"{sid}: history date invalid") from exc
    if (
        dates != sorted(set(dates))
        or dates[0] != meta.get("data_start")
        or dates[-1] != meta.get("data_end")
        or meta.get("row_count") != len(rows)
        or meta.get("available_bars") != len(rows)
    ):
        raise UpdateError(f"{sid}: coverage metadata invalid")
    if canonical != dates or meta.get("history_status") not in (
        None,
        "complete" if len(rows) >= 245 else "insufficient_history",
    ):
        raise UpdateError(f"{sid}: history date/status invalid")
    try:
        adjustment_as_of = iso(meta.get("adjustment_as_of"))
    except Exception as exc:
        raise UpdateError(f"{sid}: adjustment_as_of invalid") from exc
    if (
        adjustment_as_of != meta.get("adjustment_as_of")
        or adjustment_as_of != dates[-1]
        or meta.get("ma240_required_bars", 240) != 240
        or meta.get("direction_required_bars", 241) != 241
        or meta.get("full_study_recommended_bars", 245) != 245
    ):
        raise UpdateError(f"{sid}: adjustment/threshold metadata invalid")
    for row in rows:
        vals = {k: float(row[k]) for k in FIELDS[1:]}
        factor = vals["adjustment_factor"]
        if (
            not all(math.isfinite(v) for v in vals.values())
            or factor <= 0
            or vals["volume"] < 0
            or vals["raw_volume"] < 0
            or not math.isclose(vals["volume"], vals["raw_volume"], abs_tol=1e-9)
        ):
            raise UpdateError(f"{sid}: numeric/basis invalid")
        for prefix in ("", "raw_"):
            o, h, l, c = (vals[prefix + k] for k in ("open", "high", "low", "close"))
            if min(o, h, l, c) <= 0 or h < max(o, l, c) or l > min(o, h, c):
                raise UpdateError(f"{sid}: OHLC geometry invalid")
        if any(
            not math.isclose(
                vals[k], vals["raw_" + k] * factor, rel_tol=1e-9, abs_tol=1e-7
            )
            for k in ("open", "high", "low", "close")
        ):
            raise UpdateError(f"{sid}: factor invariant failed")
    if not math.isclose(float(rows[-1]["adjustment_factor"]), 1, abs_tol=1e-12):
        raise UpdateError(f"{sid}: latest factor is not 1")
    return rows, meta


def normalize_prices(rows: list[dict], session: str) -> dict[str, dict]:
    out = {}
    for r in rows:
        sid = str(r.get("stock_id") or "").strip()
        if len(sid) != 4 or not sid.isdigit() or sid.startswith("0"):
            continue
        if sid in out or r.get("date") != session:
            raise UpdateError("official price identity/date duplicate")
        vals = {k: float(r[k]) for k in ("open", "high", "low", "close", "volume")}
        if (
            not all(math.isfinite(x) for x in vals.values())
            or min(vals[k] for k in ("open", "high", "low", "close")) <= 0
            or vals["volume"] < 0
            or vals["high"] < max(vals["open"], vals["low"], vals["close"])
            or vals["low"] > min(vals["open"], vals["high"], vals["close"])
        ):
            raise UpdateError("official price numeric/geometry invalid")
        out[sid] = {"date": session, **vals}
    return out


def validate_market_partitions(
    twse: list[dict],
    tpex: list[dict],
    day: str,
    *,
    min_twse: int,
    min_tpex: int,
    reference: dict[str, int] | None = None,
):
    def ordinary_ids(rows):
        return [
            str(r.get("stock_id") or "").strip()
            for r in rows
            if len(str(r.get("stock_id") or "").strip()) == 4
            and not str(r.get("stock_id") or "").strip().startswith("0")
        ]

    listed, otc = ordinary_ids(twse), ordinary_ids(tpex)
    if len(listed) != len(set(listed)) or len(otc) != len(set(otc)):
        raise UpdateError(f"duplicate market identity: {day}")
    if set(listed) & set(otc):
        raise UpdateError(f"cross-market identity overlap: {day}")
    ref = reference or {}
    need_l = max(min_twse, math.ceil(ref.get("twse", 0) * 0.99))
    need_o = max(min_tpex, math.ceil(ref.get("tpex", 0) * 0.99))
    if len(listed) < need_l or len(otc) < need_o:
        raise UpdateError(
            f"truncated market partition: {day} twse={len(listed)}/{need_l} tpex={len(otc)}/{need_o}"
        )
    return {"twse": len(listed), "tpex": len(otc)}


def reconcile_actions(
    events: list[dict], sid: str, start: str, as_of: str
) -> list[dict]:
    by = {}
    for event in events:
        if event["stock_id"] != sid or not start <= event["date"] <= as_of:
            continue
        key = event["date"]
        ratio = event["reference_price"] / event["previous_close"]
        if key in by and not math.isclose(
            ratio,
            by[key]["reference_price"] / by[key]["previous_close"],
            rel_tol=2e-4,
            abs_tol=2e-4,
        ):
            raise UpdateError(f"{sid}: corporate-action source conflict {key}")
        if key in by:
            by[key]["source"] = "+".join(
                sorted(set(by[key]["source"].split("+")) | {event["source"]})
            )
        else:
            by[key] = dict(event)
    return [by[k] for k in sorted(by)]


def validate_anchors(raw: list[dict], actions: list[dict]):
    for action in actions:
        prior = [r for r in raw if r["date"] < action["date"]]
        if prior and not math.isclose(
            float(prior[-1]["close"]),
            float(action["previous_close"]),
            rel_tol=0.002,
            abs_tol=0.05,
        ):
            raise UpdateError(
                f"{action['stock_id']}: action anchor mismatch {action['date']}"
            )


def project(raw: list[dict], actions: list[dict], as_of: str) -> list[dict]:
    out = []
    for row in raw:
        factor = math.prod(
            a["reference_price"] / a["previous_close"]
            for a in actions
            if row["date"] < a["date"] <= as_of
        )
        out.append(
            {
                "date": row["date"],
                **{k: row[k] * factor for k in ("open", "high", "low", "close")},
                "volume": row["volume"],
                **{
                    "raw_" + k: row[k]
                    for k in ("open", "high", "low", "close", "volume")
                },
                "adjustment_factor": factor,
            }
        )
    if not math.isclose(out[-1]["adjustment_factor"], 1, abs_tol=1e-12):
        raise UpdateError("latest factor is not anchored at 1")
    return out


def compose(
    old: list[dict], new: list[dict], actions: list[dict], as_of: str
) -> list[dict]:
    factors = {r["date"]: float(r["adjustment_factor"]) for r in old}
    raw = []
    for r in old:
        raw.append(
            {
                "date": r["date"],
                **{k: float(r["raw_" + k]) for k in ("open", "high", "low", "close")},
                "volume": float(r["raw_volume"]),
            }
        )
    raw.extend(new)
    raw = sorted({r["date"]: r for r in raw}.values(), key=lambda r: r["date"])
    validate_anchors(raw, actions)
    projected = project(raw, actions, as_of)
    for r in projected:
        f = factors.get(r["date"], 1.0)
        r["adjustment_factor"] *= f
        for k in ("open", "high", "low", "close"):
            r[k] *= f
    return projected


def write_pair(root: Path, sid: str, rows: list[dict], meta: dict):
    d = root / "prices"
    b = root / "price_basis"
    d.mkdir(parents=True, exist_ok=True)
    b.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", dir=d, delete=False
    ) as h:
        w = csv.DictWriter(h, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
        tmp = Path(h.name)
    digest = sha(tmp.read_bytes())
    tmp.replace(d / f"{sid}.csv")
    atomic_json(b / f"{sid}.json", {**meta, "csv_sha256": digest})


def validate_projected(sid: str, rows: list[dict], meta: dict):
    if (
        not rows
        or meta["data_start"] != rows[0]["date"]
        or meta["data_end"] != rows[-1]["date"]
        or meta["adjustment_as_of"] != rows[-1]["date"]
        or meta["row_count"] != len(rows)
        or meta["available_bars"] != len(rows)
    ):
        raise UpdateError(f"{sid}: projected coverage invalid")
    expected = "complete" if len(rows) >= 245 else "insufficient_history"
    if meta["history_status"] != expected or (
        meta["ma240_required_bars"],
        meta["direction_required_bars"],
        meta["full_study_recommended_bars"],
    ) != (240, 241, 245):
        raise UpdateError(f"{sid}: projected metadata invalid")
    normalize_prices(
        [
            {
                "stock_id": sid,
                **{
                    k: rows[-1][k]
                    for k in ("date", "open", "high", "low", "close", "volume")
                },
            }
        ],
        rows[-1]["date"],
    )


def cached_partition(cache_dir: Path | None, day: str, price_api):
    path = cache_dir / f"official-prices-{day}.json" if cache_dir else None
    if path and path.exists():
        value = json.loads(path.read_text(encoding="utf-8"))
        body = {
            "date": value.get("date"),
            "twse": value.get("twse"),
            "tpex": value.get("tpex"),
        }
        if (
            body["date"] != day
            or value.get("sha256") != payload_sha(body)
            or not body["twse"]
            or not body["tpex"]
        ):
            raise UpdateError(f"invalid cached market partition: {day}")
        return body["twse"], body["tpex"], value.get("retrieved_at")
    twse, tpex = price_api.fetch_history_partitions(date.fromisoformat(day))
    if path and twse and tpex:
        body = {"date": day, "twse": twse, "tpex": tpex}
        atomic_json(
            path,
            {
                **body,
                "sha256": payload_sha(body),
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    return twse, tpex, datetime.now(timezone.utc).isoformat()


def _ids(args) -> list[str]:
    if args.stock_ids_file:
        ids = [
            x.strip() for x in args.stock_ids_file.read_text().splitlines() if x.strip()
        ]
    else:
        ids = sorted(
            p.stem
            for p in (args.output_root / "price_basis").glob("*.json")
            if json.loads(p.read_text(encoding="utf-8")).get("mode") in INPUT_MODES
        )
    if len(ids) != len(set(ids)) or any(
        len(s) != 4 or not s.isdigit() or s.startswith("0") for s in ids
    ):
        raise UpdateError("stock id list must contain unique ordinary four-digit IDs")
    return ids


def run(
    args,
    *,
    price_api=None,
    event_fetcher=fetch_events,
    calendar_fetcher=None,
    min_twse: int = MIN_TWSE_ROWS,
    min_tpex: int = MIN_TPEX_ROWS,
    now: datetime | None = None,
) -> dict:
    price_api = price_api or load_module(args.official_root, "official_price_refresh")
    ids = _ids(args)
    pairs = {sid: read_pair(args.output_root, sid) for sid in ids}
    previous = min(m["data_end"] for _, m in pairs.values())
    maximum = max(m["data_end"] for _, m in pairs.values())
    latest, latest_rows, latest_counts, latest_meta = price_api.fetch_latest_snapshot()
    sessions = {}
    market_counts = {}
    retrieved = {}
    claimed_l = int(latest_counts.get("twse", -1))
    claimed_o = int(latest_counts.get("tpex", -1))
    if min(claimed_l, claimed_o) < 0 or claimed_l + claimed_o != len(latest_rows):
        raise UpdateError("latest partition metadata/row count mismatch")
    latest_actual = validate_market_partitions(
        latest_rows[:claimed_l],
        latest_rows[claimed_l:],
        latest,
        min_twse=min_twse,
        min_tpex=min_tpex,
    )
    local_now = now or datetime.now(ZoneInfo("Asia/Taipei"))
    if local_now.tzinfo is None:
        raise UpdateError("injected clock must be timezone-aware")
    local_now = local_now.astimezone(ZoneInfo("Asia/Taipei"))
    cutoff = (
        local_now.date()
        if local_now.hour >= 16
        else local_now.date() - timedelta(days=1)
    )
    if calendar_fetcher is None:
        calendar_fetcher = load_module(
            args.official_root, "attention_disposition"
        ).fetch_trading_sessions
    official_sessions = []
    for year in range(date.fromisoformat(previous).year, cutoff.year + 1):
        through = min(date(year, 12, 31), cutoff)
        annual = calendar_fetcher(through)
        if annual != sorted(set(annual)):
            raise UpdateError("annual calendar must be sorted and unique")
        try:
            valid = all(iso(day) == day and date.fromisoformat(day).year == year
                        and date.fromisoformat(day) <= through for day in annual)
        except (ValueError, UpdateError):
            valid = False
        if not valid:
            raise UpdateError("annual calendar date or coverage mismatch")
        official_sessions.extend(annual)
    try:
        canonical_sessions = [iso(d) for d in official_sessions]
    except Exception as exc:
        raise UpdateError("official calendar contains invalid dates") from exc
    if (
        canonical_sessions != official_sessions
        or official_sessions != sorted(set(official_sessions))
        or not official_sessions
        or official_sessions[-1] > cutoff.isoformat()
    ):
        raise UpdateError(
            "official calendar must be nonempty sorted unique ISO dates through cutoff"
        )
    if latest != official_sessions[-1]:
        raise UpdateError(
            f"official price latest {latest} does not match completed session {official_sessions[-1]}"
        )
    calendar_meta = {
        "calendar_basis": "official_twse_tpex",
        "calendar_as_of": cutoff.isoformat(),
        "expected_completed_session": latest,
        "official_sessions_sha256": payload_sha(official_sessions),
    }
    if latest < maximum:
        raise UpdateError("official latest date predates verified history")
    if latest == previous:
        manifest = {
            "dataset_id": "official_adjusted_daily_update",
            "mode": MODE,
            "status": "current",
            "previous_date": previous,
            "data_as_of": latest,
            "requested": len(ids),
            "completed": 0,
            "unchanged_current": len(ids),
            **calendar_meta,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        atomic_json(
            args.output_root / "official_adjusted_update_manifest.json", manifest
        )
        return manifest
    expected = [d for d in official_sessions if previous < d < latest]
    for day in expected:
        twse, tpex, retrieved_at = cached_partition(
            getattr(args, "cache_dir", None), day, price_api
        )
        if not twse or not tpex:
            raise UpdateError(f"missing official market partition: {day}")
        actual = validate_market_partitions(
            twse,
            tpex,
            day,
            min_twse=min_twse,
            min_tpex=min_tpex,
            reference=latest_actual,
        )
        sessions[day] = normalize_prices(twse + tpex, day)
        market_counts[day] = actual
        retrieved[day] = retrieved_at
    sessions[latest] = normalize_prices(latest_rows, latest)
    market_counts[latest] = latest_actual
    retrieved[latest] = (
        latest_meta.get("retrieved_at") or datetime.now(timezone.utc).isoformat()
    )
    event_start = (date.fromisoformat(previous) + timedelta(days=1)).isoformat()
    events, event_meta = (
        event_fetcher(event_start, latest, cache_dir=getattr(args, "cache_dir", None))
        if event_fetcher is fetch_events
        else event_fetcher(event_start, latest)
    )
    price_provenance = {
        day: {
            "row_count": len(rows),
            "market_counts": market_counts[day],
            "sha256": payload_sha(rows),
            "retrieved_at": retrieved[day],
        }
        for day, rows in sessions.items()
    }
    additions = defaultdict(list)
    for day in sorted(sessions):
        for sid, row in sessions[day].items():
            if sid in pairs:
                additions[sid].append(row)
    staged = {}
    missing = []
    for sid in ids:
        oldrows, old = pairs[sid]
        oldend = old["data_end"]
        new = [r for r in additions[sid] if r["date"] > oldend]
        if oldend == latest:
            continue
        if not new or new[-1]["date"] != latest:
            missing.append(sid)
            continue
        acts = reconcile_actions(
            events,
            sid,
            (date.fromisoformat(oldend) + timedelta(days=1)).isoformat(),
            latest,
        )
        rows = compose(oldrows, new, acts, latest)
        origin = {
            "mode": old.get("mode"),
            "price_source": old.get("price_source"),
            "csv_sha256": old.get("csv_sha256"),
            "raw_sha256": old.get("raw_sha256"),
            "data_start": old.get("data_start"),
            "data_end": oldend,
        }
        history_status = "complete" if len(rows) >= 245 else "insufficient_history"
        meta = {k: v for k, v in old.items() if k != "raw_sha256"}
        meta.update(
            {
                "mode": MODE,
                "verified": True,
                "volume_basis": "raw_shares",
                "price_source": "FinMind historical baseline + TWSE/TPEx official incremental OHLCV",
                "origin": origin,
                "data_start": rows[0]["date"],
                "data_end": latest,
                "adjustment_as_of": latest,
                "row_count": len(rows),
                "available_bars": len(rows),
                "ma240_required_bars": 240,
                "direction_required_bars": 241,
                "full_study_recommended_bars": 245,
                "history_status": history_status,
                "price_sources": sorted(
                    set(
                        old.get("price_sources")
                        or [old.get("price_source", "FinMind TaiwanStockPrice")]
                    )
                    | {"TWSE/TPEx official daily OHLCV"}
                ),
                "incremental_source_dates": [d for d in sorted(sessions) if d > oldend],
                "incremental_event_sources": event_meta,
                "event_count": int(old.get("event_count", 0)) + len(acts),
                "source_requested_range": {
                    **old.get("source_requested_range", {}),
                    "as_of": latest,
                },
            }
        )
        validate_projected(sid, rows, meta)
        staged[sid] = (rows, meta)
    for sid, (rows, meta) in staged.items():
        write_pair(args.output_root, sid, rows, meta)
    completed = sorted(staged)
    manifest = {
        "dataset_id": "official_adjusted_daily_update",
        "mode": MODE,
        "previous_date": previous,
        "data_as_of": latest,
        "sessions": sorted(sessions),
        "price_partitions": price_provenance,
        "latest_partition_counts": latest_counts,
        "latest_snapshot": latest_meta,
        "event_sources": event_meta,
        "requested": len(ids),
        "completed": len(completed),
        "unchanged_current": sum(
            1 for _, m in pairs.values() if m["data_end"] == latest
        ),
        "missing_exact_latest": missing,
        "status": (
            "complete"
            if len(completed)
            + sum(1 for _, m in pairs.values() if m["data_end"] == latest)
            == len(ids)
            else "partial"
        ),
        **calendar_meta,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    atomic_json(args.output_root / "official_adjusted_update_manifest.json", manifest)
    return manifest


def parse_args(argv=None):
    here = Path(__file__).resolve()
    root = (
        here.parent
        if (here.parent / "official_price_refresh.py").exists()
        else here.parents[1]
    )
    p = argparse.ArgumentParser()
    p.add_argument("--official-root", type=Path, default=root)
    p.add_argument("--stock-ids-file", type=Path)
    p.add_argument("--data-dir", dest="output_root", type=Path, default=root / "data")
    p.add_argument("--cache-dir", type=Path)
    return p.parse_args(argv)


if __name__ == "__main__":
    print(json.dumps(run(parse_args()), ensure_ascii=False))
