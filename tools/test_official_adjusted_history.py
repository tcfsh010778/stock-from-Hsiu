import csv, hashlib, json
from argparse import Namespace
from datetime import datetime
from zoneinfo import ZoneInfo
import pytest
import update_official_adjusted_history as m

NOW = datetime(2026, 9, 8, 17, tzinfo=ZoneInfo("Asia/Taipei"))


def test_dividend_urls_match_canonical_historical_provider():
    assert (
        m.EVENT_URLS["twse_dividend"][0]
        == "https://www.twse.com.tw/rwd/zh/exRight/TWT49U"
    )
    assert (
        m.EVENT_URLS["tpex_dividend"][0]
        == "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ"
    )


def seed(root, sid="2330"):
    (root / "prices").mkdir()
    (root / "price_basis").mkdir()
    row = {k: 10.0 for k in m.FIELDS}
    row.update(date="2026-09-04", volume=100.0, raw_volume=100.0, adjustment_factor=1.0)
    p = root / "prices" / f"{sid}.csv"
    with p.open("w", encoding="utf-8", newline="") as h:
        w = csv.DictWriter(h, fieldnames=m.FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerow(row)
    meta = {
        "stock_id": sid,
        "mode": "finmind_raw_reconciled_reference_ratio_back_adjusted_v1",
        "verified": True,
        "volume_basis": "finmind_raw_shares",
        "data_start": "2026-09-04",
        "data_end": "2026-09-04",
        "row_count": 1,
        "available_bars": 1,
        "adjustment_as_of": "2026-09-04",
        "csv_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        "event_count": 0,
    }
    (root / "price_basis" / f"{sid}.json").write_text(json.dumps(meta))
    return p


def payload(fields, data, table=False):
    return (
        {"stat": "ok", "tables": [{"fields": fields, "data": data}]}
        if table
        else {"stat": "OK", "fields": fields, "data": data}
    )


def test_six_event_schemas_and_conflict():
    for source, (url, table, labels) in m.EVENT_URLS.items():
        row = [""] * len(labels)
        fields = list(labels.values())
        row[fields.index(labels["date"])] = "2026/09/05"
        row[fields.index(labels["stock"])] = "2330"
        row[fields.index(labels["before"])] = "100"
        row[fields.index(labels["ref"])] = "50"
        assert (
            m.parse_event(payload(fields, [row], table), source, table, labels)[0][
                "reference_price"
            ]
            == 50
        )


def test_explicit_twse_no_event_response_is_not_a_transport_failure():
    _,table,labels=m.EVENT_URLS['twse_reduction']
    assert m.parse_event({'stat':'很抱歉，沒有符合條件的資料!'},'twse_reduction',table,labels)==[]
    for bad in ({'stat':'service unavailable'},{'stat':'OK'},{}):
        with pytest.raises(m.UpdateError):m.parse_event(bad,'twse_reduction',table,labels)
    with pytest.raises(m.UpdateError):
        m.reconcile_actions(
            [
                {
                    "date": "2026-09-05",
                    "stock_id": "2330",
                    "previous_close": 100,
                    "reference_price": 50,
                    "source": "a",
                },
                {
                    "date": "2026-09-05",
                    "stock_id": "2330",
                    "previous_close": 100,
                    "reference_price": 60,
                    "source": "b",
                },
            ],
            "2330",
            "2026-09-05",
            "2026-09-05",
        )


def test_compose_preserves_historic_factor_and_event_direction():
    old = {k: 100.0 for k in m.FIELDS}
    old.update(date="2026-09-04", adjustment_factor=0.8)
    new = {
        "date": "2026-09-05",
        "open": 50.0,
        "high": 50.0,
        "low": 50.0,
        "close": 50.0,
        "volume": 100.0,
    }
    event = {
        "date": "2026-09-05",
        "stock_id": "2330",
        "previous_close": 100.0,
        "reference_price": 50.0,
        "source": "official",
    }
    out = m.compose([old], [new], [event], "2026-09-05")
    assert out[0]["adjustment_factor"] == 0.4 and out[-1]["adjustment_factor"] == 1


class Prices:
    @staticmethod
    def fetch_latest_snapshot():
        return (
            "2026-09-08",
            [
                {
                    "stock_id": "2330",
                    "date": "2026-09-08",
                    "open": 12,
                    "high": 12,
                    "low": 12,
                    "close": 12,
                    "volume": 100,
                },
                {
                    "stock_id": "6488",
                    "date": "2026-09-08",
                    "open": 2,
                    "high": 2,
                    "low": 2,
                    "close": 2,
                    "volume": 10,
                },
            ],
            {"twse": 1, "tpex": 1},
            {"target_date": "2026-09-08"},
        )

    @staticmethod
    def fetch_history_partitions(day):
        if day.isoformat() == "2026-09-05":
            return (
                [
                    {
                        "stock_id": "2330",
                        "date": "2026-09-05",
                        "open": 11,
                        "high": 11,
                        "low": 11,
                        "close": 11,
                        "volume": 100,
                    }
                ],
                [
                    {
                        "stock_id": "6488",
                        "date": "2026-09-05",
                        "open": 1,
                        "high": 1,
                        "low": 1,
                        "close": 1,
                        "volume": 1,
                    }
                ],
            )
        return [], []


def args(tmp):
    ids = tmp / "ids"
    ids.write_text("2330\n")
    return Namespace(stock_ids_file=ids, output_root=tmp, official_root=tmp)


def test_all_sources_prefetched_before_write(tmp_path):
    p = seed(tmp_path)
    before = p.read_bytes()
    with pytest.raises(m.UpdateError):
        m.run(
            args(tmp_path),
            price_api=Prices,
            event_fetcher=lambda a, b: (_ for _ in ()).throw(
                m.UpdateError("source failed")
            ),
            calendar_fetcher=lambda d: ["2026-09-05", "2026-09-08"],
            now=NOW,
            min_twse=1,
            min_tpex=1,
        )
    assert p.read_bytes() == before


def test_multiday_update_writes_lf_hash_and_coverage(tmp_path):
    seed(tmp_path)
    result = m.run(
        args(tmp_path),
        price_api=Prices,
        event_fetcher=lambda a, b: ([], {"six_partitions": "ok"}),
        calendar_fetcher=lambda d: ["2026-09-05", "2026-09-08"],
        now=NOW,
        min_twse=1,
        min_tpex=1,
    )
    assert (
        result["sessions"] == ["2026-09-05", "2026-09-08"]
        and result["status"] == "complete"
    )
    raw = (tmp_path / "prices/2330.csv").read_bytes()
    assert b"\r\n" not in raw
    meta = json.loads((tmp_path / "price_basis/2330.json").read_text())
    assert (
        meta["mode"] == m.MODE
        and meta["csv_sha256"] == hashlib.sha256(raw).hexdigest()
        and meta["available_bars"] == 3
    )
    assert (
        meta["price_source"]
        == "FinMind historical baseline + TWSE/TPEx official incremental OHLCV"
        and "raw_sha256" not in meta
    )


def test_missing_expected_market_partition_fails_before_write(tmp_path):
    p = seed(tmp_path)
    before = p.read_bytes()

    class Missing(Prices):
        @staticmethod
        def fetch_history_partitions(day):
            return [], []

    with pytest.raises(m.UpdateError, match="missing official market partition"):
        m.run(
            args(tmp_path),
            price_api=Missing,
            event_fetcher=lambda a, b: ([], {}),
            calendar_fetcher=lambda d: ["2026-09-05", "2026-09-08"],
            now=NOW,
            min_twse=1,
            min_tpex=1,
        )
    assert p.read_bytes() == before


def test_read_pair_rejects_invalid_geometry_even_with_matching_hash(tmp_path):
    p = seed(tmp_path)
    rows = list(csv.DictReader(p.open(encoding="utf-8")))
    rows[0]["high"] = "9"
    rows[0]["raw_high"] = "9"
    with p.open("w", encoding="utf-8", newline="") as h:
        w = csv.DictWriter(h, fieldnames=m.FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    basis = tmp_path / "price_basis/2330.json"
    meta = json.loads(basis.read_text())
    meta["csv_sha256"] = hashlib.sha256(p.read_bytes()).hexdigest()
    basis.write_text(json.dumps(meta))
    with pytest.raises(m.UpdateError, match="OHLC geometry"):
        m.read_pair(tmp_path, "2330")


def test_noop_writes_current_manifest(tmp_path):
    seed(tmp_path)

    class Current(Prices):
        @staticmethod
        def fetch_latest_snapshot():
            return (
                "2026-09-04",
                [{"stock_id": "2330"}, {"stock_id": "3324"}],
                {"twse": 1, "tpex": 1},
                {"target_date": "2026-09-04"},
            )

    result = m.run(
        args(tmp_path),
        price_api=Current,
        event_fetcher=lambda a, b: pytest.fail("events fetched"),
        calendar_fetcher=lambda d: ["2026-09-04"],
        now=datetime(2026, 9, 4, 17, tzinfo=ZoneInfo("Asia/Taipei")),
        min_twse=1,
        min_tpex=1,
    )
    assert (
        result["status"] == "current"
        and json.loads(
            (tmp_path / "official_adjusted_update_manifest.json").read_text()
        )["unchanged_current"]
        == 1
    )
    assert (
        result["calendar_basis"] == "official_twse_tpex"
        and result["official_sessions_sha256"]
    )


def test_calendar_rejects_stale_latest_before_writes(tmp_path):
    p = seed(tmp_path)
    before = p.read_bytes()

    class Current(Prices):
        @staticmethod
        def fetch_latest_snapshot():
            return (
                "2026-09-04",
                [{"stock_id": "2330"}, {"stock_id": "3324"}],
                {"twse": 1, "tpex": 1},
                {},
            )

    with pytest.raises(m.UpdateError, match="does not match completed session"):
        m.run(
            args(tmp_path),
            price_api=Current,
            calendar_fetcher=lambda d: ["2026-09-04", "2026-09-05"],
            now=datetime(2026, 9, 5, 17, tzinfo=ZoneInfo("Asia/Taipei")),
            min_twse=1,
            min_tpex=1,
        )
    assert p.read_bytes() == before


def test_partition_cache_is_hash_validated(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    day = "2026-09-05"
    body = {"date": day, "twse": [{"stock_id": "2330"}], "tpex": [{"stock_id": "3324"}]}
    (cache / f"official-prices-{day}.json").write_text(
        json.dumps({**body, "sha256": "bad"})
    )
    with pytest.raises(m.UpdateError, match="invalid cached"):
        m.cached_partition(cache, day, Prices)


def test_partition_coverage_rejects_truncated_and_overlap():
    one = lambda sid: {"stock_id": sid}
    with pytest.raises(m.UpdateError, match="truncated"):
        m.validate_market_partitions(
            [one("2330")], [one("3324")], "2026-09-08", min_twse=2, min_tpex=2
        )
    with pytest.raises(m.UpdateError, match="cross-market"):
        m.validate_market_partitions(
            [one("2330")], [one("2330")], "2026-09-08", min_twse=1, min_tpex=1
        )


def test_event_nonjson_retries_then_caches(monkeypatch, tmp_path):
    labels = {"date": "d", "stock": "s", "before": "b", "ref": "r"}
    monkeypatch.setattr(
        m, "EVENT_URLS", {"one": ("https://official.test", False, labels)}
    )
    calls = []

    class Response:
        status_code = 200
        headers = {"content-type": "text/html"}
        url = "https://official.test"
        content = b"temporary html"

        def raise_for_status(self):
            pass

        def json(self):
            if len(calls) < 3:
                raise ValueError("not json")
            return {
                "fields": ["d", "s", "b", "r"],
                "data": [["2026-09-05", "2330", "100", "99"]],
            }

    def get(*a, **k):
        calls.append(1)
        return Response()

    events, meta = m.fetch_events(
        "2026-09-05", "2026-09-05", get, cache_dir=tmp_path, retries=3
    )
    assert (
        len(calls) == 3
        and events[0]["stock_id"] == "2330"
        and meta["one"]["retrieved_at"]
    )
    m.fetch_events(
        "2026-09-05",
        "2026-09-05",
        lambda *a, **k: pytest.fail("network used"),
        cache_dir=tmp_path,
    )


@pytest.mark.parametrize(
    "value", ["115年09月07日", "115/09/07", "2026-09-07", "1150907", "20260907"]
)
def test_official_roc_event_date_formats(value):
    assert m.iso(value) == "2026-09-07"
