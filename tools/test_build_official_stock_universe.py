from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import tools.build_official_stock_universe as subject


def roster(market: str, count: int, *, start: int, future: bool = False) -> bytes:
    date_key = "上市日期" if market == "listed" else "上櫃日期"
    rows = [{"出表日期": "1150909", "公司代號": f"{start + i:04d}", "公司名稱": f"公司{i}", date_key: "2020/01/02"}
            for i in range(count)]
    if future:
        rows[-1][date_key] = "2026/09/09"
    return json.dumps(rows, ensure_ascii=False).encode()


def valid_raw(future: bool = False):
    return {"listed": roster("listed", 801 if future else 800, start=1000, future=future),
            "otc": roster("otc", 601 if future else 600, start=5000, future=future)}


def test_builds_two_market_dated_universe_with_hashes_and_listing_filter():
    payload = subject.build_universe(valid_raw(future=True), universe_as_of="2026-09-09",
                                     tdcc_date="2026-09-04", retrieved_at="2026-09-09T16:00:00Z")
    assert payload["row_count"] == 1400
    assert payload["market_counts"] == {"listed": 800, "otc": 600}
    assert payload["excluded_not_yet_listed_security_ids"] == {"listed": ["1800"], "otc": ["5600"]}
    assert len(payload["sources"]["listed"]["raw_sha256"]) == 64
    assert payload["effective_for_tdcc_date"] == "2026-09-04"


def test_decode_rejects_small_duplicate_bad_identity_and_bad_date():
    with pytest.raises(ValueError, match="too small"):
        subject.decode_rows(roster("listed", 2, start=1000), "listed")
    rows = json.loads(roster("listed", 800, start=1000)); rows[-1]["公司代號"] = rows[0]["公司代號"]
    with pytest.raises(ValueError, match="duplicate"):
        subject.decode_rows(json.dumps(rows).encode(), "listed")
    rows = json.loads(roster("otc", 600, start=5000)); rows[0]["公司代號"] = "ETF"
    with pytest.raises(ValueError, match="identity"):
        subject.decode_rows(json.dumps(rows).encode(), "otc")
    rows = json.loads(roster("otc", 600, start=5000)); rows[0]["上櫃日期"] = ""
    with pytest.raises(ValueError, match="listing date"):
        subject.decode_rows(json.dumps(rows).encode(), "otc")
    rows = json.loads(roster("otc", 600, start=5000)); rows[0]["出表日期"] = "1150908"
    with pytest.raises(ValueError, match="one report date"):
        subject.decode_rows(json.dumps(rows).encode(), "otc")


def test_cross_market_duplicate_future_target_and_effective_coverage_fail_closed():
    raw = valid_raw(); otc = json.loads(raw["otc"]); otc[0]["公司代號"] = "1000"; raw["otc"] = json.dumps(otc).encode()
    with pytest.raises(ValueError, match="both TWSE and TPEx"):
        subject.build_universe(raw, universe_as_of="2026-09-09", tdcc_date="2026-09-04",
                               retrieved_at="2026-09-09T16:00:00Z")
    with pytest.raises(ValueError, match="newer"):
        subject.build_universe(valid_raw(), universe_as_of="2026-09-09", tdcc_date="2026-09-10",
                               retrieved_at="2026-09-09T16:00:00Z")
    raw = valid_raw(); rows = json.loads(raw["otc"])
    for row in rows[:2]: row["上櫃日期"] = "2026/09/09"
    raw["otc"] = json.dumps(rows).encode()
    with pytest.raises(ValueError, match="effective official universe coverage"):
        subject.build_universe(raw, universe_as_of="2026-09-09", tdcc_date="2026-09-04",
                               retrieved_at="2026-09-09T16:00:00Z")


def test_run_fetches_each_official_source_once_and_writes_atomically(tmp_path):
    raw = valid_raw(); calls = []
    def fetch(url):
        calls.append(url)
        market = next(m for m, source in subject.SOURCES.items() if source == url)
        return raw[market]
    output = tmp_path / "universe.json"
    payload = subject.run(output=output, universe_as_of="2026-09-09", tdcc_date="2026-09-04", fetcher=fetch,
                          now=lambda: datetime(2026, 9, 9, 16, tzinfo=timezone.utc))
    assert calls == [subject.SOURCES["listed"], subject.SOURCES["otc"]]
    assert json.loads(output.read_text(encoding="utf-8"))["row_count"] == payload["row_count"]
    assert not list(tmp_path.glob("*.tmp"))


class Response:
    def __init__(self, status=200, content=b"[]"):
        self.status_code, self.content = status, content
    def raise_for_status(self):
        if self.status_code >= 400:
            raise __import__("requests").HTTPError("redacted")


def test_fetch_stops_access_responses_and_redacts_transport_details(monkeypatch):
    class Session:
        def __init__(self, values): self.values = iter(values); self.calls = 0
        def get(self, *args, **kwargs): self.calls += 1; value = next(self.values); return value
    blocked = Session([Response(429)])
    with pytest.raises(subject.UniverseSourceError, match="HTTP 429"):
        subject.fetch_bytes("https://official.invalid/secret", session=blocked)
    assert blocked.calls == 1
    class Broken:
        def get(self, *args, **kwargs): raise __import__("requests").ConnectionError("URL with sensitive query")
    monkeypatch.setattr(subject.time, "sleep", lambda _: None)
    with pytest.raises(subject.UniverseSourceError, match="ConnectionError") as error:
        subject.fetch_bytes("https://official.invalid/secret", attempts=2, session=Broken())
    assert "sensitive" not in str(error.value)
