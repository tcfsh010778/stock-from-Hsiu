from __future__ import annotations

from copy import deepcopy
from datetime import date

import pytest

from tools.update_weekly_mda_pool import (
    ARCHIVE_ID, load_reliable_universe, normalize_latest, seed_compact_snapshot, top50, update,
)


def universe():
    return [
        {"security_id": "1101", "name": "甲", "market": "listed"},
        {"security_id": "6488", "name": "乙", "market": "otc"},
    ]


def raw(day="20260904", *, missing=None, duplicate=False):
    rows = []
    for ref in universe():
        if ref["security_id"] == missing: continue
        for level in range(1, 18):
            percent = 100 if level == 17 else (2 if level in {12, 13, 14, 15} else 0)
            rows.append({"資料日期": day, "證券代號": ref["security_id"], "持股分級": str(level),
                         "人數": "10", "股數": "1000", "占集保庫存數比例%": str(percent)})
    if duplicate: rows.append(dict(rows[0]))
    return rows


def compact(day="2026-08-28", listed=7.0, otc=7.5):
    return {"date": day, "row_count": 2, "market_counts": {"listed": 1, "otc": 1}, "rows": [
        {"security_id": "1101", "name": "甲", "market": "listed", "major_percent": listed, "major_people": 40},
        {"security_id": "6488", "name": "乙", "market": "otc", "major_percent": otc, "major_people": 40},
    ]}


def archive(*snapshots):
    return {"dataset_id": ARCHIVE_ID, "snapshots": list(snapshots)}


def test_complete_two_market_update_emits_public_pool_contract():
    updated, pool = update(universe(), archive(compact()), as_of="2026-09-09", fetch_rows=raw)
    assert updated["latest_date"] == "2026-09-04"
    assert pool["dataset_id"] == "mda_weekly_top50"
    assert pool["quality"] == "complete" and pool["status"] == "ok"
    assert pool["data_date"] == "2026-09-04" and pool["previous_date"] == "2026-08-28"
    assert pool["selection_status"] == "fewer_than_50_positive_increases"
    assert [row["rank"] for row in pool["rows"]] == [1, 2]
    assert pool["paired_market_counts"] == {"listed": 1, "otc": 1}


def test_missing_stock_duplicate_level_future_date_and_one_market_fail_closed():
    with pytest.raises(ValueError, match="missing universe"):
        normalize_latest(raw(missing="6488"), universe(), as_of="2026-09-09")
    with pytest.raises(ValueError, match="duplicate"):
        normalize_latest(raw(duplicate=True), universe(), as_of="2026-09-09")
    with pytest.raises(ValueError, match="newer than"):
        normalize_latest(raw("20260910"), universe(), as_of="2026-09-09")
    listed_only = [universe()[0]]
    with pytest.raises(ValueError, match="both markets"):
        normalize_latest([row for row in raw() if row["證券代號"] == "1101"], listed_only, as_of="2026-09-09")


def test_same_week_is_idempotent_but_changed_raw_is_rejected():
    current = normalize_latest(raw(), universe(), as_of="2026-09-09")
    source = archive(compact(), current)
    first_archive, first_pool = update(universe(), deepcopy(source), as_of="2026-09-09", fetch_rows=raw)
    second_archive, second_pool = update(universe(), deepcopy(first_archive), as_of="2026-09-09", fetch_rows=raw)
    assert first_archive == second_archive and first_pool == second_pool
    changed = raw(); changed[11]["占集保庫存數比例%"] = "3"
    with pytest.raises(ValueError, match="same-date"):
        update(universe(), source, as_of="2026-09-09", fetch_rows=lambda: changed)


def test_same_date_ignores_name_and_metadata_but_rejects_duplicate_or_older_archive():
    current = normalize_latest(raw(), universe(), as_of="2026-09-09")
    renamed = deepcopy(current); renamed["rows"][0]["name"] = "名稱修訂"; renamed["note"] = "format only"
    updated, _ = update(universe(), archive(compact(), renamed), as_of="2026-09-09", fetch_rows=raw)
    assert updated["latest_date"] == "2026-09-04"
    with pytest.raises(ValueError, match="duplicate dates"):
        update(universe(), archive(compact(), deepcopy(compact())), as_of="2026-09-09", fetch_rows=raw)
    future_archive = compact("2026-09-11")
    with pytest.raises(ValueError, match="predates"):
        update(universe(), archive(future_archive), as_of="2026-09-12", fetch_rows=raw)


def test_new_and_departed_ids_are_recorded_without_forcing_fifty():
    prior = compact(); prior["rows"].append({"security_id":"9999","name":"舊","market":"otc","major_percent":9,"major_people":1})
    prior["row_count"] = 3; prior["market_counts"] = {"listed":1,"otc":2}
    current = compact("2026-09-04", listed=8, otc=8)
    current["rows"].append({"security_id":"6489","name":"新","market":"otc","major_percent":8,"major_people":1})
    current["row_count"] = 3; current["market_counts"] = {"listed":1,"otc":2}
    pool = top50(current, prior)
    assert pool["excluded_new_security_ids"] == ["6489"]
    assert pool["excluded_departed_security_ids"] == ["9999"]
    assert pool["paired_market_counts"] == {"listed": 1, "otc": 1}


def test_seed_strips_full_levels_and_reliable_universe_rejects_duplicates(tmp_path):
    full_rows = []
    for ref in universe():
        levels = {str(i): {"people": 10, "shares": 1000, "percent": 2 if i in {12, 13, 14, 15} else 0}
                  for i in range(1, 16)}
        full_rows.append({**ref, "levels": levels, "adjustment": {"people": 0, "shares": 0, "percent": 0},
                          "total": {"people": 150, "shares": 15000, "percent": 100},
                          "major_percent": 8, "major_people": 40})
    recovered = {"complete": True, "date": "2026-08-28", "row_count": 2, "expected_count": 2,
                 "market_counts": {"listed": 1, "otc": 1}, "expected_market_counts": {"listed": 1, "otc": 1},
                 "rows": full_rows}
    seeded = seed_compact_snapshot(recovered)
    assert "levels" not in seeded["rows"][0]
    path = tmp_path / "universe.json"
    path.write_text(__import__("json").dumps({"rows": universe()}), encoding="utf-8")
    loaded, digest = load_reliable_universe(path)
    assert loaded == universe() and len(digest) == 64
    path.write_text(__import__("json").dumps({"rows": universe() + [universe()[0]]}), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        load_reliable_universe(path)


def test_universe_provenance_and_date_are_recorded_and_checked():
    digest = "a" * 64
    updated, _ = update(universe(), archive(compact()), as_of="2026-09-09", universe_as_of="2026-09-09",
                        universe_source_sha256=digest, fetch_rows=raw)
    assert updated["snapshots"][-1]["universe_source_sha256"] == digest
    with pytest.raises(ValueError, match="stale or from the future"):
        update(universe(), archive(compact()), as_of="2026-09-09", universe_as_of="2026-09-03", fetch_rows=raw)
