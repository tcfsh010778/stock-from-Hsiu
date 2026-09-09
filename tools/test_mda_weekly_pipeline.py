import json
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from mda_weekly_pipeline import build_mda, validate_pool
from build_review_data import verified_frame
from tools.test_build_review_data import write_prices


def pool():
    return {'dataset_id': 'mda_weekly_top50', 'quality': 'complete', 'status': 'ok',
            'data_date': '2026-09-04', 'previous_date': '2026-08-28',
            'paired_market_counts': {'listed': 1000, 'otc': 800},
            'rows': [{'security_id': '2330', 'rank': 1, 'market': 'listed',
                      'major_400_percent': 60, 'prior_major_400_percent': 58,
                      'delta_percentage_points': 2}]}


def test_pool_rejects_missing_market_stale_nonpositive_and_duplicates():
    valid = pool()
    assert len(validate_pool(valid, '2026-09-04')) == 1
    for change in ({'paired_market_counts': {'listed': 1000, 'otc': 0}},
                   {'previous_date': '2026-08-07'}, {'quality': 'partial'},
                   {'rows': valid['rows'] * 2}):
        with pytest.raises(ValueError):
            validate_pool({**valid, **change}, '2026-09-04')
    valid['rows'][0]['delta_percentage_points'] = 0
    with pytest.raises(ValueError):
        validate_pool(valid, '2026-09-04')


def test_unrelated_legacy_candidates_never_enter_weekly_pool(tmp_path):
    (tmp_path / 'mda_weekly_top50.json').write_text(json.dumps(pool()))
    (tmp_path / 'mda_candidates.json').write_text(json.dumps({'stocks': [{'stock_id': '9999'}]}))
    def missing(*args):
        raise ValueError('missing verified price')
    result = build_mda(tmp_path, '2026-09-04', missing, analyzer=lambda *a, **k: {})
    assert [r['stock_id'] for r in result['stocks']] == ['2330']
    assert result['stocks'][0]['weekly_pool_member']
    assert result['stocks'][0]['quality'] == 'blocked'
    assert not result['stocks'][0]['candidate']


def test_reconciled_price_mode_requires_matching_volume_provenance(tmp_path):
    dates = pd.bdate_range(end='2026-09-04', periods=250)
    frame = pd.DataFrame({'date': dates, 'open': 10, 'high': 11, 'low': 9,
                          'close': 10, 'volume': 1000})
    as_of = write_prices(tmp_path, '2330', frame)
    path = tmp_path / 'price_basis/2330.json'
    metadata = json.loads(path.read_text())
    metadata['mode'] = 'finmind_raw_reconciled_reference_ratio_back_adjusted_v1'
    path.write_text(json.dumps(metadata))
    with pytest.raises(ValueError):
        verified_frame(tmp_path, '2330', as_of)
    metadata['volume_basis'] = 'finmind_raw_shares'
    metadata['csv_sha256'] = hashlib.sha256((tmp_path / 'prices/2330.csv').read_bytes()).hexdigest()
    metadata.update(data_start=str(dates[0].date()), data_end=as_of, row_count=250,
                    available_bars=250, ma240_required_bars=240, direction_required_bars=241,
                    full_study_recommended_bars=245, history_status='complete')
    path.write_text(json.dumps(metadata))
    assert len(verified_frame(tmp_path, '2330', as_of)) == 250
    with (tmp_path / 'prices/2330.csv').open('a') as handle:
        handle.write('\n')
    with pytest.raises(ValueError, match='驗證紀錄'):
        verified_frame(tmp_path, '2330', as_of)


def test_real_checklist_admission_uses_weekly_pool_not_sfz(tmp_path):
    from stock_v2_public.analysis.mda_checklist import analyze_mda_checklist
    dates = pd.bdate_range(end='2026-09-04', periods=520)
    closes = pd.Series(range(1000, 1520), dtype=float) / 10
    frame = pd.DataFrame({'date': dates, 'open': closes, 'high': closes + 1,
                          'low': closes - 1, 'close': closes, 'volume': 10000})
    as_of = write_prices(tmp_path, '2330', frame)
    (tmp_path / 'mda_weekly_top50.json').write_text(json.dumps(pool()))
    holder_dates = pd.date_range(end=as_of, periods=27, freq='W-FRI')
    evidence = {'stock_id': '2330', 'verified': True, 'data_as_of': as_of,
                'holder_series': [{'date': str(d.date()), 'major_percent_400_plus': 50+i/10,
                                   'retail_percent_20_minus': 15-i/10} for i,d in enumerate(holder_dates)],
                'margin_series': [{'date': str(d.date()), 'margin_balance': 1000+i} for i,d in enumerate(dates)]}
    (tmp_path / 'mda_evidence').mkdir()
    (tmp_path / 'mda_evidence/2330.json').write_text(json.dumps(evidence))
    result = build_mda(tmp_path, as_of, verified_frame, analyzer=analyze_mda_checklist)
    row = result['stocks'][0]
    assert row['candidate'] and row['stage'] == 'mda_waiting'
    assert row['checklist']['checks']['long_bull_A']['status'] == 'pass'
    assert row['checklist']['short_term_review'] is None
    assert result['eligible_count'] == 1
    # A previously admitted symbol remains under review after leaving Top50.
    (tmp_path / 'mda_checklist_candidates.json').write_text(json.dumps(result))
    new_pool = pool()
    new_pool['rows'][0]['security_id'] = '2317'
    (tmp_path / 'mda_weekly_top50.json').write_text(json.dumps(new_pool))
    retained = build_mda(tmp_path, as_of, verified_frame)
    old = next(r for r in retained['stocks'] if r['stock_id'] == '2330')
    assert old['watch_pool_member'] and old['candidate']
    assert old['pool_rank'] is None and not old['weekly_pool_member']
    assert old['admitted_week'] == as_of
    assert retained['weekly_count'] == 1 and retained['retained_count'] == 1


def test_price_checks_still_visible_when_holder_provider_is_unavailable(tmp_path):
    dates = pd.bdate_range(end='2026-09-04', periods=520)
    closes = pd.Series(range(1000, 1520), dtype=float) / 10
    frame = pd.DataFrame({'date': dates, 'open': closes, 'high': closes+1,
                         'low': closes-1, 'close': closes, 'volume': 10000})
    as_of = write_prices(tmp_path, '2330', frame)
    (tmp_path / 'mda_weekly_top50.json').write_text(json.dumps(pool()))
    row = build_mda(tmp_path, as_of, verified_frame)['stocks'][0]
    assert row['checklist']['checks']['long_bull_A']['status'] == 'pass'
    assert row['checklist']['checks']['long_term_B1']['status'] == 'unknown'
    assert not row['candidate'] and row['quality'] == 'blocked'


def test_chart_uses_verified_weekly_dates_without_inventing_retail(tmp_path):
    from generate_v2 import load_market_evidence
    (tmp_path / 'mda_weekly_top50.json').write_text(json.dumps(pool()))
    evidence = load_market_evidence(tmp_path, '2330', as_of='2026-09-04')
    assert evidence['source_dates']['holdings'] == '2026-09-04'
    assert [r['major'] for r in evidence['holdings']] == [58, 60]
    assert all(r['retail'] is None and r['middle'] is None for r in evidence['holdings'])
    assert load_market_evidence(tmp_path, '2330', as_of='2026-09-03')['holdings'] == []


def test_documented_suspension_pool_requires_proven_coverage_and_excludes_missing_stock():
    from copy import deepcopy
    candidate = pool()
    event = {"security_id": "6461", "name": "益得", "market": "otc", "event_type": "reduction",
             "stop_date": "2026-09-02", "resume_date": "2026-09-09", "known_at": "2026-09-09T17:00:00Z",
             "source_url": "https://www.tpex.org.tw/www/zh-tw/bulletin/revivt", "raw_sha256": "a" * 64,
             "query_start": "2026-09-04", "query_end": "2026-09-10", "reason": "capital reduction"}
    candidate["quality"] = "partial_with_documented_exchange_suspension"
    candidate["coverage"] = {"expected_count": 1801, "observed_count": 1800,
        "expected_market_counts": {"listed": 1000, "otc": 801}, "observed_market_counts": {"listed": 1000, "otc": 800},
        "excluded_official_suspensions": [event], "previous_coverage": {
            "expected_count": 1801, "observed_count": 1801, "expected_market_counts": {"listed": 1000, "otc": 801},
            "observed_market_counts": {"listed": 1000, "otc": 801}, "excluded_official_suspensions": []}}
    assert validate_pool(candidate, "2026-09-09")[0]["security_id"] == "2330"
    bad = deepcopy(candidate); bad["coverage"]["excluded_official_suspensions"] = []
    with pytest.raises(ValueError): validate_pool(bad, "2026-09-09")
    bad = deepcopy(candidate); bad["coverage"]["excluded_official_suspensions"][0]["security_id"] = "2330"
    with pytest.raises(ValueError): validate_pool(bad, "2026-09-09")
