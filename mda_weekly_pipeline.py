"""Weekly ownership Top50 -> independent, evidence-backed MDA checklist.

The weekly ranking is an input universe, not a buy signal. Original-table
observations remain distinct from engineering candidates and manual checks.
"""
from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

RULE_VERSION = 'mda-weekly-top50-checklist-v2'
SOURCE_SHA = '6a45a1038c687f2abf96627138587445c9703e60'


def load_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else {}


def validate_pool(pool: dict, as_of: str) -> list[dict]:
    if (pool.get('dataset_id') != 'mda_weekly_top50'
            or pool.get('quality') != 'complete' or pool.get('status') != 'ok'):
        raise ValueError('每週全市場大戶增加 Top 50 尚未驗證')
    current = date.fromisoformat(pool['data_date'])
    prior = date.fromisoformat(pool['previous_date'])
    if not 4 <= (current - prior).days <= 10 or not 0 <= (date.fromisoformat(as_of) - current).days <= 7:
        raise ValueError('股權兩期日期不連續或資料過期')
    counts = pool.get('paired_market_counts') or {}
    if any(not isinstance(counts.get(m), int) or counts[m] <= 0 for m in ('listed', 'otc')):
        raise ValueError('Top 50 缺上市或上櫃比較母體')
    rows = pool.get('rows') or []
    ids = [str(r.get('security_id', '')) for r in rows]
    if not rows or len(rows) > 50 or len(ids) != len(set(ids)):
        raise ValueError('Top 50 筆數或代碼不正確')
    for rank, row in enumerate(rows, 1):
        sid = str(row['security_id'])
        if (len(sid) != 4 or not sid.isdigit() or sid.startswith('0')
                or row.get('rank') != rank or row.get('market') not in {'listed', 'otc'}):
            raise ValueError('Top 50 代碼、排名或市場不正確')
        delta = float(row['delta_percentage_points'])
        if not 0 < delta <= 100:
            raise ValueError('Top 50 只接受持股比率正增加')
        current_pct, prior_pct = float(row['major_400_percent']), float(row['prior_major_400_percent'])
        if not (0 <= current_pct <= 100 and 0 <= prior_pct <= 100) or not math.isclose(current_pct-prior_pct, delta, abs_tol=0.011):
            raise ValueError('Top 50 增幅與兩期比例不一致')
    if rows != sorted(rows, key=lambda r: (-float(r['delta_percentage_points']), str(r['security_id']))):
        raise ValueError('Top 50 未依每週持股百分點增幅排序')
    return rows


def build_mda(data: Path, as_of: str, verified_frame: Callable, analyzer=None) -> dict:
    if analyzer is None:
        from stock_v2_public.analysis.mda_checklist import analyze_mda_checklist
        analyzer = analyze_mda_checklist
    pool = load_json(data / 'mda_weekly_top50.json')
    result = {'dataset_id': 'mda_candidate_pool', 'schema_version': '2.0.0',
              'rule_version': RULE_VERSION, 'data_date': as_of, 'date': as_of,
              'private_source_sha': SOURCE_SHA,
              'pool_date': pool.get('data_date'), 'quality': 'blocked',
              'price_verified': False, 'stocks': [], 'eligible_count': 0,
              'selection_source': 'mda_weekly_top50', 'pool_verified': False,
              'source': 'weekly positive 400+ holder percentage-point Top50; independent of SFZ'}
    try:
        rows = validate_pool(pool, as_of)
    except (KeyError, TypeError, ValueError) as exc:
        result['missing'] = [str(exc)]
        return result
    result['pool_verified'] = True
    previous = load_json(data / 'mda_checklist_candidates.json')
    retained = {}
    if previous.get('selection_source') == 'mda_weekly_top50' and previous.get('dataset_id') == 'mda_candidate_pool':
        if str(previous.get('data_date') or '') > as_of:
            raise ValueError('觀察池基準晚於本期日期')
        previous_rows = previous.get('stocks', [])
        if not isinstance(previous_rows, list) or len({r['stock_id'] for r in previous_rows}) != len(previous_rows):
            raise ValueError('既有觀察池格式或代碼重複')
        if any(str(r.get('data_date') or '') > as_of for r in previous_rows):
            raise ValueError('既有觀察池包含未來資料')
        retained = {r['stock_id']: r for r in previous_rows
                    if r.get('candidate') or r.get('watch_pool_member')}
    current_ids = {r['security_id'] for r in rows}
    rows = [dict(r, current_week=True) for r in rows]
    for sid, old in retained.items():
        if len(sid) != 4 or not sid.isdigit() or sid.startswith('0'):
            raise ValueError('既有觀察池代碼不正確')
        if sid not in current_ids:
            rows.append({'security_id': sid, 'name': old.get('name'), 'rank': None,
                         'current_week': False, 'delta_percentage_points': None})
    result['weekly_count'] = len(current_ids)
    result['retained_count'] = len(set(retained) - current_ids)
    for source in rows:
        sid = str(source['security_id'])
        is_weekly = source['current_week']
        row = {'stock_id': sid, 'name': source.get('name') or sid,
               'data_date': as_of, 'quality': 'blocked', 'candidate': False,
               'stage': 'weekly_review', 'missing': [], 'conflicts': [],
               'pool_rank': source['rank'], 'weekly_pool_member': is_weekly,
               'watch_pool_member': sid in retained,
               'admitted_week': retained.get(sid, {}).get('admitted_week'),
               'reasons': ([f"本週大戶增加第 {source['rank']} 名（+{source['delta_percentage_points']} 百分點）"] if is_weekly else ['先前通過檢核，持續追蹤長期型態與籌碼']),
               'next_observation': '先核對 A／X、B1 與 B2；發動後才展開短線位階。',
               'evidence': [{'id': 'holder_weekly_pool', 'data_date': pool['data_date'],
                             'summary': '本週 400 張以上大戶持股增加',
                             'metrics': {'holder_delta_pctpt': source['delta_percentage_points']}}]}
        try:
            prices = verified_frame(data, sid, as_of)
            evidence = load_json(data / 'mda_evidence' / f'{sid}.json')
            aux_valid = evidence.get('stock_id') == sid and evidence.get('verified') is True and evidence.get('data_as_of') == as_of
            holders = (pd.DataFrame(evidence['holder_series']) if aux_valid else
                       pd.DataFrame(columns=['date', 'major_percent_400_plus']))
            margin = (pd.DataFrame(evidence['margin_series']) if aux_valid else
                      pd.DataFrame(columns=['date', 'margin_balance']))
            analysis = analyzer(prices, holders, margin, stock_id=sid, as_of=as_of,
                                pool_provenance={**source, 'week_end': pool['data_date'],
                                                 'week_start': pool['previous_date']}, activated=False)
            from stock_v2_public.analysis.sfz_review import analyze_sfz
            timing = analyze_sfz(prices, stock_id=sid, as_of=as_of)
            activated = timing['stage'] in {'breakout_wait_retest', 'retest_confirmed'}
            analysis['activation'] = {'status': 'activated' if activated else 'not_started',
                                      'source': 'independent_daily_breakout_observation',
                                      'rule_version': timing['rule_version'], 'stage': timing['stage']}
            analysis['short_term_review'] = ({'timeframe': 'daily', 'data_date': as_of,
                'status': 'human_review', 'stage': timing['stage'], 'evidence': timing['evidence'],
                'note': '發動後的日線突破／回測觀察，尚未以分鐘圖確認；不自動下單。'} if activated else None)
            row['checklist'] = analysis
            row['conflicts'] = analysis.get('conflicts', [])
            row['missing'] = analysis.get('missing', [])
            if not aux_valid:
                row['missing'] = list(dict.fromkeys([*row['missing'], '長期股權／融資資料尚未驗證']))
            checks = analysis['checks']
            pattern = checks['familiar_pattern_A_or_X']['status']
            b1 = checks['long_term_B1']['status']
            b2 = checks['selling_pressure_B2']['status']
            # Engineering observations qualify for human review, never an order.
            row['candidate'] = pattern in {'pass', 'candidate'} and b1 in {'pass', 'candidate'}
            if row['candidate']:
                row['watch_pool_member'] = True
                row['admitted_week'] = row['admitted_week'] or pool['data_date']
            row['stage'] = ('mda_ready_review' if b2 in {'pass', 'candidate'} else 'mda_waiting') if row['candidate'] else 'weekly_review'
            if sid in retained and (pattern == 'fail' or b1 == 'fail'):
                row['stage'] = 'invalidated'
            row['quality'] = 'fresh' if not row['missing'] else 'blocked'
            for name in ('long_bull_A', 'reversal_X', 'long_term_B1', 'chip_price_relation', 'higher_lows_daily', 'selling_pressure_B2'):
                check = checks[name]
                row['evidence'].append({'id': name, 'data_date': as_of, 'summary': check['summary'],
                                        'metrics': {**check['metrics'], 'check_status': check['status']}})
            if row['quality'] == 'fresh':
                result['eligible_count'] += 1
        except (ValueError, KeyError, OSError, TypeError) as exc:
            row['missing'].append(str(exc)[:240])
        result['stocks'].append(row)
    result['quality'] = 'fresh' if result['eligible_count'] else 'blocked'
    result['price_verified'] = result['eligible_count'] > 0
    return result
