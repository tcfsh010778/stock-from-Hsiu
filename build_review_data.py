"""Build independent technical observations and the human review queue.

No provider calls, order logic, legacy screener mutations or AI assertions.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone, date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
PRICE_MODE = 'official_reference_ratio_back_adjusted_v1'
SFZ_SOURCE_SHA = '1a6c8387bc121e93f063439807f774cbe66e5a4b'


def read_json(path: Path, default=None):
    if not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding='utf-8-sig'))


def expected_session(now=None):
    """Conservative weekday fallback; public output discloses calendar basis."""
    now = now or datetime.now(timezone(timedelta(hours=8)))
    day = now.date()
    if now.hour < 16:
        day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day.isoformat()


def verified_frame(data: Path, stock_id: str, as_of: str) -> pd.DataFrame:
    basis = read_json(data / 'price_basis' / f'{stock_id}.json')
    if (basis.get('stock_id') != stock_id or basis.get('mode') != PRICE_MODE
            or basis.get('verified') is not True
            or basis.get('volume_basis') != 'official_raw_shares'
            or basis.get('adjustment_as_of') != as_of):
        raise ValueError('缺少當期已驗證的還原價／原始成交量資料')
    frame = pd.read_csv(data / 'prices' / f'{stock_id}.csv', dtype={'date': str})
    fields = ['open', 'high', 'low', 'close', 'volume']
    required = ['date', 'adjustment_factor', *fields, *['raw_' + k for k in fields]]
    if any(k not in frame for k in required) or frame.empty:
        raise ValueError('歷史資料欄位或觀察值不足')
    dates = frame['date'].tolist()
    if any(date.fromisoformat(d).isoformat() != d for d in dates):
        raise ValueError('日期格式不正確')
    if dates != sorted(set(dates)) or dates[-1] != as_of:
        raise ValueError('價格日期未對齊或重複')
    for k in required[1:]:
        frame[k] = pd.to_numeric(frame[k], errors='raise')
        if not frame[k].map(math.isfinite).all():
            raise ValueError('價格或成交量包含無效數值')
    factor = frame['adjustment_factor']
    if (factor <= 0).any() or not math.isclose(float(factor.iloc[-1]), 1, abs_tol=1e-12):
        raise ValueError('還原因子未驗證')
    for key in fields[:-1]:
        actual = frame[key]
        expected = frame['raw_' + key] * factor
        if (actual <= 0).any() or ((actual - expected).abs() > expected.abs() * 1e-8 + 1e-7).any():
            raise ValueError('原始價與還原價不一致')
    if (frame['volume'] < 0).any() or not (frame['volume'] == frame['raw_volume']).all():
        raise ValueError('成交量口徑不一致')
    if ((frame['high'] < frame[['open', 'low', 'close']].max(axis=1)).any()
            or (frame['low'] > frame[['open', 'close']].min(axis=1)).any()):
        raise ValueError('OHLC 關係不正確')
    return frame


def build_sfz(data: Path, as_of: str, analyzer=None):
    if analyzer is None:
        from stock_v2_public.analysis.sfz_review import analyze_sfz
        analyzer = analyze_sfz
    markets = read_json(data / 'stock_markets.json').get('markets', {})
    names = {}
    # Names are labels only, never an eligibility gate.
    for report in read_json(data / 'site_reports.json', []):
        for stock in report.get('stocks', []):
            names[str(stock.get('id', ''))] = stock.get('name', '')
    paths = [p for p in sorted((data / 'prices').glob('*.csv'))
             if len(p.stem) == 4 and p.stem.isdigit() and not p.stem.startswith('0')]
    results, missing = [], []
    for path in paths:
        sid = path.stem
        try:
            frame = verified_frame(data, sid, as_of)
            result = analyzer(frame, stock_id=sid, as_of=as_of)
            result.update(name=names.get(sid, sid), market=markets.get(sid, ''))
            result['quality'] = 'blocked' if result.get('missing') else 'fresh'
            results.append(result)
        except (ValueError, KeyError, OSError, TypeError) as exc:
            # Only known validation failures become source gaps. Coding defects surface.
            missing.append({'stock_id': sid, 'reason': str(exc)[:180]})
    return {
        'dataset_id': 'sfz_technical_candidates', 'schema_version': '1.0.0',
        'private_source_sha': SFZ_SOURCE_SHA,
        'rule_version': results[0]['rule_version'] if results else 'sfz-technical-review-candidate-v1.0.0',
        'data_date': as_of, 'date': as_of, 'quality': 'fresh' if any(r['quality'] == 'fresh' for r in results) else 'blocked',
        'price_verified': bool(results),
        'price_basis': {'mode': PRICE_MODE, 'verified': bool(results), 'as_of': as_of,
                        'volume_basis': 'official_raw_shares'},
        'source': 'independent verified price histories; no MDA eligibility filter',
        'evaluated_count': len(results), 'universe_count': len(paths),
        'candidate_count': sum(bool(r.get('candidate')) for r in results),
        'stocks': results, 'excluded': missing,
        'notes': ['教學概念的工程化觀察，非教材完整複製或 AI 判讀',
                  '缺少已驗證資料的股票不參與判讀；不以 M 大候選池限制股票池'],
    }


def prepare_mda(data: Path, as_of: str):
    payload = read_json(data / 'mda_candidates.json')
    payload['rule_version'] = 'legacy_mda_candidate_pool_v1'
    scan = {str(r.get('stock_id')): r for r in read_json(data / 'mda_universe_scan.json', [])}
    source_date = str(payload.get('date') or '')
    eligible = 0
    for row in payload.get('stocks', []):
        sid = str(row.get('stock_id') or row.get('security_id') or '')
        row['candidate'] = True  # Membership comes only from the unchanged MDA pool.
        row['quality'] = 'blocked'
        row['missing'] = []
        row['next_observation'] = '核對 A 長期結構、B1 長期籌碼是否仍在，以及 B2 賣壓變化；保留原觀察階段。'
        if source_date != as_of:
            row['missing'].append('M 大名單日期／還原價尚未通過本期驗證')
        source = scan.get(sid, {})
        row['evidence'] = [{'id': 'mda_original', 'summary': row.get('reason', ''),
                            'data_date': source_date, 'metrics': {}}]
        try:
            price = verified_frame(data, sid, as_of)
            if len(price) < 241:
                raise ValueError('MA240 及方向需要足夠歷史')
            holder = pd.read_csv(data / 'holding_shares' / f'{sid}.csv', dtype={'date': str})
            required = {'date', 'stock_id', 'HoldingSharesLevel', 'people', 'percent', 'unit'}
            if not required <= set(holder) or not (holder['stock_id'].astype(str) == sid).all():
                raise ValueError('週籌碼欄位／股票識別不符')
            for key in ('people', 'percent', 'unit'):
                values = pd.to_numeric(holder[key], errors='raise')
                if not values.map(math.isfinite).all() or (values < 0).any():
                    raise ValueError('週籌碼含無效數值')
            if holder.duplicated(['date', 'HoldingSharesLevel']).any() or (holder['percent'] > 100).any():
                raise ValueError('週籌碼層級重複或比例異常')
            dates = sorted(set(d for d in holder['date'] if d <= source_date))
            if len(dates) < 9 or (date.fromisoformat(as_of) - date.fromisoformat(dates[-1])).days > 10:
                raise ValueError('長期週籌碼不足或尚未更新')
            intervals = [(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in zip(dates[-9:-1], dates[-8:])]
            if any(gap < 4 or gap > 10 for gap in intervals):
                raise ValueError('長期週籌碼存在缺週或混合頻率')
            if any(holder.loc[holder['date'] == d, 'HoldingSharesLevel'].nunique() < 15 for d in dates[-9:]):
                raise ValueError('週籌碼分級資料不完整')
            if source.get('date') != source_date:
                raise ValueError('M 大掃描與名單日期不一致')
            # Stable original condition flags, not a new short-horizon score.
            flags = {key: source[key] for key in ('major_accumulating', 'retail_or_people_support')
                     if isinstance(source.get(key), bool)}
            if len(flags) != 2:
                raise ValueError('長期籌碼條件欄位缺漏')
            row['evidence'].append({'id': 'holder_structure', 'data_date': dates[-1],
                                    'summary': '既有四／八週股權結構條件（holding_shares 快取）', 'metrics': flags})
        except (ValueError, KeyError, OSError):
            row['missing'].append('長期籌碼／MA240 證據尚未完整驗證')
        if not row['missing']:
            row['quality'] = 'fresh'
            eligible += 1
    payload['quality'] = 'fresh' if eligible else 'blocked'
    payload['price_verified'] = bool(eligible)
    payload['price_basis'] = {'mode': PRICE_MODE, 'verified': bool(eligible), 'as_of': as_of,
                             'volume_basis': 'official_raw_shares'}
    payload['eligible_count'] = eligible
    payload['notes'] = ['保留既有 M 大條件與階段，技術與長期籌碼來源分別驗證']
    return payload


def atomic_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False,
                                    separators=(',', ':')) + '\n', encoding='utf-8')
    temporary.replace(path)


def build(data: Path, as_of: str):
    from review_queue import build_review_queue
    previous = read_json(data / 'review_queue.json', None)
    sfz = build_sfz(data, as_of)
    mda = prepare_mda(data, as_of)
    mda_date = str(mda.get('date') or '')
    mda_ids = {str(r.get('stock_id') or r.get('security_id')) for r in mda.get('stocks', [])}
    queue = build_review_queue(sfz, mda, previous=previous or None, as_of=as_of)
    for card in queue['stocks']:
        sid = card['stock_id']
        if sid.isdigit() and (ROOT / 'docs/v2/data' / f'{sid}.json').exists():
            card['detail_href'] = f'v2/stock.html?id={sid}'
    queue['calendar_basis'] = 'weekday_after_16_taipei_fallback; exchange holidays not inferred'
    queue['source_summary'] = {
        'sfz': {'status': sfz['quality'], 'data_date': as_of if sfz['evaluated_count'] else None,
                'evaluated_count': sfz['evaluated_count'], 'universe_count': sfz['universe_count'],
                'excluded_count': len(sfz['excluded'])},
        'mda': {'status': mda['quality'], 'data_date': mda_date, 'candidate_count': len(mda_ids),
                'eligible_count': mda['eligible_count']},
    }
    atomic_json(data / 'sfz_technical_candidates.json', sfz)
    atomic_json(data / 'review_queue.json', queue)
    return queue


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=Path, default=ROOT / 'data')
    parser.add_argument('--as-of', default=expected_session())
    args = parser.parse_args()
    date.fromisoformat(args.as_of)
    result = build(args.data_dir, args.as_of)
    print(json.dumps({'as_of': args.as_of, 'sources': result['source_summary']}, ensure_ascii=False))
