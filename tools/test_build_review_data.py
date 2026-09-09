import json
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

from build_review_data import build_sfz, verified_frame, prepare_mda, expected_session, PRICE_MODE
from tools.test_sfz_review import _qualifying_frame


def write_prices(root, sid, frame):
    frame = frame.copy()
    frame['date'] = pd.to_datetime(frame['date']).dt.strftime('%Y-%m-%d')
    for key in ('open', 'high', 'low', 'close', 'volume'):
        frame['raw_' + key] = frame[key]
    frame['adjustment_factor'] = 1.0
    (root / 'prices').mkdir(exist_ok=True)
    (root / 'price_basis').mkdir(exist_ok=True)
    frame.to_csv(root / 'prices' / f'{sid}.csv', index=False)
    as_of = frame['date'].iloc[-1]
    metadata = {'stock_id': sid, 'mode': PRICE_MODE, 'verified': True,
                'volume_basis': 'official_raw_shares', 'adjustment_as_of': as_of}
    (root / 'price_basis' / f'{sid}.json').write_text(json.dumps(metadata))
    return as_of


class PipelineTests(unittest.TestCase):
    def test_sfz_uses_prices_not_mda_and_excludes_unknown_basis(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            as_of = write_prices(root, '9999', _qualifying_frame())
            (root / 'mda_candidates.json').write_text(json.dumps({'stocks': []}))
            (root / 'prices/8888.csv').write_text('date,close\n2025-01-01,10\n')
            from stock_v2_public.analysis.sfz_review import analyze_sfz
            result = build_sfz(root, as_of, analyzer=analyze_sfz)
            self.assertEqual(result['universe_count'], 2)
            self.assertEqual([r['stock_id'] for r in result['stocks']], ['9999'])
            self.assertTrue(result['stocks'][0]['candidate'])
            self.assertEqual(result['excluded'][0]['stock_id'], '8888')
            self.assertEqual(json.loads((root / 'mda_candidates.json').read_text()), {'stocks': []})

    def test_mda_verification_is_independent_of_sfz_results(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            frame = pd.DataFrame({'date': pd.bdate_range('2025-01-01', periods=250),
                                  'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000})
            as_of = write_prices(root, '1111', frame)
            # No SFZ result file, and a flat frame would not qualify technically.
            (root / 'mda_candidates.json').write_text(json.dumps({'dataset_id':'mda_candidate_pool', 'date':as_of,
                'stocks':[{'stock_id':'1111', 'basket':'未發動觀察', 'reason':'原始 B1/B2 條件'}]}))
            (root / 'mda_universe_scan.json').write_text(json.dumps([{'stock_id':'1111','date':as_of,
                'major_accumulating':True,'retail_or_people_support':True}]))
            (root / 'holding_shares').mkdir()
            records = [{'date':d,'stock_id':'1111','HoldingSharesLevel':str(level), 'percent':5,'people':100,'unit':1000}
                       for d in pd.date_range(end=as_of,periods=9,freq='7D').strftime('%Y-%m-%d') for level in range(15)]
            pd.DataFrame(records).to_csv(root/'holding_shares/1111.csv',index=False)
            result = prepare_mda(root, as_of)
            self.assertEqual(result['eligible_count'], 1)
            self.assertTrue(result['stocks'][0]['candidate'])
            self.assertEqual(result['stocks'][0]['basket'], '未發動觀察')

    def test_mixed_or_stale_prices_do_not_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); as_of = write_prices(root, '9999', _qualifying_frame())
            p = root / 'prices/9999.csv'; frame = pd.read_csv(p); frame.loc[0,'close'] += 2;frame.to_csv(p,index=False)
            with self.assertRaisesRegex(ValueError, '不一致'):
                verified_frame(root,'9999',as_of)
            with self.assertRaises(ValueError):
                verified_frame(root,'9999','2099-01-01')

    def test_weekend_reference_is_disclosed_weekday_fallback(self):
        self.assertEqual(expected_session(datetime(2026,9,6,20,tzinfo=timezone(timedelta(hours=8)))), '2026-09-04')


if __name__ == '__main__':
    unittest.main()


def test_mixed_official_increment_contract_requires_hash_and_updated_coverage(tmp_path):
    from tools.test_public_history_contract import fixture
    fixture(tmp_path, 245)
    meta_path = tmp_path / 'price_basis/9001.json'
    meta = json.loads(meta_path.read_text(encoding='utf-8'))
    meta.update(mode='reference_ratio_back_adjusted_mixed_sources_v1', volume_basis='raw_shares')
    meta_path.write_text(json.dumps(meta), encoding='utf-8')
    assert len(verified_frame(tmp_path, '9001', '2026-09-04')) == 245
    meta['available_bars'] = 244
    meta_path.write_text(json.dumps(meta), encoding='utf-8')
    import pytest
    with pytest.raises(ValueError, match='驗證紀錄'):
        verified_frame(tmp_path, '9001', '2026-09-04')
    meta['available_bars'] = 245
    meta_path.write_text(json.dumps(meta), encoding='utf-8')
    with (tmp_path / 'prices/9001.csv').open('ab') as handle:
        handle.write(b'\n')
    with pytest.raises(ValueError, match='驗證紀錄'):
        verified_frame(tmp_path, '9001', '2026-09-04')


def test_official_holiday_manifest_overrides_weekday_fallback(tmp_path):
    import json
    from datetime import datetime, timezone, timedelta
    from build_review_data import expected_session
    now = datetime(2026, 9, 9, 18, tzinfo=timezone(timedelta(hours=8)))
    manifest = {"dataset_id": "official_adjusted_daily_update", "status": "current",
                "calendar_basis": "official_twse_tpex", "calendar_as_of": "2026-09-09",
                "expected_completed_session": "2026-09-08", "data_as_of": "2026-09-08",
                "official_sessions_sha256": "a"*64, "generated_at": now.isoformat()}
    path=tmp_path/'official_adjusted_update_manifest.json'
    path.write_text(json.dumps(manifest),encoding='utf-8')
    assert expected_session(now,data_dir=tmp_path)=='2026-09-08'
    manifest['calendar_as_of']='2026-09-08'
    path.write_text(json.dumps(manifest),encoding='utf-8')
    assert expected_session(now,data_dir=tmp_path)=='2026-09-09'
