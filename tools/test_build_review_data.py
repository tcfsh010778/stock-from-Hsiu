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
            result = build_sfz(root, as_of)
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
