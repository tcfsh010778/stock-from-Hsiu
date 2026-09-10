import copy
import tempfile
import unittest
import json
from unittest.mock import patch
from urllib.error import HTTPError
from pathlib import Path

import pandas as pd

from workspace_public.model import VERSION, candles, chip_windows, growth, detect_patterns
from workspace_public.official_data import parse_revenue, institutional_rows, valid_fetch
from workspace_public.build import build, write


def test_old_stock_and_strategy_routes_redirect_to_the_new_workspace(tmp_path):
    from generate_workspace import retire_routes
    for name in ('index.html','carybot.html','backtest.html','stocks/2330.html','v2/stocks/6488.html','mda_candidates/2330.html','daily/2026-09-09.html'):
        path=tmp_path/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('legacy',encoding='utf-8')
    retire_routes(tmp_path)
    assert (tmp_path/'index.html').read_text()=='legacy'
    assert 'url=./' in (tmp_path/'carybot.html').read_text(encoding='utf-8')
    assert 'url=../?stock=2330' in (tmp_path/'stocks/2330.html').read_text(encoding='utf-8')
    assert 'url=../../?stock=6488' in (tmp_path/'v2/stocks/6488.html').read_text(encoding='utf-8')
    assert 'url=../?stock=2330' in (tmp_path/'mda_candidates/2330.html').read_text(encoding='utf-8')
    assert 'url=../' in (tmp_path/'daily/2026-09-09.html').read_text(encoding='utf-8')


def test_retired_payload_cleanup_preserves_new_workspace_and_sources(tmp_path):
    from generate_workspace import retire_routes
    for name in ('site/data/index.json','site/data/stocks/2330.json','site/data/carybot_signals.json','site/v2/data/stocks/2330.json','data/prices/2330.csv'):
        path=tmp_path/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('preserved',encoding='utf-8')
    retire_routes(tmp_path/'site')
    assert not (tmp_path/'site/data/carybot_signals.json').exists()
    assert not (tmp_path/'site/v2/data/stocks/2330.json').exists()
    for name in ('site/data/index.json','site/data/stocks/2330.json','data/prices/2330.csv'):
        assert (tmp_path/name).read_text()=='preserved'


def bars(count=260):
    return [{'date':d.strftime('%Y-%m-%d'),'open':100+i,'high':102+i,'low':99+i,'close':101+i,'volume':1000+i}
            for i,d in enumerate(pd.bdate_range('2025-01-01',periods=count))]


def snapshot(member=False,as_of='2026-09-09',eligible=True,version='v1'):
    state={'member':member,'stage':'入選' if member else '未入選','notification_eligible':eligible,'rule_version':version}
    return {'schema_version':VERSION,'data_date':as_of,'stocks':[{'stock_id':'2330','name':'台積電','sfz':state,'mda':{**state,'notification_eligible':False}}]}


class DataTests(unittest.TestCase):
    def test_growth_missing_is_not_zero(self):
        for v in [None,0,-5,'NaN']:
            self.assertIsNone(growth(10,v))
        self.assertAlmostEqual(growth(110,100),10)

    def test_full_history_before_chart_trim(self):
        rows=bars();data=candles(rows,rows[-1]['date'],limit=10)
        self.assertEqual(len(data),10)
        self.assertAlmostEqual(data[-1]['ma240'],sum(r['close'] for r in rows[-240:])/240)

    def test_partial_week_and_month_explicit(self):
        rows=bars(7);as_of=rows[-1]['date']
        self.assertFalse(candles(rows,as_of,'week')[-1]['complete'])
        self.assertFalse(candles(rows,as_of,'month')[-1]['complete'])
        self.assertEqual(candles(rows,as_of,'week')[-1]['time'],as_of)

    def test_future_is_excluded(self):
        rows=bars(80);cutoff=rows[59]['date']
        self.assertEqual(candles(rows,cutoff),candles(rows[:60],cutoff))
        self.assertEqual(detect_patterns(rows,cutoff),detect_patterns(rows[:60],cutoff))

    def test_duplicate_date_rejected(self):
        rows=bars(8)
        with self.assertRaises(ValueError):candles(rows+[rows[-1]],rows[-1]['date'])

    def test_chip_gap_not_zero_and_exact_sessions(self):
        days=[r['date'] for r in bars(10)]
        history=[{'date':d,'rows':[{'security_id':'2330','foreign_net':1,'investment_trust_net':2,'dealer_net':3,'institutional_total_net':6}]} for d in days]
        result=chip_windows(history,days,'2330')
        self.assertEqual(result['windows']['10']['values']['institutional_total_net'],60)
        result=chip_windows(history[1:],days,'2330')
        self.assertIsNone(result['windows']['10']['values']['foreign_net'])
        self.assertTrue(result['windows']['5']['complete'])

    def test_chip_missing_dealer_never_complete(self):
        days=[r['date'] for r in bars(5)]
        history=[{'date':d,'rows':[{'security_id':'2330','foreign_net':0,'investment_trust_net':0}]} for d in days]
        window=chip_windows(history,days,'2330')['windows']['5']
        self.assertFalse(window['complete']);self.assertEqual(window['values']['foreign_net'],0)

    def test_revenue_official_table(self):
        html='<table><tr><th>公司代號</th><th>當月營收</th><th>上月營收</th><th>去年當月營收</th></tr><tr><td>2330</td><td>110</td><td>100</td><td>0</td></tr></table>'
        result=parse_revenue(html.encode('big5'),'listed','2026-08','https://mopsov.twse.com.tw/test','2026-09-10')[0]
        self.assertAlmostEqual(result['mom_pct'],10);self.assertIsNone(result['yoy_pct'])

    def test_missing_institutional_fields_rejected(self):
        raw=json.dumps({'date':'20260909','fields':['證券代號','證券名稱'],'data':[['2330','台積電']]}).encode()
        with self.assertRaises(ValueError):institutional_rows(raw,'listed','2026-09-09',None)

    def test_bad_cache_can_recover(self):
        import hashlib
        with tempfile.TemporaryDirectory() as folder:
            url='https://example.invalid/data'
            path=Path(folder)/(hashlib.sha256(url.encode()).hexdigest()+'.raw')
            path.write_bytes(b'bad')
            def validator(raw):
                if raw==b'bad':raise ValueError('not published')
                return 'valid'
            with patch('workspace_public.official_data.fetch',side_effect=[b'bad',b'good']) as fetch:
                self.assertEqual(valid_fetch(url,folder,validator),(b'good','valid'))
                self.assertEqual(fetch.call_count,2)

    def test_roster_preserved_and_latest_chip_gap(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);data=root/'source/data';supplement=root/'supplement';output=root/'preview'
            rows=bars(10);as_of=rows[-1]['date'];week='2025-01-10'
            write(data/'official_adjusted_update_manifest.json',{'data_as_of':as_of})
            write(data/'stock_markets.json',{'markets':{'2330':'上市','1589':'上市'},'stocks':{'2330':{'name':'台積電'},'1589':{'name':'永冠-KY'}}})
            write(data/'daily_market_flow.json',{'institutional_history':{'snapshots':[{'date':r['date']} for r in rows[:-1]]}})
            write(data/'mda_weekly_top50.json',{'status':'ok','data_date':week,'rows':[{'security_id':'1589','name':'永冠-KY'}]})
            fields={'foreign_net':1,'investment_trust_net':2,'dealer_net':3,'institutional_total_net':6,'security_id':'1589'}
            write(supplement/'institutional.json',{'history':[{'date':r['date'],'rows':[fields]} for r in rows[:-1]]})
            result=build(root/'source',output,lambda *a:pd.DataFrame(rows),lambda *a:None,lambda **k:as_of,supplement)
            self.assertEqual({s['stock_id'] for s in result['stocks']},{'2330','1589'})
            stock=next(s for s in result['stocks'] if s['stock_id']=='1589')
            self.assertEqual(stock['name'],'永冠-KY');self.assertFalse(stock['price_verified'])
            self.assertFalse(stock['chips']['10']['complete']);self.assertEqual(stock['chips']['10']['end'],as_of)
