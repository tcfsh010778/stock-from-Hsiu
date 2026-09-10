import copy
from datetime import date,timedelta
import json
from pathlib import Path
import subprocess
import shutil
import unittest
import tempfile
import hashlib

from workspace_public.research import six_week_trend,source_evidence,TABLE,MACRO_LABELS,notification_eligible
from workspace_public.research_collect import daily_rows,industry_members,ownership_rows
from workspace_public.chart_patterns import annotate


class ResearchTests(unittest.TestCase):
    def test_asset_hash_is_stable_across_git_line_endings(self):
        from workspace_public.build import export_assets
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); source=root/'source'; output=root/'output'
            source.mkdir(); output.mkdir()
            (source/'app.js').write_bytes(b'\xef\xbb\xbfconst x=1;\r\nconst y=2;\r\n')
            hashes=export_assets(source,output,['app.js'])
            canonical=b'const x=1;\nconst y=2;\n'
            self.assertEqual((output/'app.js').read_bytes(),canonical)
            self.assertEqual(hashes['app.js'],hashlib.sha256(canonical).hexdigest())
            (source/'app.js').write_bytes(canonical)
            self.assertEqual(export_assets(source,output,['app.js']),hashes)

    def test_six_changes_need_seven_endpoints_and_retracement_allowed(self):
        rows=[{'date':(date(2026,7,24)+timedelta(days=7*i)).isoformat(),'major':{'400':v}} for i,v in enumerate([40,41,42,43,42.5,44,45])]
        self.assertEqual(six_week_trend(rows[1:],'2026-09-10')['status'],'unknown')
        r=six_week_trend(rows,'2026-09-10');self.assertTrue(r['candidate']);self.assertEqual(r['rhythm'],'進三退一片段')
        self.assertEqual(six_week_trend(rows[:3]+rows[4:],'2026-09-10')['status'],'unknown')
        self.assertEqual(six_week_trend(rows,'2026-10-10')['status'],'unknown')

    def test_full_table_preserves_distinct_review_rows(self):
        self.assertEqual(len(MACRO_LABELS),14)
        self.assertIn('專利',TABLE['C']);self.assertIn('擴廠',TABLE['C']);self.assertIn('併購',TABLE['C'])
        self.assertIn('400–600 張增加',TABLE['B1']);self.assertNotIn('評價分數',str(TABLE))

    def test_foreign_gap_breaks_streak(self):
        r=source_evidence({'series':[{'foreign_net':2},{'foreign_net':None},{'foreign_net':3}]},[],[],'2026-09-10')
        self.assertEqual(r['foreign_consecutive']['sessions'],1);self.assertFalse(r['foreign_consecutive']['candidate'])

    def test_missing_foreign_cannot_send_mda_removal(self):
        src={'holder_six_weeks':{'status':'measured'},'foreign_consecutive':{'complete':False},'margin_observation':{'latest_change_lots':-4}}
        self.assertFalse(notification_eligible({'source_labels':[],'sources':src},True,True))
        self.assertFalse(notification_eligible({'source_labels':['每日漲幅'],'sources':src},True,True))
        self.assertFalse(notification_eligible({'source_labels':['每日漲幅'],'sources':src},False,True))

    def test_margin_gap_is_not_daily_change(self):
        r=source_evidence({'series':[{'date':'2026-09-09','foreign_net':0},{'date':'2026-09-10','foreign_net':0}]},[],
                         [{'date':'2026-09-08','value':50},{'date':'2026-09-10','value':80}],'2026-09-10')
        self.assertIsNone(r['margin_observation']['latest_change_lots'])

    def test_margin_behaviour_has_joint_price_evidence_and_requires_all_sessions(self):
        days=[(date(2026,8,1)+timedelta(days=i)).isoformat() for i in range(21)]
        prices=[{'date':d,'close':100-i} for i,d in enumerate(days)]
        margin=[{'date':d,'value':100+i} for i,d in enumerate(days)]
        r=source_evidence({'series':[]},[],margin,days[-1],prices,days)['margin_observation']['windows']['20']
        self.assertTrue(r['complete']);self.assertEqual(r['change_lots'],20);self.assertEqual(r['increase_sessions'],20)
        self.assertAlmostEqual(r['price_change_pct'],-20)
        r=source_evidence({'series':[]},[],margin[1:],days[-1],prices,days)['margin_observation']['windows']['20']
        self.assertFalse(r['complete']);self.assertIsNone(r['change_lots'])

    def test_price_gap_cannot_shift_margin_window(self):
        days=['2026-09-02','2026-09-03','2026-09-04','2026-09-07','2026-09-08','2026-09-09','2026-09-10']
        margin=[{'date':d,'value':v} for d,v in zip(days,[100,200,196,190,150,130,106])]
        prices=[{'date':d,'close':100} for d in days if d!='2026-09-03']
        r=source_evidence({'series':[]},[],margin,days[-1],prices,days)['margin_observation']['windows']['5']
        self.assertFalse(r['complete']);self.assertIsNone(r['change_lots']);self.assertEqual(r['start'],'2026-09-03')

    def test_holdings_shares_to_lots_and_missing_not_zero(self):
        p={'date':'20260910','fields':['證券代號','全體外資及陸資持有股數'],'data':[['2330','1,234,500']]}
        self.assertEqual(daily_rows(json.dumps(p).encode(),'listed','foreign','2026-09-10')['2330'],1234.5)
        with self.assertRaises(ValueError):daily_rows(json.dumps(p).encode(),'listed','foreign','2026-09-09')
        p['data'][0][1]='--'
        with self.assertRaises(ValueError):daily_rows(json.dumps(p).encode(),'listed','foreign','2026-09-10')

    def test_margin_twse_repeated_header_uses_margin_not_shorts(self):
        p={'date':'20260910','tables':[{'fields':['代號','名稱','今日餘額','今日餘額'],'data':[['2330','台積電','21','99']]}]}
        self.assertEqual(daily_rows(json.dumps(p).encode(),'listed','margin','2026-09-10')['2330'],21)

    def test_industry_both_official_layouts_deduplicate(self):
        body='<div id="sc_link_D310">► 晶圓製造 (26家)</div><table id="sc_company_D310"><a href="company_basic.php?stk_code=2330">台積電</a></table>'
        result=industry_members(body.encode(),'D000','半導體','https://ic.tpex.org.tw/')
        self.assertEqual(result['2330'][0]['name'],'晶圓製造')
        body='<div id="companyList_Q500" title="冷熱軋鋼板捲"><a href="company_basic.php?stk_code=2002">中鋼</a></div>'
        result=industry_members((body*2).encode(),'Q000','鋼鐵','https://ic.tpex.org.tw/')
        self.assertEqual(len(result['2002']),1)

    def test_ownership_retail_major_and_total_people(self):
        rows=[]
        for n in range(1,18):
            rows.append({'資料日期':'20260904','證券代號':'2330  ','持股分級':n,'人數':10 if n<=15 else 0 if n==16 else 150,
                         '股數':1000 if n<=15 else 100 if n==16 else 14900,'占集保庫存數比例%':6 if n<15 else 16 if n==15 else 0 if n==16 else 100})
        r=ownership_rows(json.dumps(rows).encode(),'2026-09-10')['2330']
        self.assertEqual(r['retail'],{'30':36,'40':42,'50':48});self.assertEqual(r['major']['400'],34);self.assertEqual(r['shareholders'],150)
        with self.assertRaises(ValueError):ownership_rows(json.dumps(rows[:-1]).encode(),'2026-09-10')
        rows[-1]['股數']=15000
        with self.assertRaises(ValueError):ownership_rows(json.dumps(rows).encode(),'2026-09-10')

    def test_incomplete_candle_is_not_annotated(self):
        bars=[{'time':'2026-09-01','open':100,'close':99,'low':98,'high':101,'volume':10,'complete':True},
              {'time':'2026-09-02','open':98,'close':102,'low':97,'high':103,'volume':20,'complete':False}]
        self.assertFalse(any(e['id']=='bullish_engulfing' for e in annotate(bars)['events']))
        bars[-1]['complete']=True;events=annotate(bars)['events']
        self.assertTrue(any(e['id']=='bullish_engulfing' and e['confirmed_at']=='2026-09-02' for e in events))
        self.assertEqual(annotate(bars[:1])['events'],[e for e in events if e['end']<='2026-09-01'])

    def test_tick_profile_conservation_ties_wrong_security(self):
        node=shutil.which('node')
        if not node:self.skipTest('Node unavailable')
        file=Path(__file__).resolve().parents[1]/'workspace_public/web/profile.js'
        script="const {parseTicks,volumeProfile}=require(process.argv[1]);const assert=require('assert'); const csv='stock_id,time,price,volume\\n2330,2026-09-10 09:00:00,100.1,1000\\n2330,2026-09-10 09:00:01,101.1,1000';const t=parseTicks(csv,'2330');const p=volumeProfile(t,1);assert.equal(p.total,2000);assert.equal(p.poc.low,100);assert.equal(p.rows.reduce((s,r)=>s+r.volume,0),p.total);assert.throws(()=>parseTicks(csv,'2317'));assert.throws(()=>volumeProfile(t,0));"
        subprocess.run([node,'-e',script,str(file)],check=True,capture_output=True,text=True)


if __name__=='__main__':unittest.main()
