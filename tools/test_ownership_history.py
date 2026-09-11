from datetime import date,timedelta
from types import SimpleNamespace
import copy
from workspace_public.ownership_history import backfill, distribution_point, full_point, priority_stocks


def distribution():
    return {'levels':{str(i):{'people':1,'shares':100,'percent':6.67} for i in range(1,16)},
            'adjustment':{'people':0,'shares':-10,'percent':-.05},
            'total':{'people':15,'shares':1490,'percent':100}}


class Client:
    dates=[(date(2026,7,24)+timedelta(days=i*7)).isoformat() for i in range(7)]
    last_response=SimpleNamespace(content=b'verified synthetic form')
    def __init__(self,fail_at=100):self.calls=[];self.fail_at=fail_at
    def bootstrap(self):pass
    def query(self,sid,day):
        if len(self.calls)>=self.fail_at:raise RuntimeError('temporary source failure')
        self.calls.append((sid,day));return distribution()


def test_signed_adjustment_and_all_chart_bands():
    p=distribution_point(distribution(),'2026-09-04','s')
    assert full_point(p)
    assert p['shareholders']==15 and p['major']['400']==26.68 and p['retail']['30']==40.02
    bad=distribution();bad['adjustment']['shares']=10
    import pytest
    with pytest.raises(ValueError):distribution_point(bad,'2026-09-04','s')
    assert not full_point({'major':{'400':20}})


def test_shareholder_count_accepts_integral_json_numbers_only():
    point=distribution_point(distribution(),'2026-09-04','s')
    for people in (15,15.0):
        assert full_point({**point,'shareholders':people})
    for people in (True,False,None,'15',15.5,0,-1,float('nan'),float('inf')):
        assert not full_point({**point,'shareholders':people})


def test_complete_float_counts_do_not_consume_backfill_budget(tmp_path):
    points=[distribution_point(distribution(),day,'s') for day in Client.dates]
    for point in points:point['shareholders']=float(point['shareholders'])
    result={'stocks':{'2330':{'ownership':points}}};client=Client()
    backfill(result,{},['2026-09-10'],tmp_path,watch_ids=['2330'],budget=120,client=client)
    assert not client.calls
    assert result['ownership_backfill']['missing_points']==0


def test_priority_uses_three_actual_dates_or_explicit_watch():
    days=['2026-09-08','2026-09-09','2026-09-10']
    inst={'history':[{'date':day,'rows':[{'security_id':'2330','foreign_net':1,'investment_trust_net':-1},
        {'security_id':'1101','foreign_net':-1,'investment_trust_net':1}]} for day in days]}
    assert priority_stocks(inst,days,['6488'])==['1101','2330','6488']
    inst['history'].pop(1)
    assert priority_stocks(inst,days,['6488'])==['6488']


def test_budget_persists_then_reuses_exact_dates(tmp_path):
    result={};client=Client()
    backfill(result,{},['2026-09-10'],tmp_path,watch_ids=['2330'],budget=2,client=client)
    assert len(client.calls)==2 and len(result['stocks']['2330']['ownership'])==2
    assert result['ownership_backfill']['missing_points']==5
    second={};other=Client()
    backfill(second,{},['2026-09-10'],tmp_path,watch_ids=['2330'],budget=5,client=other)
    assert len(other.calls)==5 and second['ownership_backfill']['cache_hits']==2
    assert len(second['stocks']['2330']['ownership'])==7
    again=Client()
    backfill(second,{},['2026-09-10'],tmp_path,watch_ids=['2330'],budget=120,client=again)
    assert not again.calls


def test_source_failure_keeps_successes_and_never_fabricates_missing_dates(tmp_path):
    result={};client=Client(fail_at=1)
    backfill(result,{},['2026-09-10'],tmp_path,watch_ids=['2330'],client=client)
    assert result['ownership_backfill']['status']=='interrupted'
    assert len(result['stocks']['2330']['ownership'])==1
    assert result['stocks']['2330']['ownership'][0]['date']=='2026-07-24'
    assert len(result['sources'])==1


def test_invalid_cache_identity_and_totals_are_not_trusted(tmp_path):
    import json
    (tmp_path/'2330_2026-07-24.json').write_text(json.dumps({'stock_id':'1101','date':'2026-07-24'}))
    client=Client();result={}
    backfill(result,{},['2026-09-10'],tmp_path,watch_ids=['2330'],budget=1,client=client)
    assert len(client.calls)==1 and result['ownership_backfill']['cache_hits']==0
