import copy
import json

import numpy as np
import pandas as pd
import pytest

from workspace_public.mda_conditions import both, either, price_conditions
from workspace_public.research import checklist, assign_sources, notification_eligible


def bars(n=500, values=None):
    close=np.array(values if values is not None else np.linspace(100,150,n), dtype=float)
    dates=pd.bdate_range(end='2026-09-10',periods=len(close))
    return pd.DataFrame({'date':dates,'open':close,'close':close,'high':close+1,'low':close-1,
                         'volume':np.full(len(close),100000.)})


def x_bars():
    values=np.interp(np.arange(100),[0,15,30,45,55,70,90,99],[105,90,100,80,94,86,100,103])
    return bars(values=values)


def table_rows(table):
    return {r['id']:r for s in table['sections'] for r in s['rows']}


def src(holder=True):
    return {'holder_six_weeks':{'status':'measured','candidate':holder},
            'foreign_consecutive':{'candidate':True,'decision':True},
            'trust_consecutive':{'candidate':False,'decision':False}}


def test_boolean_unknown_gates():
    assert both(False,None) is False
    assert both(True,None) is None
    assert either(True,None) is True
    assert either(False,None) is None


def test_a_can_admit_without_x_and_without_all_table_rows_passing():
    t=checklist(bars(), '2026-09-10', src())
    assert t['familiar_pattern']['A']['decision'] is True
    assert t['familiar_pattern']['X']['decision'] is None
    assert t['familiar_pattern']['decision'] is True
    assert table_rows(t)['C:2']['decision'] is None
    assert t['qualified'] is None


def test_x_uses_confirmed_structure_without_legacy_magnitude_thresholds():
    t=checklist(x_bars(),'2026-09-10',src())
    x=t['familiar_pattern']['X']
    assert x['decision'] is True
    assert t['familiar_pattern']['A']['decision'] is None
    assert t['familiar_pattern']['decision'] is True
    assert all(p['confirmed_at']<='2026-09-10' for p in x['metrics']['pivots'])
    assert 'required_prior_decline_pct' not in x['metrics']


def test_x_is_invalidated_before_a_new_low_can_be_confirmed():
    f=x_bars(); f.loc[len(f)-1,['open','high','low','close']]=[70,71,69,70]
    assert checklist(f,'2026-09-10',src())['familiar_pattern']['X']['decision'] is False


def test_future_bars_never_change_current_condition_results():
    f=x_bars(); later=bars(values=[999,1000]);later['date']=pd.bdate_range('2026-09-11',periods=2)
    before=checklist(f,'2026-09-10',src())
    after=checklist(pd.concat([f,later]),'2026-09-10',src())
    assert before==after
    prefix=f.iloc[:73]
    _,_,g=price_conditions(prefix,str(prefix.date.iloc[-1].date()))
    assert len(g['X']['metrics']['pivots'])<3


def test_deduction_is_removed_bar_not_the_oldest_bar_still_in_average():
    f=bars(241); f.loc[0,['open','high','low','close']]=[200,201,199,200]
    r=table_rows(checklist(f,'2026-09-10',src()))['A乙:2']
    assert r['metrics']['deduction_close']==200
    assert r['decision'] is False


def test_composite_one_year_high_does_not_pass_on_ma_alone():
    t=checklist(bars(300),'2026-09-10',src())
    assert t['familiar_pattern']['A']['decision'] is True
    assert table_rows(t)['A甲:2']['decision'] is None


def test_band_changes_subtract_cumulative_groups_and_missing_stays_unknown():
    research={'ownership':[{'date':d,'major':m,'retail':{'30':10},'shareholders':100} for d,m in [
        ('2026-08-28',{'400':50,'600':40,'800':30,'1000':20}),
        ('2026-09-04',{'400':51,'600':43,'800':35,'1000':26})]]}
    result=table_rows(checklist(bars(),'2026-09-10',src(),research=research))
    assert result['B1:5']['metrics']['delta_percentage_points']==6
    assert result['B1:6']['metrics']['delta_percentage_points']==-1
    assert result['B1:7']['metrics']['delta_percentage_points']==-2
    assert result['B1:8']['metrics']['delta_percentage_points']==-2
    assert result['B1:5']['decision'] is True
    assert result['B1:6']['decision'] is False
    del research['ownership'][0]['major']['800']
    assert table_rows(checklist(bars(),'2026-09-10',src(),research=research))['B1:6']['decision'] is None


def test_unchanged_band_cannot_pass_from_binary_float_subtraction_noise():
    research={'ownership':[{'date':d,'major':{'600':a,'800':b}} for d,a,b in [
        ('2026-08-28',50.01,30.01),('2026-09-04',50.02,30.02)]]}
    row=table_rows(checklist(bars(),'2026-09-10',src(),research=research))['B1:7']
    assert row['metrics']['delta_percentage_points']==0
    assert row['decision'] is False


def test_trust_inventory_field_and_shared_market_calendar():
    days=[str(d.date()) for d in pd.bdate_range(end='2026-09-10',periods=22)]
    source={**src(),'market_sessions':days[-21:]}
    research={'foreign':[{'date':d,'value':1000-i} for i,d in enumerate(days)],
              'trust_holdings':[{'date':d,'value':1000+i} for i,d in enumerate(days)]}
    result=table_rows(checklist(bars(),'2026-09-10',source,research=research))['B1:1']
    assert result['decision'] is True
    assert result['metrics']['trust']['observed']==21
    assert result['metrics']['trust']['start']==days[-21]
    no_price=checklist(None,'2026-09-10',source,research=research)
    assert table_rows(no_price)['B1:1']['decision'] is True
    assert no_price['familiar_pattern']['decision'] is None
    research['trust_holdings']=[r for r in research['trust_holdings'] if r['date']!='2026-09-03']
    f=bars();f=f[f.date!=pd.Timestamp('2026-09-03')]
    result=table_rows(checklist(f,'2026-09-10',source,research=research))['B1:1']
    assert result['decision'] is None
    assert result['metrics']['trust']['observed']==20
    assert table_rows(checklist(f,'2026-09-10',src(),research=research))['B1:1']['decision'] is None


def test_identity_profit_rationality_are_not_inferred_or_forced_manual():
    t=checklist(bars(),'2026-09-10',src())
    for key in ('B1:2','B1:3','B2:4','B2:11'):
        assert table_rows(t)[key]['decision'] is None
    assert len(table_rows(t))==48
    assert all(r['status'] in {'pass','fail','unknown'} and r['method'] for r in table_rows(t).values())
    assert not any(k in json.dumps(t) for k in ('"score"','"weight"','"support_price"','"stop_loss"'))


@pytest.mark.parametrize('gate,holder,expected',[(True,True,True),(False,True,False),(None,True,None),(True,False,False)])
def test_observation_membership_requires_intersection_and_a_or_x(gate,holder,expected):
    stock={'stock_id':'2330','change_pct':1,'mda':{'sources':src(holder),'familiar_pattern':{'decision':gate},'retained_watch':True}}
    assign_sources([stock]); mda=stock['mda']
    assert mda['selection_decision'] is expected
    assert mda['member'] is (expected is True)
    assert notification_eligible(mda,True,True) is (expected is not None)
    assert not notification_eligible(mda,False,True)


def test_missing_or_stale_prices_never_admit_to_observation():
    for f in (None,bars().iloc[:-1]):
        t=checklist(f,'2026-09-10',src())
        assert t['familiar_pattern']['decision'] is None


def test_exact_prior_volume_windows_and_missing_history():
    f=bars(65); f.loc[:59,'volume']=1000;f.loc[60:,'volume']=100
    r=table_rows(checklist(f,'2026-09-10',src()))['B2:1']
    assert r['decision'] is True
    f.loc[64,'low']=50
    assert table_rows(checklist(f,'2026-09-10',src()))['B2:1']['decision'] is False
    assert table_rows(checklist(f.iloc[-20:],'2026-09-10',src()))['B2:1']['decision'] is None
