from datetime import date, timedelta
from workspace_public.research import (assign_sources, checklist, consecutive_buys,
    intersection_decision, MACRO_LABELS, macro_overview, notification_eligible)


def sources(holder=True, foreign=True, trust=False):
    return {'holder_six_weeks':{'status':'unknown' if holder is None else 'measured', 'candidate':holder},
            'foreign_consecutive':{'candidate':foreign is True, 'decision':foreign},
            'trust_consecutive':{'candidate':trust is True, 'decision':trust},
            'margin_observation':{'candidate':True, 'latest_change_lots':1000}}


def test_intersection_requires_holder_and_at_least_one_institution():
    for h,f,t,expected in [(True,True,False,True),(True,False,True,True),
            (True,False,False,False),(False,True,True,False),(None,True,False,None),
            (True,None,False,None),(True,None,True,True),(None,False,False,False)]:
        assert intersection_decision(sources(h,f,t)) is expected


def test_gain_margin_and_old_retained_pool_cannot_bypass_intersection():
    stock={'stock_id':'2330','change_pct':9.5,'sfz':{},'mda':{'sources':sources(False,True,True),'retained_watch':True}}
    assign_sources([stock])
    assert not stock['mda']['member']
    assert stock['mda']['sources']['daily_gainers']['rank']==1
    assert '每日漲幅' not in stock['mda']['source_labels']
    assert stock['mda']['rule_version']=='mda-intersection-and-ax-20260911-v4'


def test_unknown_intersection_does_not_notify_removal_even_with_source_labels():
    mda={'sources':sources(None,True,True),'source_labels':['外資連買至少3日']}
    assert not notification_eligible(mda,True,True)
    mda['sources']=sources(True,None,True)
    assert not notification_eligible(mda,True,True)
    mda['familiar_pattern']={'decision':True}
    assert notification_eligible(mda,True,True)
    assert not notification_eligible(mda,False,True)
    assert not notification_eligible(mda,True,False)


def test_three_actual_sessions_and_gap_stale_handling():
    days=['2026-09-04','2026-09-07','2026-09-08','2026-09-09','2026-09-10']
    rows=[{'date':d,'trust':1} for d in days]
    assert consecutive_buys(rows,'trust',days[-1],days)['sessions']==5
    assert consecutive_buys(rows[-2:],'trust',days[-1],days)['decision'] is None
    assert consecutive_buys(rows[:-1],'trust',days[-1],days)['decision'] is None
    rows[-1]['trust']=0
    assert consecutive_buys(rows,'trust',days[-1],days)['decision'] is False
    assert consecutive_buys(rows[:-1],'trust',days[-1])['decision'] is None


def test_macro_is_global_and_stock_table_has_only_five_sections():
    table=checklist(None,'2026-09-10',{})
    assert [s['id'] for s in table['sections']]==['A甲','A乙','B1','B2','C']
    assert sum(len(s['rows']) for s in table['sections'])==48
    assert len(MACRO_LABELS)==14
    macro=macro_overview([], '2026-09-10')
    assert len(macro['items'])==14 and macro['ai_status']=='not_configured'
    assert table['previous_version']=='mda-stock-table-20260910-v3'
