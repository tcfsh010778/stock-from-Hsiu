"""Human-designed geometry counterexamples, distinct from trading performance."""
import copy
import json
from pathlib import Path
import pytest
from workspace_public.chart_patterns import annotate

CASES=json.loads((Path(__file__).parent/'fixtures/workspace_pattern_cases.json').read_text(encoding='utf-8'))


@pytest.mark.parametrize('case,pattern',[
    ('rising_channel','rising_channel'),('falling_channel','falling_channel'),
    ('ascending_triangle','ascending_triangle'),('descending_triangle','descending_triangle'),
    ('symmetrical_triangle','symmetrical_triangle'),('bull_pennant_short','bull_pennant'),
    ('bear_pennant_short','bear_pennant'),('head_shoulders_candidate','head_shoulders'),
    ('inverse_head_shoulders_candidate','inverse_head_shoulders')])
def test_named_geometry_is_detected(case,pattern):
    assert pattern in {o['id'] for o in annotate(CASES[case])['observations']}


def test_expanding_boundaries_are_not_triangles_and_types_are_exclusive():
    triangles={'ascending_triangle','descending_triangle','symmetrical_triangle'}
    assert not triangles.intersection(o['id'] for o in annotate(CASES['diverging_not_triangle'])['observations'])
    for case in triangles:
        assert triangles.intersection(o['id'] for o in annotate(CASES[case])['observations'])=={case}


@pytest.mark.parametrize('case,pattern',[
    ('broken_higher_lows','higher_lows'),('head_shoulders_invalidated','head_shoulders'),
    ('inverse_head_shoulders_invalidated','inverse_head_shoulders'),
    ('not_head_shoulders_flat_head','head_shoulders')])
def test_invalidated_structure_leaves_active_filter(case,pattern):
    assert pattern not in {o['id'] for o in annotate(CASES[case])['observations']}


def test_neckline_confirmation_is_first_observable_break_and_survives_recross():
    bars=copy.deepcopy(CASES['head_shoulders_broken'])
    obs=next(o for o in annotate(bars)['observations'] if o['id']=='head_shoulders')
    assert obs['state']=='neckline_broken' and obs['confirmed_at']=='2026-02-04'
    bars.append({**bars[-1],'time':'2026-02-05','open':102,'close':104,'low':101,'high':105})
    again=next(o for o in annotate(bars)['observations'] if o['id']=='head_shoulders')
    assert again['state']=='neckline_broken' and again['confirmed_at']==obs['confirmed_at']


def test_unclosed_tail_and_price_scale_do_not_invent_confirmation():
    for bars in CASES.values():
        for n in range(20,len(bars)):
            tail=copy.deepcopy(bars[:n+1]);tail[-1]['complete']=False
            assert annotate(tail)==annotate(bars[:n])
        scaled=[{**r,**{k:r[k]*10 for k in ('open','high','low','close')}} for r in bars]
        assert [o['id'] for o in annotate(scaled)['observations']]==[o['id'] for o in annotate(bars)['observations']]
