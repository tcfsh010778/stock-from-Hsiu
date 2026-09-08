"""Keep the textbook universe independent from individual entry observations."""
from stock_v2_public.analysis.sfz_universe import analyze_sfz_universe
from stock_v2_public.analysis.sfz_review import analyze_sfz


def analyze_sfz_route(frame, *, stock_id, as_of):
    universe = analyze_sfz_universe(frame, stock_id, as_of)
    timing = analyze_sfz(frame, stock_id=stock_id, as_of=as_of)
    missing = [c['summary'] for c in universe['checks'] if c['status'] == 'unknown']
    stage = universe['stage']
    if universe['candidate'] and timing['stage'] in {'breakout_wait_retest', 'retest_confirmed', 'box_forming'}:
        stage = timing['stage']
    return {**universe, 'stage': stage, 'universe_stage': universe['stage'],
            'missing': missing, 'conflicts': timing.get('conflicts', []) if universe['candidate'] else [],
            'reasons': (['通過 SFZ 教材初篩；仍需核對週線與日線型態'] if universe['candidate'] else
                        [c['summary'] for c in universe['checks'] if c['status'] in {'fail', 'unknown'}]),
            'evidence': [{'id': c['id'], 'data_date': as_of, 'summary': c['summary'],
                          'metrics': {**c['metrics'], 'check_status': c['status']}} for c in universe['checks']],
            'technical_observations': {'box_breakout': timing,
                                      'other_patterns': universe['entry_observations']},
            'next_observation': '核對週線上升結構、日線盤整／行進與 MA34 反應；初篩資格不等於進場。'}
