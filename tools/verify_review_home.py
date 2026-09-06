"""Validate published review identity, source gates, uniqueness and UI shape."""
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]


def verify(root: Path = ROOT):
    public = json.loads((root / 'docs/data/review_queue.json').read_text(encoding='utf-8'))
    private = json.loads((root / 'data/review_queue.json').read_text(encoding='utf-8'))
    assert public == private, 'published queue differs from generated state'
    assert public['dataset_id'] == 'review_queue'
    ids = [s['stock_id'] for s in public['stocks']]
    assert len(ids) == len(set(ids)), 'duplicate stock cards'
    cards = {s['stock_id']: s for s in public['stocks']}
    for event in public['alerts']:
        card = cards[event['stock_id']]
        route = next(r for r in card['routes'] if r['route_id'] == event['route_id'])
        assert route['quality']['alert_eligible'], 'unverified source generated alert'
        assert route['data_date'] == public['as_of'], 'stale route generated alert'
    if public['status'] == 'blocked':
        assert not public['alerts'], 'blocked data generated current alerts'
    sfz = json.loads((root / 'docs/data/sfz_technical_candidates.json').read_text(encoding='utf-8'))
    assert sfz['dataset_id'] == 'sfz_technical_candidates'
    assert 'no MDA eligibility filter' in sfz['source']
    for card in public['stocks']:
        assert 'score' not in card, 'mixed score added'
    html = (root / 'docs/index.html').read_text(encoding='utf-8')
    assert html.count('id="review-list"') == 1
    assert '今日操作總覽' not in html and 'daily-decision-card' not in html
    assert '<summary>市場背景與其他分析</summary>' in html
    assert html.index('<summary>市場背景與其他分析</summary>') < html.index('<!-- daily-market-flow:start -->')
    for rel in ('review-pool.html', 'review-rules.html', 'css/review-home.css', 'js/review-home.js'):
        assert (root / 'docs' / rel).exists(), f'missing asset {rel}'
    print(json.dumps({'stocks': len(ids), 'alerts': len(public['alerts']), 'status': public['status'],
                      'as_of': public['as_of'], 'source_summary': public['source_summary']}, ensure_ascii=False))


if __name__ == '__main__':
    verify()
