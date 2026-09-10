import argparse
import json
import hashlib
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def verify(output,allow_stale=False):
    index=json.loads((output/'data/index.json').read_text(encoding='utf-8'))
    assert index['schema_version']=='sfz-mda-workspace-1'
    assert allow_stale or index['fresh'], 'Latest completed session is not covered'
    assert len(index['stocks'])==len({s['stock_id'] for s in index['stocks']}), 'Duplicate securities'
    assert index['coverage']['verified']>=1400, 'Unexpected verified-price coverage regression'
    forbidden={'score','stop_loss','take_profit','target_price','support_price','fixed_return'}
    def walk(value):
        if isinstance(value,dict):
            assert not forbidden.intersection(value), 'Retired prescription in public contract'
            for child in value.values():walk(child)
        elif isinstance(value,list):
            for child in value:walk(child)
    for stock in index['stocks']:
        walk(stock)
        path=output/stock['detail']
        assert path.resolve().is_relative_to(output.resolve()), 'Detail outside public root'
        packet=json.loads(path.read_text(encoding='utf-8'))
        assert hashlib.sha256(path.read_bytes()).hexdigest()==stock['detail_sha256'], 'Packet generation hash mismatch'
        assert packet['stock_id']==stock['stock_id'] and packet['data_date']==index['data_date']
        for period in ('1','5','10'):
            assert packet['chips']['windows'][period]['end']==index['data_date']
        for frequency,rows in packet['candles'].items():
            assert all(row['time']<=index['data_date'] for row in rows)
            assert len({row['time'] for row in rows})==len(rows)
        assert isinstance(packet['mda']['checks'],list)
        assert packet['mda']['qualified'] is None, 'Manual MDA table must not claim automatic qualification'
        assert sum(len(s['rows']) for s in packet['mda_table']['sections'])==62
        for frequency,annotations in packet.get('annotations',{}).items():
            closed={row['time'] for row in packet['candles'][frequency] if row['complete']}
            assert all(e['start'] in closed and e['end'] in closed and e['confirmed_at'] in closed for e in annotations['events'])
        for rows in packet.get('research',{}).values():
            if isinstance(rows,list):assert all(r['date']<=index['data_date'] for r in rows)
        walk(packet)
    html=(output/'index.html').read_text(encoding='utf-8')
    assert 'SFZ' in html and 'AI 看圖' in html
    assert 'CaryBot' not in html and '歷史回測' not in html
    for asset in ('index.html','app.js','style.css','research.js','profile.js'):
        assert (output/asset).is_file()
        assert hashlib.sha256((output/asset).read_bytes()).hexdigest()==index['asset_sha256'][asset]
    print(json.dumps({'verified':len(index['stocks']),'data_date':index['data_date'],'fresh':index['fresh']}))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,default=ROOT/'docs');parser.add_argument('--allow-stale',action='store_true')
    args=parser.parse_args();verify(args.output,args.allow_stale)
