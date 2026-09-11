"""Build a portable, price-bound cache from the verified committed snapshot.

This is a manual build tool, with no market refresh, publication or messaging.
"""
import argparse
import hashlib
import json
from pathlib import Path

from build_review_data import verified_frame
from workspace_public.candle_annotations import calculate, source_hash, VERSION


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();root=Path(__file__).resolve().parents[1];data=root/'data'
    manifest=json.loads((data/'official_adjusted_update_manifest.json').read_text(encoding='utf-8'))
    roster=json.loads((data/'stock_markets.json').read_text(encoding='utf-8'))['markets']
    as_of=manifest['data_as_of'];stocks={};gaps=[]
    for sid in sorted(roster):
        try:frame=verified_frame(data,sid,as_of)
        except (ValueError,KeyError,TypeError,FileNotFoundError):
            gaps.append(sid);continue
        basis=json.loads((data/'price_basis'/f'{sid}.json').read_text(encoding='utf-8'))
        raw=(data/'prices'/f'{sid}.csv').read_bytes()
        if hashlib.sha256(raw).hexdigest()!=basis['csv_sha256']:raise ValueError('price SHA mismatch')
        stocks[sid]={'price_sha256':basis['csv_sha256'],'annotations':calculate(frame,as_of,sid)}
        if len(stocks)%200==0:print(f'{len(stocks)} verified stocks calculated',flush=True)
    if len(stocks)<1400:raise ValueError('unexpected verified snapshot coverage loss')
    payload={'version':VERSION,'source_sha256':source_hash(),'data_date':as_of,'stocks':stocks,'price_gaps':gaps}
    raw=(json.dumps(payload,ensure_ascii=False,allow_nan=False,separators=(',',':'))+'\n').encode('utf-8')
    args.output.mkdir(parents=True,exist_ok=True);(args.output/'candles.json').write_bytes(raw)
    result={'sha256':hashlib.sha256(raw).hexdigest(),'data_date':as_of,'source_sha256':source_hash(),
            'verified_stocks':len(stocks),'price_gaps':len(gaps),
            'events':sum(len(s['annotations']['events']) for s in stocks.values())}
    (args.output/'manifest.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__':main()
