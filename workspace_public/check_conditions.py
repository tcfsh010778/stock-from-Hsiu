"""Recompute the condition table from a dated, exported stock packet."""
import argparse
import hashlib
import json
from pathlib import Path

from .research import checklist


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--packet',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    raw=args.packet.read_bytes(); packet=json.loads(raw)
    bars=[{'date':r['time'],**{k:r[k] for k in ('open','high','low','close','volume')}}
          for r in packet['candles']['day'] if r.get('complete')]
    result=checklist(bars or None,packet['data_date'],packet['mda']['sources'],
        research=packet.get('research'),industry=packet.get('industry'),revenue=packet.get('revenue'))
    result.update(stock_id=packet['stock_id'],input_sha256=hashlib.sha256(raw).hexdigest())
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({'stock_id':packet['stock_id'],'data_date':packet['data_date'],
                      'A_or_X':result['familiar_pattern']['decision'],'matched':result['matched_conditions']},ensure_ascii=False))


if __name__=='__main__': main()
