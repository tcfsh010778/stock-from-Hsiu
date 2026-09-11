"""Bounded, resumable TDCC full-band history for institutional screening.

Latest bulk data remains owned by research_collect. This adapter fills missing
dated endpoints through TDCC's public form, with one serial client and no keys.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import time

from tools.recover_weekly_holder_pool import Client, URL, atomic_write
from .research import consecutive_buys
from .research_collect import merge_point


def distribution_point(row, day, source_key):
    levels={int(k):v for k,v in row['levels'].items()}
    if set(levels)!=set(range(1,16)):raise ValueError('missing TDCC population band')
    adjustment=row.get('adjustment') or {'people':0,'shares':0,'percent':0}
    total=row['total']
    for record in [*levels.values(), adjustment, total]:
        for field in ('people','shares','percent'):
            value=record[field]
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
                raise ValueError('invalid TDCC number')
    if any(v['people']<0 or v['shares']<0 or not 0<=v['percent']<=100 for v in levels.values()):
        raise ValueError('invalid TDCC population value')
    if total['people']<=0 or total['shares']<=0 or abs(total['percent']-100)>.01:
        raise ValueError('invalid TDCC total')
    if any(v['people']!=int(v['people']) or v['shares']!=int(v['shares']) for v in [*levels.values(),adjustment,total]):
        raise ValueError('noninteger TDCC count')
    if sum(v['people'] for v in levels.values())!=total['people']:raise ValueError('TDCC people mismatch')
    # Historical HTML adjustment is signed; OpenAPI uses the opposite convention.
    if abs(sum(v['shares'] for v in levels.values())+adjustment['shares']-total['shares'])>1:
        raise ValueError('TDCC shares mismatch')
    if abs(sum(v['percent'] for v in levels.values())+adjustment['percent']-100)>.2:
        raise ValueError('TDCC percentage mismatch')
    return {'date':day,'major':{str(k):round(sum(levels[n]['percent'] for n in range(start,16)),4)
                    for k,start in [(400,12),(600,13),(800,14),(1000,15)]},
            'retail':{str(k):round(sum(levels[n]['percent'] for n in range(1,end+1)),4)
                    for k,end in [(30,6),(40,7),(50,8)]},
            'shareholders':total['people'],'source_key':source_key}


def full_point(point):
    values=[point.get('major',{}).get(k) for k in ('400','600','800','1000')]
    values += [point.get('retail',{}).get(k) for k in ('30','40','50')]
    people=point.get('shareholders')
    valid_people=(isinstance(people,(int,float)) and not isinstance(people,bool)
                  and math.isfinite(people) and people>0 and people==int(people))
    return all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and 0<=v<=100 for v in values) and valid_people


def priority_stocks(institutional, sessions, watch_ids=()):
    by_stock={}
    for snapshot in institutional.get('history',[]):
        for row in snapshot.get('rows',[]):
            by_stock.setdefault(str(row['security_id']),[]).append({'date':snapshot['date'],**row})
    priority=set(str(s) for s in watch_ids)
    for sid, rows in by_stock.items():
        if any(consecutive_buys(rows,k,sessions[-1],sessions)['candidate'] for k in ('foreign_net','investment_trust_net')):
            priority.add(sid)
    return sorted(s for s in priority if re.fullmatch(r'\d{4}',s))


class HistoryClient(Client):
    def __init__(self):
        super().__init__(interval=1,timeout=30)
        self.last_response=None

    def _request(self, method, **kwargs):
        result=super()._request(method,**kwargs)
        self.last_response=result
        return result

    def query(self, code, day):
        try:return super().query(code,day)
        except RuntimeError as exc:
            if 'token missing' not in str(exc):raise
            # One ordinary form-session refresh. Never retry challenge/rate pages.
            body=self.last_response.text if self.last_response is not None else ''
            if re.search(r'captcha|驗證碼|access denied|too many requests|request blocked|request rejected|查詢過於頻繁|存取被拒|拒絕存取|封鎖|操作過於頻繁',body,re.I):raise
            time.sleep(10)
            self.session.close()
            super().__init__(interval=1,timeout=30)
            self.bootstrap()
            return super().query(code,day)


def backfill(result, institutional, sessions, cache, *, watch_ids=(), universe=(), budget=120, client=None, checkpoint=None):
    """Prioritize current candidates, then slowly fill the rest of the roster.

    Successes persist individually; a later transport/schema failure stops this
    batch without clearing prior observations. No false source date is inserted.
    """
    root=Path(cache);root.mkdir(parents=True,exist_ok=True)
    result.setdefault('stocks',{});result.setdefault('sources',[])
    priority=priority_stocks(institutional,sessions,watch_ids)
    universe=sorted({str(s) for s in universe if re.fullmatch(r'\d{4}',str(s))})
    targets=priority+[s for s in universe if s not in set(priority)]
    client=client or HistoryClient()
    status={'as_of':sessions[-1],'priority_stocks':len(priority),'targets':len(targets),'request_limit':budget,
            'requests':0,'accepted':0,'cache_hits':0,'missing_points':0,'status':'complete','dates':[]}
    source_map={s['key']:s for s in result['sources']}
    last_checkpoint=0
    def persist():
        result['sources']=list(source_map.values());result['ownership_backfill']=status.copy()
        if checkpoint:checkpoint(result)
    try:
        client.bootstrap()
        dates=sorted({d for d in client.dates if d<=sessions[-1]})[-7:]
        status['dates']=dates
        if len(dates)!=7:raise ValueError('seven official TDCC dates unavailable')
        for sid in targets:
            stock=result['stocks'].setdefault(sid,{})
            known={r['date']:r for r in stock.get('ownership',[])}
            for day in dates:
                if full_point(known.get(day,{})):continue
                key=f'tdcc-history:{sid}:{day}';path=root/f'{sid}_{day}.json'
                cached=None
                if path.exists():
                    try:
                        saved=json.loads(path.read_text(encoding='utf-8'))
                        source=saved['source']
                        if saved['stock_id']!=sid or saved['date']!=day or source['key']!=key:raise ValueError('cache identity mismatch')
                        if not re.fullmatch(r'[a-f0-9]{64}',source.get('sha256','')) or source.get('url')!=URL:raise ValueError('cache provenance missing')
                        cached=(distribution_point(saved,day,key),source)
                    except (ValueError,KeyError,TypeError):pass
                if cached:
                    point,source=cached;status['cache_hits']+=1
                elif status['requests']>=budget:
                    status['missing_points']+=1;status['status']='partial';continue
                else:
                    status['requests']+=1
                    row=client.query(sid,day)
                    if row is None:
                        status['missing_points']+=1;status['status']='partial';continue
                    point=distribution_point(row,day,key)
                    source={'key':key,'url':URL,'sha256':hashlib.sha256(client.last_response.content).hexdigest(),
                            'observed_at':datetime.now(timezone.utc).isoformat(),'stock_id':sid,'data_date':day,'method':'public_history_form'}
                    atomic_write(path,{'stock_id':sid,'date':day,**row,'source':source})
                    status['accepted']+=1
                merge_point(stock,'ownership',point);known[day]=point;source_map[key]=source
            if status['accepted']+status['cache_hits']-last_checkpoint>=20:
                persist();last_checkpoint=status['accepted']+status['cache_hits']
    except Exception as exc:
        status['status']='interrupted';status['error']=type(exc).__name__
        # No response body, session token or exception URL enters published state.
    finally:
        persist()
        if hasattr(client,'session'):client.session.close()
    return result
