"""Dated official chart series and TPEx industry-chain memberships.

Raw responses remain in an external cache. Missing rows are never imputed.
"""
from datetime import date, datetime, timezone
import hashlib
import json
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse

from lxml import html
import requests
from .official_data import valid_fetch
from .model import number


def numeric(value, *, integer=False, percent=False):
    v = number(value)
    if v is None or v < 0 or (integer and int(v) != v) or (percent and v > 100):
        raise ValueError('invalid official number')
    return v


def daily_rows(raw, market, kind, day):
    p = json.loads(raw.decode('utf-8-sig'))
    if str(p.get('date')) != day.replace('-', ''):
        raise ValueError('official report date mismatch')
    if kind == 'foreign':
        table = p if market == 'listed' else p.get('tables', [{}])[0]
        code = '證券代號' if market == 'listed' else '代號'
        field = '全體外資及陸資持有股數' if market == 'listed' else '僑外資及陸資持有股數(C)'
    elif market == 'listed':
        table = next((t for t in p.get('tables', []) if t.get('fields', [])[:2] == ['代號','名稱']), {})
        code, field = '代號', '今日餘額'
    else:
        table = p.get('tables', [{}])[0]
        code, field = '代號', '資餘額'
    fields = table.get('fields', [])
    if code not in fields or field not in fields:
        raise ValueError('official report fields changed')
    if kind == 'margin' and market == 'otc' and '前資餘額(張)' not in fields:
        raise ValueError('margin lot unit missing')
    # TWSE repeats 今日餘額 for margin and shorts; the first is margin.
    at, value_at = fields.index(code), fields.index(field)
    result = {}
    for r in table.get('data', []):
        if len(r) != len(fields):
            raise ValueError('official report row width changed')
        sid = str(r[at]).strip()
        if not re.fullmatch(r'[1-9][0-9]{3}', sid):
            continue
        if sid in result:
            raise ValueError('duplicate official security')
        result[sid] = numeric(r[value_at], integer=True) / (1000 if kind == 'foreign' else 1)
    if not result:
        raise ValueError('empty official report')
    return result


def industry_links(raw):
    doc = html.fromstring(raw.decode('utf-8-sig'))
    result = {}
    for a in doc.xpath('//a[contains(@href,"introduce.php?ic=")]'):
        code = parse_qs(urlparse(a.get('href')).query).get('ic', [''])[0]
        if re.fullmatch(r'[A-Z0-9]{4}', code):
            result[code] = ' '.join(a.text_content().split())
    if len(result) < 20:
        raise ValueError('industry catalog incomplete')
    return result


def industry_members(raw, chain, name, url):
    doc = html.fromstring(raw.decode('utf-8-sig'))
    result = {}
    for table in doc.xpath('//table[starts-with(@id,"sc_company_")] | //div[starts-with(@id,"companyList_")]'):
        if table.get('id').startswith('sc_company_'):
            code = table.get('id').removeprefix('sc_company_')
            labels = doc.xpath('//*[@id=$name]', name='sc_link_' + code)
            if not labels:raise ValueError('industry subgroup label missing')
            label = re.sub(r'\s*\([\d,]+家\).*$', '', labels[0].text_content()).lstrip('►').strip()
        else:
            code=table.get('id').removeprefix('companyList_');label=table.get('title','').strip()
            if not label:raise ValueError('industry subgroup label missing')
        for a in table.xpath('.//a[contains(@href,"company_basic.php?stk_code=")]'):
            sid = parse_qs(urlparse(a.get('href')).query).get('stk_code', [''])[0]
            if re.fullmatch(r'[1-9][0-9]{3}', sid):
                item = {'id':chain+':'+code, 'chain':name, 'name':label, 'source_url':url}
                if item not in result.setdefault(sid, []):
                    result[sid].append(item)
    if not result:
        raise ValueError('no industry memberships')
    return result


def ownership_rows(raw, as_of):
    rows = json.loads(raw.decode('utf-8-sig'))
    if not isinstance(rows, list) or not rows:
        raise ValueError('empty TDCC response')
    groups, dates = {}, set()
    for row in rows:
        r = {k.lstrip('\ufeff'):v for k,v in row.items()}
        d = str(r['資料日期']); day = date.fromisoformat(f'{d[:4]}-{d[4:6]}-{d[6:]}').isoformat()
        dates.add(day)
        sid = str(r['證券代號']).strip()
        if not re.fullmatch(r'[1-9][0-9]{3}', sid):continue
        level = int(r['持股分級'])
        if level in groups.setdefault(sid, {}) or level not in range(1,18):
            raise ValueError('duplicate TDCC band')
        if level == 16:
            values = {key:number(r[field]) for key,field in [('percent','占集保庫存數比例%'),('people','人數'),('shares','股數')]}
            if any(v is None for v in values.values()):raise ValueError('invalid TDCC adjustment')
            groups[sid][level] = values
        else:
            groups[sid][level] = {'percent':numeric(r['占集保庫存數比例%'],percent=True),
                                  'people':numeric(r['人數'],integer=True),'shares':numeric(r['股數'],integer=True)}
    if len(dates) != 1 or next(iter(dates)) > as_of:
        raise ValueError('TDCC date mismatch')
    day = next(iter(dates)); result = {}
    for sid, levels in groups.items():
        if set(levels) != set(range(1,18)):
            raise ValueError('incomplete TDCC bands')
        if abs(sum(levels[n]['shares'] for n in range(1,16))-levels[16]['shares']-levels[17]['shares']) > 1:
            raise ValueError('TDCC share totals mismatch')
        if sum(levels[n]['people'] for n in range(1,16)) != levels[17]['people']:
            raise ValueError('TDCC people totals mismatch')
        if abs(levels[17]['percent']-100)>.01 or abs(sum(levels[n]['percent'] for n in range(1,16))-levels[16]['percent']-100)>.2:
            raise ValueError('TDCC percentage totals mismatch')
        result[sid] = {'date':day,
            'major':{str(k):round(sum(levels[n]['percent'] for n in range(start,16)),4) for k,start in [(400,12),(600,13),(800,14),(1000,15)]},
            'retail':{str(k):round(sum(levels[n]['percent'] for n in range(1,end+1)),4) for k,end in [(30,6),(40,7),(50,8)]},
            'shareholders':levels[17]['people']}
    return result


def merge_point(stock, key, point):
    rows = {r['date']:r for r in stock.get(key, [])}
    rows[point['date']] = point
    stock[key] = [rows[d] for d in sorted(rows)][-520:]


def collect(sessions, cache, previous=None, *, include_industry=True, checkpoint=None):
    result = previous or {'stocks':{}, 'industries':{}, 'sources':[]}
    result.setdefault('stocks', {}); result.setdefault('industries', {}); result.setdefault('sources', [])
    result['failures'] = []; as_of = sessions[-1]
    result['as_of'] = as_of; result['observed_at'] = datetime.now(timezone.utc).isoformat()
    result['schema_version'] = 'official-chart-research-1'
    def obtain(url, parse, key):
        try:
            for attempt in range(3):
                try:
                    raw, parsed = valid_fetch(url, cache, parse, force=key=='tdcc:latest' or key.endswith(':'+as_of))
                    break
                except (HTTPError, URLError, TimeoutError, requests.RequestException) as exc:
                    status=getattr(exc,'code',None) or (exc.response.status_code if isinstance(exc,requests.HTTPError) and exc.response is not None else None)
                    if status and status not in {408,429,500,502,503,504}:raise
                    if attempt==2:raise
                    time.sleep(2**(attempt+1))
            source = {'key':key, 'url':url, 'sha256':hashlib.sha256(raw).hexdigest(),'observed_at':result['observed_at']}
            result['sources'] = [s for s in result['sources'] if s['key'] != key] + [source]
            return parsed
        except Exception as exc:
            status=getattr(exc,'code',None) or (exc.response.status_code if isinstance(exc,requests.HTTPError) and exc.response is not None else None)
            result['failures'].append({'key':key,'url':url,'error':type(exc).__name__,'http_status':status})
            return None
    for day in sessions:
        ymd = day.replace('-',''); slash = day.replace('-','/')
        urls = [('foreign','listed',f'https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?date={ymd}&selectType=ALLBUT0999&response=json'),
                ('foreign','otc',f'https://www.tpex.org.tw/www/zh-tw/insti/qfii?date={slash}&response=json'),
                ('margin','listed',f'https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?response=json&selectType=ALL&date={ymd}'),
                ('margin','otc',f'https://www.tpex.org.tw/www/zh-tw/margin/balance?date={slash}&response=json')]
        for kind, market, url in urls:
            key=f'{kind}:{market}:{day}'
            parsed = obtain(url,lambda raw:daily_rows(raw,market,kind,day),key)
            if parsed is not None:
                for sid, value in parsed.items():
                    merge_point(result['stocks'].setdefault(sid, {}),kind,{'date':day,'value':value,'source_key':key})
        if checkpoint:checkpoint(result)
    url='https://openapi.tdcc.com.tw/v1/opendata/1-5'
    parsed=obtain(url,lambda raw:ownership_rows(raw,as_of),'tdcc:latest')
    if parsed:
        source=next(s for s in result['sources'] if s['key']=='tdcc:latest')
        dated_key='tdcc:'+next(iter(parsed.values()))['date']
        result['sources']=[s for s in result['sources'] if s['key']!=dated_key]+[{**source,'key':dated_key}]
        for sid, point in parsed.items():merge_point(result['stocks'].setdefault(sid,{}),'ownership',{**point,'source_key':dated_key})
    if include_industry:
        links=obtain('https://ic.tpex.org.tw/',industry_links,'industry:catalog')
        if links:
            memberships={}; successful=set()
            for chain,name in links.items():
                url=urljoin('https://ic.tpex.org.tw/',f'introduce.php?ic={chain}')
                parsed=obtain(url,lambda raw:industry_members(raw,chain,name,url),'industry:'+chain)
                if parsed:
                    successful.add(chain)
                    for sid,items in parsed.items():memberships.setdefault(sid,[]).extend(items)
            for sid,items in result['industries'].items():
                memberships.setdefault(sid,[]).extend(i for i in items if i['id'].split(':')[0] not in successful)
            result['industries']=memberships
    if checkpoint:checkpoint(result)
    return result
