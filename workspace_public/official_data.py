"""Official public data only. Cache raw responses outside source checkouts."""
from __future__ import annotations

import hashlib
import io
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.parse import urlencode

import pandas as pd

from .model import revenue_row, number


def fetch(url, cache, force=False):
    path = Path(cache) / (hashlib.sha256(url.encode()).hexdigest() + '.raw')
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force and time.time()-path.stat().st_mtime < 7*86400:
        return path.read_bytes()
    request = Request(url, headers={'User-Agent': 'StockWorkspace/1.0 (official-data reader)'})
    with urlopen(request, timeout=45) as response:
        raw = response.read(12_000_000)
    if not raw or len(raw) >= 12_000_000:
        raise ValueError('invalid response size')
    temporary = path.with_suffix('.tmp')
    temporary.write_bytes(raw)
    temporary.replace(path)
    time.sleep(.4)
    return raw


def valid_fetch(url,cache,validator,force=False):
    path=Path(cache)/(hashlib.sha256(url.encode()).hexdigest()+'.raw')
    cached=path.exists() and not force
    try:
        raw=fetch(url,cache,force)
        return raw,validator(raw)
    except Exception:
        if path.exists():path.unlink()
        if not cached:raise
    raw=fetch(url,cache,True)
    try:return raw,validator(raw)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def institutional_rows(raw,market,day,market_flow):
    payload=json.loads(raw.decode('utf-8-sig'))
    if market=='listed':
        fields=payload.get('fields',[])
        required=['證券代號','證券名稱','外陸資買進股數(不含外資自營商)','外陸資賣出股數(不含外資自營商)',
                  '外陸資買賣超股數(不含外資自營商)','投信買進股數','投信賣出股數','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']
        if not set(required)<=set(fields):raise ValueError('missing institutional fields')
        for values in payload.get('data',[]):
            if len(values)!=len(fields) or any(number(values[fields.index(k)]) is None for k in required[2:]):
                raise ValueError('missing institutional values')
        part=market_flow.normalize_twse_payload(payload,day.replace('-',''))
    else:
        table=payload.get('tables',[{}])[0]
        fields=table.get('fields',[])
        if len(fields)!=24 or fields[:2]!=['代號','名稱'] or fields[-1]!='三大法人買賣超股數合計':
            raise ValueError('changed institutional schema')
        for values in table.get('data',[]):
            if len(values)!=24 or any(number(values[i]) is None for i in range(2,24)):
                raise ValueError('missing institutional values')
        part=market_flow.normalize_tpex_history_payload(payload)
    if not part or {r.get('trading_date') for r in part}!={day}:
        raise ValueError('institutional date mismatch')
    return part


def parse_revenue(raw, market, period, url, observed_at):
    text = raw.decode('big5', errors='replace')
    rows = {}
    for table in pd.read_html(io.StringIO(text)):
        keys = [''.join(str(c[-1] if isinstance(c, tuple) else c).split()) for c in table.columns]
        needed = ['公司代號', '當月營收', '上月營收', '去年當月營收']
        if not all(k in keys for k in needed):
            continue
        for values in table.itertuples(index=False, name=None):
            row = dict(zip(keys, values))
            sid = str(row['公司代號']).strip()
            if not (sid.isdigit() and len(sid) == 4):
                continue
            parsed = revenue_row({'stock_id': sid, 'revenue': row['當月營收'],
                                  'previous_month': row['上月營收'], 'previous_year': row['去年當月營收']},
                                 market, period, url, observed_at)
            if sid in rows and rows[sid] != parsed:
                raise ValueError('conflicting revenue identity')
            rows[sid] = parsed
    if not rows:
        raise ValueError('no official revenue rows')
    return list(rows.values())


def collect_revenue(as_of, cache, months=13):
    # Current calendar month's revenue cannot yet be a complete monthly observation.
    year, month = map(int, as_of[:7].split('-'))
    index = year * 12 + month - 2
    rows, sources, failures = [], [], []
    observed = datetime.now(timezone.utc).isoformat()
    for offset in range(months):
        y, m = divmod(index - offset, 12)
        m += 1
        period = f'{y:04d}-{m:02d}'
        for market, segment in [('listed', 'sii'), ('otc', 'otc')]:
            url = f'https://mopsov.twse.com.tw/nas/t21/{segment}/t21sc03_{y-1911}_{m}_0.html'
            try:
                raw, parsed = valid_fetch(url,cache,lambda raw:parse_revenue(raw,market,period,url,observed),force=offset==0)
                rows.extend(parsed)
                sources.append({'market': market, 'period': period, 'url': url,
                                'sha256': hashlib.sha256(raw).hexdigest(), 'rows': len(parsed)})
            except Exception as exc:
                failures.append({'market': market, 'period': period, 'error': type(exc).__name__})
    return {'expected_period': f'{index//12:04d}-{index%12+1:02d}', 'observed_at': observed,
            'rows': rows, 'sources': sources, 'failures': failures, 'source': 'MOPS monthly revenue'}


def collect_institutional(sessions, cache, market_flow):
    history, failures = [], []
    for day in sessions[-10:]:
        endpoints = [
            ('listed', market_flow.TWSE_URL, {'response':'json','selectType':'ALLBUT0999','date':day.replace('-','')}),
            ('otc', market_flow.TPEX_HISTORY_URL, {'type':'Daily','sect':'EW','date':day.replace('-','/'),'response':'json'})]
        rows, sources = [], []
        for market, base, params in endpoints:
            url = base + '?' + urlencode(params)
            try:
                raw,part = valid_fetch(url,cache,lambda raw:institutional_rows(raw,market,day,market_flow))
                rows.extend(part)
                sources.append({'market': market, 'url': url, 'sha256':hashlib.sha256(raw).hexdigest()})
            except Exception as exc:
                failures.append({'market': market, 'date': day, 'error':type(exc).__name__})
        history.append({'date':day, 'rows':rows, 'sources':sources})
    return {'history': history, 'failures': failures, 'unit':'shares'}
