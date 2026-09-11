"""Build a minimal, whitelisted public workspace from verified source artifacts."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from .model import VERSION, PATTERNS, candles, chip_windows, detect_patterns, digest, growth
from .research import source_evidence, checklist, assign_sources, notification_eligible, macro_overview, TABLE
from .chart_patterns import annotate
from .candle_annotations import annotations as daily_annotations, load_cache, NAMES, TREND_IDS


def read(path, default=None):
    return json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else (default if default is not None else {})


def write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(',',':')) + '\n'
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(raw, encoding='utf-8', newline='\n')
    temporary.replace(path)


def export_assets(source, output, names):
    """Hash the canonical UTF-8/LF bytes that Git and the host will serve."""
    hashes = {}
    for name in names:
        raw = (source/name).read_text(encoding='utf-8-sig').encode('utf-8')
        temporary = (output/name).with_suffix(Path(name).suffix + '.tmp')
        temporary.write_bytes(raw)
        temporary.replace(output/name)
        hashes[name] = hashlib.sha256(raw).hexdigest()
    return hashes


def build(source, output, verified_frame, analyze_sfz_universe, expected_session, supplement=None):
    data, output = Path(source)/'data', Path(output)
    supplement = Path(supplement) if supplement else data/'workspace'
    manifest = read(data/'official_adjusted_update_manifest.json')
    as_of = manifest['data_as_of']
    expected = expected_session(data_dir=data)
    fresh = as_of == expected
    sfz_old = read(data/'sfz_technical_candidates.json')
    mda_source = read(data/'mda_checklist_candidates.json')
    mda_map = {s['stock_id']:s for s in mda_source.get('stocks',[])}
    pool = read(data/'mda_weekly_top50.json')
    pool_map = {s['security_id']:s for s in pool.get('rows',[])}
    weekly_date = pool.get('data_date') or pool.get('data_as_of') or pool.get('date')
    # Inspect the producer's actual date field; a missing date disables eligibility.
    weekly_date = weekly_date or pool.get('current_week')
    weekly_good = bool(weekly_date and pool.get('status') == 'ok' and 0 <= (date.fromisoformat(as_of)-date.fromisoformat(weekly_date)).days <= 7)
    sfz_names = {s['stock_id']:s.get('name',s['stock_id']) for s in sfz_old.get('stocks',[])}
    roster = read(data/'stock_markets.json')
    markets = roster.get('markets',{})
    names = {sid:r.get('name',sid) for sid,r in roster.get('stocks',{}).items()}
    sfz_names = {**names, **sfz_names}
    holders = read(data/'holder_weekly_snapshots.json')
    holder_history = [s for s in holders.get('snapshots',[]) if s['date']<=as_of]
    latest_holder = next((s for s in holder_history if s['date']==weekly_date),{})
    previous_holder = next((s for s in holder_history if s['date']==pool.get('previous_date')), {})
    prev_map = {r['security_id']:r for r in previous_holder.get('rows',[])}
    holder_map = {}
    for h in latest_holder.get('rows',[]):
        p = prev_map.get(h['security_id'],{})
        holder_map[h['security_id']] = {'major_400_percent':h.get('major_percent'),
            'delta_percentage_points': h['major_percent']-p['major_percent'] if h.get('major_percent') is not None and p.get('major_percent') is not None else None,
            'rank':pool_map.get(h['security_id'],{}).get('rank')}
    holder_map.update(pool_map)
    flow = read(data/'daily_market_flow.json')
    margins = {r['security_id']:r for r in flow.get('workbench_details',{}).get('margin',[])}
    inst = read(supplement/'institutional.json')
    history = inst.get('history',[])
    # Index once; a full market build must not repeatedly scan millions of rows.
    by_stock = {}
    for snapshot in history:
        for row in snapshot['rows']:
            by_stock.setdefault(row['security_id'],[]).append({'date':snapshot['date'],'rows':[row]})
    calendar = verified_frame(data,'2330',as_of)
    market_sessions = sorted(str(s)[:10] for s in calendar['date'])
    sessions = market_sessions[-10:]
    if len(sessions)!=10 or sessions[-1]!=as_of:
        raise ValueError('verified session calendar unavailable')
    revenues = read(supplement/'revenue.json')
    revenue_map = {}
    research = read(supplement/'research.json')
    research_stocks = research.get('stocks',{})
    candle_cache=load_cache(os.environ.get('WORKSPACE_CANDLE_CACHE'))
    for row in revenues.get('rows',[]):
        revenue_map.setdefault(row['stock_id'],[]).append(row)
    stocks, failures = [], []
    frames = [data/'prices'/f'{sid}.csv' for sid in sorted(markets)]
    for path in frames:
        sid = path.stem
        if not (len(sid)==4 and sid.isdigit()):
            continue
        try:
            basis = read(data/'price_basis'/f'{sid}.json')
            if hashlib.sha256(path.read_bytes()).hexdigest() != basis.get('csv_sha256'):
                raise ValueError('price hash mismatch')
            frame = verified_frame(data,sid,as_of)
            sfz_raw = analyze_sfz_universe(frame,sid,as_of)
            checks = sfz_raw['checks']
            known = not any(c['status']=='unknown' for c in checks)
            member = bool(sfz_raw.get('candidate'))
            sfz = {'member':member, 'stage':'符合初篩' if member else '未符合' if known else '資料不足',
                   'notification_eligible': fresh and known, 'rule_version':digest({'version':sfz_raw.get('rule_version'),'config':sfz_raw.get('config')}),
                   'checks':checks, 'config':sfz_raw.get('config'), 'note':'教材示例初篩；進出場仍需另行定義。'}
            prior = mda_map.get(sid,{})
            mda_checks = prior.get('checklist',{}).get('checks',[])
            if isinstance(mda_checks,dict):
                mda_checks = [{'id':key,'status':value['status'],'summary':value['summary'],'threshold_basis':value.get('threshold_basis')} for key,value in mda_checks.items()]
            if not mda_checks:
                mda_checks = [{'id':e['id'],'summary':e['summary'],'status':e.get('metrics',{}).get('check_status','unknown')}
                              for e in prior.get('evidence',[]) if e['id']!='holder_weekly_pool']
            in_pool = sid in pool_map
            retained = prior.get('watch_pool_member') is True
            # Top50 membership is reported separately from the complete A/X/B checklist.
            mda = {'member':in_pool or retained, 'weekly_pool_member':in_pool, 'retained_watch':retained,
                   'stage':'週籌碼觀察池' if in_pool else '持續觀察' if retained else '未在觀察池',
                   'qualified':bool(prior.get('candidate')) if prior.get('data_date')==as_of and prior.get('quality') not in {'blocked',None} else None,
                   'notification_eligible':fresh and weekly_good,
                   'rule_version':'mda-weekly-and-admitted-watch-v1', 'checks':mda_checks,
                   'note':'週 Top50 是觀察池；A 或 X、長期 B1、B2 與人工 C 必須分開核對。'}
            rev = sorted(revenue_map.get(sid,[]),key=lambda r:r['period'])
            chips = chip_windows(by_stock.get(sid,[]),sessions,sid)
            research_series = research_stocks.get(sid,{})
            mda['sources'] = source_evidence(chips,research_series.get('ownership',[]),research_series.get('margin',[]),as_of,frame,market_sessions[-21:])
            table=checklist(frame,as_of,mda['sources'],research=research_series,
                            industry=research.get('industries',{}).get(sid),revenue=rev[-1] if rev else None)
            mda['familiar_pattern']=table['familiar_pattern']
            mda['matched_conditions']=table['matched_conditions']
            mda['checks']=[{'id':r['id'],'status':r['status'],'summary':r['method'],'threshold_basis':r['basis']}
                           for section in table['sections'] for r in section['rows']]
            patterns = detect_patterns(frame,as_of)
            chart_candles={f:candles(frame,as_of,f,limit=len(frame)) for f in ('day','week','month')}
            annotations={f:annotate(chart_candles[f]) for f in chart_candles}
            neutral=daily_annotations(frame,as_of,sid,basis['csv_sha256'],candle_cache)
            # The reviewed daily whitelist is the sole candle-marker owner.
            # Trend geometries stay separate; never transplant daily patterns
            # onto aggregated or unfinished weekly/monthly bars.
            for frequency in annotations: annotations[frequency]['events']=[]
            annotations['day']['events']=neutral['events']
            # Each pattern has one owner; old overlapping detectors must not
            # reintroduce rejected triangles or invalidated higher lows.
            patterns['observations']=[p for p in patterns['observations'] if p['id'] in {'range','double_bottom','double_top'}]
            patterns['observations'] += annotations['day']['observations']
            patterns['observations'] += [p for p in annotations['day']['events'] if p['end']==as_of]
            patterns['version']='pattern-geometry-v3'
            quote = frame.iloc[-1]
            row = {'stock_id':sid,'name':sfz_names.get(sid,pool_map.get(sid,{}).get('name',sid)),
                   'market':markets.get(sid,{}).get('market') if isinstance(markets.get(sid),dict) else markets.get(sid),
                   'close':float(quote['raw_close']), 'change_pct':growth(quote['close'],frame.iloc[-2]['close']) if len(frame)>1 else None,
                   'data_date':as_of, 'sfz':sfz,'mda':mda,
                   'patterns':sorted({p['id'] for p in patterns['observations']} | {p['id'] for p in annotations['day']['events'] if p['end']==as_of}),
                   'chips':chips['windows'], 'revenue':rev[-1] if rev else None,
                   'holder':holder_map.get(sid), 'detail':f'data/stocks/{sid}.json','price_verified':True}
            detail = {**row,'schema_version':VERSION,'candles':chart_candles,'annotations':annotations,
                      'patterns':patterns,'chips':chips,'revenue_history':rev,
                      'candlestick_calculation':{k:v for k,v in neutral.items() if k!='events'},
                      'price_basis':{'mode':basis['mode'],'verified':True,'csv_sha256':basis['csv_sha256'],
                                     'volume_unit':'shares','chart':'還原價格／原始成交股數','bars':len(frame)},
                      'weekly_date':weekly_date,'expected_revenue_period':revenues.get('expected_period'),
                      'margin':margins.get(sid),
                      'research':research_series,
                      'mda_table':table,
                      'industry':research.get('industries',{}).get(sid),
                      'ai':{'status':'not_configured','observations':[]}}
            write(output/'data'/'stocks'/f'{sid}.json',detail)
            stocks.append(row)
        except (ValueError,KeyError,TypeError,FileNotFoundError) as exc:
            failures.append({'stock_id':sid,'reason':type(exc).__name__})
            if sid not in markets:
                continue
            chips = chip_windows(by_stock.get(sid,[]),sessions,sid)
            research_series = research_stocks.get(sid,{})
            rev = sorted(revenue_map.get(sid,[]),key=lambda r:r['period'])
            retained = mda_map.get(sid,{}).get('watch_pool_member') is True
            row = {'stock_id':sid,'name':sfz_names.get(sid,pool_map.get(sid,{}).get('name',sid)),
                   'market':markets.get(sid),'close':None,'change_pct':None,'data_date':as_of,'price_verified':False,
                   'sfz':{'member':False,'stage':'價格資料未驗證','notification_eligible':False,'rule_version':'unavailable','checks':[],'note':'補齊驗證資料後再計算，這次不判定移出。'},
                   'mda':{'member':sid in pool_map or retained,'weekly_pool_member':sid in pool_map,'retained_watch':retained,
                          'stage':'週籌碼觀察池' if sid in pool_map else '持續觀察' if retained else '未在觀察池',
                          'qualified':None,'notification_eligible':fresh and weekly_good,'rule_version':'mda-weekly-and-admitted-watch-v1','checks':[],
                          'note':'週股權觀察保留；價格未驗證，完整檢核待補。'},
                   'patterns':[],'chips':chips['windows'],'revenue':rev[-1] if rev else None,'holder':holder_map.get(sid),
                   'detail':f'data/stocks/{sid}.json'}
            row['mda']['sources'] = source_evidence(chips,research_series.get('ownership',[]),research_series.get('margin',[]),as_of)
            write(output/'data/stocks'/f'{sid}.json',{**row,'schema_version':VERSION,'candles':{f:[] for f in ('day','week','month')},
                'chips':chips,'revenue_history':rev,'patterns':{'observations':[]},'weekly_date':weekly_date,
                'expected_revenue_period':revenues.get('expected_period'),'price_basis':{'verified':False},'ai':{'status':'insufficient_data'},
                'research':research_series,'industry':research.get('industries',{}).get(sid)})
            packet=read(output/'data/stocks'/f'{sid}.json')
            packet['mda_table']=checklist(None,as_of,row['mda']['sources'],research=research_series,
                                          industry=research.get('industries',{}).get(sid),revenue=rev[-1] if rev else None)
            row['mda']['familiar_pattern']=packet['mda_table']['familiar_pattern']
            row['mda']['matched_conditions']=packet['mda_table']['matched_conditions']
            row['mda']['checks']=[{'id':r['id'],'status':r['status'],'summary':r['method'],'threshold_basis':r['basis']}
                                  for section in packet['mda_table']['sections'] for r in section['rows']]
            write(output/'data/stocks'/f'{sid}.json',packet)
            stocks.append(row)
    if not stocks:
        raise ValueError('no verified stocks; retain last successful workspace')
    assign_sources(stocks)
    for row in stocks:
        path=output/'data/stocks'/f"{row['stock_id']}.json"
        packet=read(path); packet['mda']=row['mda']; row['industry']=packet.get('industry')
        row['turnover']=research_stocks.get(row['stock_id'],{}).get('turnover')
        row['mda']['notification_eligible'] = notification_eligible(row['mda'],fresh,row['price_verified'])
        write(path,packet)
        row['detail_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    stocks.sort(key=lambda s:(not(s['sfz']['member'] or s['mda']['member']),s['stock_id']))
    assets = Path(__file__).parent/'web'
    asset_names=('index.html','app.js','style.css','research.js','profile.js')
    asset_hashes = export_assets(assets, output, asset_names)
    index = {'schema_version':VERSION,'data_date':as_of,'expected_session':expected,'fresh':fresh,
             'asset_sha256':asset_hashes,
             'generated_at':datetime.now(timezone.utc).isoformat(),'stocks':stocks,'patterns':{**PATTERNS,**NAMES},
             'pattern_catalogs':{'geometry':{**{k:v for k,v in PATTERNS.items() if k in TREND_IDS},**NAMES},'ai':PATTERNS},
             'condition_catalog':[{'id':f'{section}:{i+1}','label':label,'section':section} for section,labels in TABLE.items() for i,label in enumerate(labels)],
             'coverage':{'verified':sum(s['price_verified'] for s in stocks),'universe':len(frames),'rejected':len(failures),'visible':len(stocks),
                         'revenue':sum(s['revenue'] is not None for s in stocks),
                         'complete_chips_10':sum(s['chips']['10']['complete'] for s in stocks)},
             'weekly_date':weekly_date,'expected_revenue_period':revenues.get('expected_period'),
             'services':{'telegram':'尚未設定','ai':'尚未設定','holdings':'永豐持倉尚未串接'},
             'research_sources':research.get('sources',[]),
             'macro':macro_overview(stocks,as_of,flow),
             'ownership_backfill':research.get('ownership_backfill',{}),
             'data_issues':{'prices':failures,'revenue':revenues.get('failures',[]),'institutional':inst.get('failures',[]),'research':research.get('failures',[])}}
    write(output/'data'/'index.json',index)
    return index
