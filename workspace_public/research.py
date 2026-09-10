"""MDA research sources and full observation table, independent of trade rules."""
from datetime import date
from .model import number, prepare_bars

TABLE = {
 '大環境': ['大盤趨勢','大盤量能','櫃台趨勢','櫃台量能','台幣動態','美元指數','外資動態','融資餘額','2330 動態','0050 動態','台指期','微台／小台','美國費半','美國 NAS'],
 'A甲': ['低基期窄幅、低量整理','曾創一年新高，240 日線上揚或將上揚','上升過程有數次放量與籌碼交換','低點墊高或量縮價穩','下跌後快速站回全部均線','跌到觀察中的支撐或慣性均線反彈','多頭下跌途中爆量並完成籌碼交換'],
 'A乙': ['下跌過程爆量並出現籌碼交換','240 日扣抵價將低於目前股價','特定籌碼開始穩定吸收','下跌趨勢明顯轉變'],
 'B1': ['外資或投信長期增加持股','獲利仍留場、上漲時增加的融資','新增股東在獲利後仍持有','大股東增加、小股東減少','1000 張以上增加','800–1000 張增加','600–800 張增加','400–600 張增加','其他持續投入跡象'],
 'B2': ['量縮到極致後不再下跌','整理時低點墊高','下跌中主力或大戶持續增加','主要賣方僅調節，未離開','量縮跌勢中出量承接','乖離已修正至合理範圍','回到先前量縮平台','常見下影線或觀察中的支撐','成交量漸增（5 日均量大於 20 日）','20 日扣抵量偏低','非理性的超跌情形','突然利空且成交爆量','其他賣壓觀察'],
 'C': ['所屬產業或概念','產業前景','產業邏輯改變','研發新技術','打入供應鏈','景氣循環','富爸爸入駐','政策支持','擴廠','併購','專利','主升後跌破前低','主升後跌破大量區','主升後跌破慣性趨勢線','其他長期理由'],
}

MACRO_LABELS = TABLE.pop('大環境')


def macro_overview(stocks, as_of, flow=None):
    """One shared market context; never repeat it as a per-stock manual check."""
    reference = next((s for s in stocks if s['stock_id'] == '2330' and s.get('price_verified')), None)
    items = []
    for i, label in enumerate(MACRO_LABELS):
        evidence = '待接市場資料'
        status = 'missing'
        if label == '2330 動態' and reference:
            status = 'measured'
            evidence = f"收盤 {reference['close']:.2f} 元；日變動 {reference['change_pct']:.2f}%" if reference.get('change_pct') is not None else f"收盤 {reference['close']:.2f} 元"
        if label == '外資動態' and flow and flow.get('date')==as_of:
            markets=flow.get('markets',{})
            parts=[(name,number(markets.get(key,{}).get('foreign_net'))) for key,name in [('listed','上市'),('otc','上櫃')]]
            if all(value is not None for _,value in parts):
                status='measured';evidence='；'.join(f'{name}淨買賣 {value/1000:,.0f} 張' for name,value in parts)
        items.append({'id':f'macro:{i+1}', 'label':label, 'status':status, 'evidence':evidence})
    return {'version':'market-context-1', 'data_date':as_of, 'ai_status':'not_configured',
            'items':items, 'note':'大環境全站共用；觀察項目可調整。未來 AI 依各市場的有日期資料判讀，不參與個股人工表格。'}


def consecutive_buys(series, field, as_of, trading_sessions=None):
    days = [d for d in (trading_sessions or []) if d <= as_of][-10:]
    if days:
        by_date = {r.get('date'):r for r in series}
        series = [by_date.get(d, {'date':d}) for d in days]
    elif any(r.get('date') for r in series):
        series = sorted((r for r in series if r.get('date','') <= as_of), key=lambda r:r.get('date',''))
        if not series or series[-1].get('date') != as_of:
            series = [*series, {'date':as_of}]
    values = [number(r.get(field)) for r in series]
    streak = 0
    for value in reversed(values):
        if value is None or value <= 0: break
        streak += 1
    recent = values[-3:]
    gate = False if any(v is not None and v <= 0 for v in recent) else True if len(recent)==3 and all(v is not None for v in recent) else None
    return {'sessions':streak, 'minimum_sessions':3, 'observed_sessions':sum(v is not None for v in values),
            'complete':bool(values) and all(v is not None for v in values), 'gate_known':gate is not None,
            'latest_known':bool(values) and values[-1] is not None,
            'at_least':bool(values) and (streak==len(values) or values[len(values)-streak-1] is None),
            'candidate':gate is True, 'decision':gate}


def six_week_trend(rows, as_of):
    """Six weekly changes require seven actual weekly endpoints."""
    rows = sorted((r for r in rows if r['date'] <= as_of), key=lambda r:r['date'])[-7:]
    result={'status':'unknown','observed_points':len(rows),'required_points':7,'required_changes':6,'points':rows,
            'net_change':None,'up_weeks':None,'down_weeks':None,'rhythm':None,
            'basis':'七期端點；六次週增減。進退字樣只描述觀察，不證明主力身分。'}
    if len(rows)!=7 or len({r['date'] for r in rows})!=7:
        return result
    days=[date.fromisoformat(r['date']) for r in rows]
    values=[number(r.get('major',{}).get('400')) for r in rows]
    if any(v is None for v in values) or not all(4<=(b-a).days<=10 for a,b in zip(days,days[1:])) or (date.fromisoformat(as_of)-days[-1]).days>10:
        return result
    deltas=[b-a for a,b in zip(values,values[1:])]
    signs=''.join('+' if d>0 else '-' if d<0 else '0' for d in deltas)
    up=sum(d>0 for d in deltas); down=sum(d<0 for d in deltas)
    result.update(status='measured',net_change=values[-1]-values[0],up_weeks=up,down_weeks=down,
                  deltas=deltas,rhythm='進三退一片段' if '+++-' in signs else '進二退一片段' if '++-' in signs else '連續增加' if signs=='++++++' else '混合增減',
                  candidate=values[-1]>values[0] and up>down)
    return result


def source_evidence(chips, ownership, margin, as_of, price_rows=None, trading_sessions=None):
    series=chips.get('series',[])
    margin=sorted((r for r in margin if r['date']<=as_of),key=lambda r:r['date'])
    delta=None
    prior_session=series[-2].get('date') if len(series)>=2 else None
    if len(margin)>1 and margin[-1]['date']==as_of and margin[-2]['date']==prior_session:
        a,b=number(margin[-2].get('value')),number(margin[-1].get('value'))
        if a is not None and b is not None:delta=b-a
    prices={str(r['date'])[:10]:number(r.get('close')) for r in (price_rows.to_dict('records') if hasattr(price_rows,'to_dict') else price_rows or [])}
    sessions=sorted(d for d in (trading_sessions or []) if d<=as_of); by_date={r['date']:number(r.get('value')) for r in margin}; windows={}
    for n in (5,10,20):
        days=sessions[-n-1:]; values=[by_date.get(d) for d in days]
        complete=len(days)==n+1 and days[-1]==as_of and all(v is not None for v in values) and all(prices.get(d) is not None and prices[d]>0 for d in days)
        price_return=(prices[days[-1]]/prices[days[0]]-1)*100 if complete and prices[days[0]] else None
        windows[str(n)]={'complete':complete,'change_lots':values[-1]-values[0] if complete else None,
                         'increase_sessions':sum(b>a for a,b in zip(values,values[1:])) if complete else None,
                         'price_change_pct':price_return,'start':days[0] if days else None,'end':as_of}
    return {'holder_six_weeks':six_week_trend(ownership,as_of),
            'foreign_consecutive':consecutive_buys(series,'foreign_net',as_of,trading_sessions),
            'trust_consecutive':consecutive_buys(series,'investment_trust_net',as_of,trading_sessions),
            'margin_observation':{'latest_change_lots':delta,'candidate':delta is not None and delta>0,'windows':windows,
                'identity':'公開餘額沒有持有人分類；大額資金可能使用融資，但金額本身不能確認法人或外資身分。','note':'輔助觀察：搭配 5／10／20 日餘額與同期間股價、長期留場逐項判讀。融資增加不再直接納入初篩。'}}


def checklist(rows, as_of, sources):
    f=prepare_bars(rows,as_of) if rows is not None else None; n=len(f) if f is not None else 0; auto={}
    if n>=240:
        auto[('A乙',1)]=('measured',f'240 日扣抵價 {f.close.iloc[-240]:.2f}；目前 {f.close.iloc[-1]:.2f}')
    if n>=20:
        auto[('B2',8)]=('pass' if f.volume.tail(5).mean()>f.volume.tail(20).mean() else 'fail',
                        f'5 日均量 {f.volume.tail(5).mean()/1000:,.0f} 張；20 日均量 {f.volume.tail(20).mean()/1000:,.0f} 張')
    behavior=sources.get('margin_observation',{}).get('windows',{}).get('20',{})
    if behavior.get('complete'):
        auto[('B1',1)]=('measured',f"20 日融資變化 {behavior['change_lots']:,.0f} 張；{behavior['increase_sessions']} 日增加；股價變化 {behavior['price_change_pct']:.2f}%。身分與持續留場仍需人工判讀。")
    sections=[]
    for section,labels in TABLE.items():
        items=[]
        for i,label in enumerate(labels):
            status,evidence=auto.get((section,i),('manual','需結合長期圖形與資料逐項判讀'))
            items.append({'id':f'{section}:{i+1}','label':label,'status':status,'evidence':evidence})
        sections.append({'id':section,'title':section,'rows':items})
    return {'version':'mda-stock-table-20260910-v3','previous_version':'mda-full-table-20260910-v2','sections':sections,'qualified':None,
            'source':'使用者提供的 M 哥說明書第 5、8–9、20–24 頁及選股表',
            'note':'A甲／A乙是不同觀察路徑，並非所有格都必須勾選。B1、B2、C 各自檢查；不計分、不自動宣稱買進。'}


def assign_sources(stocks):
    gainers=sorted((s for s in stocks if number(s.get('change_pct')) is not None and s['change_pct']>0),key=lambda s:(-s['change_pct'],s['stock_id']))
    rank={s['stock_id']:i+1 for i,s in enumerate(gainers)}
    for s in stocks:
        src=s['mda']['sources']; reasons=[]
        if src['holder_six_weeks'].get('candidate'):reasons.append('六週股權趨勢')
        if src['foreign_consecutive']['candidate']:reasons.append('外資連買至少3日')
        if src['trust_consecutive']['candidate']:reasons.append('投信連買至少3日')
        src['daily_gainers']={'rank':rank.get(s['stock_id']),'change_pct':s.get('change_pct'),'candidate':s['stock_id'] in rank}
        decision=intersection_decision(src)
        s['mda'].update(member=decision is True,source_labels=reasons,
                       initial_pool_member=decision is True,selection_known=decision is not None,
                       stage='初篩待逐項檢核' if decision is True else '未符合交集' if decision is False else '交集資料待補',
                       qualified=None,rule_version='mda-holder-and-institutional-3days-v3',
                       note='六週大戶股權趨勢 ∩（外資連買至少 3 日 或 投信連買至少 3 日）→ A甲／A乙、B1、B2、C。漲幅與融資只作輔助觀察。')


def intersection_decision(src):
    holder=src.get('holder_six_weeks',{})
    h=holder.get('candidate') if holder.get('status')=='measured' and isinstance(holder.get('candidate'),bool) else None
    f=src.get('foreign_consecutive',{}).get('decision')
    t=src.get('trust_consecutive',{}).get('decision')
    institutional=True if f is True or t is True else False if f is False and t is False else None
    if h is False or institutional is False:return False
    if h is True and institutional is True:return True
    return None


def sources_complete(src):
    return intersection_decision(src) is not None


def notification_eligible(mda, fresh, price_verified):
    # Three-valued gates: a missing observation never fabricates a pool removal.
    return bool(fresh and price_verified and sources_complete(mda['sources']))
