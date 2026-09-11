"""Causal MDA table measurements. No score, inferred owner, or trade decision.

The table wording comes from the existing registry. Numeric interpretations
are explicit engineering definitions, not additional textbook requirements.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from .model import number, prepare_bars

VERSION = 'mda-conditions-20260911-v1'
PARAMETERS = {'pivot_left': 3, 'pivot_right': 3, 'recent_bars': 5,
              'volume_baseline': 20, 'context_bars': 60, 'year_bars': 240,
              'retail_lots': 30}


def both(*values):
    return False if False in values else True if all(v is True for v in values) else None


def either(*values):
    return True if True in values else False if all(v is False for v in values) else None


def subtract(a,b):
    """Respect decimal source percentages rather than binary subtraction noise."""
    return float(Decimal(str(a))-Decimal(str(b))) if a is not None and b is not None else None


def check(value, method, metrics=None, *, basis='engineering', reason=None):
    return {'status': 'pass' if value is True else 'fail' if value is False else 'unknown',
            'decision': value, 'method': method, 'metrics': metrics or {},
            'basis': basis, 'reason': reason}


def confirmed_lows(frame):
    values = frame.low.to_numpy()
    result = []
    left, right = PARAMETERS['pivot_left'], PARAMETERS['pivot_right']
    for i in range(left, len(frame)-right):
        if values[i] < min(values[i-left:i]) and values[i] <= min(values[i+1:i+right+1]):
            result.append({'index': i, 'date': str(frame.date.iloc[i].date()),
                           'price': float(values[i]),
                           'confirmed_at': str(frame.date.iloc[i+right].date())})
    return result


def price_conditions(rows, as_of):
    frame = prepare_bars(rows, as_of) if rows is not None else None
    if frame is None or frame.empty or str(frame.date.iloc[-1].date()) != as_of:
        unknown = check(None, '使用截至資料日的完整日 K', reason='沒有當日已驗證價格')
        return None, {}, {'A': unknown, 'X': unknown, 'decision': None, 'operator': 'OR'}
    f = frame; n = len(f); close = f.close; volume = f.volume
    ma = {k: close.rolling(k, min_periods=k).mean() for k in (5, 20, 60, 120, 240)}
    a = check(bool(ma[240].iloc[-1] > ma[240].iloc[-2]) if n >= 241 else None,
              'A：MA240 高於前一交易日；股價位置另列，不要求再創新高',
              {'bars': n, 'required_bars': 241, 'ma240': number(ma[240].iloc[-1]),
               'previous_ma240': number(ma[240].iloc[-2]) if n >= 2 else None},
              basis='source_observation', reason='不足 241 根日 K' if n < 241 else None)
    pivots = confirmed_lows(f)
    recent = pivots[-3:]
    higher = None if len(pivots) < 2 else bool(pivots[-1]['price'] > pivots[-2]['price'] and
                  f.low.iloc[pivots[-1]['index']+1:].min() >= pivots[-1]['price'])
    # A structural decline followed by a higher low and a break of the
    # intervening rebound high. No inherited 15%/1.2x magnitude requirement.
    x_value = None
    rebound = None
    if len(recent) == 3:
        p0, p1, p2 = recent
        rebound = float(f.high.iloc[p1['index']+1:p2['index']].max())
        x_value = bool(p1['price'] < p0['price'] and higher and close.iloc[-1] > rebound)
    x = check(x_value,
              'X：三個已確認低點先降低再墊高，現價突破後兩低點間的反彈高點，且未再跌破最新低點',
              {'pivots': recent, 'rebound_high': rebound, 'close': float(close.iloc[-1]),
               'pivot_left': 3, 'pivot_right': 3},
              reason='不足三個已確認低點；不把歷史不足判成不符合' if x_value is None else None)
    results = {}
    def put(key, value, method, metrics=None, **kw):
        results[key] = check(value, method, metrics, **kw)
    vol20 = volume.shift(1).rolling(20, min_periods=20).mean()
    up_volume = (volume > vol20) & (close > close.shift(1))
    down_volume = (volume > vol20) & (close < close.shift(1))
    v5 = number(volume.tail(5).mean()) if n >= 5 else None
    prev20 = number(volume.iloc[-25:-5].mean()) if n >= 25 else None
    floor = number(f.low.iloc[-25:-5].min()) if n >= 25 else None
    holds = bool(f.low.tail(5).min() >= floor) if floor is not None else None
    contracts = bool(v5 < prev20) if prev20 is not None and prev20 > 0 else None
    context = {'volume_5': v5, 'preceding_volume_20': prev20, 'prior_low_20': floor,
               'recent_low_5': number(f.low.tail(5).min())}
    if n >= 80:
        recent_width = float(f.high.tail(20).max()-f.low.tail(20).min())
        prior_width = float(f.high.iloc[-80:-20].max()-f.low.iloc[-80:-20].min())
        midpoint = float((f.high.tail(80).max()+f.low.tail(80).min())/2)
        put('A甲:1', both(bool(close.iloc[-1] <= midpoint), bool(recent_width < prior_width), contracts),
            '80 根區間下半部；近 20 根振幅小於此前 60 根；近 5 根均量低於此前 20 根',
            {**context, 'range_20': recent_width, 'preceding_range_60': prior_width, 'range_midpoint_80': midpoint})
    high_history = None
    if n >= 480:
        prior_high = close.shift(1).rolling(240, min_periods=240).max()
        high_history = bool((close.tail(240) >= prior_high.tail(240)).any())
    put('A甲:2', both(high_history, a['decision']), '近 240 根曾創當時前 240 根收盤新高，且目前 MA240 上揚',
        {'required_bars': 480, 'bars': n, 'one_year_high': high_history, 'A': a['decision']},
        reason='上揚尚未成立時，不預測「即將上揚」；扣抵條件另列')
    put('A甲:3', None, '近 60 根中，收紅且量大於此前 20 根均量的次數',
        {'expanded_up_days_60': int(up_volume.tail(60).sum())} if n >= 80 else {},
        reason='可量化放量次數，成交量本身無法證明籌碼換手完成')
    put('A甲:4', either(higher, both(contracts, holds)), '已確認低點墊高，或近 5 根量縮且未跌破此前 20 根低點',
        {**context, 'higher_lows': higher, 'pivots': pivots[-2:]})
    recover = None
    if n >= 245:
        above = [bool(all(close.iloc[i] > ma[k].iloc[i] for k in ma)) for i in range(n-5, n)]
        recover = bool(above[-1] and not all(above[:-1]) and close.iloc[-1] > close.iloc[-2])
    put('A甲:5', recover, '近 5 根曾未站上全部 MA5/20/60/120/240，目前全部站回且收盤上升', {'bars': n, 'required_bars': 245})
    put('A甲:6', None, '不預設個股支撐線或慣性均線', reason='缺少已定義的個股線位，不自造支撐條件')
    for key in ('A甲:7', 'A乙:1'):
        put(key, None, '近 20 根下跌且成交量高於此前 20 根均量',
            {'expanded_down_days_20': int(down_volume.tail(20).sum())} if n >= 40 else {},
            reason='放量下跌是可觀測子條件；無法直接證明籌碼交換完成')
    put('A乙:2', bool(close.iloc[-1] > close.iloc[-241]) if n >= 241 else None,
        '目前收盤高於本根 MA240 所扣除的第 241 根收盤',
        {'close': float(close.iloc[-1]), 'deduction_close': float(close.iloc[-241]) if n >= 241 else None}, basis='source_observation')
    put('A乙:3', None, '週股權與法人持股增加可觀測，持續吸收意圖不可識別', reason='公開彙總資料沒有同一買方身分與意圖')
    results['A乙:4'] = x
    extreme = None
    if n >= 65:
        past = volume.iloc[-65:-5].rolling(5, min_periods=5).mean().dropna()
        extreme = bool(v5 <= past.min())
    put('B2:1', both(extreme, holds), '近 5 根均量不高於此前 60 根所有完整 5 根均量，且未跌破此前 20 根低點',
        {**context, 'volume_at_60bar_minimum': extreme})
    put('B2:2', higher, '最近兩個已確認低點墊高，確認後至今沒有跌破最新低點', {'pivots': pivots[-2:]})
    put('B2:5', None, '近 5 根均量低於此前 20 根後，當日放量且收盤上升',
        {'volume_contracts': contracts, 'last_expanded_up': bool(up_volume.iloc[-1]) if n >= 21 else None},
        reason='量價反應不等於已辨識有人承接')
    deviation = abs(float(close.iloc[-1]/ma[20].iloc[-1]-1))*100 if n >= 20 else None
    put('B2:6', None, '收盤相對 MA20 的絕對乖離', {'absolute_deviation_ma20_pct': deviation},
        reason='教材未給「合理」數值，不自訂通過門檻')
    put('B2:7', None, '需要已定義的歷史平台', reason='不預設支撐或自行指定平台')
    shadows = None
    if n >= 5:
        tail = f.tail(5); body = (tail.close-tail.open).abs()
        lower = tail[['open','close']].min(axis=1)-tail.low
        shadows = int(((lower > 0) & (lower >= 2*body) & (tail.close >= (tail.high+tail.low)/2)).sum())
    put('B2:8', bool(shadows >= 2) if shadows is not None else None,
        '近 5 根至少 2 根下影線長度為實體 2 倍以上且收盤在全 K 上半部；不設定支撐線', {'long_lower_shadows_5': shadows})
    put('B2:9', bool(volume.tail(5).mean() > volume.tail(20).mean()) if n >= 20 else None,
        '5 日均量 > 20 日均量', {'volume_ma5': v5, 'volume_ma20': number(volume.tail(20).mean()) if n >= 20 else None}, basis='source_explicit')
    put('B2:10', bool(volume.iloc[-21] < volume.tail(20).mean()) if n >= 21 else None,
        '本根 MA20 扣除量低於目前 20 日均量',
        {'deduction_volume': float(volume.iloc[-21]) if n >= 21 else None, 'volume_ma20': number(volume.tail(20).mean()) if n >= 20 else None})
    put('B2:11', None, '不從跌幅推定非理性', reason='缺少可檢證的非理性定義')
    put('B2:12', None, '需有日期的利空公告與同日量能',
        {'expanded_volume_today': bool(volume.iloc[-1] > vol20.iloc[-1]) if n >= 21 else None},
        reason='尚未接入結構化公告事件；不能只憑爆量推定利空')
    put('C:12', None, '收盤是否跌破最近已確認低點',
        {'close': float(close.iloc[-1]), 'latest_confirmed_low': pivots[-1] if pivots else None,
         'below_low': bool(close.iloc[-1] < pivots[-1]['price']) if pivots else None},
        reason='可列破低現象，但「主升後」尚無明確定義；不產生賣出訊號')
    return f, results, {'A': a, 'X': x, 'decision': either(a['decision'], x['decision']), 'operator': 'OR'}


def evaluate(rows, as_of, table, *, research=None, industry=None, sources=None, revenue=None):
    research, sources = research or {}, sources or {}
    f, calculated, familiar = price_conditions(rows, as_of)
    def put(key, value, method, metrics=None, **kw):
        calculated[key] = check(value, method, metrics, **kw)
    ownership = sorted((r for r in research.get('ownership', []) if r.get('date', '') <= as_of), key=lambda r:r['date'])
    weekly_valid = (len(ownership) >= 2 and len({r['date'] for r in ownership}) == len(ownership) and
                    0 <= (date.fromisoformat(as_of)-date.fromisoformat(ownership[-1]['date'])).days <= 10 and
                    4 <= (date.fromisoformat(ownership[-1]['date'])-date.fromisoformat(ownership[-2]['date'])).days <= 10)
    def delta(group, key):
        if not weekly_valid: return None
        values = [number(r.get(group, {}).get(key)) for r in ownership[-2:]]
        return subtract(values[1],values[0])
    d400, d600, d800, d1000 = [delta('major', k) for k in ('400','600','800','1000')]
    for key, value, label in [('B1:5', d1000, '1000 張以上'),
             ('B1:6', subtract(d800,d1000), '800–1000 張'),
             ('B1:7', subtract(d600,d800), '600–800 張'),
             ('B1:8', subtract(d400,d600), '400–600 張')]:
        put(key, bool(value > 0) if value is not None else None, f'{label}持股比例較前一週增加（相鄰級距相減，不把累積級距混用）',
            {'delta_percentage_points': value, 'dates': [r['date'] for r in ownership[-2:]]}, basis='source_observation',
            reason=None if value is not None else '缺少兩期可比級距')
    six = ownership[-7:]
    six_valid = len(six)==7 and weekly_valid and all(4 <= (date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days <= 10 for a,b in zip(six,six[1:]))
    joint = None; joint_metrics = {'retail_lots': PARAMETERS['retail_lots'], 'periods': len(six)}
    if six_valid:
        big = [number(r.get('major', {}).get('400')) for r in six]
        small = [number(r.get('retail', {}).get(str(PARAMETERS['retail_lots']))) for r in six]
        if all(v is not None for v in big+small):
            joint = bool(big[-1] > big[0] and small[-1] < small[0])
            joint_metrics.update(major_delta=big[-1]-big[0], retail_delta=small[-1]-small[0], start=six[0]['date'], end=six[-1]['date'])
    put('B1:4', joint, '七期端點／六週：400 張以上淨增加，30 張以下淨減少；不代表同一批人交易', joint_metrics)
    holdings = {}
    days=sources.get('market_sessions',[])[-21:]
    if len(days)==21 and days[-1]==as_of and days==sorted(set(days)):
        for key in ('foreign', 'trust'):
            series=research.get('trust_holdings' if key=='trust' else key, [])
            records = {r['date']: number(r.get('value')) for r in series if r.get('date','')<=as_of}
            values = [records.get(d) for d in days]
            complete = all(v is not None for v in values) and len({r['date'] for r in series if r['date'] in days})==sum(r['date'] in days for r in series)
            passed = bool(values[-1] > values[0] and sum(b>a for a,b in zip(values,values[1:])) > sum(b<a for a,b in zip(values,values[1:]))) if complete else None
            holdings[key] = {'decision': passed, 'observed': sum(v is not None for v in values), 'required': 21,
                             'change_lots': values[-1]-values[0] if complete else None, 'start': days[0], 'end': days[-1]}
    put('B1:1', either(*(holdings.get(k,{}).get('decision') for k in ('foreign','trust'))),
        '20 個交易日持股淨增加且增加日多於減少日；外資或投信任一成立。此窗只代表近月觀察', holdings,
        reason='不以買賣超累加冒充持股；投信缺值保留未知')
    margin = sources.get('margin_observation', {}).get('windows', {}).get('20', {})
    put('B1:2', None, '20 日融資餘額與股價同期間變動', margin,
        reason='不能從彙總餘額確認持有人獲利或同一資金留場')
    people = [number(r.get('shareholders')) for r in ownership[-2:]]
    put('B1:3', None, '股東總人數較前週變化', {'shareholder_delta': people[1]-people[0] if weekly_valid and len(people)==2 and all(v is not None for v in people) else None},
        reason='人數無法識別是否為同一股東，更無法確認其獲利後仍持有')
    decline = None
    interval = {'major_delta_percentage_points': d400}
    if weekly_valid and f is not None:
        prices = []
        for p in ownership[-2:]:
            eligible = f[f.date.dt.date <= date.fromisoformat(p['date'])].tail(1)
            if eligible.empty or (date.fromisoformat(p['date'])-eligible.date.iloc[0].date()).days > 4: break
            prices.append(float(eligible.close.iloc[0]))
        if len(prices)==2:
            decline = bool(prices[1] < prices[0]); interval['close_endpoints'] = prices
    put('B2:3', both(decline, bool(d400>0) if d400 is not None else None), '最近一個週股權區間，價格下跌而 400 張以上持股比例增加；不推定主力身分', interval)
    put('B2:4', None, '需要可追蹤的賣方持股', reason='公開資料無法確認主要賣方是否仍留場')
    put('C:1', True if industry else None, '官方細產業分類有對應', {'industries': industry or []}, basis='source_observation',
        reason='分類資料不足' if not industry else '有分類不等於產業前景良好')
    put('C:2', None, '月營收是可觀測資訊，不能單獨證明產業前景', revenue or {}, reason='未定義產業前景的可檢證條件')
    sections=[]
    for section, labels in table.items():
        items=[]
        for i,label in enumerate(labels):
            key=f'{section}:{i+1}'
            item=calculated.get(key, check(None, '尚無可檢證的結構化條件', basis='not_defined',
                reason='此項尚未定義' if '其他' in label else '需要公司公告或明確事件／線位定義，目前無法自動證實'))
            items.append({'id': key, 'label': label, **item, 'evidence': item['method']})
        sections.append({'id': section, 'title': section, 'rows': items})
    flat=[r for s in sections for r in s['rows']]
    return {'version': VERSION, 'previous_version': 'mda-stock-table-20260910-v3',
            'data_date': as_of, 'sections': sections, 'parameters': PARAMETERS,
            'familiar_pattern': familiar, 'qualified': None,
            'matched_conditions': [r['id'] for r in flat if r['status']=='pass'],
            'source': '使用者選股表與既有 M 大 A／X 規則紀錄；數值工程定義逐項公開',
            'note': '逐項列出符合、不符合與未知，不計分、不加權。工程條件是可重現的觀察定義；未知不強迫人工打勾，也不宣稱完整教材條件已成立。'}
