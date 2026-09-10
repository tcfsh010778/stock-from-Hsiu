"""Causal chart annotations. Geometry is an explicit engineering filter, not a trade rule."""
from .model import PATTERNS




def annotate(bars):
    # An incomplete week/month cannot confirm a candlestick or a pivot.
    b=[r for r in bars if r.get('complete',True)]
    events=[]; lines=[]; observations=[]
    def event(pid,start,end,evidence):
        events.append({'id':pid,'name':PATTERNS[pid],'start':b[start]['time'],'end':b[end]['time'],
                       'confirmed_at':b[end]['time'],'price':b[end]['high'],'evidence':evidence})
    def line(pid, points):
        lines.append({'id':pid,'points':[{'time':b[i]['time'],'price':v} for i,v in points]})
    def observation(pid,start,evidence,confirmed=None,state='candidate'):
        observations.append({'id':pid,'name':PATTERNS[pid],'start':b[start]['time'],'end':b[-1]['time'],
            'confirmed_at':b[confirmed if confirmed is not None else len(b)-1]['time'],
            'evidence':evidence,'state':state,'basis':'engineering_geometry_v3','role':'pattern_filter_only'})
    for i,r in enumerate(b):
        span=r['high']-r['low']; body=abs(r['close']-r['open'])
        if span<=0:continue
        if body<=span*.1:event('doji',i,i,'實體 ≤ 全幅 10%')
        if min(r['open'],r['close'])-r['low']>=max(body*2,span*.5) and r['high']-max(r['open'],r['close'])<=span*.2:
            event('hammer',i,i,'下影 ≥ 實體 2 倍及全幅 50%；上影 ≤ 全幅 20%')
        if r['high']-max(r['open'],r['close'])>=max(body*2,span*.5) and min(r['open'],r['close'])-r['low']<=span*.2:
            event('shooting_star',i,i,'上影 ≥ 實體 2 倍及全幅 50%；下影 ≤ 全幅 20%')
        if i:
            p=b[i-1]
            for bull in [True,False]:
                if (p['close']<p['open'] if bull else p['close']>p['open']) and (r['close']>r['open'] if bull else r['close']<r['open']):
                    if min(r['open'],r['close'])<=min(p['open'],p['close']) and max(r['open'],r['close'])>=max(p['open'],p['close']):
                        event('bullish_engulfing' if bull else 'bearish_engulfing',i-1,i,'反向實體包覆前根實體')
                    elif min(r['open'],r['close'])>min(p['open'],p['close']) and max(r['open'],r['close'])<max(p['open'],p['close']):
                        event('bullish_harami' if bull else 'bearish_harami',i-1,i,'反向小實體位於前根實體內')
        if i>=2:
            a,p=b[i-2],b[i-1]; first=abs(a['close']-a['open'])
            if first>0 and abs(p['close']-p['open'])<=first*.3:
                if a['close']<a['open'] and r['close']>r['open'] and r['close']>(a['open']+a['close'])/2 and p['close']<a['open']:
                    event('morning_star',i-2,i,'中間小實體 ≤ 首根 30%，末根紅 K 收復首根實體中點；未要求跳空')
                if a['close']>a['open'] and r['close']<r['open'] and r['close']<(a['open']+a['close'])/2 and p['close']>a['open']:
                    event('evening_star',i-2,i,'中間小實體 ≤ 首根 30%，末根黑 K 跌破首根實體中點；未要求跳空')
            if all(x['close']>x['open'] for x in [a,p,r]) and a['close']<p['close']<r['close'] and a['open']<p['open']<a['close'] and p['open']<r['open']<p['close']:
                event('three_white',i-2,i,'連續三紅、收盤墊高，後根開在前根實體內')
            if all(x['close']<x['open'] for x in [a,p,r]) and a['close']>p['close']>r['close'] and a['close']<p['open']<a['open'] and p['close']<r['open']<p['open']:
                event('three_black',i-2,i,'連續三黑、收盤降低，後根開在前根實體內')
    if len(b)>=20:
        lows=[i for i in range(max(3,len(b)-120),len(b)-3) if b[i]['low']<min(x['low'] for x in b[i-3:i]) and b[i]['low']<=min(x['low'] for x in b[i+1:i+4])]
        highs=[i for i in range(max(3,len(b)-120),len(b)-3) if b[i]['high']>max(x['high'] for x in b[i-3:i]) and b[i]['high']>=max(x['high'] for x in b[i+1:i+4])]
        if (len(lows)>=2 and b[lows[-1]]['low']>b[lows[-2]]['low']*1.01
                and min(r['low'] for r in b[lows[-1]+1:])>=b[lows[-1]]['low']):
            observation('higher_lows',lows[-2],'兩個已確認低點上移 >1%；各需右側 3 根確認；後續未跌破最後低點',lows[-1]+3,state='active')
            line('higher_lows',[(i,b[i]['low']) for i in lows[-2:]])
        if len(lows)>=2 and len(highs)>=2:
            l1,l2=lows[-2:];h1,h2=highs[-2:]
            ls=(b[l2]['low']-b[l1]['low'])/(l2-l1);hs=(b[h2]['high']-b[h1]['high'])/(h2-h1)
            end=len(b)-1;start=min(l1,h1)
            lv=lambda i:b[l1]['low']+ls*(i-l1)
            hv=lambda i:b[h1]['high']+hs*(i-h1)
            inside=all(lv(i)*.98<=b[i]['low'] and b[i]['high']<=hv(i)*1.02 for i in range(max(l1,h1),end+1))
            if inside and hv(end)>lv(end) and max(l1,h1)<min(l2,h2):
                pids=[]
                if ls*hs>0 and min(abs(ls),abs(hs))/max(abs(ls),abs(hs))>=.65:
                    pids.append('rising_channel' if ls>0 else 'falling_channel')
                # Converging boundaries are mandatory. Mutually exclusive types
                # prevent a contracting triangle from receiving all three labels.
                overlap=max(l1,h1)
                converging=ls>hs and 0<hv(end)-lv(end)<hv(overlap)-lv(overlap)
                if converging:
                    if ls>0 and hs<0:pids.append('symmetrical_triangle')
                    elif ls>0 and abs(b[h2]['high']/b[h1]['high']-1)<=.02:pids.append('ascending_triangle')
                    elif hs<0 and abs(b[l2]['low']/b[l1]['low']-1)<=.02:pids.append('descending_triangle')
                if 'symmetrical_triangle' in pids and start>=10 and end-start<=30:
                    pole=b[start]['close']/b[start-10]['close']-1
                    if abs(pole)>=.1:pids.append('bull_pennant' if pole>0 else 'bear_pennant')
                for pid in pids:
                    observation(pid,start,'轉折右側 3 根確認；邊界容差 2%。通道斜率比 ≥0.65；旗桿為前 10 根變動 ≥10%（工程定義）')
                    line(pid,[(start,hv(start)),(end,hv(end))]);line(pid,[(start,lv(start)),(end,lv(end))])
        for inverted,pivots,opposites,field in [(False,highs,lows,'high'),(True,lows,highs,'low')]:
            if len(pivots)<3:continue
            a,h,c=pivots[-3:]; values=[b[i][field] for i in [a,h,c]]
            between1=[i for i in opposites if a<i<h];between2=[i for i in opposites if h<i<c]
            head=values[1]<min(values[0],values[2])*.97 if inverted else values[1]>max(values[0],values[2])*1.03
            if head and abs(values[0]/values[2]-1)<=.04 and between1 and between2:
                pid='inverse_head_shoulders' if inverted else 'head_shoulders';f='high' if inverted else 'low'
                n1,n2=between1[-1],between2[-1];slope=(b[n2][f]-b[n1][f])/(n2-n1)
                neck=b[n1][f]+slope*(len(b)-1-n1)
                # A completed pattern that recovers beyond its right shoulder is
                # no longer an active filter candidate; do not relabel it pending.
                invalid=any(r['low']<values[2] if inverted else r['high']>values[2]
                            for r in b[c+1:])
                if invalid:continue
                crossings=[i for i in range(c+1,len(b))
                           if (b[i]['close']>b[n1][f]+slope*(i-n1) if inverted
                               else b[i]['close']<b[n1][f]+slope*(i-n1))]
                broken=bool(crossings)
                confirmed=max(c+3,crossings[0]) if broken else c+3
                observation(pid,a,('曾有收盤越過頸線；突破日 '+b[crossings[0]]['time'] if broken else '尚未突破頸線，僅為候選')+'；肩差 ≤4%，頭突出 ≥3%，左右各 3 根確認；未越過右肩失效',confirmed,state='neckline_broken' if broken else 'candidate')
                line(pid,[(i,b[i][field if i in [a,h,c] else f]) for i in [a,n1,h,n2,c]])
                line(pid,[(n1,b[n1][f]),(len(b)-1,neck)])
    return {'version':'chart-annotations-v3','events':events,'lines':lines,'observations':observations,
        'parameters':{'pivot_left':3,'pivot_right':3,'boundary_tolerance_pct':2,'shoulder_tolerance_pct':4,'head_prominence_pct':3},
        'note':'已完成 K 棒才確認；轉折需右側 3 根。型態為幾何觀察，不給目標價或停損。'}
