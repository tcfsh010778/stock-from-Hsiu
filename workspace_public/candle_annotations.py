"""Compact neutral daily markers from the reviewed PR13/28 implementation."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .model import prepare_bars

VERSION = 'workspace-neutral-candles-1'
NAMES = {'doji':'十字', 'hammer_shape':'錘形線', 'inverted_hammer_shape':'倒錘形線',
         'engulfing_up_body':'陽吞噬', 'engulfing_down_body':'陰吞噬',
         'morning_star_shape':'晨星型態', 'evening_star_shape':'夜星型態',
         'three_white_soldiers_shape':'紅三兵型態', 'three_black_crows_shape':'黑三鴉型態',
         'long_bullish_body':'長紅 K', 'long_bearish_body':'長黑 K',
         'high_zone_long_bearish':'高檔長黑', 'low_zone_long_bullish':'低檔長紅',
         'long_upper_shadow':'長上影', 'long_lower_shadow':'長下影'}
TREND_IDS={'range','higher_lows','double_bottom','double_top','ascending_triangle','descending_triangle',
           'rising_channel','falling_channel','symmetrical_triangle','bull_pennant','bear_pennant',
           'head_shoulders','inverse_head_shoulders'}


def engine():
    from stock_v2_public.analysis import candlesticks
    return candlesticks


def source_hash():
    # Normalize EOL so a Windows consumer can verify Linux build provenance.
    return hashlib.sha256(Path(engine().__file__).read_text(encoding='utf-8-sig').encode('utf-8')).hexdigest()


def calculate(rows, as_of, stock_id):
    f = prepare_bars(rows, as_of)
    if str(f.date.iloc[-1].date()) != as_of:
        raise ValueError('daily marker price date mismatch')
    envelope = engine().build_candlestick_event_envelope(f, symbol=stock_id,
        price_basis='verified_adjusted', source_id='verified_official_daily', public_only=True)
    dates = [str(d.date()) for d in f.date]; offsets = {d:i for i,d in enumerate(dates)}
    events=[]
    for e in envelope['events']:
        if e['pattern_id'] not in NAMES or e['bar_status'] != 'closed':
            raise ValueError('non-whitelisted daily marker')
        position=offsets[e['bar_date']]
        start=dates[max(0,position-e['geometry']['component_bars']+1)]
        metrics={k:e['geometry'].get(k) for k in ('body_ratio','upper_shadow_ratio','lower_shadow_ratio')}
        metrics['relative_volume_prev20']=e['context'].get('relative_volume_prev20')
        events.append({'id':e['pattern_id'],'name':e['label_zh'],'start':start,'end':e['bar_date'],
                       'confirmed_at':e['bar_date'],'engine':e['engine'],'raw':e['raw'],'metrics':metrics,
                       'evidence':'已收盤日 K 的幾何註記；TA-Lib 原始值不是買賣評分'})
    return {'version':VERSION,'source_sha256':source_hash(),'data_date':as_of,
            'status':'ok','catalog':envelope['talib'],'events':events}


def load_cache(root):
    if root is None: return {}
    root=Path(root)
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    raw=(root/'candles.json').read_bytes()
    if manifest.get('sha256')!=hashlib.sha256(raw).hexdigest():
        raise ValueError('candle cache manifest mismatch')
    cache=json.loads(raw)
    if cache.get('version')!=VERSION or cache.get('source_sha256')!=source_hash():
        raise ValueError('candle cache engine mismatch')
    return cache.get('stocks',{})


def annotations(rows, as_of, stock_id, price_sha256, cache):
    cached=cache.get(stock_id)
    if cached is not None:
        value=cached.get('annotations',{})
        if cached.get('price_sha256')!=price_sha256 or value.get('data_date')!=as_of or value.get('source_sha256')!=source_hash() or value.get('version')!=VERSION:
            raise ValueError('candle cache price/date/source mismatch')
        return value
    try:
        return calculate(rows,as_of,stock_id)
    except (ImportError,RuntimeError):
        return {'version':VERSION,'status':'unavailable','data_date':as_of,'events':[],
                'reason':'TA-Lib runtime unavailable; no detection fabricated'}
