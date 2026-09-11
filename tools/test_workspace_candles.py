import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from workspace_public import candle_annotations as c


def test_cache_binds_price_date_and_source(tmp_path,monkeypatch):
    monkeypatch.setattr(c,'source_hash',lambda:'engine-sha')
    result={'version':c.VERSION,'data_date':'2026-09-10','source_sha256':'engine-sha','events':[],'status':'ok'}
    cache={'version':c.VERSION,'source_sha256':'engine-sha','stocks':{'2330':{'price_sha256':'price-sha','annotations':result}}}
    raw=json.dumps(cache).encode();(tmp_path/'candles.json').write_bytes(raw)
    (tmp_path/'manifest.json').write_text(json.dumps({'sha256':hashlib.sha256(raw).hexdigest()}))
    loaded=c.load_cache(tmp_path)
    assert c.annotations(None,'2026-09-10','2330','price-sha',loaded)==result
    with pytest.raises(RuntimeError):c.annotations(None,'2026-09-10','2330','changed-price',loaded)
    with pytest.raises(RuntimeError):c.annotations(None,'2026-09-11','2330','price-sha',loaded)
    (tmp_path/'candles.json').write_bytes(raw+b' ')
    with pytest.raises(ValueError):c.load_cache(tmp_path)


def test_missing_runtime_is_explicit_and_has_no_fake_detections(monkeypatch):
    def fail(*args):raise RuntimeError('runtime unavailable')
    monkeypatch.setattr(c,'calculate',fail)
    result=c.annotations(None,'2026-09-10','2330','sha',{})
    assert result['status']=='unavailable' and result['events']==[]


def test_neutral_markers_keep_whitelist_and_no_future_outcomes():
    try:import talib
    except ImportError:pytest.skip('TA-Lib native runtime unavailable on this host; cloud tests required')
    dates=pd.bdate_range(end='2026-09-10',periods=90)
    rows=pd.DataFrame({'date':dates,'open':100.,'high':102.,'low':98.,'close':100.,'volume':1000.})
    result=c.calculate(rows,'2026-09-10','2330')
    assert result['catalog']['pattern_function_count']==61
    assert result['catalog']['selection']=='public_whitelist'
    assert result['events']
    for event in result['events']:
        assert event['id'] in c.NAMES
        assert event['confirmed_at']==event['end']<='2026-09-10'
        assert not {'outcome','forward_returns','score','signal','action'}.intersection(event)
