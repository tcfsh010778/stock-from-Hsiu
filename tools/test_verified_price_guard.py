import pandas as pd
import pytest
from official_price_refresh import merge_price_rows
from verified_price_guard import protected_ids
from tools.test_build_review_data import write_prices


def test_raw_refresh_cannot_mutate_any_adjusted_pair(tmp_path):
    f = pd.DataFrame({'date': ['2026-09-04'], 'open': [10], 'high': [11],
                      'low': [9], 'close': [10], 'volume': [1000]})
    write_prices(tmp_path, '2330', f)
    assert protected_ids(tmp_path) == {'2330'}
    before = (tmp_path / 'prices/2330.csv').read_bytes()
    rows = [{'stock_id': sid, 'date': '2026-09-07', 'open': 10, 'high': 11,
             'low': 9, 'close': 10, 'volume': 1000} for sid in ['2317', '2330']]
    with pytest.raises(ValueError, match='protected adjusted'):
        merge_price_rows(tmp_path / 'prices', {'2317', '2330'}, rows)
    assert (tmp_path / 'prices/2330.csv').read_bytes() == before
    assert not (tmp_path / 'prices/2317.csv').exists()


def test_adjusted_columns_protected_even_without_sidecar(tmp_path):
    f = pd.DataFrame({'date': ['2026-09-04'], 'open': [10], 'high': [11],
                      'low': [9], 'close': [10], 'volume': [1000]})
    write_prices(tmp_path, '2330', f)
    (tmp_path / 'price_basis/2330.json').unlink()
    with pytest.raises(ValueError, match='adjusted columns'):
        merge_price_rows(tmp_path / 'prices', {'2330'}, [{'stock_id': '2330'}])
