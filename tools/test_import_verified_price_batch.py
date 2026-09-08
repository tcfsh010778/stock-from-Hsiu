import hashlib
import json
from tools.import_verified_price_batch import import_batch
from tools.test_public_history_contract import fixture


def test_cross_platform_lf_import_preserves_prices_and_hash(tmp_path):
    source = tmp_path / 'source'
    destination = tmp_path / 'destination'
    frame = fixture(source, 250)
    path = source / 'prices/9001.csv'
    crlf = path.read_bytes().replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
    path.write_bytes(crlf)
    meta_path = source / 'price_basis/9001.json'
    meta = json.loads(meta_path.read_text())
    meta['csv_sha256'] = hashlib.sha256(crlf).hexdigest()
    meta_path.write_text(json.dumps(meta))
    result = import_batch(source, destination, '2026-09-04')
    assert result['completed'] == ['9001']
    exported = (destination / 'prices/9001.csv').read_bytes()
    assert exported == crlf.replace(b'\r\n', b'\n')
    updated = json.loads((destination / 'price_basis/9001.json').read_text())
    assert updated['csv_sha256'] == hashlib.sha256(exported).hexdigest()
    assert updated['source_csv_sha256'] == hashlib.sha256(crlf).hexdigest()
    # Invalid next import cannot replace the last validated pair.
    path.write_bytes(crlf + b'broken')
    result = import_batch(source, destination, '2026-09-04')
    assert result['failed'] == {'9001': 'ValueError'}
    assert (destination / 'prices/9001.csv').read_bytes() == exported
