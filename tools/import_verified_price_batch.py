"""Import validated external price pairs, using portable canonical LF bytes."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_review_data import read_json, verified_frame


def import_batch(source: Path, destination: Path, as_of: str) -> dict:
    if source.resolve() == destination.resolve():
        raise ValueError('source and destination must differ')
    result = {'dataset_id': 'verified_price_import', 'as_of': as_of, 'completed': [], 'failed': {}}
    for path in sorted((source / 'price_basis').glob('*.json')):
        sid = path.stem
        if len(sid) != 4 or not sid.isdigit() or sid.startswith('0'):
            continue
        try:
            verified_frame(source, sid, as_of)
            meta = read_json(path)
            original = (source / 'prices' / f'{sid}.csv').read_bytes()
            if hashlib.sha256(original).hexdigest() != meta.get('csv_sha256'):
                raise ValueError('source changed during import')
            # Serialization normalization only; every numeric token is preserved.
            canonical = original.replace(b'\r\n', b'\n')
            digest = hashlib.sha256(canonical).hexdigest()
            meta.update(csv_sha256=digest, serialization='utf8_lf_csv',
                        source_csv_sha256=hashlib.sha256(original).hexdigest())
            for folder in ('prices', 'price_basis'):
                (destination / folder).mkdir(parents=True, exist_ok=True)
            csv_path = destination / 'prices' / f'{sid}.csv'
            csv_temp = csv_path.with_suffix('.csv.tmp')
            csv_temp.write_bytes(canonical)
            csv_temp.replace(csv_path)
            meta_path = destination / 'price_basis' / f'{sid}.json'
            meta_temp = meta_path.with_suffix('.json.tmp')
            meta_temp.write_bytes((json.dumps(meta, ensure_ascii=False, allow_nan=False, separators=(',', ':'))+'\n').encode('utf8'))
            meta_temp.replace(meta_path)
            verified_frame(destination, sid, as_of)
            result['completed'].append(sid)
        except (ValueError, KeyError, TypeError) as exc:
            result['failed'][sid] = type(exc).__name__
        except OSError:
            result['failed'][sid] = 'file_missing_or_unreadable'
    result['status'] = 'partial' if result['failed'] else 'imported_available_pairs'
    result['note'] = 'Imported pairs do not imply complete market coverage.'
    (destination / 'verified_price_import.json').write_bytes((json.dumps(result, ensure_ascii=False, separators=(',', ':'))+'\n').encode('utf8'))
    return result


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', type=Path, required=True)
    ap.add_argument('--destination', type=Path, default=Path(__file__).resolve().parents[1] / 'data')
    ap.add_argument('--as-of', required=True)
    args = ap.parse_args()
    result = import_batch(args.source, args.destination, args.as_of)
    print(json.dumps({'imported': len(result['completed']), 'failed': len(result['failed']), 'as_of': args.as_of}))
