"""Protect validated adjusted histories from legacy raw-price writers."""
from pathlib import Path


def protected_ids(data_dir: Path) -> set[str]:
    from build_review_data import read_json, verified_frame
    ids = set()
    for path in sorted((data_dir / 'price_basis').glob('*.json')):
        meta = read_json(path)
        if meta.get('mode') in {None, 'none'}:
            continue
        verified_frame(data_dir, path.stem, str(meta.get('adjustment_as_of') or ''))
        ids.add(path.stem)
    return ids


if __name__ == '__main__':
    ids = protected_ids(Path(__file__).resolve().parent / 'data')
    print(f'verified adjusted pairs: {len(ids)}')
