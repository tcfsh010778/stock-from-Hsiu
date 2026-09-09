"""Resolve a current verified price session before publishing dependent products."""
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_review_data import expected_session


def resolve(data_dir: Path = Path("data")) -> str:
    manifest = json.loads((data_dir / "official_adjusted_update_manifest.json").read_text(encoding="utf-8"))
    prices = json.loads((data_dir / "price_refresh_summary.json").read_text(encoding="utf-8"))
    expected = expected_session(data_dir=data_dir)
    if (manifest.get("dataset_id") != "official_adjusted_daily_update"
            or manifest.get("calendar_basis") != "official_twse_tpex"
            or manifest.get("status") not in {"complete", "partial", "current"}
            or manifest.get("data_as_of") != expected
            or prices.get("status") != "fresh" or prices.get("latest_data_date") != expected):
        raise ValueError(f"verified price session is not current: expected={expected}, adjusted={manifest.get('data_as_of')}, raw={prices.get('latest_data_date')}")
    return expected


if __name__ == "__main__":
    print(resolve())
